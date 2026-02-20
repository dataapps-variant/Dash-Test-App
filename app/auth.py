"""
Authentication System for Variant Analytics Dashboard - Dash Version
- GCS-backed persistent user database
- GCS-backed session storage for cross-instance persistence
- Simple username/password auth
- Roles: super_admin (full control), admin (manage readonly), readonly (selected dashboards/apps)
"""

import json
import os
import uuid
import hashlib
from datetime import datetime, timezone, timedelta
from functools import wraps

from flask import session, redirect, url_for, request
from app.config import (
    DEFAULT_USERS, DASHBOARDS, ROLE_DISPLAY,
    GCS_USERS_FILE, GCS_SESSIONS_PREFIX,
    SESSION_TTL_DEFAULT, SESSION_TTL_REMEMBER
)

# GCS Bucket name from environment
GCS_BUCKET_NAME = os.environ.get("GCS_CACHE_BUCKET", "")

# In-memory cache for users (loaded from GCS)
_users_cache = {
    "data": None,
    "loaded_at": None
}

# In-memory session storage fallback (used when GCS is not available)
_memory_sessions = {}

# =============================================================================
# GCS HELPER FUNCTIONS
# =============================================================================

def get_gcs_bucket():
    """Get GCS bucket client"""
    if not GCS_BUCKET_NAME:
        return None
    try:
        from google.cloud import storage
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET_NAME)
        return bucket if bucket.exists() else None
    except Exception as e:
        print(f"[AUTH] GCS error: {e}")
        return None


def load_users_from_gcs():
    """Load users from GCS JSON file"""
    bucket = get_gcs_bucket()
    if bucket is None:
        return None
    
    try:
        blob = bucket.blob(GCS_USERS_FILE)
        if not blob.exists():
            return None
        
        data = json.loads(blob.download_as_text())
        return data
    except Exception as e:
        print(f"[AUTH] Error loading users from GCS: {e}")
        return None


def save_users_to_gcs(users):
    """Save users to GCS JSON file"""
    bucket = get_gcs_bucket()
    if bucket is None:
        return False
    
    try:
        blob = bucket.blob(GCS_USERS_FILE)
        blob.upload_from_string(
            json.dumps(users, indent=2),
            content_type='application/json'
        )
        return True
    except Exception as e:
        print(f"[AUTH] Error saving users to GCS: {e}")
        return False


# =============================================================================
# SESSION STORAGE (GCS-backed)
# =============================================================================

def generate_session_id():
    """Generate a unique session ID"""
    return str(uuid.uuid4())


def get_session_path(session_id):
    """Get GCS path for a session"""
    return f"{GCS_SESSIONS_PREFIX}{session_id}.json"


def load_session_from_gcs(session_id):
    """Load session data from GCS (with in-memory fallback)"""
    bucket = get_gcs_bucket()

    # Fallback to in-memory storage if GCS is not available
    if bucket is None:
        data = _memory_sessions.get(session_id)
        if data is None:
            return None
        # Check expiry
        if "expires_at" in data:
            expires_at = datetime.fromisoformat(data["expires_at"])
            if datetime.now(timezone.utc) > expires_at:
                delete_session_from_gcs(session_id)
                return None
        return data

    try:
        blob = bucket.blob(get_session_path(session_id))
        if not blob.exists():
            return None

        data = json.loads(blob.download_as_text())

        # Check expiry
        if "expires_at" in data:
            expires_at = datetime.fromisoformat(data["expires_at"])
            if datetime.now(timezone.utc) > expires_at:
                # Session expired, delete it
                delete_session_from_gcs(session_id)
                return None

        return data
    except Exception as e:
        print(f"[AUTH] Error loading session: {e}")
        return None


def save_session_to_gcs(session_id, data):
    """Save session data to GCS (with in-memory fallback)"""
    bucket = get_gcs_bucket()

    # Fallback to in-memory storage if GCS is not available
    if bucket is None:
        _memory_sessions[session_id] = data
        return True

    try:
        blob = bucket.blob(get_session_path(session_id))
        blob.upload_from_string(
            json.dumps(data, default=str),
            content_type='application/json'
        )
        return True
    except Exception as e:
        print(f"[AUTH] Error saving session: {e}")
        return False


def delete_session_from_gcs(session_id):
    """Delete session from GCS (with in-memory fallback)"""
    bucket = get_gcs_bucket()

    # Fallback to in-memory storage if GCS is not available
    if bucket is None:
        if session_id in _memory_sessions:
            del _memory_sessions[session_id]
        return True

    try:
        blob = bucket.blob(get_session_path(session_id))
        if blob.exists():
            blob.delete()
        return True
    except Exception as e:
        print(f"[AUTH] Error deleting session: {e}")
        return False


# =============================================================================
# USER DATABASE MANAGEMENT
# =============================================================================

def get_users_db():
    """
    Get users database with caching
    Priority: Memory cache -> GCS -> DEFAULT_USERS
    """
    global _users_cache
    
    # Check memory cache (valid for 5 minutes)
    if _users_cache["data"] is not None and _users_cache["loaded_at"] is not None:
        age = (datetime.now() - _users_cache["loaded_at"]).total_seconds()
        if age < 300:  # 5 minutes
            return _users_cache["data"]
    
    # Try loading from GCS
    users = load_users_from_gcs()
    if users is not None:
        _users_cache["data"] = users
        _users_cache["loaded_at"] = datetime.now()
        return users
    
    # Fallback to defaults and save to GCS
    users = DEFAULT_USERS.copy()
    save_users_to_gcs(users)
    _users_cache["data"] = users
    _users_cache["loaded_at"] = datetime.now()
    return users


def update_users_db(users):
    """Update users database in memory and GCS"""
    global _users_cache
    
    _users_cache["data"] = users
    _users_cache["loaded_at"] = datetime.now()
    save_users_to_gcs(users)


def invalidate_users_cache():
    """Force reload of users from GCS"""
    global _users_cache
    _users_cache["data"] = None
    _users_cache["loaded_at"] = None


# =============================================================================
# AUTHENTICATION FUNCTIONS
# =============================================================================

def create_session(user_data, remember_me=False):
    """Create a new session for authenticated user"""
    session_id = generate_session_id()
    
    # Set expiry based on remember_me
    ttl = SESSION_TTL_REMEMBER if remember_me else SESSION_TTL_DEFAULT
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl)
    
    session_data = {
        "session_id": session_id,
        "user": user_data,
        "authenticated": True,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "expires_at": expires_at.isoformat(),
        "remember_me": remember_me
    }
    
    # Save to GCS
    save_session_to_gcs(session_id, session_data)
    
    return session_id, expires_at


def authenticate(username, password, remember_me=False):
    """
    Authenticate user with username and password
    Returns (success, session_id, expires_at) tuple
    """
    users = get_users_db()
    
    if username in users:
        if users[username]["password"] == password:
            user_data = {
                "username": username,
                "role": users[username]["role"],
                "name": users[username]["name"],
                "dashboards": users[username]["dashboards"],
                "app_access": users[username].get("app_access", {})
            }
            session_id, expires_at = create_session(user_data, remember_me)
            return True, session_id, expires_at
    
    return False, None, None


def get_session_data(session_id):
    """Get session data from GCS"""
    if not session_id:
        return None
    return load_session_from_gcs(session_id)


def logout(session_id):
    """Log out user by deleting session"""
    if session_id:
        delete_session_from_gcs(session_id)


def is_authenticated(session_id):
    """Check if session is valid"""
    session_data = get_session_data(session_id)
    return session_data is not None and session_data.get("authenticated", False)


def get_current_user(session_id):
    """Get current logged in user info"""
    session_data = get_session_data(session_id)
    if session_data:
        return session_data.get("user")
    return None


def is_admin(session_id):
    """Check if current user is admin or super_admin"""
    user = get_current_user(session_id)
    if user:
        return user.get("role") in ("admin", "super_admin")
    return False


def is_super_admin(session_id):
    """Check if current user is super_admin"""
    user = get_current_user(session_id)
    if user:
        return user.get("role") == "super_admin"
    return False


def can_access_dashboard(session_id, dashboard_id):
    """Check if current user can access a specific dashboard"""
    user = get_current_user(session_id)
    if not user:
        return False
    
    # Check if dashboard is enabled
    dashboard = next((d for d in DASHBOARDS if d["id"] == dashboard_id), None)
    if not dashboard or not dashboard.get("enabled", False):
        return False
    
    # Admin and super_admin have access to all
    if user.get("role") in ("admin", "super_admin") or user.get("dashboards") == "all":
        return True
    
    return dashboard_id in user.get("dashboards", [])


def get_accessible_dashboards(session_id):
    """Get list of dashboards accessible to current user"""
    user = get_current_user(session_id)
    if not user:
        return []
    
    if user.get("role") in ("admin", "super_admin") or user.get("dashboards") == "all":
        return DASHBOARDS
    
    accessible = []
    for dashboard in DASHBOARDS:
        if dashboard["id"] in user.get("dashboards", []):
            accessible.append(dashboard)
    
    return accessible


def get_dashboard_access_for_user(username):
    """Get list of dashboard IDs a user has access to"""
    users = get_users_db()
    
    if username not in users:
        return []
    
    user_data = users[username]
    if user_data["role"] in ("admin", "super_admin") or user_data["dashboards"] == "all":
        return "all"
    
    return user_data.get("dashboards", [])


def get_readonly_users_for_dashboard(dashboard_id):
    """Get list of readonly users who have access to a specific dashboard"""
    users = get_users_db()
    
    readonly_users = []
    for username, user_data in users.items():
        if user_data["role"] == "readonly":
            if user_data["dashboards"] == "all" or dashboard_id in user_data.get("dashboards", []):
                readonly_users.append(user_data["name"])
    
    return readonly_users


# =============================================================================
# APP ACCESS FUNCTIONS
# =============================================================================

def get_user_allowed_apps(user, dashboard_id):
    """
    Get allowed app names for a user on a specific dashboard.
    Returns None for full access (admin/super_admin or no restrictions set).
    Returns a list of app names for restricted access.
    """
    if not user:
        return []
    
    role = user.get("role", "readonly")
    
    # Admin and super_admin always have full access
    if role in ("admin", "super_admin"):
        return None
    
    # For readonly users, check app_access
    app_access = user.get("app_access", {})
    
    # No app_access configured at all = full access (backward compatibility)
    if not app_access:
        return None
    
    # Dashboard not in app_access = full access for this dashboard
    if dashboard_id not in app_access:
        return None
    
    # Return the list of allowed apps for this dashboard
    return app_access.get(dashboard_id, [])


def get_user_app_access_from_db(username):
    """Get app_access dict for a user directly from the database"""
    users = get_users_db()
    if username not in users:
        return {}
    return users[username].get("app_access", {})


# =============================================================================
# ROLE HIERARCHY FUNCTIONS
# =============================================================================

def can_manage_user(current_role, target_role):
    """Check if current role can edit/manage a user with target role"""
    if current_role == "super_admin":
        return True  # Super admin can manage everyone
    if current_role == "admin" and target_role == "readonly":
        return True  # Admin can manage readonly users
    return False


def can_delete_user(current_role, target_user_id, target_role):
    """Check if current role can delete a specific user"""
    # Super admin account can never be deleted from UI
    if target_role == "super_admin":
        return False
    if current_role == "super_admin":
        return True  # Super admin can delete admin and readonly
    if current_role == "admin" and target_role == "readonly":
        return True  # Admin can delete readonly
    return False


def get_assignable_roles(current_role):
    """Get list of roles that the current role can assign to users"""
    if current_role == "super_admin":
        return ["admin", "readonly"]  # Can assign admin or readonly (not super_admin)
    if current_role == "admin":
        return ["readonly"]  # Can only assign readonly
    return []


# =============================================================================
# ADMIN FUNCTIONS
# =============================================================================

def get_all_users():
    """Get all users (admin only)"""
    return get_users_db()


def add_user(user_id, password, role, name, dashboards, app_access=None):
    """Add a new user"""
    users = get_users_db()
    
    if user_id in users:
        return False, "User ID already exists"
    
    # Super admin role cannot be created from UI
    if role == "super_admin":
        return False, "Cannot create Super Admin users"
    
    users[user_id] = {
        "password": password,
        "role": role,
        "name": name,
        "dashboards": dashboards if role == "readonly" else "all",
        "app_access": app_access if app_access and role == "readonly" else {}
    }
    
    update_users_db(users)
    return True, "User created successfully"


def update_user(user_id, password=None, role=None, name=None, dashboards=None, app_access=None):
    """Update existing user"""
    users = get_users_db()
    
    if user_id not in users:
        return False, "User not found"
    
    if password:
        users[user_id]["password"] = password
    if role:
        users[user_id]["role"] = role
        # If role changed to admin, set dashboards to all and clear app_access
        if role in ("admin", "super_admin"):
            users[user_id]["dashboards"] = "all"
            users[user_id]["app_access"] = {}
    if name:
        users[user_id]["name"] = name
    if dashboards is not None and users[user_id]["role"] == "readonly":
        users[user_id]["dashboards"] = dashboards
    if app_access is not None and users[user_id]["role"] == "readonly":
        users[user_id]["app_access"] = app_access
    
    update_users_db(users)
    return True, "User updated successfully"


def delete_user(user_id):
    """Delete a user"""
    users = get_users_db()
    
    if user_id not in users:
        return False, "User not found"
    
    # Protect super_admin from deletion
    if users[user_id]["role"] == "super_admin":
        return False, "Cannot delete Super Admin user"
    
    del users[user_id]
    update_users_db(users)
    return True, "User deleted successfully"


def get_role_display(role):
    """Get display name for role"""
    return ROLE_DISPLAY.get(role, role)
