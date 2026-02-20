"""
Admin Panel Business Logic Layer
Handles user management, audit logging, and permission enforcement
"""

import json
from datetime import datetime, timezone
from app.auth import (
    get_users_db, update_users_db, get_gcs_bucket
)
from app.config import GCS_AUDIT_LOG_FILE, DASHBOARDS

# =============================================================================
# AUDIT LOG FUNCTIONS
# =============================================================================

# Local fallback for audit log
_local_audit_log = []


def get_audit_log():
    """Load audit log from GCS or local fallback"""
    global _local_audit_log

    bucket = get_gcs_bucket()
    if bucket is None:
        return _local_audit_log

    try:
        blob = bucket.blob(GCS_AUDIT_LOG_FILE)
        if not blob.exists():
            return []
        return json.loads(blob.download_as_text())
    except Exception as e:
        print(f"[AUDIT] Error loading audit log: {e}")
        return _local_audit_log


def save_audit_log(log_entries):
    """Save audit log to GCS or local fallback"""
    global _local_audit_log

    bucket = get_gcs_bucket()
    if bucket is None:
        _local_audit_log = log_entries
        return True

    try:
        blob = bucket.blob(GCS_AUDIT_LOG_FILE)
        blob.upload_from_string(
            json.dumps(log_entries, indent=2, default=str),
            content_type='application/json'
        )
        return True
    except Exception as e:
        print(f"[AUDIT] Error saving audit log: {e}")
        _local_audit_log = log_entries
        return False


def log_audit_action(actor_user_id, action, target_user_id, metadata=None):
    """Log an action to the audit log"""
    log_entries = get_audit_log()

    entry = {
        "id": len(log_entries) + 1,
        "actor_user_id": actor_user_id,
        "action": action,
        "target_user_id": target_user_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "metadata": metadata or {}
    }

    log_entries.append(entry)

    # Keep only last 500 entries
    if len(log_entries) > 500:
        log_entries = log_entries[-500:]

    save_audit_log(log_entries)
    return entry


def get_recent_audit_log(limit=50):
    """Get recent audit log entries"""
    log_entries = get_audit_log()
    return list(reversed(log_entries[-limit:]))


# =============================================================================
# ENHANCED USER MANAGEMENT
# =============================================================================

def get_users_with_metadata():
    """Get all users with additional metadata for display"""
    users = get_users_db()
    result = []

    for user_id, user_info in users.items():
        result.append({
            "user_id": user_id,
            "name": user_info.get("name", ""),
            "role": user_info.get("role", "readonly"),
            "password": user_info.get("password", ""),
            "dashboards": user_info.get("dashboards", []),
            "app_access": user_info.get("app_access", {}),
            "is_active": user_info.get("is_active", True),
            "created_at": user_info.get("created_at", ""),
            "created_by": user_info.get("created_by", "system"),
            "updated_at": user_info.get("updated_at", ""),
            "updated_by": user_info.get("updated_by", ""),
            "last_login": user_info.get("last_login", "")
        })

    return result


def count_active_super_admins():
    """Count active super admin users"""
    users = get_users_db()
    count = 0
    for user_id, user_info in users.items():
        if user_info.get("role") == "super_admin" and user_info.get("is_active", True):
            count += 1
    return count


def create_user(actor_user_id, user_id, password, role, name, dashboards, app_access=None):
    """Create a new user with audit logging"""
    users = get_users_db()

    if user_id in users:
        return False, "User ID already exists"

    if role == "super_admin":
        return False, "Cannot create Super Admin users"

    now = datetime.now(timezone.utc).isoformat()

    users[user_id] = {
        "password": password,
        "role": role,
        "name": name,
        "dashboards": dashboards if role == "readonly" else "all",
        "app_access": app_access if app_access and role == "readonly" else {},
        "is_active": True,
        "created_at": now,
        "created_by": actor_user_id,
        "updated_at": now,
        "updated_by": actor_user_id,
        "last_login": ""
    }

    update_users_db(users)
    log_audit_action(actor_user_id, "CREATE_USER", user_id, {"role": role})

    return True, "User created successfully"


def edit_user(actor_user_id, actor_role, user_id, password=None, role=None, name=None, dashboards=None, app_access=None):
    """Update existing user with audit logging and permission checks"""
    users = get_users_db()

    if user_id not in users:
        return False, "User not found"

    target_role = users[user_id].get("role")

    # SECURITY: Super Admin cannot be edited by anyone except themselves (password/name only)
    if target_role == "super_admin" and actor_user_id != user_id:
        return False, "Super Admin cannot be edited"

    # SECURITY: Admin can only edit readonly users
    if actor_role == "admin" and target_role != "readonly":
        return False, "You can only edit Read Only users"

    # SECURITY: Prevent role escalation
    if role and role != target_role:
        if actor_role == "admin" and role != "readonly":
            return False, "You can only assign Read Only role"
        if role == "super_admin":
            return False, "Cannot escalate to Super Admin"

    now = datetime.now(timezone.utc).isoformat()
    changes = {}

    if password and password != users[user_id].get("password"):
        users[user_id]["password"] = password
        changes["password"] = "changed"

    if role and role != target_role:
        # Don't allow downgrading last super_admin
        if target_role == "super_admin" and count_active_super_admins() <= 1:
            return False, "Cannot change role of last Super Admin"
        users[user_id]["role"] = role
        changes["role"] = {"from": target_role, "to": role}
        if role in ("admin", "super_admin"):
            users[user_id]["dashboards"] = "all"
            users[user_id]["app_access"] = {}

    if name and name != users[user_id].get("name"):
        users[user_id]["name"] = name
        changes["name"] = name

    if dashboards is not None and users[user_id]["role"] == "readonly":
        users[user_id]["dashboards"] = dashboards
        changes["dashboards"] = "updated"

    if app_access is not None and users[user_id]["role"] == "readonly":
        users[user_id]["app_access"] = app_access
        changes["app_access"] = "updated"

    users[user_id]["updated_at"] = now
    users[user_id]["updated_by"] = actor_user_id

    update_users_db(users)

    if changes:
        log_audit_action(actor_user_id, "UPDATE_USER", user_id, changes)

    return True, "User updated successfully"


def soft_delete_user(actor_user_id, actor_role, user_id):
    """Soft delete a user (set is_active=False) with permission checks"""
    users = get_users_db()

    if user_id not in users:
        return False, "User not found"

    target_role = users[user_id]["role"]

    # SECURITY: Super Admin cannot be deleted
    if target_role == "super_admin":
        return False, "Super Admin cannot be deleted"

    # SECURITY: Only Super Admin can delete users
    if actor_role != "super_admin":
        return False, "Only Super Admin can delete users"

    # SECURITY: Cannot delete self
    if actor_user_id == user_id:
        return False, "Cannot delete yourself"

    users[user_id]["is_active"] = False
    users[user_id]["updated_at"] = datetime.now(timezone.utc).isoformat()
    users[user_id]["updated_by"] = actor_user_id

    update_users_db(users)
    log_audit_action(actor_user_id, "DELETE_USER", user_id)

    return True, "User deleted successfully"


def toggle_user_status(actor_user_id, actor_role, user_id):
    """Toggle user active/inactive status"""
    users = get_users_db()

    if user_id not in users:
        return False, "User not found"

    target_role = users[user_id]["role"]

    # SECURITY: Super Admin status cannot be changed
    if target_role == "super_admin":
        return False, "Super Admin status cannot be changed"

    # SECURITY: Only Super Admin can change status
    if actor_role != "super_admin":
        return False, "Only Super Admin can change user status"

    current_status = users[user_id].get("is_active", True)
    new_status = not current_status

    users[user_id]["is_active"] = new_status
    users[user_id]["updated_at"] = datetime.now(timezone.utc).isoformat()
    users[user_id]["updated_by"] = actor_user_id

    update_users_db(users)

    action = "ENABLE_USER" if new_status else "DISABLE_USER"
    log_audit_action(actor_user_id, action, user_id)

    status_text = "enabled" if new_status else "disabled"
    return True, f"User {status_text} successfully"


# =============================================================================
# PERMISSION HELPERS
# =============================================================================

def can_view_admin_panel(role):
    """Check if role can view admin panel"""
    return role in ("super_admin", "admin")


def can_create_role(current_role, new_role):
    """Check if current role can create a user with new_role"""
    if current_role == "super_admin":
        return new_role in ("admin", "readonly")
    if current_role == "admin":
        return new_role == "readonly"
    return False


def can_edit_user(current_role, current_user_id, target_role, target_user_id):
    """Check if current user can edit target user"""
    # Super Admin can edit admin and readonly (not other super_admins unless self)
    if current_role == "super_admin":
        if target_role == "super_admin":
            return current_user_id == target_user_id  # Can only edit own super_admin account
        return True
    # Admin can only edit readonly
    if current_role == "admin":
        return target_role == "readonly"
    return False


def can_delete_user(current_role, target_role):
    """Check if current role can delete target role"""
    # Only super_admin can delete, and cannot delete super_admin
    if current_role == "super_admin" and target_role != "super_admin":
        return True
    return False


def get_dashboard_name(dashboard_id):
    """Get dashboard display name from ID"""
    for d in DASHBOARDS:
        if d["id"] == dashboard_id:
            return d["name"]
    return dashboard_id
