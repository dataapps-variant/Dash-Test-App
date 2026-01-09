# Variant Analytics Dashboard - Dash Version

A comprehensive analytics dashboard built with Dash (Plotly), featuring BigQuery integration, GCS caching, and multi-level authentication.

## Features

- **Authentication System**: GCS-backed persistent user database with role-based access (Admin/Read Only)
- **Session Persistence**: Sessions survive Cloud Run instance restarts via GCS storage
- **Remember Me**: Extended session expiry (30 days) for convenience
- **Multi-level Caching**: App-level → GCS → BigQuery for optimal performance
- **Dark/Light Themes**: Full theme support with persistent preference
- **Interactive Charts**: Plotly charts with zoom, pan, and export
- **AG Grid Pivot Tables**: Professional data tables with filtering and sorting
- **Admin Panel**: User management interface for administrators

## Project Structure

```
variant-dashboard-dash/
├── app/
│   ├── __init__.py
│   ├── app.py              # Main Dash application
│   ├── auth.py             # Authentication & session management
│   ├── bigquery_client.py  # Data layer with caching
│   ├── charts.py           # Plotly chart components
│   ├── colors.py           # Color utilities
│   ├── config.py           # Configuration & constants
│   ├── theme.py            # Theme CSS & components
│   └── assets/             # Static assets (logo, etc.)
├── requirements.txt
├── Dockerfile
├── cloudbuild.yaml
├── .gitignore
├── .dockerignore
├── .gcloudignore
└── README.md
```

## Local Development

### Prerequisites

- Python 3.11+
- Google Cloud credentials (for BigQuery/GCS access)

### Setup

1. Clone the repository:
```bash
git clone <your-repo-url>
cd variant-dashboard-dash
```

2. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set environment variables:
```bash
export GCS_CACHE_BUCKET="your-gcs-bucket-name"
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
export SECRET_KEY="your-secret-key-here"
```

5. Run the application:
```bash
# Development mode
python app/app.py

# Or with Gunicorn (production-like)
gunicorn --bind 0.0.0.0:8080 app.app:server
```

6. Open http://localhost:8080 in your browser

### Default Credentials

- **Admin**: username `admin`, password `admin123`
- **Viewer**: username `viewer`, password `viewer123`

## Deployment to Cloud Run

### Prerequisites

1. Google Cloud project with billing enabled
2. Cloud Build API enabled
3. Cloud Run API enabled
4. Service account with permissions:
   - BigQuery Data Viewer
   - Storage Object Admin (for GCS bucket)

### Deploy via Cloud Build

1. Create GCS bucket for caching:
```bash
gsutil mb gs://variant-dashboard-cache-$PROJECT_ID
```

2. Create service account:
```bash
gcloud iam service-accounts create variant-dashboard-sa \
    --display-name="Variant Dashboard Service Account"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:variant-dashboard-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataViewer"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:variant-dashboard-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"
```

3. Deploy:
```bash
gcloud builds submit --config=cloudbuild.yaml
```

### Manual Deploy

```bash
# Build image
docker build -t gcr.io/$PROJECT_ID/variant-dashboard .

# Push to Container Registry
docker push gcr.io/$PROJECT_ID/variant-dashboard

# Deploy to Cloud Run
gcloud run deploy variant-dashboard \
    --image gcr.io/$PROJECT_ID/variant-dashboard \
    --platform managed \
    --region us-central1 \
    --memory 4Gi \
    --cpu 2 \
    --min-instances 1 \
    --allow-unauthenticated \
    --set-env-vars "GCS_CACHE_BUCKET=variant-dashboard-cache-$PROJECT_ID"
```

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `GCS_CACHE_BUCKET` | GCS bucket name for caching | Yes |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account JSON | Local only |
| `SECRET_KEY` | Secret key for session encryption | Recommended |

## Architecture

### Caching Layers

1. **App-level Cache**: In-memory cache within each instance (fastest)
2. **GCS Cache**: Parquet files persisted in GCS (survives restarts)
3. **BigQuery**: Source of truth (slowest, used only when caches miss)

### Session Management

- Sessions are stored as JSON files in GCS under `cache/sessions/`
- Each session has a unique ID stored in the client's localStorage
- Sessions expire based on "Remember Me" setting:
  - Default: 1 day
  - Remember Me: 30 days

### User Database

- Users are stored in `cache/users.json` in GCS
- Loaded into memory with 5-minute cache
- Changes are immediately persisted to GCS

## Differences from Streamlit Version

| Feature | Streamlit | Dash |
|---------|-----------|------|
| State Management | `st.session_state` | `dcc.Store` + GCS |
| Caching | `@st.cache_data` | App-level dict + GCS |
| Server | Built-in | Gunicorn (4 workers) |
| Concurrency | Limited (10) | High (80) |
| Stateful | Yes | Stateless (better for Cloud Run) |

## Troubleshooting

### "No data available" error

1. Check BigQuery connection and table exists
2. Verify service account has BigQuery Data Viewer role
3. Check GCS bucket is configured correctly

### Session not persisting

1. Verify GCS bucket is accessible
2. Check service account has Storage Object Admin role
3. Ensure `SECRET_KEY` environment variable is set

### Slow initial load

1. First request loads data from BigQuery → GCS cache
2. Subsequent requests use cached data
3. Consider pre-warming cache after deployment

## License

Proprietary - Variant Group
