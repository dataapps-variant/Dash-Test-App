# Deployment Guide - Variant Dashboard (Dash Version)

This guide covers deploying the Variant Analytics Dashboard to Google Cloud Run.

## Prerequisites

### 1. Google Cloud Setup

```bash
# Set your project ID
export PROJECT_ID="your-project-id"
gcloud config set project $PROJECT_ID

# Enable required APIs
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
```

### 2. Create GCS Bucket for Caching

```bash
# Create bucket
gsutil mb -l us-central1 gs://variant-dashboard-cache-$PROJECT_ID

# Set lifecycle policy (optional - delete old sessions after 30 days)
cat > lifecycle.json << EOF
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {
          "age": 30,
          "matchesPrefix": ["cache/sessions/"]
        }
      }
    ]
  }
}
EOF
gsutil lifecycle set lifecycle.json gs://variant-dashboard-cache-$PROJECT_ID
```

### 3. Create Service Account

```bash
# Create service account
gcloud iam service-accounts create variant-dashboard-sa \
    --display-name="Variant Dashboard Service Account"

# Grant BigQuery access
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:variant-dashboard-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataViewer"

# Grant GCS access
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:variant-dashboard-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"
```

## Deployment Options

### Option 1: Cloud Build (Recommended)

```bash
# Submit build
gcloud builds submit --config=cloudbuild.yaml

# Or with custom substitutions
gcloud builds submit --config=cloudbuild.yaml \
    --substitutions=_SECRET_KEY="your-production-secret-key"
```

### Option 2: Manual Deployment

```bash
# Build locally
docker build -t gcr.io/$PROJECT_ID/variant-dashboard:latest .

# Push to Container Registry
docker push gcr.io/$PROJECT_ID/variant-dashboard:latest

# Deploy to Cloud Run
gcloud run deploy variant-dashboard \
    --image gcr.io/$PROJECT_ID/variant-dashboard:latest \
    --platform managed \
    --region us-central1 \
    --memory 4Gi \
    --cpu 2 \
    --timeout 300 \
    --concurrency 80 \
    --min-instances 1 \
    --max-instances 10 \
    --allow-unauthenticated \
    --service-account variant-dashboard-sa@$PROJECT_ID.iam.gserviceaccount.com \
    --set-env-vars "GCS_CACHE_BUCKET=variant-dashboard-cache-$PROJECT_ID"
```

### Option 3: Terraform (Infrastructure as Code)

```hcl
# main.tf
provider "google" {
  project = var.project_id
  region  = "us-central1"
}

resource "google_storage_bucket" "cache" {
  name     = "variant-dashboard-cache-${var.project_id}"
  location = "US-CENTRAL1"
}

resource "google_service_account" "dashboard" {
  account_id   = "variant-dashboard-sa"
  display_name = "Variant Dashboard Service Account"
}

resource "google_project_iam_member" "bigquery" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.dashboard.email}"
}

resource "google_project_iam_member" "storage" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dashboard.email}"
}

resource "google_cloud_run_service" "dashboard" {
  name     = "variant-dashboard"
  location = "us-central1"

  template {
    spec {
      service_account_name = google_service_account.dashboard.email
      containers {
        image = "gcr.io/${var.project_id}/variant-dashboard:latest"
        
        resources {
          limits = {
            cpu    = "2000m"
            memory = "4Gi"
          }
        }
        
        env {
          name  = "GCS_CACHE_BUCKET"
          value = google_storage_bucket.cache.name
        }
      }
    }
    
    metadata {
      annotations = {
        "autoscaling.knative.dev/minScale" = "1"
        "autoscaling.knative.dev/maxScale" = "10"
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }
}

resource "google_cloud_run_service_iam_member" "public" {
  service  = google_cloud_run_service.dashboard.name
  location = google_cloud_run_service.dashboard.location
  role     = "roles/run.invoker"
  member   = "allUsers"
}
```

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GCS_CACHE_BUCKET` | Yes | - | GCS bucket for caching data and sessions |
| `SECRET_KEY` | Recommended | (built-in) | Secret key for session encryption |
| `GOOGLE_APPLICATION_CREDENTIALS` | Local only | - | Path to service account JSON |

### Production Secret Key

For production, use Google Secret Manager:

```bash
# Create secret
echo -n "your-secure-secret-key" | gcloud secrets create variant-dashboard-secret --data-file=-

# Grant access to service account
gcloud secrets add-iam-policy-binding variant-dashboard-secret \
    --member="serviceAccount:variant-dashboard-sa@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/secretmanager.secretAccessor"
```

Then update your Cloud Run deployment to use the secret:

```bash
gcloud run deploy variant-dashboard \
    --set-secrets="SECRET_KEY=variant-dashboard-secret:latest"
```

## Monitoring

### View Logs

```bash
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=variant-dashboard" --limit 50
```

### View Metrics

Go to Cloud Console → Cloud Run → variant-dashboard → Metrics

Key metrics to monitor:
- Request count
- Request latency
- Container instance count
- Memory utilization
- CPU utilization

### Set Up Alerting

```bash
# Create notification channel (email)
gcloud alpha monitoring channels create \
    --display-name="Dashboard Alerts" \
    --type=email \
    --channel-labels=email_address=your-email@example.com

# Create alerting policy for high latency
gcloud alpha monitoring policies create \
    --display-name="Dashboard High Latency" \
    --conditions="condition-display-name='High Latency',condition-filter='resource.type=\"cloud_run_revision\" AND metric.type=\"run.googleapis.com/request_latencies\"',condition-threshold-value=5000"
```

## Scaling Considerations

### Memory

- Default: 4Gi
- Increase if handling large datasets or many concurrent users
- BigQuery data is cached in memory for fast access

### CPU

- Default: 2 CPUs
- Gunicorn runs 4 workers, each can use up to 0.5 CPU
- Increase for CPU-intensive data processing

### Concurrency

- Default: 80 requests per instance
- Dash is stateless, so high concurrency is supported
- Lower if experiencing memory pressure

### Min Instances

- Default: 1
- Prevents cold starts
- Increase for high-traffic applications

## Troubleshooting

### Cold Start Timeout

If the first request times out:

```bash
# Increase startup CPU boost
gcloud run deploy variant-dashboard --cpu-boost

# Or increase timeout
gcloud run deploy variant-dashboard --timeout 600
```

### Memory Errors

```bash
# Increase memory
gcloud run deploy variant-dashboard --memory 8Gi
```

### BigQuery Connection Issues

1. Verify service account has BigQuery Data Viewer role
2. Check BigQuery table exists and is accessible
3. Verify project ID in config.py matches your BigQuery project

### GCS Access Issues

1. Verify service account has Storage Object Admin role
2. Check bucket exists and name matches GCS_CACHE_BUCKET env var
3. Verify bucket is in same region for optimal performance

## Updating the Application

### Rolling Update

```bash
# Build new image
docker build -t gcr.io/$PROJECT_ID/variant-dashboard:v2 .
docker push gcr.io/$PROJECT_ID/variant-dashboard:v2

# Deploy new version
gcloud run deploy variant-dashboard \
    --image gcr.io/$PROJECT_ID/variant-dashboard:v2
```

### Rollback

```bash
# List revisions
gcloud run revisions list --service variant-dashboard

# Route traffic to previous revision
gcloud run services update-traffic variant-dashboard \
    --to-revisions=variant-dashboard-00001-abc=100
```

## Cost Optimization

1. **Set min-instances to 0** for non-production environments
2. **Use committed use discounts** for production workloads
3. **Enable request-based billing** if traffic is sporadic
4. **Right-size memory and CPU** based on actual usage

```bash
# Development configuration (cost-optimized)
gcloud run deploy variant-dashboard-dev \
    --memory 2Gi \
    --cpu 1 \
    --min-instances 0 \
    --max-instances 3 \
    --concurrency 40
```
