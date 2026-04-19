"""
config.py — Central configuration and authentication module.

Single source of truth for:
- Loading environment variables (.env for local dev, env vars for CI/prod)
- Creating the BigQuery client
- Shared constants

All other modules import from here instead of duplicating this logic.
"""
import os
import json

from google.cloud import bigquery
from google.oauth2 import service_account

# Load .env file for local development (silently skipped if not installed/found)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------
DATASET_ID = "cartola_analytics"


# ---------------------------------------------------------------------------
# BigQuery client factory
# ---------------------------------------------------------------------------
def get_bq_client() -> bigquery.Client:
    """
    Creates and returns an authenticated BigQuery client.

    Priority order:
    1. credentials.json file (local dev convenience — DO NOT commit this file)
    2. GCP_SERVICE_ACCOUNT environment variable (JSON string — used in CI/prod)
    3. Application Default Credentials (gcloud auth application-default login)

    Raises:
        ValueError: If GCP_SERVICE_ACCOUNT env var is set but cannot be parsed.
        google.auth.exceptions.DefaultCredentialsError: If no credentials found.
    """
    # 1. Local credentials file (kept in .gitignore)
    if os.path.exists("credentials.json"):
        return bigquery.Client.from_service_account_json("credentials.json")

    # 2. Environment variable (GitHub Actions secrets / cloud env)
    gcp_json = os.getenv("GCP_SERVICE_ACCOUNT")
    if gcp_json:
        try:
            info = json.loads(gcp_json) if isinstance(gcp_json, str) else gcp_json
            creds = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(credentials=creds, project=info["project_id"])
        except (json.JSONDecodeError, KeyError) as exc:
            raise ValueError(
                "GCP_SERVICE_ACCOUNT env var is set but could not be parsed. "
                "Ensure it contains valid JSON."
            ) from exc

    # 3. Application Default Credentials (gcloud SDK on developer machine)
    return bigquery.Client()
