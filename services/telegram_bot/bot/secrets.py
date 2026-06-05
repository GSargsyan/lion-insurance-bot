"""Secret Manager helpers for the telegram bot service."""
import os
import json

from google.cloud import secretmanager

GCP_PROJECT = os.environ.get("GCP_PROJECT", "lionins")


def _get_secret(secret_name: str) -> str:
    """Fetch the latest version of a secret from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{GCP_PROJECT}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


def get_telegram_token() -> str:
    secret_name = os.environ.get("TELEGRAM_SECRET_NAME", "telegram-bot-key")
    return _get_secret(secret_name)


def get_openai_key() -> str:
    secret_name = os.environ.get("OPENAI_SECRET_NAME", "openai-key")
    return _get_secret(secret_name)


def get_gmail_service_account_info() -> dict:
    secret_name = os.environ.get("GMAIL_SECRET_NAME", "gmail-service-account")
    raw = _get_secret(secret_name)
    return json.loads(raw)
