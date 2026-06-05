"""Gmail helpers for the telegram bot service.

Currently provides one function:
- create_loss_run_draft: create a Gmail draft in Tony's mailbox.
"""
import base64
import time
from email.message import EmailMessage

from google.oauth2 import service_account
from googleapiclient.discovery import build

_GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.compose",
]

_FROM_EMAIL = "tony@lioninsurance.us"


def _gmail_service(sa_info: dict):
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=_GMAIL_SCOPES,
        subject=_FROM_EMAIL,
    )
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def create_loss_run_draft(insured_name: str, sa_info: dict) -> str | None:
    """Create a Gmail draft Loss Run Request email in Tony's mailbox.

    Args:
        insured_name: The matched insured company name (without .pdf extension).
        sa_info: Service account JSON dict from Secret Manager.

    Returns:
        The Gmail draft ID, or None on failure.
    """
    # Strip .pdf extension for display in the email
    display_name = insured_name.removesuffix(".pdf")

    subject = f"Loss Runs Request: {display_name}"
    body = f"Hello,\nPlease issue the loss runs for {display_name}.\nThank you."

    msg = EmailMessage()
    msg["From"] = f"Tony Lion Insurance <{_FROM_EMAIL}>"
    msg["Subject"] = subject
    msg.set_content(body)

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

    service = _gmail_service(sa_info)
    start = time.time()
    try:
        draft = (
            service.users()
            .drafts()
            .create(userId="me", body={"message": {"raw": raw}})
            .execute()
        )
        print(f"[TIMING] Gmail drafts.create: {time.time() - start:.2f}s")
        draft_id = draft.get("id")
        print(f"[GMAIL] Draft created: ID={draft_id}, subject={subject!r}")
        return draft_id
    except Exception as exc:
        print(f"[GMAIL] Failed to create draft: {exc}")
        return None
