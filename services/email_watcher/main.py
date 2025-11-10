import os
import json
import base64
import time
from datetime import datetime

import requests
from flask import Flask, request
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import secretmanager
from google.cloud import storage
from google.cloud import firestore

app = Flask(__name__)

# TODO: change id
TELEGRAM_CHAT_ID = 828259521 # Grig
# TODO: check url
TELEGRAM_CLOUD_RUN = 'https://telegram-bot-142497757030.us-west1.run.app'

GMAIL_SECRET = os.environ.get("SECRET_NAME", "gmail-service-account")
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
# Configure once
BUCKET_NAME = "lion-insurance"
LOG_PATH = "coi_bot/output.log"

db = firestore.Client(database='lion-ins')
FSTORE_COLLECTION = "email_state"
FSTORE_DOCUMENT = "last_processed"
PROCESSING_COLLECTION = "email_processing_locks"

COI_GENERATOR_CLOUD_RUN = 'https://coi-generator-142497757030.us-west1.run.app'

# Centralized Firestore step logger for observability across the flow
def log_step(step: str, status: str = "ok", thread_id: str | None = None, data: dict | None = None, error: str | None = None):
    try:
        payload = {
            "step": step,
            "status": status,
            "thread_id": thread_id,
            "service": "email_watcher",
            "timestamp": datetime.utcnow().isoformat(),
        }
        if data:
            payload["data"] = data
        if error:
            payload["error"] = error

        if thread_id:
            doc_ref = db.collection("coi_flow_logs").document(f"thread_{thread_id}")
            # ensure parent doc exists/updates a heartbeat
            doc_ref.set({
                "thread_id": thread_id,
                "updated_at": datetime.utcnow().isoformat(),
                "service_seen": firestore.ArrayUnion(["email_watcher"]),
            }, merge=True)
            # append event
            doc_ref.collection("events").add(payload)
        else:
            # fallback: log as a single document per step at top-level
            doc_ref = db.collection("coi_flow_logs").document(f"step_{step}")
            doc_ref.set(payload, merge=True)
    except Exception as e:
        # As a fallback, still print so we don't lose the context entirely
        print(f"[OBS-ERR] Failed to log step '{step}': {e}")


def get_gmail_credentials(user_email):
    sm_client = secretmanager.SecretManagerServiceClient()
    name = f"projects/lionins/secrets/{GMAIL_SECRET}/versions/latest"
    secret = sm_client.access_secret_version(request={"name": name})
    sa_info = json.loads(secret.payload.data.decode("utf-8"))
    creds = service_account.Credentials.from_service_account_info(
        sa_info, scopes=SCOPES, subject=user_email
    )
    return creds

EMAILS = ['tony@lioninsurance.us']

def extract_text(payload):
    if "body" in payload and "data" in payload["body"]:
        return base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="ignore")
    elif "parts" in payload:
        for part in payload["parts"]:
            text = extract_text(part)
            if text:
                return text
    return ""


def get_last_processed_id():
    doc = db.collection(FSTORE_COLLECTION).document(FSTORE_DOCUMENT).get()
    if doc.exists:
        return doc.to_dict().get("last_thread_id")
    return None


def save_last_processed_id(thread_id):
    db.collection(FSTORE_COLLECTION).document(FSTORE_DOCUMENT).set({
        "last_thread_id": thread_id,
        "updated_at": datetime.utcnow().isoformat()
    })


def acquire_processing_lock(thread_id: str) -> bool:
    """Try to acquire a short-lived processing lock for a thread.
    Returns True if acquired, False if already locked recently.
    """
    lock_ref = db.collection(PROCESSING_COLLECTION).document(f"thread_{thread_id}")
    doc = lock_ref.get()
    now_iso = datetime.utcnow().isoformat()
    if doc.exists:
        # Already being processed or recently processed; skip duplicate run
        return False
    try:
        lock_ref.create({
            "thread_id": thread_id,
            "status": "processing",
            "created_at": now_iso,
        })
        return True
    except Exception:
        return False


def release_processing_lock(thread_id: str):
    lock_ref = db.collection(PROCESSING_COLLECTION).document(f"thread_{thread_id}")
    try:
        lock_ref.delete()
    except Exception:
        pass


def handle_email(
    thread_id: str,
    user: str,
    subject: str,
    body_text: str,
    to_emails: list[str],
    cc_emails: list[str],
    from_email: str,
    last_message_id: str
):
    """ Analyze email and do the required actions.
    Currently only supports COI request handling.

    """
    log_step("email_received", thread_id=thread_id, data={"subject": subject, "to": to_emails, "cc": cc_emails})
    log_step("email_body", thread_id=thread_id, data={"body_text": body_text})

    print(f"[INFO] Analyzing email with subject: {subject}")

    start = time.time()
    requests.post(COI_GENERATOR_CLOUD_RUN, json={
        "action": "analyze_for_coi_request",
        "thread_id": thread_id,
        "subject": subject,
        "body_text": body_text,
        "to_emails": to_emails,
        "cc_emails": cc_emails,
        "from_email": from_email,
        "last_message_id": last_message_id
    }, timeout=30)
    print(f"[TIMING] COI Generator analyze_for_coi_request: {time.time() - start:.2f}s")

    log_step("coi_analysis_completed", thread_id=thread_id)


def get_latest_thread(gmail: build):
    """Return the thread that contains the most recent message by internalDate.
    We scan a window of recent threads and pick the newest message across them.
    """
    start = time.time()
    threads = gmail.users().threads().list(
        userId="me",
        q="newer_than:1m to:me -from:me label:inbox",
        maxResults=3
        # TODO: TMP testing
        # q='subject:"CERT HOLDER" from:carriersetup@relyonrts.com label:inbox',
        # q='subject:"Certificate of Insurance - MM EXPRESS INC * 1732307*" label:inbox',
        # maxResults=1
    ).execute().get("threads", [])
    print(f"[TIMING] Gmail threads.list: {time.time() - start:.2f}s")

    if not threads:
        return None

    latest_thread = None
    latest_ts = -1

    for t in threads:
        thread_id = t['id']
        start = time.time()
        thread = gmail.users().threads().get(userId="me", id=thread_id).execute()
        messages = thread.get('messages', [])

        for m in messages:
            try:
                ts = int(m.get('internalDate', '0'))
            except Exception:
                ts = 0
            if ts > latest_ts:
                latest_ts = ts
                latest_thread = thread

    return latest_thread


def get_last_email_contents(msg_data):
    headers = msg_data.get("payload", {}).get("headers", [])
    subject = next((h["value"] for h in headers if h["name"] == "Subject"), "(No Subject)")
    sender = next((h["value"] for h in headers if h["name"] == "From"), "Unknown Sender")

    # get the emails in the to and cc's
    to_emails = next((h["value"] for h in headers if h["name"] == "To"), "").split(",")
    cc_emails = next((h["value"] for h in headers if h["name"] == "Cc"), "").split(",")
    from_email = next((h["value"] for h in headers if h["name"] == "From"), "")
    
    body_text = extract_text(msg_data["payload"]) or msg_data.get("snippet", "")
    last_message_id = msg_data.get("id")
    return subject, sender, body_text, to_emails, cc_emails, from_email, last_message_id


@app.route("/", methods=["POST"])
def email_watcher(request):
    request_start = time.time()
    envelope = request.get_json(force=True)
    msg = envelope.get("message", {})
    if "data" not in msg:
        return ("", 204)
    data = json.loads(base64.b64decode(msg["data"]).decode("utf-8"))

    user = data["emailAddress"]
    gmail = build("gmail", "v1", credentials=get_gmail_credentials("tony@lioninsurance.us"))

    thread = get_latest_thread(gmail)
    if not thread:
        return ("", 204)
    
    thread_id = thread['id']

    # Acquire processing lock to avoid duplicate concurrent runs for the same thread
    if not acquire_processing_lock(thread_id):
        log_step("processing_lock_held", thread_id=thread_id, data={"note": "duplicate trigger skipped"})
        print(f"[INFO] Thread with ID - {thread_id} is already processed, skipping!")
        return ("", 204)
    
    try:
        # Use the most recent message in the thread by internalDate
        messages = thread.get('messages', [])
        if not messages:
            print(f"[INFO] Thread with ID - {thread_id} has no messages, skipping!")
            return ("", 204)
        msg_data = max(messages, key=lambda m: int(m.get('internalDate', '0')))
        subject, sender, body_text, to_emails, cc_emails, from_email, last_message_id = get_last_email_contents(msg_data)

        if thread_id == get_last_processed_id():
            print(f"[INFO] Thread with ID - {thread_id} is already processed, skipping!")
            return ("", 204)

        handle_email(thread_id, user, subject, body_text, to_emails, cc_emails, from_email, last_message_id)
        
        save_last_processed_id(thread_id)
    except Exception as e:
        log_step("email_watcher_failed", status="error", thread_id=thread_id, error=str(e))
        print(f"[ERR] email_watcher failed: {e}")
        return ("", 204)
    finally:
        release_processing_lock(thread_id)

    print(f"[TIMING] Total email_watcher request: {time.time() - request_start:.2f}s")
    return ("", 204)