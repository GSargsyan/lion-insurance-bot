import os
import json
import base64
from datetime import datetime

import requests
from flask import Flask, request
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import secretmanager
from google.cloud import storage
from google.cloud import firestore

app = Flask(__name__)

# TODO: Create another scheduled cloud run to periodically register gmail watch

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

# setup the common service cloud run url
# TODO: check url
COI_GENERATOR_CLOUD_RUN = 'https://coi-generator-142497757030.us-west1.run.app'


def log_to_gcs(file_prefix: str, message: str):
    # GCS configuration
    BUCKET_NAME = "lion-insurance"
    FOLDER = "coi_bot_logs"

    # Timestamped file name up to milliseconds
    timestamp = datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S_%f")
    file_name = f"{file_prefix}_{timestamp}.txt"

    # Full path inside the bucket
    blob_path = f"{FOLDER}/{file_name}"

    # Initialize client and upload
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_path)

    blob.upload_from_string(message.strip() + "\n")


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
GMAILS = {email: build("gmail", "v1", credentials=get_gmail_credentials(email)) for email in EMAILS}


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


def handle_email(
    thread_id: str,
    user: str,
    subject: str,
    body_text: str,
    to_emails: list[str],
    cc_emails: list[str]
):
    """ Analyze email and do the required actions.
    Currently only supports COI request handling.

    """
    analysis = requests.post(COI_GENERATOR_CLOUD_RUN, json={
        "action": "analyze_for_coi_request",
        "subject": subject,
        "body_text": body_text,
        "to_emails": to_emails,
        "cc_emails": cc_emails
    }, timeout=10).json()

    if analysis['is_likely_coi_request']:
        print(f"COI REQUEST DETECTED! Subject: {subject}")

        # notify telegram bot about the coi request
        payload = {
            "thread_id": thread_id,
            "subject": subject,
            "chat_id": TELEGRAM_CHAT_ID,
            "insured_inferred": analysis['insured_inferred'],
            "insured_name": analysis['insured_name'],
            "holder_inferred": analysis['holder_inferred'],
            "holder_name": analysis['holder_name'],
            "holder_addr_1": analysis['holder_addr_1'],
            "holder_addr_2": analysis['holder_addr_2'],
            "send_to_email": analysis['send_to_email'],
        }

        try:
            r = requests.post(TELEGRAM_CLOUD_RUN, json=payload, timeout=10)
            r.raise_for_status()
            print(f"[OK] Notified Telegram bot for email '{subject}'")
        except Exception as e:
            print(f"[ERR] Failed to notify Telegram bot: {e}")
    else:
        print(f"not a coi request")


def get_latest_thread(gmail: build):
    # testing with a specific subject
    threads = gmail.users().threads().list(
        userId="me",
        q='subject:"Request for certificate for ROAD GRIP TRANSPORT LLC, DOT# 2939106"',
        labelIds=["INBOX"],
        maxResults=1
    ).execute().get("threads", [])

    if not threads:
        return None
    
    # Fetch the full thread with messages using the thread ID
    thread_id = threads[0]['id']
    thread = gmail.users().threads().get(userId="me", id=thread_id).execute()
    return thread


def get_last_email_contents(msg_data):
    headers = msg_data.get("payload", {}).get("headers", [])
    subject = next((h["value"] for h in headers if h["name"] == "Subject"), "(No Subject)")
    sender = next((h["value"] for h in headers if h["name"] == "From"), "Unknown Sender")

    # get the emails in the to and cc's
    to_emails = next((h["value"] for h in headers if h["name"] == "To"), "").split(",")
    cc_emails = next((h["value"] for h in headers if h["name"] == "Cc"), "").split(",")
    
    body_text = extract_text(msg_data["payload"]) or msg_data.get("snippet", "")
    return subject, sender, body_text, to_emails, cc_emails


@app.route("/", methods=["POST"])
def email_watcher(request):
    envelope = request.get_json(force=True)
    msg = envelope.get("message", {})
    if "data" not in msg:
        return ("", 204)
    data = json.loads(base64.b64decode(msg["data"]).decode("utf-8"))

    user = data["emailAddress"]
    gmail = GMAILS[user]

    thread = get_latest_thread(gmail)
    thread_id = thread['id']
    
    # TODO: TESTING
    # if len(thread['messages']) > 1:
    #     continue

    # msg_data = thread["messages"][-1]
    # TESTING
    # msg_data = thread['messages'][0]
    subject, sender, body_text, to_emails, cc_emails = get_last_email_contents(thread['messages'][-1])

    log_to_gcs('email_body', body_text)

    if thread_id == get_last_processed_id():
        print(f"Thread with ID - {thread_id} is already processed, skipping!")
        return ("", 204)

    handle_email(thread_id, user, subject, body_text, to_emails, cc_emails)
    save_last_processed_id(thread_id)

    return ("", 204)