import os
import json
import base64
import mimetypes
import requests
from flask import Flask, request
from google.cloud import firestore
from google.cloud import secretmanager
from google.oauth2 import service_account
from datetime import datetime
from email.message import EmailMessage
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import storage


app = Flask(__name__)

# --- Environment variables ---
COI_GENERATOR_CLOUD_RUN = 'https://coi-generator-142497757030.us-west1.run.app'
TELEGRAM_SECRET = os.environ.get("SECRET_NAME", "telegram-bot-key")
GMAIL_SECRET = os.environ.get("SECRET_NAME", "gmail-service-account")
SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
BUCKET_NAME = 'lion-insurance'

def get_telegram_key():
    sm_client = secretmanager.SecretManagerServiceClient()
    name = f"projects/lionins/secrets/{TELEGRAM_SECRET}/versions/latest"
    secret = sm_client.access_secret_version(request={"name": name})
    return secret.payload.data.decode("utf-8")

TELEGRAM_API = f"https://api.telegram.org/bot{get_telegram_key()}"
db = firestore.Client(database='lion-ins')

def get_gmail_credentials(user_email: str):
    """Load service-account JSON from Secret Manager and build delegated creds."""
    sm_client = secretmanager.SecretManagerServiceClient()
    name = f"projects/lionins/secrets/{GMAIL_SECRET}/versions/latest"
    secret = sm_client.access_secret_version(request={"name": name})
    sa_info = json.loads(secret.payload.data.decode("utf-8"))
    creds = service_account.Credentials.from_service_account_info(
        sa_info, scopes=SCOPES, subject=user_email
    )
    return creds




def send_message(chat_id, text, buttons=None):
    """Send a Telegram message with optional inline buttons."""
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}

    if buttons:
        payload["reply_markup"] = {"inline_keyboard": buttons}
    requests.post(f"{TELEGRAM_API}/sendMessage", json=payload)


def handle_callback(data):
    """ Handle the button clicks from telegram 

    """
    cq = data["callback_query"]
    callback_data = cq.get("data", "")
    chat_id = cq["from"]["id"]

    # Acknowledge callback (stops Telegram spinner)
    callback_id = cq.get("id")
    requests.post(f"{TELEGRAM_API}/answerCallbackQuery", json={"callback_query_id": callback_id})

    # Parse "send:<thread_id>" or "nosend:<thread_id>"
    if ":" in callback_data:
        action, thread_id = callback_data.split(":", 1)
    else:
        action, thread_id = callback_data, None

    doc_id = f"msg_{thread_id}"
    doc_ref = db.collection("pending_requests").document(doc_id)
    doc = doc_ref.get()
    subject = None
    if doc.exists:
        doc_dict = doc.to_dict()
    else:
        send_message(chat_id, "Request is already processed")
        return

    subject = doc.to_dict().get("subject")

    if action == "send":
        print(f"User approved sending COI for thread {thread_id}")
        doc_ref.update({"status": "sent"})
        send_message(chat_id, f"✅ Sending COI for:\n*{subject or thread_id}*")

        requests.post(COI_GENERATOR_CLOUD_RUN, json={
            "action": "generate_coi",
            "insured_inferred": doc_dict.get("insured_inferred", False),
            "insured_name": doc_dict.get("insured_name", ""),
            "holder_inferred": doc_dict.get("holder_inferred", False),
            "holder_name": doc_dict.get("holder_name", ""),
            "holder_addr_1": doc_dict.get("holder_addr_1", ""),
            "holder_addr_2": doc_dict.get("holder_addr_2", ""),
            "send_to_email": doc_dict.get("send_to_email", ""),
            "thread_id": thread_id,
            "subject_text": f"Re: {subject or 'Certificate Request'}",
            "body_text": "Please find attached the Certificate of Insurance."
        })

    elif action == "nosend":
        print(f"User declined COI for thread {thread_id}")
        doc_ref.update({"status": "skipped"})
        send_message(chat_id, f"🚫 Skipped COI for:\n*{subject or thread_id}*")


def notify_about_coi_request(data):
    """ Called when email was detected as a COI request.
    Will send the user a message with a button to approve or reject sending the COI.

    """
    thread_id = data["thread_id"]
    doc_id = f"msg_{thread_id}"
    doc_ref = db.collection("pending_requests").document(doc_id)

    doc_ref.set({
        "thread_id": thread_id,
        "subject": data["subject"],
        "chat_id": data["chat_id"],
        "status": "pending",
        "insured_inferred": data["insured_inferred"],
        "insured_name": data["insured_name"],
        "holder_inferred": data["holder_inferred"],
        "holder_name": data["holder_name"],
        "holder_addr_1": data["holder_addr_1"],
        "holder_addr_2": data["holder_addr_2"],
        "send_to_email": data["send_to_email"],
        "timestamp": datetime.utcnow().isoformat()
    })

    if not data["insured_inferred"]:
        text = (f"Email likely a COI request:\n*{data['subject']}*\n\n"
                "🚨 Could not infer insured name, please check manually")
        send_message(data["chat_id"], text)

    elif not data["holder_inferred"]:
        text = (f"Email likely a COI request:\n*{data['subject']}*\n\n"
                "🚨 Could not infer holder name, please check manually")
        send_message(data["chat_id"], text)
    else:
        buttons = [[
            {"text": "✅ Send", "callback_data": f"send:{thread_id}"},
            {"text": "🚫 Don't send", "callback_data": f"nosend:{thread_id}"}
        ]]

        send_message(data["chat_id"], f"Email likely a COI request:\n*{data['subject']}*", buttons)


@app.route("/", methods=["POST"])
def telegram_bot(request):
    data = request.get_json()
    print('GOT /telegram_bot REQUEST WITH BODY:')

    if data and "callback_query" in data:
        handle_callback(data)
    elif "thread_id" in data:
        notify_about_coi_request(data)
    else:
        print(f"Invalid request received")
        return ("", 400)

    return ("", 204)