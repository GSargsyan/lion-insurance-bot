import os
import json
import re
import base64
from datetime import datetime

from flask import Flask, request
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import secretmanager
from google.cloud import firestore
from google.cloud import storage
from google.auth import default
from openai import OpenAI

app = Flask(__name__)

GMAIL_SECRET = os.environ.get("SECRET_NAME", "gmail-service-account")
OPENAI_SECRET = os.environ.get("OPENAI_SECRET_NAME", "openai-key")
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
TOPIC = "projects/lionins/topics/gmail-notifications"
USERS = ["tony@lioninsurance.us"]

WATCH_BODY = {
    "topicName": TOPIC,
    "labelFilterAction": "include",       # only consider these labels
    "labelIds": ["INBOX"],                # new messages that land in INBOX
    "historyTypes": ["messageAdded"]      # <-- only notify for NEWLY ADDED messages
}

DB = firestore.Client(database='lion-ins')
WATCH_STATE_COLLECTION = "gmail_watch_state"

DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
DRIVE_CERTS_FOLDER_ID = "1KIeq3LHWWklQBanmADUz6XVYodlF2id6"
BUCKET_NAME = "lion-insurance"
CLIENTS_OUTPUT_PATH = "coi_bot/clients.json"
storage_client = storage.Client()
OPENAI_CLIENT = None


def get_gmail_credentials(user_email):
    sm_client = secretmanager.SecretManagerServiceClient()
    name = f"projects/lionins/secrets/{GMAIL_SECRET}/versions/latest"
    secret = sm_client.access_secret_version(request={"name": name})
    sa_info = json.loads(secret.payload.data.decode("utf-8"))
    creds = service_account.Credentials.from_service_account_info(
        sa_info, scopes=SCOPES, subject=user_email
    )
    return creds


def get_openai_key():
    sm_client = secretmanager.SecretManagerServiceClient()
    name = f"projects/lionins/secrets/{OPENAI_SECRET}/versions/latest"
    secret = sm_client.access_secret_version(request={"name": name})
    return secret.payload.data.decode("utf-8")


def get_openai_client():
    global OPENAI_CLIENT
    if OPENAI_CLIENT is None:
        OPENAI_CLIENT = OpenAI(api_key=get_openai_key())
    return OPENAI_CLIENT


def build_drive_service():
    creds, _ = default(scopes=DRIVE_SCOPES)
    return build("drive", "v3", credentials=creds)


def list_clients():
    drive_service = build_drive_service()
    clients: list[str] = []
    page_token = None
    query = (
        f"'{DRIVE_CERTS_FOLDER_ID}' in parents "
        "and mimeType = 'application/pdf' and trashed = false"
    )

    while True:
        response = drive_service.files().list(
            q=query,
            fields="nextPageToken, files(name)",
            pageSize=200,
            pageToken=page_token,
        ).execute()

        items = response.get("files", [])
        for item in items:
            name = item.get("name", "")
            if not name:
                continue
            base_name = os.path.splitext(name)[0]
            clients.append(base_name)

        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return clients


def deduplicate_clients(raw_names: list[str]) -> list[str]:
    if not raw_names:
        return []

    openai_client = get_openai_client()
    prompt = (
        "You are given file names pulled from a Google Drive folder that stores "
        "insurance certificates. Each file name usually includes a trucking company "
        "name, sometimes with extra words like ADDITIONAL REMARKS or (PD ONLY). "
        "Return a JSON object with a single key `company_names` whose value is a list "
        "of unique company names, uppercased, stripped of extra words and punctuation. "
        "Respond with valid JSON in this exact schema:\n"
        '{"company_names": ["COMPANY ONE LLC", "COMPANY TWO INC"]}'
        "\n\n"
        f"File names:\n{json.dumps(raw_names)}"
    )

    response = openai_client.chat.completions.create(
        model="gpt-5-mini",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "You clean and normalize company names for an insurance agency."},
            {"role": "user", "content": prompt},
        ],
    ).choices[0].message.content

    try:
        data = json.loads(response)
        names = data.get("company_names", [])
        if not isinstance(names, list):
            return []
        return [str(n).strip().upper() for n in names if str(n).strip()]
    except Exception:
        print("Failed to parse OpenAI response for company names.")
        return []


def extract_text(payload):
    if "body" in payload and "data" in payload["body"]:
        return base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="ignore")
    elif "parts" in payload:
        for part in payload["parts"]:
            text = extract_text(part)
            if text:
                return text
    return ""


def extract_email_address(text: str) -> str | None:
    match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    if match:
        return match.group(0)
    return None


def find_signer_email_for_company(gmail, company_name: str) -> str | None:
    query = f'subject:"{company_name} has been signed by"'
    messages = (
        gmail.users()
        .messages()
        .list(userId="me", q=query, maxResults=5)
        .execute()
        .get("messages", [])
    )

    for message in messages:
        msg = (
            gmail.users()
            .messages()
            .get(userId="me", id=message["id"], format="full")
            .execute()
        )
        body_text = extract_text(msg.get("payload", {})) or msg.get("snippet", "")
        email = extract_email_address(body_text)
        if email:
            return email

    return None


def gather_clients_emails_mapping():
    company_file_names = list_clients()
    cleaned_company_names = deduplicate_clients(company_file_names)

    if not cleaned_company_names:
        return {}

    creds = get_gmail_credentials("tony@lioninsurance.us")
    gmail = build("gmail", "v1", credentials=creds)

    results = {}
    for company in cleaned_company_names:
        email = find_signer_email_for_company(gmail, company)

        if email:
            results[company] = email

        doc_ref = DB.collection("clients_emails_mapping").document(company)
        doc_ref.set({
            "client": company,
            "email": email,
            "updated_at": datetime.now().isoformat(),
        })

    return results


def upload_results_to_gcs(data: dict):
    blob = storage_client.bucket(BUCKET_NAME).blob(CLIENTS_OUTPUT_PATH)
    blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")


def store_watch_state(user: str, expiration: int, history_id: str):
    doc_ref = DB.collection(WATCH_STATE_COLLECTION).document(user)
    doc_ref.set({
        "expiration": expiration,
        "history_id": history_id,
        "updated_at": datetime.utcnow().isoformat(),
    }, merge=True)


def register_gmail_watches():
    for user in USERS:
        creds = get_gmail_credentials(user)
        gmail = build("gmail", "v1", credentials=creds)
        resp = gmail.users().watch(userId="me", body=WATCH_BODY).execute()
        print(f"Watch set for {user}: expiration={resp.get('expiration')}, historyId={resp.get('historyId')}")

        if "expiration" in resp and "historyId" in resp:
            store_watch_state(user, resp["expiration"], resp["historyId"])


@app.route("/", methods=["POST", "GET"])
def main(request):
    try:
        # 1. Refresh Gmail watches
        register_gmail_watches()

        # 2. Gather company names
        clients_emails_mapping = gather_clients_emails_mapping()
        upload_results_to_gcs(clients_emails_mapping)
    except Exception as e:
        print(f"Error running daily cron tasks: {e}")

    return ("", 204)