import os
import json
from datetime import datetime

from flask import Flask, request
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import secretmanager
from google.cloud import firestore

app = Flask(__name__)

GMAIL_SECRET = os.environ.get("SECRET_NAME", "gmail-service-account")
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


def get_gmail_credentials(user_email):
    sm_client = secretmanager.SecretManagerServiceClient()
    name = f"projects/lionins/secrets/{GMAIL_SECRET}/versions/latest"
    secret = sm_client.access_secret_version(request={"name": name})
    sa_info = json.loads(secret.payload.data.decode("utf-8"))
    creds = service_account.Credentials.from_service_account_info(
        sa_info, scopes=SCOPES, subject=user_email
    )
    return creds


def store_watch_state(user: str, expiration: int, history_id: str):
    doc_ref = DB.collection(WATCH_STATE_COLLECTION).document(user)
    doc_ref.set({
        "expiration": expiration,
        "history_id": history_id,
        "updated_at": datetime.utcnow().isoformat(),
    }, merge=True)


@app.route("/", methods=["POST", "GET"])
def main(request):
    try:
        for user in USERS:
            creds = get_gmail_credentials(user)
            gmail = build("gmail", "v1", credentials=creds)
            resp = gmail.users().watch(userId="me", body=WATCH_BODY).execute()
            print(f"Watch set for {user}: expiration={resp.get('expiration')}, historyId={resp.get('historyId')}")
            
            # Store expiration and historyId in Firestore
            if "expiration" in resp and "historyId" in resp:
                store_watch_state(user, resp["expiration"], resp["historyId"])
        
        return ("Gmail watches registered successfully", 200)
    except Exception as e:
        print(f"Error registering Gmail watches: {e}")
        return (f"Error: {str(e)}", 500)