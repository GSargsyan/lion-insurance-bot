from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
SA_FILE = "secrets/gmail_service_account.json"
TOPIC = "projects/lionins/topics/gmail-notifications"
USERS = ["tony@lioninsurance.us"]

WATCH_BODY = {
    "topicName": TOPIC,
    "labelFilterAction": "include",       # only consider these labels
    "labelIds": ["INBOX"],                # new messages that land in INBOX
    "historyTypes": ["messageAdded"]      # <-- only notify for NEWLY ADDED messages
}

for user in USERS:
    creds = service_account.Credentials.from_service_account_file(
        SA_FILE, scopes=SCOPES, subject=user  # domain-wide delegation assumed
    )
    gmail = build("gmail", "v1", credentials=creds)
    resp = gmail.users().watch(userId="me", body=WATCH_BODY).execute()
    print("Watch set for", user, resp)
    # Store resp["expiration"] (ms since epoch) and resp["historyId"] in Firestore