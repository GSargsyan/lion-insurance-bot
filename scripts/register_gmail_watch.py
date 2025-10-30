from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
SA_FILE = "secrets/gmail-service-account.json"
TOPIC = "projects/lionins/topics/gmail-notifications"
USERS = ["tony@lioninsurance.us"]

for user in USERS:
    creds = service_account.Credentials.from_service_account_file(
        SA_FILE, scopes=SCOPES, subject=user
    )
    gmail = build("gmail", "v1", credentials=creds)
    body = {"labelIds": ["INBOX"], "topicName": TOPIC}
    resp = gmail.users().watch(userId="me", body=body).execute()
    print("Watch set for", user, resp)