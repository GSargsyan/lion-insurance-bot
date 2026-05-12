import os
import json
import base64
from io import BytesIO

from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from openai import OpenAI
import PyPDF2
import logging

# --- Configuration ---
KEYS_DIR = os.path.join(os.path.dirname(__file__), "..", "keys")
LOG_FILE = os.path.join(os.path.dirname(__file__), "..", "autopays.log")
AUTOPAYS_DIR = os.path.join(os.path.dirname(__file__), "..", "autopays")

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
RENEWALS_DOC_ID = "1qPO_OurfEGjEE3OZxV3iFZqADM2oeQrHif_XWmS56YQ"
SENDER_EMAIL = "tony@lioninsurance.us"

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/gmail.readonly"
]

def load_openai_key():
    openai_key_path = os.path.join(KEYS_DIR, "openai_key.txt")
    if os.path.exists(openai_key_path):
        with open(openai_key_path, "r") as f:
            return f.read().strip()
    return os.environ.get("OPENAI_API_KEY", "")

def get_google_services():
    creds_path = os.path.join(KEYS_DIR, "google_credentials.json")
    drive_creds = None
    gmail_creds = None

    if os.path.exists(creds_path):
        with open(creds_path, 'r') as f:
            creds_data = json.load(f)
        
        if "type" in creds_data and creds_data["type"] == "service_account":
            # Using Service Account with domain-wide delegation
            print("Using Service Account credentials...")
            drive_creds = service_account.Credentials.from_service_account_info(
                creds_data, scopes=["https://www.googleapis.com/auth/drive.readonly"]
            )
            gmail_creds = service_account.Credentials.from_service_account_info(
                creds_data, scopes=["https://www.googleapis.com/auth/gmail.readonly"], subject=SENDER_EMAIL
            )
        else:
            # Using standard OAuth 2.0 Client credentials
            print("Using OAuth 2.0 Client credentials...")
            token_path = os.path.join(KEYS_DIR, "token.json")
            creds = None
            if os.path.exists(token_path):
                creds = Credentials.from_authorized_user_file(token_path, SCOPES)
            
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
                    creds = flow.run_local_server(port=0)
                
                with open(token_path, "w") as token:
                    token.write(creds.to_json())
            
            drive_creds = creds
            gmail_creds = creds
    else:
        print(f"Error: {creds_path} not found. Please provide Google credentials.")
        return None, None

    drive_service = build("drive", "v3", credentials=drive_creds)
    gmail_service = build("gmail", "v1", credentials=gmail_creds)
    
    return drive_service, gmail_service

def get_clients_for_month(openai_client, drive_service, month, year):
    try:
        request = drive_service.files().export_media(fileId=RENEWALS_DOC_ID, mimeType='text/plain')
        doc_content = request.execute().decode('utf-8')
    except Exception as e:
        print(f"Error fetching Google Doc {RENEWALS_DOC_ID}: {e}")
        return []

    prompt = f"""
    You are given the text content of a Google Document named "Lion Insurance Renewals".
    Extract the list of client/company names that are scheduled for renewal in {month} {year}.
    Return the result as a JSON object with a single key `clients` containing a list of strings.
    Only the capitalized names of clients are needed, not dates or anything else that is written.
    If none are found, return an empty list.

    Document Content:
    {doc_content}
    """
    
    response = openai_client.chat.completions.create(
        model="gpt-5-mini",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "You carefully extract structured information from documents."},
            {"role": "user", "content": prompt},
        ],
    ).choices[0].message.content
    
    try:
        data = json.loads(response)
        return data.get("clients", [])
    except Exception as e:
        print("Failed to parse OpenAI response for clients.", e)
        return []

def extract_autopay_documents(gmail_service, company_name):
    query = f'subject:"{company_name} has been signed by" has:attachment newer_than:3m'
    
    try:
        messages_response = gmail_service.users().messages().list(userId='me', q=query, maxResults=20).execute()
        messages = messages_response.get('messages', [])
    except Exception as e:
        print(f"Error querying Gmail for {company_name}: {e}")
        return

    found_documents = 0

    for message in messages:
        msg_id = message['id']
        try:
            msg = gmail_service.users().messages().get(userId='me', id=msg_id, format='full').execute()
        except Exception as e:
            print(f"Error fetching message {msg_id}: {e}")
            continue
            
        parts = msg.get('payload', {}).get('parts', [])
        
        def process_parts(parts_list):
            nonlocal found_documents
            for part in parts_list:
                filename = part.get('filename')
                if filename and filename.lower().endswith('.pdf'):
                    attachment_id = part['body'].get('attachmentId')
                    if attachment_id:
                        print(f"  -> Downloading attachment: {filename}")
                        attachment = gmail_service.users().messages().attachments().get(
                            userId='me', messageId=msg_id, id=attachment_id
                        ).execute()
                        
                        file_data = base64.urlsafe_b64decode(attachment['data'])
                        
                        try:
                            pdf_reader = PyPDF2.PdfReader(BytesIO(file_data))
                            for page_num in range(len(pdf_reader.pages)):
                                page = pdf_reader.pages[page_num]
                                text = page.extract_text()
                                if text and "Commercial ACH Debit Authorization" in text:
                                    print(f"  -> Found 'Commercial ACH Debit Authorization' on page {page_num + 1} of {filename}")
                                    
                                    pdf_writer = PyPDF2.PdfWriter()
                                    pdf_writer.add_page(page)
                                    
                                    if not os.path.exists(AUTOPAYS_DIR):
                                        os.makedirs(AUTOPAYS_DIR)
                                        
                                    output_path = os.path.join(AUTOPAYS_DIR, filename)
                                    base, ext = os.path.splitext(filename)
                                    counter = 1
                                    while os.path.exists(output_path):
                                        output_path = os.path.join(AUTOPAYS_DIR, f"{base}_{counter}{ext}")
                                        counter += 1
                                        
                                    with open(output_path, "wb") as out_file:
                                        pdf_writer.write(out_file)
                                    print(f"  -> Saved extracted page to {output_path}")
                                    logging.info(f"Saved extracted autopay page for {company_name} to {output_path}")
                                    found_documents += 1
                        except Exception as e:
                            print(f"  -> Warning: Could not parse PDF {filename}: {e}")
                            logging.error(f"Warning: Could not parse PDF {filename}: {e}")
                elif 'parts' in part:
                    process_parts(part['parts'])

        process_parts(parts)
        
    if found_documents == 0:
        print("  -> No Commercial ACH Debit Authorization documents found.")

def main():
    target_month = input("Enter the month (e.g., January): ").strip()
    target_year = input("Enter the year (e.g., 2024): ").strip()
    
    openai_key = load_openai_key()
    if not openai_key:
        print("Error: OpenAI key is missing. Please create keys/openai_key.txt")
        logging.error("OpenAI key is missing. Script aborted.")
        return
        
    openai_client = OpenAI(api_key=openai_key)
    
    print("Initializing Google Services...")
    drive_service, gmail_service = get_google_services()
    if not drive_service or not gmail_service:
        return
        
    print(f"\nFetching clients for {target_month} {target_year}...")
    clients = get_clients_for_month(openai_client, drive_service, target_month, target_year)
    print(f"Found {len(clients)} clients: {clients}")
    
    for client in clients:
        print(f"\n--- Processing client: {client} ---")
        extract_autopay_documents(gmail_service, client)
        
    print("\n✅ Finished processing all clients.")

if __name__ == "__main__":
    main()
