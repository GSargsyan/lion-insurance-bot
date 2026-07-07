import os
import json
import base64
import re
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
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
LOG_FILE = os.path.join(os.path.dirname(__file__), "..", "declarations.log")

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
RENEWALS_DOC_ID = "1qPO_OurfEGjEE3OZxV3iFZqADM2oeQrHif_XWmS56YQ"
DRIVERS_FOLDER_ID = "1NLt-FqU-wvr8VU3fDbiRSK2dcz8DakPl"
SENDER_EMAIL = "tony@lioninsurance.us"

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.compose",
    "https://www.googleapis.com/auth/gmail.labels",
    "https://www.googleapis.com/auth/gmail.modify"
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
                creds_data, scopes=["https://www.googleapis.com/auth/gmail.readonly", "https://www.googleapis.com/auth/gmail.compose", "https://www.googleapis.com/auth/gmail.labels", "https://www.googleapis.com/auth/gmail.modify"], subject=SENDER_EMAIL
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
    messages_response = (
        gmail.users()
        .messages()
        .list(userId="me", q=query, maxResults=5)
        .execute()
    )
    messages = messages_response.get("messages", [])

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

def search_and_download_binders(openai_client, gmail_service, company_name):
    query = f'"{company_name}" label:binders has:attachment newer_than:2m'
    messages_response = gmail_service.users().messages().list(userId='me', q=query, maxResults=20).execute()
    messages = messages_response.get('messages', [])
    
    documents_info = []
    downloaded_files = {}

    for message in messages:
        msg_id = message['id']
        msg = gmail_service.users().messages().get(userId='me', id=msg_id, format='full').execute()
        
        parts = msg.get('payload', {}).get('parts', [])
        # If payload itself is multipart, check parts. Sometimes it's nested.
        def find_attachments(parts_list):
            for part in parts_list:
                filename = part.get('filename')
                if filename and filename.lower().endswith('.pdf'):
                    attachment_id = part['body'].get('attachmentId')
                    if attachment_id:
                        attachment = gmail_service.users().messages().attachments().get(
                            userId='me', messageId=msg_id, id=attachment_id
                        ).execute()
                        
                        file_data = base64.urlsafe_b64decode(attachment['data'])
                        
                        try:
                            pdf_reader = PyPDF2.PdfReader(BytesIO(file_data))
                            if pdf_reader.is_encrypted:
                                try:
                                    pdf_reader.decrypt("")
                                except Exception as decrypt_err:
                                    logging.warning(f"Could not decrypt PDF {filename}: {decrypt_err}")
                            n_pages = len(pdf_reader.pages)
                            first_page_text = pdf_reader.pages[0].extract_text() if n_pages > 0 else ""
                        except Exception as e:
                            print(f"Warning: Could not parse PDF {filename}: {e}")
                            n_pages = 0
                            first_page_text = ""
                        
                        documents_info.append({
                            "filename": filename,
                            "n_pages": n_pages,
                            "first_page_text": first_page_text[:1500]
                        })
                        
                        downloaded_files[filename] = file_data
                elif 'parts' in part:
                    find_attachments(part['parts'])

        find_attachments(parts)

    if not documents_info:
        return {}

    prompt = f"""
    We are looking for Policy documents (Declaration documents)
    for the company "{company_name}".
    We found the following PDF attachments in their emails.
    There are 3 types of coverages: AL (Auto-liability), MTC (Motor Truck Cargo), and PD (Physical Damage).
    Based on the filenames and the first page text, determine which documents are the actual policy/declaration documents.
    Here are some examples of declaration documents file names
    "EXAMPLE LOGISTICS LLC MTC POLICY 04.10.25.pdf"
    "CWIS-Policy-AL-COMPANYNAME-1230727323-2-23953...... .pdf"
    "Insured - Renewal Policy Issue.pdf"
    "Companyname Policy EFF 02.02.2025.pdf"
    "A-ONE .... .pdf"
    "Policy Packet.pdf"
    "DRNCA102179 POLICY.pdf"
    Typically they are long documents like 50-80 pages long. If they are very short like 2-10 pages,
    then it's not a declaration document for sure.

    Return a JSON object mapping the exact filename to the coverage type it represents (e.g., "AL", "MTC", "PD").
    Only include the documents that are indeed policy/declaration documents.

    Documents found:
    {json.dumps(documents_info, indent=2)}
    """

    response = openai_client.chat.completions.create(
        model="gpt-5-mini",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "You classify insurance documents."},
            {"role": "user", "content": prompt},
        ],
    ).choices[0].message.content

    try:
        selected_docs_mapping = json.loads(response)
        final_files = {}
        for filename, coverage_type in selected_docs_mapping.items():
            if filename in downloaded_files:
                final_files[filename] = {
                    "coverage_type": coverage_type,
                    "data": downloaded_files[filename]
                }
        return final_files
    except Exception as e:
        print("Failed to parse LLM response for binders.", e)
        return {}

def get_driver_names(openai_client, drive_service, company_name):
    # Escape single quotes for Google Drive query syntax
    escaped_company = company_name.replace("'", "\\'")
    query = f"'{DRIVERS_FOLDER_ID}' in parents and name contains '{escaped_company}' and trashed = false"
    response = drive_service.files().list(q=query, fields="files(id, name, mimeType)").execute()
    files = response.get('files', [])
    
    if not files:
        return []
    
    file_id = files[0]['id']
    mime_type = files[0]['mimeType']
    
    try:
        if 'google-apps.document' in mime_type:
            request = drive_service.files().export_media(fileId=file_id, mimeType='text/plain')
            file_content = request.execute().decode('utf-8', errors='ignore')
        else:
            request = drive_service.files().get_media(fileId=file_id)
            file_content = request.execute().decode('utf-8', errors='ignore')
    except Exception as e:
        print(f"Error fetching drivers file for {company_name}: {e}")
        return []

    prompt = f"""
    You are given a text containing driver, and possibly some other
    information for an insurance policy.
    Extract the names of the drivers. Output only the driver names, with first letters capitalized, 
    one per item in a JSON list under the key `drivers`.
    
    File content:
    {file_content}
    """

    llm_response = openai_client.chat.completions.create(
        model="gpt-5-mini",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "You accurately extract driver names from unstructured text."},
            {"role": "user", "content": prompt},
        ],
    ).choices[0].message.content

    try:
        data = json.loads(llm_response)
        return data.get("drivers", [])
    except Exception as e:
        print("Failed to parse drivers.", e)
        return []

def create_draft_email(gmail_service, to_email, company_name, attachments, drivers):
    message = MIMEMultipart()
    if to_email:
        message['to'] = to_email
    message['from'] = SENDER_EMAIL
    message['subject'] = f"Declaration Documents: {company_name.upper()}"
    
    drivers_list_str = "\n".join([f" * {driver}" for driver in drivers])
    
    body = f"""Hello,

Please see the declaration documents of your policies attached for your reference.
Please see the drivers included in the policies below:
{drivers_list_str}

Thank you,
Sincerely,
Tony Vardanyan,
Lion Insurance Services."""

    msg = MIMEText(body)
    message.attach(msg)
    
    for filename, file_info in attachments.items():
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(file_info['data'])
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
        message.attach(part)
        
    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')
    
    draft_body = {
        'message': {
            'raw': raw_message,
        }
    }
    
    try:
        draft = gmail_service.users().drafts().create(userId='me', body=draft_body).execute()
        print(f"Draft created for {company_name} to {to_email}. Draft ID: {draft['id']}")
        
        attached_docs = ", ".join([f"{k} ({v['coverage_type']})" for k, v in attachments.items()])
        if not attached_docs:
            attached_docs = "None"
        logging.info(f"Successfully created draft for '{company_name}' to {to_email}. Attached docs: {attached_docs}")
        
        # Add label
        labels_results = gmail_service.users().labels().list(userId='me').execute()
        labels = labels_results.get('labels', [])
        target_label_id = None
        for label in labels:
            # Gmail labels might be "Declaration Documents" (with spaces)
            if label['name'].lower().replace(" ", "-") == "declaration-documents":
                target_label_id = label['id']
                break
                
        if not target_label_id:
            label_body = {
                "name": "Declaration Documents",
                "labelListVisibility": "labelShow",
                "messageListVisibility": "show"
            }
            created_label = gmail_service.users().labels().create(userId='me', body=label_body).execute()
            target_label_id = created_label['id']
            
        if target_label_id:
            msg_id = draft['message']['id']
            gmail_service.users().messages().modify(
                userId='me', 
                id=msg_id, 
                body={'addLabelIds': [target_label_id]}
            ).execute()
            print(f"Added label 'declaration-documents' to draft.")
    except Exception as e:
        print(f"An error occurred while creating draft for {company_name}: {e}")
        logging.error(f"Error creating draft for '{company_name}': {e}")

def main():
    target_month = input("Enter the month of the previous month (e.g., January): ").strip()
    target_year = input("Enter the year of the previous month (e.g., 2024): ").strip()
    
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
        
        email = find_signer_email_for_company(gmail_service, client)
        if not email:
            print(f"-> Could not find main email for {client}. Draft will be created without 'To' address.")
            logging.warning(f"Could not find main email for '{client}'. Draft will be created without 'To' address.")
            email = ""
        else:
            print(f"-> Found main email: {email}")
        
        print("-> Searching for declaration documents in Gmail...")
        attachments = search_and_download_binders(openai_client, gmail_service, client)
        if attachments:
            found_docs = [f"{k} ({v['coverage_type']})" for k, v in attachments.items()]
            print(f"-> Found valid policy documents: {', '.join(found_docs)}")
        else:
            print("-> Could not find suitable declaration documents. Creating draft without attachments.")
            logging.warning(f"Could not find suitable declaration documents for '{client}'. Draft will be created without attachments.")
            
        print("-> Searching for drivers in Google Drive...")
        drivers = get_driver_names(openai_client, drive_service, client)
        print(f"-> Found {len(drivers)} drivers: {drivers}")
        
        print("-> Creating draft email...")
        create_draft_email(gmail_service, email, client, attachments, drivers)
        
    print("\n✅ Finished processing all clients.")

if __name__ == "__main__":
    main()
