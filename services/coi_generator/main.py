""" Cloud Run service to analyze emails for COI requests, generate and send COIs. """
import base64
import io
import json
import mimetypes
import os
from datetime import date

import fitz  # PyMuPDF
from flask import Flask, request
from fillpdf import fillpdfs
from google.auth import default
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import secretmanager
from google.oauth2 import service_account
from google.cloud import firestore
from pypdf import PdfReader, PdfWriter
from googleapiclient.errors import HttpError
from email.message import EmailMessage
from openai import OpenAI

app = Flask(__name__)

db = firestore.Client(database='lion-ins')
# Initialize OpenAI client
OPENAI_SECRET = os.environ.get("SECRET_NAME", "openai-key")
GMAIL_SECRET = os.environ.get("SECRET_NAME", "gmail-service-account")
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

# Define the scope to access to search and download files
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
DRIVE_FOLDER_ID = "1KIeq3LHWWklQBanmADUz6XVYodlF2id6"  # Drive folder
BUCKET_NAME = "lion-insurance"  # Cloud Storage bucket name
storage_client = storage.Client()
BUCKET = storage_client.bucket(BUCKET_NAME)

def get_openai_key():
    sm_client = secretmanager.SecretManagerServiceClient()
    name = f"projects/lionins/secrets/{OPENAI_SECRET}/versions/latest"
    secret = sm_client.access_secret_version(request={"name": name})
    return secret.payload.data.decode("utf-8")

OPENAI_CLIENT = OpenAI(api_key=get_openai_key())

def get_gmail_credentials(user_email: str):
    sm_client = secretmanager.SecretManagerServiceClient()
    name = f"projects/lionins/secrets/{GMAIL_SECRET}/versions/latest"
    secret = sm_client.access_secret_version(request={"name": name})
    sa_info = json.loads(secret.payload.data.decode("utf-8"))
    creds = service_account.Credentials.from_service_account_info(
        sa_info, scopes=GMAIL_SCOPES, subject=user_email
    )
    return creds


def download_from_drive(insured: str):
    res = []

    # Use the Cloud Run service account credentials
    creds, _ = default(scopes=DRIVE_SCOPES)
    drive_service = build("drive", "v3", credentials=creds)

    # --- Search for matching PDFs in Drive ---
    query = (f"name contains '{insured}' and '{DRIVE_FOLDER_ID}' "
             f"in parents")
    results = drive_service.files().list(
        q=query,
        pageSize=10,
        fields="nextPageToken, files(id, name)",
    ).execute()

    items = results.get("files", [])
    if not items:
        print(f"No files found for {insured}")
        return res

    print("Files found:")
    if len(items) > 2:
        items = items[:2]

    # --- Download and upload to Cloud Storage ---
    for item in items:
        name = item["name"]
        if not name.endswith(".pdf"):
            continue

        print(f"Downloading {name}...")
        file_id = item["id"]
        request = drive_service.files().get_media(fileId=file_id)

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                print(f"Download {int(status.progress() * 100)}%")

        fh.seek(0)

        blob = BUCKET.blob(f"certificates/{name}")
        blob.upload_from_file(fh, content_type="application/pdf")
        print(f"Uploaded to gs://{BUCKET_NAME}/certificates/{name}")
        res.append(name)

    return res


def fill_pdf(input_filename: str, output_filename: str, coi_holder: tuple[str, str, str]):
    """
    Reads a PDF from GCS, fills form fields, and writes back to GCS.
    """
    input_blob = BUCKET.blob(f"certificates/{input_filename}")
    pdf_bytes = input_blob.download_as_bytes()

    reader = PdfReader(io.BytesIO(pdf_bytes))
    writer = PdfWriter()
    writer.append(reader)

    field_values = {
        "Form_CompletionDate_A": f'{date.today().strftime("%m/%d/%Y")}',
        "CertificateHolder_FullName_A": coi_holder[0],
        "CertificateHolder_MailingAddress_LineOne_A": coi_holder[1],
        "CertificateHolder_MailingAddress_LineTwo_A": coi_holder[2],
    }

    writer.update_page_form_field_values(
        writer.pages[0], field_values, auto_regenerate=False
    )

    # Write to memory
    output_buffer = io.BytesIO()
    writer.write(output_buffer)
    output_buffer.seek(0)

    # Upload updated PDF back to GCS
    output_blob = BUCKET.blob(f"certificates/{output_filename}")
    output_blob.upload_from_file(
        output_buffer, content_type="application/pdf"
    )


def add_signature_and_flatten(file_name: str):
    """
    Adds signature image and flattens PDF in GCS.
    Uses /tmp/ local files for fillpdfs.
    """
    # --- Download PDF and signature image ---
    pdf_blob = BUCKET.blob(f"certificates/{file_name}")
    pdf_bytes = pdf_blob.download_as_bytes()

    signature_blob = BUCKET.blob("certificates/signature.png")
    signature_bytes = signature_blob.download_as_bytes()

    # --- Work in memory with fitz ---
    pdf_stream = io.BytesIO(pdf_bytes)
    doc = fitz.open("pdf", pdf_stream)

    # --- Insert signature image ---
    page = doc[0]
    page_rect = page.rect
    margin_right = 80
    margin_bottom = 45
    signature_width = 100
    signature_height = 50
    
    signature_rect = fitz.Rect(
        page_rect.width - signature_width - margin_right,
        page_rect.height - signature_height - margin_bottom,
        page_rect.width - margin_right,
        page_rect.height - margin_bottom
    )
    
    # Use the signature bytes directly
    page.insert_image(signature_rect, stream=signature_bytes, keep_proportion=True)

    signed_name = file_name.replace(".pdf", "_signed.pdf")
    local_signed = f"/tmp/{signed_name}"
    local_flattened = f"/tmp/{signed_name.replace('.pdf', '_flat.pdf')}"

    # --- Save to /tmp/ ---
    doc.save(local_signed, incremental=False, deflate=True)
    doc.close()

    # --- Flatten form fields ---
    fillpdfs.flatten_pdf(local_signed, local_flattened)

    # --- Upload flattened version back to GCS ---
    signed_blob = BUCKET.blob(f"certificates/{file_name}")
    with open(local_flattened, "rb") as f:
        signed_blob.upload_from_file(f, content_type="application/pdf")


def analyze_for_coi_request(subject: str, content: str):
    prompt = f"""
    Your task is to analyze weather the email asks for certificate of insurance (is a COI request) or no.
    When they ask for a COI, they also specify certificate holder name and address, but sometimes they just write company name,
    not mentioning that it's the COI holder.
    Typically, but not always brockers and clients send a request, with texts such as:
      - please send insurance
      - coi needed
      - urgent, load on hold
      - ins cert request
      - proof of insurance needed
    
    Respond with ONLY a valid JSON object in this exact format:
    {{"is_likely_coi_request": true/false}}
    
    Be conservative - only return true if there are clear indicators of a COI request.
    
    Subject: {subject}
    Content: {content}
    """

    response = OPENAI_CLIENT.chat.completions.create(
        model="gpt-4o-mini",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "You are a commercial trucking insurance email monitoring bot."},
            {"role": "user", "content": prompt}
        ]
    ).choices[0].message.content

    try:
        rsp_json = json.loads(response)
        rsp_json['is_likely_coi_request']
    except Exception:
        # save the response to the db, document being timestamped subject
        doc_id = f"{datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')}_{subject.replace(' ', '_')}"
        db.collection("coi_generator_errors").document(doc_id).set({
            "subject": subject,
            "content": content,
            "llm_response": response,
            "timestamp": datetime.now().isoformat()
        })
        return {"is_likely_coi_request": False}
 
    return rsp_json


def infer_coi_request_info(subject: str, content: str, to_emails: list[str], cc_emails: list[str]):
    prompt = f"""
    You are given a raw Certificate of Insurance (COI) request email subject and body.
    Your task is to analyze the email, infer and extract the following information:
        1. Weather the insured client/company name for which the client or the broker asks for COI is mentioned in the email.
        2. The insured client/company name for which the client or the broker asks for COI.
        3. Weather the certificate holder information is mentioned, which consists of:
            a. The name of their company
            b. Address line 1 (just the street address)
            c. Address line 2 with the format: <city>, <state 2 letter code> <zip code>
        3. Infer the main email address that the COI needs to be sent to, if none is mentioned, leave it blank
    
    Respond with ONLY a valid JSON object in the exact format:
    {{
        "insured_inferred": true/false,
        "insured_name": string,
        "holder_inferred": true/false,
        "holder_name": string,
        "holder_addr_1": string,
        "holder_addr_2": string,
        "send_to_email": string
    }}

    Example:
    {{
        "insured_inferred": true,
        "insured_name": "RAPID TRUCKING INC",
        "holder_inferred": true,
        "holder_name": "Highway App, Inc.",
        "holder_addr_1": "5931 Greenville Ave, Unit #5620",
        "holder_addr_2": "Dallas, TX 75206",
        "send_to_email": "insurance@certs.highway.com"
    }}

    Set holder_inferred false if you can't infer it from the email.
    Here is the email you need to analyze:
    
    To emails: {", ".join(to_emails)}
    CC emails: {", ".join(cc_emails)}
    Subject: {subject}
    Content: {content}
    """
    
    '''
    response = GEMINI_CLIENT.models.generate_content( 
        model="gemini-2.5-flash-preview-05-20",
        contents=prompt 
    )
    '''
    response = OPENAI_CLIENT.chat.completions.create(
        model="gpt-4o-mini",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "You are a commercial trucking insurance COI handling bot."},
            {"role": "user", "content": prompt}
        ]
    ).choices[0].message.content
 
    try:
        rsp_json = json.loads(response)
        rsp_json['insured_inferred']
        rsp_json['holder_inferred']
    except Exception:
        doc_id = f"{datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')}_{subject.replace(' ', '_')}"
        db.collection("coi_generator_errors").document(doc_id).set({
            "subject": subject,
            "content": content,
            "llm_response": response,
            "timestamp": datetime.now().isoformat()
        })
        return {}

    return rsp_json


def get_gmail_credentials(user_email: str):
    """Load service-account JSON from Secret Manager and build delegated creds."""
    
    GMAIL_SECRET = os.environ.get("GMAIL_SECRET_NAME", "gmail-service-account")
    
    sm_client = secretmanager.SecretManagerServiceClient()
    name = f"projects/lionins/secrets/{GMAIL_SECRET}/versions/latest"
    secret = sm_client.access_secret_version(request={"name": name})
    sa_info = json.loads(secret.payload.data.decode("utf-8"))
    creds = service_account.Credentials.from_service_account_info(
        sa_info, scopes=GMAIL_SCOPES, subject=user_email
    )
    return creds


def send_coi(
    thread_id: str,
    to_email: str,
    subject_text: str,
    body_text: str,
    file_names: list[str]
):
    """
    Replies to an existing Gmail thread (thread_id) via Tony’s Workspace Gmail.
    """
    # TODO: testing
    to_email = "g.sargsyan1995@gmail.com"
    try:
        creds = get_gmail_credentials("tony@lioninsurance.us")
        service = build("gmail", "v1", credentials=creds)

        # TODO: TO reply to the thread
        '''
        msg = EmailMessage()
        msg["To"] = to_email
        msg["From"] = "tony@lioninsurance.us"
        msg["Subject"] = f"Re: {subject_text}"
        msg["In-Reply-To"] = original_message_id
        msg["References"] = original_message_id
        msg.set_content(body_text)

        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        message = {
            "raw": raw,
            "threadId": thread_id,  # attach to the conversation
        }

        service.users().messages().send(userId="me", body=message).execute()
        '''

        # Build a NEW email
        msg = EmailMessage()
        msg["To"] = to_email
        msg["From"] = "tony@lioninsurance.us"
        msg["Subject"] = subject_text
        msg.set_content(body_text)

        # --- Attach PDFs from GCS ---
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)

        for file_name in file_names:
            blob_path = f"certificates/{file_name}"
            blob = bucket.blob(blob_path)

            # Skip if not found
            if not blob.exists():
                print(f"⚠️ File not found in GCS: {blob_path}")
                continue

            file_bytes = blob.download_as_bytes()
            maintype, subtype = mimetypes.guess_type(file_name)[0].split("/", 1) if mimetypes.guess_type(file_name)[0] else ("application", "octet-stream")

            msg.add_attachment(
                file_bytes,
                maintype=maintype,
                subtype=subtype,
                filename=file_name,
            )
            print(f"Attached {file_name}")

        # --- Encode + Send ---
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        message = {"raw": raw}

        sent = (
            service.users()
            .messages()
            .send(userId="me", body=message)
            .execute()
        )

        print(f"Gmail API: sent new message ID {sent.get('id')}")
        return True

    except HttpError as e:
        print(f"Gmail API error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False


@app.route("/", methods=["POST"])
def coi_generator(request):
    data = request.get_json(force=True)
    action = data.get("action")

    if action == "analyze_for_coi_request":
        subject = data.get("subject")
        content = data.get("body_text")
        to_emails = data.get("to_emails")
        cc_emails = data.get("cc_emails")
        analysis = analyze_for_coi_request(subject, content)

        if analysis['is_likely_coi_request']:
            inferred_data = infer_coi_request_info(subject, content, to_emails, cc_emails)

            return {
                'is_likely_coi_request': True,
                'insured_inferred': inferred_data['insured_inferred'],
                'insured_name': inferred_data['insured_name'],
                'holder_inferred': inferred_data['holder_inferred'],
                'holder_name': inferred_data['holder_name'],
                'holder_addr_1': inferred_data['holder_addr_1'],
                'holder_addr_2': inferred_data['holder_addr_2'],
                'send_to_email': inferred_data['send_to_email']
            }
        else:
            return {
                'is_likely_coi_request': False,
                'insured_inferred': False,
                'insured_name': '',
                'holder_inferred': False,
                'holder_name': '',
                'holder_addr_1': '',
                'holder_addr_2': '',
                'send_to_email': ''
            }
    elif action == "generate_coi":
        insured_inferred = data.get("insured_inferred")
        insured_name = data.get("insured_name")
        holder_name = data.get("holder_name")
        holder_addr_1 = data.get("holder_addr_1")
        holder_addr_2 = data.get("holder_addr_2")
        send_to_email = data.get("send_to_email")
        thread_id = data.get("thread_id")
        subject_text = data.get("subject_text")
        body_text = data.get("body_text")

        file_names = download_from_drive(insured_name)
        coi_holder = (holder_name, holder_addr_1, holder_addr_2)
        main_file = next((f for f in file_names if "additional" not in f.lower()), None)
        fill_pdf(main_file, main_file, coi_holder)
        add_signature_and_flatten(main_file)

        send_coi(
            thread_id=thread_id,
            to_email=send_to_email,
            subject_text=f"Re: {subject_text}",
            body_text=body_text,
            file_names=file_names
        )
        return ("", 204)
    
    return ("", 400)