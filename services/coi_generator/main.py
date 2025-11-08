""" Cloud Run service to analyze emails for COI requests, generate and send COIs. """
import base64
import io
import json
import mimetypes
import os
import time
from datetime import date, datetime

from tempfile import NamedTemporaryFile
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
from email.message import EmailMessage
from openai import OpenAI


from clients import INSURED_COMPANY_NAMES

app = Flask(__name__)

db = firestore.Client(database='lion-ins')
# Initialize OpenAI client
OPENAI_SECRET = os.environ.get("SECRET_NAME", "openai-key")
GMAIL_SECRET = os.environ.get("SECRET_NAME", "gmail-service-account")
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send", "https://www.googleapis.com/auth/gmail.compose"]

# Define the scope to access to search and download files
DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
DRIVE_FOLDER_ID = "1KIeq3LHWWklQBanmADUz6XVYodlF2id6"  # Drive folder
BUCKET_NAME = "lion-insurance"  # Cloud Storage bucket name
storage_client = storage.Client()
BUCKET = storage_client.bucket(BUCKET_NAME)

# Centralized Firestore step logger for observability across the flow
def log_step(step: str, status: str = "ok", thread_id: str | None = None, data: dict | None = None, error: str | None = None):
    try:
        payload = {
            "step": step,
            "status": status,
            "thread_id": thread_id,
            "service": "coi_generator",
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
                "service_seen": firestore.ArrayUnion(["coi_generator"]),
            }, merge=True)
            doc_ref.collection("events").document(step).set(payload, merge=True)
        else:
            # fallback without thread grouping
            db.collection("coi_flow_logs").add(payload)
    except Exception as e:
        print(f"[OBS-ERR] Failed to log step '{step}': {e}")

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
    start = time.time()
    results = drive_service.files().list(
        q=query,
        pageSize=10,
        fields="nextPageToken, files(id, name)",
    ).execute()
    print(f"[TIMING] Drive files.list: {time.time() - start:.2f}s")

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
        start = time.time()
        while not done:
            status, done = downloader.next_chunk()
            if status:
                print(f"Download {int(status.progress() * 100)}%")
        print(f"[TIMING] Drive files.get_media ({name}): {time.time() - start:.2f}s")

        fh.seek(0)

        blob = BUCKET.blob(f"certificates/{name}")
        start = time.time()
        blob.upload_from_file(fh, content_type="application/pdf")
        print(f"[TIMING] GCS upload ({name}): {time.time() - start:.2f}s")
        print(f"Uploaded to gs://{BUCKET_NAME}/certificates/{name}")
        res.append(name)

    return res


def fill_coi(pdf_path: str, coi_holder: tuple[str, str, str]):
    """
    Download PDF from GCS at certificates/{...}, fill COI fields, and upload back.
    pdf_path is the blob path inside the bucket (e.g., 'certificates/file.pdf').
    """
    blob = BUCKET.blob(pdf_path)
    if not blob.exists():
        print(f"⚠️ PDF not found in GCS: {pdf_path}")
        return

    try:
        # Download to temp file
        with NamedTemporaryFile(suffix=",.pdf", delete=False) as tmp_in:
            blob.download_to_filename(tmp_in.name)
            input_path = tmp_in.name

        # Fill fields
        reader = PdfReader(input_path)
        writer = PdfWriter()
        writer.append(reader)

        field_values = {
            "Form_CompletionDate_A": f'        {date.today().strftime("%m/%d/%Y")}',
            "CertificateHolder_FullName_A": coi_holder[0],
            "CertificateHolder_MailingAddress_LineOne_A": coi_holder[1],
            "CertificateHolder_MailingAddress_LineTwo_A": coi_holder[2],
        }

        writer.update_page_form_field_values(
            writer.pages[0], field_values, auto_regenerate=False
        )

        with NamedTemporaryFile(suffix=",.pdf", delete=False) as tmp_out:
            writer.write(tmp_out)
            output_path = tmp_out.name

        # Upload back to same blob
        with open(output_path, "rb") as f:
            blob.upload_from_file(f, content_type="application/pdf")

    except Exception as e:
        print(f"❌ fill_coi failed for {pdf_path}: {e}")


def fill_pdf_old(input_filename: str, output_filename: str, coi_holder: tuple[str, str, str]):
    """
    Reads a PDF from GCS, fills form fields, and writes back to GCS.
    """
    input_blob = BUCKET.blob(f"certificates/{input_filename}")
    pdf_bytes = input_blob.download_as_bytes()

    reader = PdfReader(io.BytesIO(pdf_bytes))
    writer = PdfWriter()
    writer.append(reader)

    field_values = {
        "Form_CompletionDate_A": f'        {date.today().strftime("%m/%d/%Y")}',
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


def add_signature_and_flatten(pdf_path: str):
    """
    Add signature to first page and flatten PDF. Operates on GCS blob path provided.
    pdf_path is the blob path inside the bucket (e.g., 'certificates/file.pdf').
    """
    source_blob = BUCKET.blob(pdf_path)
    if not source_blob.exists():
        print(f"⚠️ Source PDF not found in GCS: {pdf_path}")
        return

    signature_blob = BUCKET.blob("signature.png")
    signature_bytes = None
    if signature_blob.exists():
        signature_bytes = signature_blob.download_as_bytes()
    else:
        print("⚠️ Signature image not found at certificates/signature.png — proceeding without signature")

    try:
        # Download bytes and open with fitz
        pdf_bytes = source_blob.download_as_bytes()
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            if signature_bytes and len(doc) > 0:
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
                    page_rect.height - margin_bottom,
                )
                page.insert_image(signature_rect, stream=signature_bytes, keep_proportion=True)

            # Save with signature to temp
            with NamedTemporaryFile(suffix=",.pdf", delete=False) as tmp_signed:
                doc.save(tmp_signed.name, incremental=False, deflate=True)
                intermediate_path = tmp_signed.name

        # Flatten by rasterizing each page
        with fitz.open(intermediate_path) as signed_doc, fitz.open() as flattened:
            mat = fitz.Matrix(3.0, 3.0)
            for p in range(len(signed_doc)):
                page = signed_doc[p]
                pix = page.get_pixmap(matrix=mat)
                rect = page.rect
                out_page = flattened.new_page(width=rect.width, height=rect.height)
                out_page.insert_image(rect, pixmap=pix)

            with NamedTemporaryFile(suffix=",.pdf", delete=False) as tmp_flat:
                flattened.save(tmp_flat.name, incremental=False, deflate=True)
                upload_path = tmp_flat.name

        with open(upload_path, "rb") as f:
            source_blob.upload_from_file(f, content_type="application/pdf")

    except Exception as e:
        print(f"❌ add_signature_and_flatten failed for {pdf_path}: {e}")


def is_coi_request(subject: str, content: str, from_email: str):
    prompt = f"""
    Your task is to analyze weather the email asks for certificate of insurance (is a COI request) or no.
    When they ask for a COI, they also specify certificate holder name and address, but sometimes they just write company name,
    not mentioning that it's the COI holder.
    A COI request is an email that explicitly or implicitly asks for an insurance certificate.
    Common examples:
    - "please send insurance"
    - "need COI"
    - "load on hold until we receive insurance"
    - "proof of insurance needed"
    - "certificate holder is..."

    Typically but not always brokers are the ones asking for a COI, or our client asks for it giving broker details.
    
    Respond with ONLY a valid JSON object in this exact format:
    {{"is_likely_coi_request": true/false}}
    
    Be conservative - only return true if there are clear indicators of a COI request and
    there is clear indication of the certificate holder, name and address.

    Cases to return false
    1. If the sender (From email) is coi@lioninsurance.us and the body hints that a COI was already made by us and is being sent
    2. If the exact name "Certificial" appears in the body or subject (company that sends COI requests, but we don't work with them)
    
    From email: {from_email}
    Subject: {subject}
    Content: {content}
    """

    start = time.time()
    response = OPENAI_CLIENT.chat.completions.create(
        model="gpt-4o-mini",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "You are a commercial trucking insurance agency's email monitoring bot. The name of our company is Lion Insurance Services."},
            {"role": "user", "content": prompt}
        ]
    ).choices[0].message.content
    print(f"[TIMING] OpenAI is_coi_request: {time.time() - start:.2f}s")

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


def infer_coi_request_info(subject: str, content: str, to_emails: list[str], cc_emails: list[str], from_email: str):
    prompt = f"""
    You are monitoring the email address tony@lioninsurance.us for COI requests, the results you return will be used to automatically send the COI to the appropriate email addresses.
    You are given an incoming email, which was marked as a 'likely' COI request.
    Your task is to analyze whether the email indeed asks for certificate of insurance, i.e. is a COI request, and extract the necessary information.
    When they ask for a COI, they also specify certificate holder name and address, but sometimes they just write holder's name and address,
    not mentioning that it's the COI holder.
    Your task is to analyze the email, infer and extract the following information:
        1. "insured_name": The insured client/company name for which the client or the broker asks for COI.
        2. "holder_name": The certificate holder information, which consists of:
            a. "holder_name": The name of their company.
            b. "holder_addr_1": Address line 1 (just the street address).
            c. "holder_addr_2": Address line 2 with the format: <city>, <state 2 letter code> <zip code>.
        3. "to_emails": Try to infer the main email address(es) that the COI needs to be sent to.
           This may be in the body, To's or CC's. If there is only client's email, then needs to be sent to the client, otherwise it's the broker or other agency that needs to be sent to.
           This cannot be one of our email addresses: (coi@lioninsurance.us, tony@lioninsurance.us, etc..). Leave it empty if you can't infer it.
           Holder name might give a clue on who to send the COI to. If you can't infer, then it is "Original From Email".
        4. "cc_emails": Try to infer the email addresses that should be CC'd. Usually CC's are the following:
           The email you are monitoring is tony@lioninsurance.us, so you don't need to CC him,
           but if other lioninsurance.us emails are in the CC's or To's, include them so when we automatically send the COI, they are also notified.
           If there is client's email present in the "Original To Emails" or "Original CC Emails", always include the client's email in the CC's.
           So they know we are sending the COI to the requested email addresses.
    
    Respond with ONLY a valid JSON object, with the following keys.
    If the email is not a COI request, leave the values empty strings.
    If the email is coming from coi@lioninsurance.us, tony@lioninsurance.us, etc.., leave the values empty strings. It's not a COI request.

    Respond in this exact format:
    {{
        "insured_name": string,
        "holder_name": string,
        "holder_addr_1": string,
        "holder_addr_2": string,
        "to_emails_inferred": list[string],
        "cc_emails_inferred": list[string]
    }}

    Example:
    {{
        "insured_name": "PUMPKEN TRUCKING LLC",
        "holder_name": "Highway App, Inc.",
        "holder_addr_1": "5931 Greenville Ave, Unit #5620",
        "holder_addr_2": "Dallas, TX 75206",
        "to_emails_inferred": ["insurance@certs.highway.com"],
        "cc_emails_inferred": ["pumpken_trucking@yahoo.com"]
    }}

    Here is the list of all our client names (insured companies), so you don't mix up holder and insured names.
    {INSURED_COMPANY_NAMES}

    And finally this is the email you need to analyze:

    Original To Emails: {", ".join(to_emails)}
    Original CC Emails: {", ".join(cc_emails)}
    Original From Email: {from_email}
    Subject: {subject}
    Content: {content}
    """

    start = time.time()
    response = OPENAI_CLIENT.chat.completions.create(
        model="gpt-5-mini",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "You are a commercial trucking insurance agency's email monitoring bot. The name of our company is Lion Insurance Services."},
            {"role": "user", "content": prompt}
        ]
    ).choices[0].message.content
    print(f"[TIMING] OpenAI infer_coi_request_info: {time.time() - start:.2f}s")
 
    try:
        rsp_json = json.loads(response)
        rsp_json['insured_name']
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


def create_draft_coi_reply(
    thread_id: str,
    to_emails: list[str],
    cc_emails: list[str],
    subject_text: str,
    body_text: str,
    file_names: list[str],
    last_message_id: str | None = None,
):
    """
    Creates a draft reply email in an existing Gmail thread (thread_id) via Tony's Workspace Gmail using
    the no-reply alias. Attaches provided PDFs. Honors To/CC lists. Does NOT send the email.
    """
    try:
        creds = get_gmail_credentials("tony@lioninsurance.us")
        service = build("gmail", "v1", credentials=creds)

        msg = EmailMessage()
        if to_emails:
            msg["To"] = ", ".join(to_emails)
        if cc_emails:
            msg["Cc"] = ", ".join(cc_emails)
        # Send from no-reply alias with display name
        msg["From"] = "Tony Lion Insurance <tony@lioninsurance.us>"
        msg["Subject"] = subject_text
        if last_message_id:
            msg["In-Reply-To"] = last_message_id
            msg["References"] = last_message_id
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

            start = time.time()
            file_bytes = blob.download_as_bytes()
            type_guess = mimetypes.guess_type(file_name)[0]
            if type_guess:
                maintype, subtype = type_guess.split("/", 1)
            else:
                maintype, subtype = "application", "octet-stream"

            msg.add_attachment(
                file_bytes,
                maintype=maintype,
                subtype=subtype,
                filename=file_name,
            )
            print(f"Attached {file_name} to draft")

        # --- Encode + Create Draft ---
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        draft_message = {"message": {"raw": raw, "threadId": thread_id}} if thread_id else {"message": {"raw": raw}}

        start = time.time()
        draft = (
            service.users()
            .drafts()
            .create(userId="me", body=draft_message)
            .execute()
        )
        print(f"[TIMING] Gmail drafts.create: {time.time() - start:.2f}s")

        print(f"Gmail API: created draft ID {draft.get('id')} in thread {thread_id}")
        log_step(
            "draft_created",
            thread_id=thread_id,
            data={
                "to": to_emails,
                "cc": cc_emails,
                "draft_id": draft.get('id'),
                "files": file_names,
            },
        )
        return True
    except Exception as e:
        print(f"Unexpected error creating draft: {e}")
        log_step("draft_create_failed", status="error", thread_id=thread_id, data={"to": to_emails, "cc": cc_emails}, error=str(e))
        return False


def generate_coi_files(
    insured_name: str,
    holder_name: str,
    holder_addr_1: str,
    holder_addr_2: str,
    thread_id: str,
):
    """
    Generate COI files by downloading from Drive, filling, signing, and flattening.
    Returns list of generated file names.
    """
    log_step("download_from_drive_started", thread_id=thread_id, data={"insured_name": insured_name})
    file_names = download_from_drive(insured_name)
    log_step("download_from_drive_finished", thread_id=thread_id, data={"files": file_names})
    
    if not file_names:
        log_step("no_files_found", status="error", thread_id=thread_id, data={"insured_name": insured_name})
        return []
    
    coi_holder = (holder_name, holder_addr_1, holder_addr_2)
    main_file = next((f for f in file_names if "additional" not in f.lower()), None)
    
    if main_file:
        log_step("coi_fill_started", thread_id=thread_id, data={"main_file": main_file, "holder": coi_holder})
        try:
            fill_coi(f'certificates/{main_file}', coi_holder)
            log_step("coi_fill_finished", thread_id=thread_id, data={"main_file": main_file})
        except Exception as e:
            log_step("coi_fill_failed", status="error", thread_id=thread_id, data={"main_file": main_file}, error=str(e))

        log_step("signature_flatten_started", thread_id=thread_id, data={"file": main_file})
        add_signature_and_flatten(f'certificates/{main_file}')
        log_step("signature_flatten_finished", thread_id=thread_id, data={"file": main_file})

        # Also sign any additional file(s) if present
        additional_files = [f for f in file_names if "additional" in f.lower()]
        for add_file in additional_files:
            log_step("additional_signature_flatten_started", thread_id=thread_id, data={"file": add_file})
            try:
                add_signature_and_flatten(f'certificates/{add_file}')
                log_step("additional_signature_flatten_finished", thread_id=thread_id, data={"file": add_file})
            except Exception as e:
                log_step("additional_signature_flatten_failed", status="error", thread_id=thread_id, data={"file": add_file}, error=str(e))
    
    return file_names


def analyze_for_coi_request(data: dict):
    """ Analyze email for COI request and generate COI files if necessary.

    """
    subject = data.get("subject")
    content = data.get("body_text")
    original_to_emails = data.get("to_emails")
    original_cc_emails = data.get("cc_emails")
    from_email = data.get("from_email")
    thread_id = data.get("thread_id")
    log_step("is_coi_request_started", thread_id=thread_id, data={"subject": subject})
    analysis = is_coi_request(subject, content, from_email)
    log_step("is_coi_request_finished", thread_id=thread_id, data=analysis)

    if analysis['is_likely_coi_request']:
        log_step("infer_coi_request_info_started", thread_id=thread_id)
        inferred_data = infer_coi_request_info(subject, content, original_to_emails, original_cc_emails, from_email)
        log_step("infer_coi_request_info_finished", thread_id=thread_id, data=inferred_data)

        # If COI is detected and we have the necessary information, generate COI and create draft
        if inferred_data.get('insured_name') and \
            inferred_data.get('holder_name') and \
            inferred_data.get('holder_addr_1') and \
            inferred_data.get('holder_addr_2'):
            
            last_message_id = data.get("last_message_id")
            
            log_step("generate_coi_files_started", thread_id=thread_id, data={
                "insured_name": inferred_data['insured_name'],
                "holder_name": inferred_data['holder_name']
            })

            # Generate COI files
            file_names = generate_coi_files(
                insured_name=inferred_data['insured_name'],
                holder_name=inferred_data['holder_name'],
                holder_addr_1=inferred_data['holder_addr_1'],
                holder_addr_2=inferred_data['holder_addr_2'],
                thread_id=thread_id
            )

            if file_names:
                final_to_emails = inferred_data.get('to_emails_inferred', [])
                final_cc_emails = inferred_data.get('cc_emails_inferred', [])

                # Create draft reply email
                subject_text = f"Re: {subject}"
                body_text = "Hello,\nPlease see the COI attached."
                
                create_draft_coi_reply(
                    thread_id=thread_id,
                    to_emails=final_to_emails,
                    cc_emails=final_cc_emails,
                    subject_text=subject_text,
                    body_text=body_text,
                    file_names=file_names,
                    last_message_id=last_message_id,
                )
                
                # Log all COI generation data to Firestore for monitoring
                try:
                    doc_ref = db.collection("coi_generations").document(f"thread_{thread_id}")
                    doc_ref.set({
                        "thread_id": thread_id,
                        "last_message_id": last_message_id,
                        "timestamp": datetime.utcnow().isoformat(),
                        "insured_name_inferred": inferred_data.get('insured_name'),
                        "holder_name_inferred": inferred_data.get('holder_name'),
                        "holder_addr_1_inferred": inferred_data.get('holder_addr_1'),
                        "holder_addr_2_inferred": inferred_data.get('holder_addr_2'),
                        "to_emails_inferred": inferred_data.get('to_emails_inferred'),
                        "cc_emails_inferred": inferred_data.get('cc_emails_inferred'),
                        "from_email": from_email,
                        "subject": subject,
                        "body_text": content,
                        "original_to_emails": original_to_emails,
                        "original_cc_emails": original_cc_emails,
                    }, merge=False)
                    print(f"[INFO] Logged COI generation data to Firestore for thread {thread_id}")
                except Exception as e:
                    print(f"[ERR] Failed to log COI generation data to Firestore: {e}")
                    log_step("firestore_log_failed", status="error", thread_id=thread_id, error=str(e))
                
                log_step("generate_coi_files_completed", thread_id=thread_id, data={"files": file_names})
            else:
                log_step("generate_coi_files_failed", status="error", thread_id=thread_id, 
                        data={"insured_name": inferred_data['insured_name']}, 
                        error="No files found or generated")
        else:
            log_step("generate_coi_files_skipped", thread_id=thread_id,
                    data={"reason": "Missing required information", "inferred_data": inferred_data})


@app.route("/", methods=["POST"])
def coi_generator(request):
    start_time = time.time()
    data = request.get_json(force=True)
    action = data.get("action")

    if action == "analyze_for_coi_request":
        analyze_for_coi_request(data)
        print(f"[TIMING] Total analyze_for_coi_request: {time.time() - start_time:.2f}s")

    return ("", 200)
