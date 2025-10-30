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

    start = time.time()
    response = OPENAI_CLIENT.chat.completions.create(
        model="gpt-4o-mini",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "You are a commercial trucking insurance email monitoring bot."},
            {"role": "user", "content": prompt}
        ]
    ).choices[0].message.content
    print(f"[TIMING] OpenAI analyze_for_coi_request: {time.time() - start:.2f}s")

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
    start = time.time()
    response = OPENAI_CLIENT.chat.completions.create(
        model="gpt-5-mini",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "You are a commercial trucking insurance agency COI handling bot."},
            {"role": "user", "content": prompt}
        ]
    ).choices[0].message.content
    print(f"[TIMING] OpenAI infer_coi_request_info: {time.time() - start:.2f}s")
 
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
    to_emails: list[str],
    cc_emails: list[str],
    subject_text: str,
    body_text: str,
    file_names: list[str],
    last_message_id: str | None = None,
):
    """
    Replies to an existing Gmail thread (thread_id) via Tony’s Workspace Gmail using
    the no-reply alias. Attaches provided PDFs. Honors To/CC lists.
    """
    try:
        creds = get_gmail_credentials("tony@lioninsurance.us")
        service = build("gmail", "v1", credentials=creds)

        # PRODUCTION (reply inline) — commented out for testing
        # msg = EmailMessage()
        # if to_emails:
        #     msg["To"] = ", ".join(to_emails)
        # if cc_emails:
        #     msg["Cc"] = ", ".join(cc_emails)
        # msg["From"] = "no-reply@lioninsurance.us"
        # msg["Subject"] = subject_text
        # if last_message_id:
        #     msg["In-Reply-To"] = last_message_id
        #     msg["References"] = last_message_id
        # msg.set_content(body_text)

        # TESTING: send to Grig from Tony, include intended To/CC in body
        display_to = ", ".join(to_emails or [])
        display_cc = ", ".join(cc_emails or [])
        testing_body = (
            f"{body_text}\n\n---\n[TEST INFO]\n"
            f"Would send To: {display_to or '(none)'}\n"
            f"Would CC: {display_cc or '(none)'}\n"
            f"Thread: {thread_id}\n"
        )
        msg = EmailMessage()
        msg["To"] = "g.sargsyan1995@gmail.com"
        msg["From"] = "tony@lioninsurance.us"
        msg["Subject"] = subject_text
        msg.set_content(testing_body)

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
        # For testing, do NOT attach to thread; send as a new message
        message = {"raw": raw}

        start = time.time()
        sent = (
            service.users()
            .messages()
            .send(userId="me", body=message)
            .execute()
        )
        print(f"[TIMING] Gmail messages.send: {time.time() - start:.2f}s")

        print(f"Gmail API: sent message ID {sent.get('id')} in thread {thread_id}")
        log_step(
            "email_sent",
            thread_id=thread_id,
            data={
                "to": to_emails,
                "cc": cc_emails,
                "message_id": sent.get('id'),
                "files": file_names,
            },
        )
        return True
    except Exception as e:
        print(f"Unexpected error: {e}")
        log_step("email_send_failed", status="error", thread_id=thread_id, data={"to": to_emails, "cc": cc_emails}, error=str(e))
        return False


def normalize_recipient_list(items):
    res = []
    for x in items or []:
        if not x:
            continue
        # strip display names if present
        addr = x.strip()
        if '<' in addr and '>' in addr:
            addr = addr[addr.find('<')+1:addr.find('>')]
        addr = addr.strip()
        if addr:
            res.append(addr)
    # dedupe preserving order
    seen = set()
    out = []
    for a in res:
        if a.lower() not in seen:
            seen.add(a.lower())
            out.append(a)
    return out


@app.route("/", methods=["POST"])
def coi_generator(request):
    request_start = time.time()
    data = request.get_json(force=True)
    action = data.get("action")

    if action == "analyze_for_coi_request":
        subject = data.get("subject")
        content = data.get("body_text")
        to_emails = data.get("to_emails")
        cc_emails = data.get("cc_emails")
        thread_id = data.get("thread_id")
        log_step("llm_analysis_started", thread_id=thread_id, data={"subject": subject})
        analysis = analyze_for_coi_request(subject, content)
        log_step("llm_analysis_finished", thread_id=thread_id, data=analysis)

        if analysis['is_likely_coi_request']:
            log_step("llm_infer_started", thread_id=thread_id)
            inferred_data = infer_coi_request_info(subject, content, to_emails, cc_emails)
            log_step("llm_infer_finished", thread_id=thread_id, data=inferred_data)

            print(f"[TIMING] Total analyze_for_coi_request: {time.time() - request_start:.2f}s")
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
            print(f"[TIMING] Total analyze_for_coi_request: {time.time() - request_start:.2f}s")
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
        to_emails_incoming = data.get("to_emails", [])
        cc_emails_incoming = data.get("cc_emails", [])
        last_message_id = data.get("last_message_id")
        thread_id = data.get("thread_id")
        subject_text = data.get("subject_text")
        body_text = data.get("body_text")

        log_step("download_from_drive_started", thread_id=thread_id, data={"insured_name": insured_name})
        file_names = download_from_drive(insured_name)
        log_step("download_from_drive_finished", thread_id=thread_id, data={"files": file_names})
        coi_holder = (holder_name, holder_addr_1, holder_addr_2)
        main_file = next((f for f in file_names if "additional" not in f.lower()), None)
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

        orig_to = normalize_recipient_list(to_emails_incoming)
        orig_cc = normalize_recipient_list(cc_emails_incoming)
        inferred = (send_to_email or '').strip()

        if inferred:
            final_to = [inferred]
            # everyone else to CC, excluding inferred and excluding from address
            others = [e for e in orig_to + orig_cc if e.lower() != inferred.lower()]
            final_cc = []
            seen = set([inferred.lower()])
            for e in others:
                el = e.lower()
                if el not in seen:
                    seen.add(el)
                    final_cc.append(e)
        else:
            final_to = orig_to
            final_cc = [e for e in orig_cc if e.lower() not in {x.lower() for x in orig_to}]

        send_coi(
            thread_id=thread_id,
            to_emails=final_to,
            cc_emails=final_cc,
            subject_text=subject_text,
            body_text=body_text,
            file_names=file_names,
            last_message_id=last_message_id,
        )
        print(f"[TIMING] Total generate_coi: {time.time() - request_start:.2f}s")
        return ("", 204)
    
    return ("", 400)