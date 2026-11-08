import os
import sys
import json
import base64
import difflib
import re
import time
import logging
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
from googleapiclient.http import MediaIoBaseDownload
from openai import OpenAI
import PyPDF2
from pypdf import PdfReader
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# --- Configuration ---
KEYS_DIR = os.path.join(os.path.dirname(__file__), "..", "keys")
LOG_FILE = os.path.join(os.path.dirname(__file__), "..", "declarations.log")
DOWNLOADS_DIR = os.path.join(os.path.dirname(__file__), "..", "downloads")

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
RENEWALS_DOC_ID = "1qPO_OurfEGjEE3OZxV3iFZqADM2oeQrHif_XWmS56YQ"
DRIVERS_FOLDER_ID = "1NLt-FqU-wvr8VU3fDbiRSK2dcz8DakPl"
DRIVE_CERTS_FOLDER_ID = "1KIeq3LHWWklQBanmADUz6XVYodlF2id6"
SENDER_EMAIL = "tony@lioninsurance.us"

GAIG_CREDS_PATH = os.path.join(KEYS_DIR, "gaig_credentials.json")
PARAMOUNT_CREDS_PATH = os.path.join(KEYS_DIR, "paramount_credentials.json")
TRINITY_CREDS_PATH = os.path.join(KEYS_DIR, "trinity_credentials.json")

# Physical Damage is never a proper AcroForm field on this agency's ACORD 25s --
# it's always the first line of the free-text remarks block, e.g.
# "A    PHYSICAL DAMAGE    <policy#>    <eff> - <exp>". Mirrors the same regex
# used by the Django AMS (lion.clients.services.PD_REMARKS_RE).
PD_REMARKS_RE = re.compile(
    r'([A-F])\s+PHYSICAL DAMAGE\s+(.+?)\s+(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})',
    re.IGNORECASE,
)

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
    Extract all client/company names listed for that month regardless of whether they are written in ALL CAPS or mixed case.
    Do NOT include policy numbers, coverages (e.g. AL, APD, MTC), dates, or notes (e.g. 'cancelled').
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
                # GAIG emails its packets named by policy number, e.g.
                # "IMPF57941400 New Business Policy Issue 09_18_2026.pdf", and
                # GAIG only writes the agency's cargo line -- so that's MTC no
                # matter what the classifier guessed (it has labeled the same
                # file AL on one run and MTC on the next).
                if GAIG_FILENAME_RE.search(filename):
                    coverage_type = "MTC"
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


# ── Portal-only carrier detection (GAIG / Paramount / Trinity) ─────────────
#
# These 3 insurer groups never email declaration documents -- they each sit
# behind their own agent portal with no API, so the only way to get a dec
# page is to log in and download it (see PortalSessions below). Which of a
# client's coverage lines (if any) belongs to one of them is determined by
# the policy number format, not the carrier's legal name on the COI (that
# name varies a lot -- "Great American Insurance Group/Company/Co.", etc --
# while the TPA-assigned policy number format is consistent). Patterns
# confirmed against real COIs pulled from the certs Drive folder:
#   GAIG:      "IMP F123456 00"           (MTC line, insurer = Great American)
#   Trinity:   ends in "-LION"            (PD or GL line, never AL)
#   Paramount: starts with "AGPGA3-"      (AL line, insurer = Agricultural
#                                          Workers Mutual Auto Ins) -- only
#                                          confirmed on 2 real examples so
#                                          far; a policy that doesn't match
#                                          any of the 3 just falls through to
#                                          the normal email search above, so
#                                          a gap here is a missed optimization,
#                                          not a regression.
GAIG_FILENAME_RE = re.compile(r"\bIMP\s*F\s*\d{6}", re.I)


def classify_portal_group(policy_number: str) -> str | None:
    pn = (policy_number or "").upper()
    if re.search(r"IMP\s*F\d", pn):
        return "gaig"
    if "LION" in pn:
        return "trinity"
    if pn.startswith("AGPGA3"):
        return "paramount"
    return None


# ACORD 25 certs come in two builds. The classic AcroForm one names fields
# plainly ("NamedInsured_FullName_A"); the newer XFA-based one (certs issued
# from ~Sept 2026 on) wraps the same names in a form path:
# "F[0].P1[0].NamedInsured_FullName_A[0]". Re-key by the plain name so the
# lookups below work on both (mirrors lion.integrations.pdf in the AMS).
_ACORD_FIELD_PATH_RE = re.compile(r"^(?:.*\.)?([^.\[\]]+)(?:\[\d+\])?$")


def _normalize_acord_fields(fields: dict) -> dict:
    out = {}
    for k, v in (fields or {}).items():
        m = _ACORD_FIELD_PATH_RE.match(k)
        short = m.group(1) if m else k
        if short not in out or k == short:
            out[short] = v
    return out


def _clean_field(fields: dict, name: str) -> str:
    v = fields.get(name)
    if v is None:
        return ""
    raw = v.value if hasattr(v, "value") else v.get("/V")
    if raw is None:
        return ""
    if hasattr(raw, "get_object"):
        raw = raw.get_object()
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="ignore")
    return str(raw).strip()


def get_coi_policy_lines(drive_service, company_name: str):
    """Download *company_name*'s COI from the certs Drive folder (same folder
    the AMS backend syncs from) and return (insured_name, [{"coverage_type",
    "policy_number"}, ...]) for its AL/MTC/GL/PD lines. (None, []) if no COI
    is on file. insured_name is the exact "NamedInsured_FullName_A" value --
    what we search each portal with, since it's what the carrier has on file
    (more reliable than the Renewals-doc-extracted spelling)."""
    escaped = company_name.replace("'", "\\'")
    query = f"'{DRIVE_CERTS_FOLDER_ID}' in parents and name contains '{escaped}' and trashed = false"
    try:
        response = drive_service.files().list(q=query, pageSize=5, fields="files(id, name)").execute()
    except Exception:
        logging.exception(f"COI lookup failed for {company_name!r}")
        return None, []
    files = response.get("files", [])
    if not files:
        return None, []

    file_id = files[0]["id"]
    request = drive_service.files().get_media(fileId=file_id)
    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)

    reader = PdfReader(fh)
    fields = _normalize_acord_fields(reader.get_fields())

    insured_name = _clean_field(fields, "NamedInsured_FullName_A")

    lines = []
    al_policy = _clean_field(fields, "Policy_AutomobileLiability_PolicyNumberIdentifier_A")
    if al_policy:
        lines.append({"coverage_type": "AL", "policy_number": al_policy})

    mtc_desc = _clean_field(fields, "OtherPolicy_OtherPolicyDescription_A")
    mtc_policy = _clean_field(fields, "OtherPolicy_PolicyNumberIdentifier_A")
    if mtc_policy and "cargo" in mtc_desc.lower():
        lines.append({"coverage_type": "MTC", "policy_number": mtc_policy})

    gl_policy = _clean_field(fields, "Policy_GeneralLiability_PolicyNumberIdentifier_A")
    if gl_policy:
        lines.append({"coverage_type": "GL", "policy_number": gl_policy})

    remarks = _clean_field(fields, "CertificateOfLiabilityInsurance_ACORDForm_RemarkText_A")
    m = PD_REMARKS_RE.search(remarks)
    if m:
        lines.append({"coverage_type": "PD", "policy_number": m.group(2).strip()})

    return insured_name or company_name, lines


class PortalSessions:
    """Lazily logs into GAIG / Paramount / Trinity's agent portals, at most
    once each per script run (a full monthly batch can then look up many
    companies without repeating GAIG's ~90s email-OTP wait for every one).
    A portal that fails to log in is not retried for the rest of the run --
    every client that would have needed it just falls back to whatever the
    normal email search already found (possibly nothing), same as if the
    portal integration didn't exist."""

    # GAIG names the dec packet by transaction: "Insured - Renewal Policy
    # Issue" on a renewal (policy # suffix 01, 02, ...) but "Insured - New
    # Business Policy Issue" on a first term (suffix 00). The "Agent/Company -
    # ..." twin of each is the agent copy, not what the client gets.
    GAIG_DOC_NAME = re.compile(r"^\s*Insured - .*Policy Issue", re.I)
    PARAMOUNT_DOC_NAME = "AGW Domestic Policy Document"
    TRINITY_DOC_NAME = "Evidence of Insurance"

    def __init__(self, gmail_service):
        self.gmail_service = gmail_service
        self._pw = None
        self._browser = None
        self._pages = {}
        self._failed = set()

    def __enter__(self):
        self._pw = sync_playwright().start()
        # Uses the system-installed Chrome (channel="chrome") rather than
        # Playwright's own bundled Chromium download -- avoids needing a
        # ~150MB browser download on every machine this ever runs on.
        self._browser = self._pw.chromium.launch(channel="chrome", headless=True)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._browser:
            self._browser.close()
        if self._pw:
            self._pw.stop()

    def download(self, group: str, insured_name: str, policy_number: str = "") -> list[tuple[str, bytes]]:
        page = self._get_page(group)
        if not page:
            return []
        # One retry: portal pages sometimes load too slowly and time out (Oct
        # 2026 batch lost EVERGREEN's and US 1's decs that way). A "not found"
        # returns [] without raising, so it isn't retried.
        for attempt in (1, 2):
            try:
                if group == "gaig":
                    return self._download_gaig(page, insured_name, policy_number)
                if group == "paramount":
                    return self._download_paramount(page, insured_name)
                if group == "trinity":
                    return self._download_trinity(page, insured_name)
                return []
            except Exception:
                logging.exception(f"{group} portal download failed for {insured_name!r} (attempt {attempt})")
                if attempt == 1:
                    print(f"    -> {group.upper()} portal download failed for {insured_name}, retrying once...")
                else:
                    print(f"    -> {group.upper()} portal download failed for {insured_name}.")
        return []

    def _get_page(self, group):
        if group in self._pages:
            return self._pages[group]
        if group in self._failed:
            return None
        try:
            if group == "gaig":
                page = self._login_gaig()
            elif group == "paramount":
                page = self._login_paramount()
            elif group == "trinity":
                page = self._login_trinity()
            else:
                return None
        except Exception:
            logging.exception(f"{group} portal login failed")
            print(f"    -> Could not log into the {group.upper()} portal this run: skipping it for the rest of the batch.")
            self._failed.add(group)
            return None
        self._pages[group] = page
        return page

    # --- GAIG ---------------------------------------------------------

    def _find_gaig_verification_code(self, after_ts: float, timeout: int = 90) -> str | None:
        deadline = time.time() + timeout
        seen = set()
        while time.time() < deadline:
            resp = self.gmail_service.users().messages().list(
                userId="me", q='from:noreply@gaig.com subject:"One-time verification code" newer_than:1h', maxResults=5
            ).execute()
            for m in resp.get("messages", []):
                if m["id"] in seen:
                    continue
                msg = self.gmail_service.users().messages().get(userId="me", id=m["id"], format="full").execute()
                internal_ts = int(msg.get("internalDate", 0)) / 1000
                if internal_ts < after_ts - 15:
                    seen.add(m["id"])
                    continue
                body = extract_text(msg.get("payload", {})) or ""
                match = re.search(r"Enter a code instead:\s*(\d{6})", body)
                if match:
                    return match.group(1)
                seen.add(m["id"])
            time.sleep(3)
        return None

    def _login_gaig(self):
        with open(GAIG_CREDS_PATH) as f:
            creds = json.load(f)
        print("    [GAIG] Logging in...")
        page = self._browser.new_page(accept_downloads=True)
        # The portal embeds a Maze user-survey widget that pops up at random
        # as an <iframe id="maze-contextual-widget-host"> covering the page;
        # when it lands over a Download link Playwright refuses to click
        # through it and the download times out. Block it at the network.
        page.route(re.compile(r"^https?://[^/]*maze[^/]*/"), lambda route: route.abort())
        page.goto("https://login-registration.gaig.com/agent")
        page.get_by_placeholder("Enter in your username").fill(creds["username"])
        page.get_by_placeholder("Enter in your password").fill(creds["password"])
        page.get_by_role("button", name="Log In").click()

        page.wait_for_selector("text=Send me an email", timeout=20000)
        sent_at = time.time()
        page.get_by_role("button", name="Send me an email").click()
        page.get_by_text("Enter a verification code instead").click()
        page.wait_for_selector('input[name="credentials.passcode"]', timeout=10000)

        print("    [GAIG] Waiting for the email verification code (up to 90s)...")
        code = self._find_gaig_verification_code(sent_at)
        if not code:
            raise RuntimeError("GAIG: verification code email never arrived")

        page.fill('input[name="credentials.passcode"]', code)
        page.locator('input[type="submit"]').click()
        page.wait_for_url(re.compile(r"agentportal\.gaig\.com"), timeout=30000)
        print("    [GAIG] Logged in.")
        return page

    @staticmethod
    def _gaig_policy_key(policy_number: str) -> tuple[str, str] | None:
        """"IMP F406082 01" / "IMP F580840-00" -> ("F406082", "01")."""
        m = re.search(r"F\s*(\d{6})\D*(\d{2})", (policy_number or "").upper())
        return (f"F{m.group(1)}", m.group(2)) if m else None

    def _download_gaig(self, page, insured_name: str, policy_number: str = "") -> list[tuple[str, bytes]]:
        # Search by the policy's F-number when we have one (the portal matches
        # it, but not the full "IMPF40608201" form) -- it's unique, whereas the
        # insured name on the COI can differ from the portal's spelling. Then
        # pick the row for this exact term: a renewed client shows the EXPIRED
        # "00" row next to the INFORCE "01" one.
        key = self._gaig_policy_key(policy_number)
        attempts = ([(key[0], re.compile(rf"{key[0]}\s*-?\s*{key[1]}"))] if key else []) + [(insured_name, insured_name)]
        row = None
        for term, row_text in attempts:
            page.goto("https://agentportal.gaig.com/policies")
            page.get_by_placeholder("Search by Policy Number or Insured Name").fill(term)
            page.get_by_test_id("policies-filter-box-search-button").click()
            page.wait_for_timeout(2000)
            candidate = page.locator("tbody tr", has_text=row_text).first
            if candidate.count() > 0:
                row = candidate
                break
        if row is None:
            print(f"    [GAIG] No policy found for {insured_name!r} ({policy_number or 'no policy #'}).")
            return []
        row.get_by_role("link").first.click()
        page.wait_for_load_state("networkidle")

        page.get_by_text("Policy Documents", exact=True).click()

        # The documents table loads asynchronously. A fixed 2s wait wasn't
        # enough when the portal was slow (Oct 2026 batch: HAULIX's table was
        # still empty at 2s although its packet was there), so wait for the
        # row itself.
        doc_row = page.locator("tr", has_text=self.GAIG_DOC_NAME).first
        try:
            doc_row.wait_for(timeout=30000)
        except PlaywrightTimeoutError:
            pass
        if doc_row.count() == 0:
            names = [t.strip() for t in page.locator("tbody tr td:first-child").all_inner_texts()]
            print(f"    [GAIG] No 'Insured - ... Policy Issue' document for {insured_name!r}. Documents listed: {names}")
            return []

        # Belt and braces for the survey widget (see _login_gaig): drop any
        # instance that still made it into the DOM before clicking.
        page.evaluate("document.querySelectorAll('#maze-contextual-widget-host').forEach(e => e.remove())")

        os.makedirs(DOWNLOADS_DIR, exist_ok=True)
        with page.expect_download() as download_info:
            doc_row.get_by_role("button", name=re.compile("Download", re.I)).click()
        download = download_info.value
        tmp_path = os.path.join(DOWNLOADS_DIR, "_tmp_gaig.pdf")
        download.save_as(tmp_path)
        with open(tmp_path, "rb") as f:
            data = f.read()
        os.remove(tmp_path)

        print(f"    [GAIG] Downloaded declaration for {insured_name!r}.")
        return [(f"{insured_name} - GAIG Declaration.pdf", data)]

    # --- Paramount ------------------------------------------------------

    def _login_paramount(self):
        with open(PARAMOUNT_CREDS_PATH) as f:
            creds = json.load(f)
        print("    [Paramount] Logging in...")
        page = self._browser.new_page(accept_downloads=True)
        page.add_init_script(
            "window.__openedUrls = []; "
            "const o = window.open; "
            "window.open = function(url, ...args) { window.__openedUrls.push(url); return o.apply(window, [url, ...args]); };"
        )
        page.goto("https://paramount.onlinemga.com/amp/login?iss=https%3A%2F%2Fauth.onlinemga.com%2F")
        page.get_by_role("button", name="CONTINUE TO LOG IN").wait_for(timeout=20000)
        page.get_by_role("button", name="CONTINUE TO LOG IN").click()
        page.wait_for_selector("#username", timeout=20000)
        page.fill("#username", creds["username"])
        page.fill("#password", creds["password"])
        page.get_by_role("button", name="Continue").click()
        page.wait_for_url("**/coverages", timeout=20000)
        page.wait_for_timeout(1500)

        page.get_by_role("link", name="OMGA").click()
        page.wait_for_url("**/amp/list", timeout=20000)
        page.wait_for_timeout(1500)
        print("    [Paramount] Logged in.")
        return page

    def _download_paramount(self, page, insured_name: str) -> list[tuple[str, bytes]]:
        page.goto("https://paramount.onlinemga.com/amp/list")
        page.wait_for_timeout(1000)
        page.get_by_text("Advanced", exact=True).click()
        page.wait_for_timeout(1000)
        page.fill('input[name="company_name"]', insured_name)
        page.get_by_role("button", name="Search").click()
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(2000)

        rows = page.locator("tr.itemRow")
        if rows.count() == 0:
            print(f"    [Paramount] No policy found for {insured_name!r}.")
            return []
        # The search lists every application for the insured, quotes included,
        # and the first row isn't necessarily the policy (Oct 2026: EVERGREEN's
        # first row was an archived, expired quote with no documents). Use the
        # newest Bound one; application ids increase over time.
        bound_ids = []
        for i in range(rows.count()):
            row = rows.nth(i)
            if re.search(r"\bBound\b", row.inner_text()):
                row_id = row.get_attribute("data-id") or ""
                if row_id.isdigit():
                    bound_ids.append(int(row_id))
        if not bound_ids:
            print(f"    [Paramount] No bound policy found for {insured_name!r} (only quotes).")
            return []
        app_id = max(bound_ids)

        page.goto(f"https://paramount.onlinemga.com/amp/detail/view/{app_id}#doctab-1")
        page.wait_for_selector("tr.policyDocumentLink", timeout=20000)
        page.wait_for_timeout(1500)

        row = page.locator("tr.policyDocumentLink", has_text=self.PARAMOUNT_DOC_NAME)
        if row.count() == 0:
            print(f"    [Paramount] No '{self.PARAMOUNT_DOC_NAME}' document for {insured_name!r}.")
            return []

        page.evaluate("window.__openedUrls = []")
        row.first.locator("span.file").evaluate("e => e.click()")
        page.wait_for_timeout(2000)

        opened = page.evaluate("window.__openedUrls")
        if not opened:
            print(f"    [Paramount] Document click did not open a document URL for {insured_name!r}.")
            return []

        doc_url = "https://paramount.onlinemga.com" + opened[-1]
        resp = page.context.request.get(doc_url)
        if resp.status != 200:
            print(f"    [Paramount] Download failed (status {resp.status}) for {insured_name!r}.")
            return []

        print(f"    [Paramount] Downloaded declaration for {insured_name!r}.")
        return [(f"{insured_name} - Paramount Declaration.pdf", resp.body())]

    # --- Trinity ----------------------------------------------------------

    def _login_trinity(self):
        with open(TRINITY_CREDS_PATH) as f:
            creds = json.load(f)
        print("    [Trinity] Logging in...")
        page = self._browser.new_page(accept_downloads=True)
        page.goto("https://tru.tmsystems.ai/sign-in")
        page.wait_for_selector("#email", timeout=20000)
        page.fill("#email", creds["email"])
        page.keyboard.press("Enter")
        page.wait_for_selector("#password", timeout=20000)
        page.fill("#password", creds["password"])
        page.keyboard.press("Enter")
        page.wait_for_url("https://tru.tmsystems.ai/", timeout=20000)
        page.wait_for_timeout(1500)
        print("    [Trinity] Logged in.")
        return page

    def _download_trinity(self, page, insured_name: str) -> list[tuple[str, bytes]]:
        page.goto("https://tru.tmsystems.ai/bound-accounts")
        page.wait_for_timeout(1500)
        page.fill('input[placeholder="Search"]', insured_name)
        page.wait_for_timeout(2000)

        first_row = page.locator("tr[data-row-index]").first
        if first_row.count() == 0:
            print(f"    [Trinity] No submission found for {insured_name!r}.")
            return []
        first_row.locator("button").first.click()
        page.wait_for_url("**/submissions/bound/**", timeout=20000)
        page.wait_for_timeout(2000)

        page.get_by_text("Policy Docs", exact=True).first.click()
        page.wait_for_timeout(2000)

        section_headers = page.locator(".lob-specific-accordion__title")
        for i in range(section_headers.count()):
            section_headers.nth(i).click()
            page.wait_for_timeout(800)

        rows = page.locator("tr", has_text=self.TRINITY_DOC_NAME)
        n_rows = rows.count()
        if n_rows == 0:
            print(f"    [Trinity] No '{self.TRINITY_DOC_NAME}' document for {insured_name!r}.")
            return []

        os.makedirs(DOWNLOADS_DIR, exist_ok=True)
        results = []
        for i in range(n_rows):
            row = rows.nth(i)
            try:
                with page.expect_download(timeout=15000) as dl_info:
                    row.get_by_text("Download", exact=True).click()
                download = dl_info.value
                tmp_path = os.path.join(DOWNLOADS_DIR, f"_tmp_trinity_{i}.pdf")
                download.save_as(tmp_path)
                with open(tmp_path, "rb") as f:
                    data = f.read()
                os.remove(tmp_path)
                suffix = f" ({i + 1})" if n_rows > 1 else ""
                results.append((f"{insured_name} - Trinity Declaration{suffix}.pdf", data))
            except Exception:
                logging.exception(f"Trinity row download failed for {insured_name!r}")

        print(f"    [Trinity] Downloaded {len(results)} declaration(s) for {insured_name!r}.")
        return results


def _coverage_types_of(coverage_type) -> set:
    """A binder's coverage_type is usually a single string ("AL"), but the
    LLM classifier in search_and_download_binders correctly returns a list
    when one combined PDF covers multiple lines (e.g. a small carrier's
    single packet for AL+MTC+PD) -- normalize either shape to a set."""
    if isinstance(coverage_type, (list, tuple, set)):
        return set(coverage_type)
    return {coverage_type}


# What an emailed dec packet from each portal carrier is called, so an email
# hit can be trusted to cover that carrier's line. GAIG names packets by
# policy number ("IMPF57941400 New Business Policy Issue ...pdf"); Trinity's
# are the "Evidence of Insurance" exports from its TMS portal ("SUB-xxxxxxxx-
# Evidence-of-Insurance-APD.pdf"); Paramount's carry the AGPGA3- policy #.
_PORTAL_FILENAME_RES = {
    "gaig": GAIG_FILENAME_RE,
    "trinity": re.compile(r"evidence.of.insurance|-LION|SUB-[0-9A-F]{8}", re.I),
    "paramount": re.compile(r"AGPGA3", re.I),
}


def fetch_portal_only_documents(portals: PortalSessions, drive_service, company_name: str, email_found: dict) -> dict:
    """For *company_name*, look at its COI's coverage lines and try a portal
    download for any line that (a) looks like it belongs to GAIG/Paramount/
    Trinity by policy number, and (b) isn't already covered by an emailed
    file *from that carrier*. *email_found* is search_and_download_binders'
    result. A combined packet from some other carrier that the classifier
    says covers "AL, MTC, PD" can't contain GAIG's MTC dec, so only a file
    recognizably from the line's own carrier counts as covering it.
    Returns a dict in the same {filename: {coverage_type, data}} shape, to be
    merged into it."""
    insured_name, policy_lines = get_coi_policy_lines(drive_service, company_name)
    if not policy_lines:
        return {}

    found = {}
    for line in policy_lines:
        group = classify_portal_group(line["policy_number"])
        if not group:
            continue
        own_carrier_files = [
            fn for fn, v in email_found.items()
            if line["coverage_type"] in _coverage_types_of(v["coverage_type"]) and _PORTAL_FILENAME_RES[group].search(fn)
        ]
        if own_carrier_files:
            continue
        print(f"  -> {line['coverage_type']} policy {line['policy_number']!r} looks like {group.upper()} -- trying the portal...")
        for filename, data in portals.download(group, insured_name, line["policy_number"]):
            found[filename] = {"coverage_type": line["coverage_type"], "data": data}
    return found


# Order coverages so they read naturally in a sentence, and use the wording
# the agency/clients actually use -- "APD", not "PD", for Physical Damage
# (matches the Renewals doc's own "AL/APD/MTC" column and the carriers'
# policy-number naming, e.g. "-APD-").
_COVERAGE_ORDER = ["AL", "MTC", "PD", "GL"]
_COVERAGE_LABELS = {"AL": "AL", "MTC": "MTC", "PD": "APD", "GL": "GL"}


def _join_list(items: list[str]) -> str:
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    return ' '.join(items[:-1]) + ' and ' + items[-1]


def _coverage_phrase(coverage_types: set) -> str:
    # GL doesn't count toward the "all 3 core coverages found" check, and
    # isn't named either way -- only AL/MTC/PD are meaningful for this
    # sentence. When all 3 are present, naming them is redundant (every
    # client normally gets all 3), so it falls back to the plain original
    # wording; naming them is only useful when the set is partial.
    relevant = {c for c in coverage_types if c != 'GL'}
    if {'AL', 'MTC', 'PD'} <= relevant:
        return "policies"

    labels = [_COVERAGE_LABELS.get(c, c) for c in _COVERAGE_ORDER if c in relevant]
    labels += sorted(_COVERAGE_LABELS.get(c, c) for c in relevant if c not in _COVERAGE_ORDER)
    if not labels:
        return ""
    word = 'policy' if len(labels) == 1 else 'policies'
    return f"{_join_list(labels)} {word}"


def create_draft_email(gmail_service, to_email, company_name, attachments, drivers):
    message = MIMEMultipart()
    if to_email:
        message['to'] = to_email
    message['from'] = SENDER_EMAIL
    message['subject'] = f"Declaration Documents: {company_name.upper()}"

    coverage_types = set()
    for info in attachments.values():
        coverage_types |= _coverage_types_of(info['coverage_type'])
    coverage_phrase = _coverage_phrase(coverage_types)

    if coverage_phrase:
        coverage_line = f"Please see the declaration documents of your {coverage_phrase} attached for your reference."
    else:
        coverage_line = "We were unable to locate declaration documents for your policies this cycle -- please let us know if you have them on hand."

    drivers_list_str = "\n".join([f" * {driver}" for driver in drivers])

    body = f"""Hello,

{coverage_line}
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

def _normalize_company(name: str) -> str:
    return re.sub(r"\s+", " ", name.upper().replace("&", " & ")).strip()


def resolve_client_names(drive_service, clients: list[str]) -> list[str]:
    """Swap a misspelled Renewals-doc name for the client's cert filename, so
    the Gmail/Drive/portal searches use the name the client is actually on
    file under (Oct 2026: the doc's "Mission Transportation Soultions LLC" is
    really "MISSION TRANSPORT SOLUTIONS LLC", and every exact-text search
    missed). Names that already match a cert (ignoring case/spacing) are left
    as-is. A correction needs a clear winner: similar names score high too
    (PZ vs ZR TRANSPORTATION is 0.95), so the best match must score >= 0.85
    and beat the runner-up by >= 0.1. Otherwise the name is kept and flagged."""
    cert_names, page_token = [], None
    try:
        while True:
            resp = drive_service.files().list(
                q=f"'{DRIVE_CERTS_FOLDER_ID}' in parents and trashed = false",
                pageSize=1000, fields="nextPageToken, files(name)", pageToken=page_token,
            ).execute()
            cert_names += [f["name"][:-4] for f in resp.get("files", []) if f["name"].lower().endswith(".pdf")]
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
    except Exception:
        logging.exception("Listing the certs folder for name matching failed")
        return clients

    by_norm = {_normalize_company(n): n for n in cert_names}
    resolved = []
    for client in clients:
        norm = _normalize_company(client)
        if norm in by_norm:
            resolved.append(client)
            continue
        scored = sorted(
            ((difflib.SequenceMatcher(None, norm, k).ratio(), by_norm[k]) for k in by_norm), reverse=True
        )
        best = scored[0] if scored else (0, None)
        runner_up = scored[1][0] if len(scored) > 1 else 0
        if best[0] >= 0.85 and best[0] - runner_up >= 0.1:
            print(f"!! '{client}' (Renewals doc) has no cert under that name -- using cert name '{best[1]}'")
            logging.warning(f"Renamed '{client}' -> '{best[1]}' (score {best[0]:.2f}, runner-up {runner_up:.2f})")
            resolved.append(best[1])
        else:
            print(f"!! '{client}' (Renewals doc) has no cert under that name and no clear match -- searches may come up empty")
            logging.warning(f"No cert match for '{client}' (best {best})")
            resolved.append(client)
    return resolved


def _parse_only_arg(argv: list[str]) -> list[str] | None:
    """`--only "CLIENT A,CLIENT B"` re-runs just those clients (exact names as
    they appear in the Renewals doc) instead of a whole month -- for redoing
    a few drafts after fixing something, without recreating everyone else's."""
    for i, a in enumerate(argv):
        if a == "--only" and i + 1 < len(argv):
            return [c.strip() for c in argv[i + 1].split(",") if c.strip()]
        if a.startswith("--only="):
            return [c.strip() for c in a[len("--only="):].split(",") if c.strip()]
    return None


def main():
    only_clients = _parse_only_arg(sys.argv[1:])
    if only_clients is None:
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

    if only_clients is not None:
        clients = only_clients
        print(f"\nRe-running only {len(clients)} client(s): {clients}")
    else:
        print(f"\nFetching clients for {target_month} {target_year}...")
        clients = get_clients_for_month(openai_client, drive_service, target_month, target_year)
        print(f"Found {len(clients)} clients: {clients}")

    clients = resolve_client_names(drive_service, clients)

    with PortalSessions(gmail_service) as portals:
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
                print(f"-> Found valid policy documents by email: {', '.join(found_docs)}")
            else:
                print("-> Could not find suitable declaration documents by email.")

            print("-> Checking COI for GAIG/Paramount/Trinity coverage lines...")
            portal_docs = fetch_portal_only_documents(portals, drive_service, client, attachments)
            if portal_docs:
                found_docs = [f"{k} ({v['coverage_type']})" for k, v in portal_docs.items()]
                print(f"-> Found additional documents via portals: {', '.join(found_docs)}")
                attachments.update(portal_docs)

            if not attachments:
                print("-> No declaration documents found from any source. Creating draft without attachments.")
                logging.warning(f"Could not find any declaration documents for '{client}'. Draft will be created without attachments.")

            print("-> Searching for drivers in Google Drive...")
            drivers = get_driver_names(openai_client, drive_service, client)
            print(f"-> Found {len(drivers)} drivers: {drivers}")

            print("-> Creating draft email...")
            create_draft_email(gmail_service, email, client, attachments, drivers)

    print("\nFinished processing all clients.")

if __name__ == "__main__":
    main()
