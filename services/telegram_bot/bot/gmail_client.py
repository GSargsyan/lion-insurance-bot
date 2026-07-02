"""Gmail helpers for the telegram bot service.

Functions:
  create_loss_run_drafts  — create Gmail drafts for Loss Run Requests.
  fetch_ach_forms         — download Commercial ACH Debit Authorization pages.
  fetch_notices_pdfs      — download FIRST Insurance Funding Notice PDFs.
  fetch_quickquote_first_page — download first page of a quick-quote form.
  send_autopay_email      — send filled autopay PDFs to tony@lioninsurance.us.
"""
import base64
import re
import time
from email.message import EmailMessage
from io import BytesIO

from google.oauth2 import service_account
from googleapiclient.discovery import build
from pypdf import PdfReader, PdfWriter

_GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.compose",
    "https://www.googleapis.com/auth/gmail.labels",
    "https://www.googleapis.com/auth/gmail.modify",
]

_GMAIL_READ_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
]

_FROM_EMAIL = "tony@lioninsurance.us"
_AUTOPAY_ALIAS = "no-reply@lioninsurance.us"



def _gmail_service(sa_info: dict):
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=_GMAIL_SCOPES,
        subject=_FROM_EMAIL,
    )
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def _format_policy_list(policy_numbers: list[str]) -> str:
    """Format a list of policy numbers as 'pol1 pol2 and pol3'.

    - 1 policy  → "pol1"
    - 2 policies → "pol1 and pol2"
    - 3+ policies → "pol1 pol2 and pol3"
    """
    if not policy_numbers:
        return ""
    if len(policy_numbers) == 1:
        return policy_numbers[0]
    return " ".join(policy_numbers[:-1]) + " and " + policy_numbers[-1]


def create_loss_run_drafts(
    draft_specs: list[dict],
    insured_name: str,
    sa_info: dict,
) -> list[str]:
    """Create Gmail draft Loss Run Request emails in Tony's mailbox.

    Creates one draft per insurer (i.e. one per item in *draft_specs*).

    Args:
        draft_specs: List of dicts from openai_client.resolve_loss_run_drafts().
                     Each has keys: "to_email", "insurer_name", "policy_numbers".
        insured_name: The insured company name (used in subject and body).
        sa_info: Service account JSON dict from Secret Manager.

    Returns:
        List of created Gmail draft IDs (may be shorter than draft_specs if some fail).
    """
    service = _gmail_service(sa_info)
    draft_ids: list[str] = []

    for spec in draft_specs:
        to_email = spec.get("to_email", "")
        policy_numbers = spec.get("policy_numbers", [])

        if not policy_numbers:
            print("[GMAIL] Skipping draft spec with no policy numbers")
            continue

        policy_str = _format_policy_list(policy_numbers)

        subject = f"Loss Runs Request: {policy_str} {insured_name}"
        body = (
            f"Hello,\n"
            f"Please issue the loss runs for {policy_str} {insured_name}\n"
            f"Thank you\n"
            f"Sincerely."
        )

        msg = EmailMessage()
        msg["From"] = f"Tony Lion Insurance <{_FROM_EMAIL}>"
        msg["Subject"] = subject
        if to_email:
            msg["To"] = to_email
        msg.set_content(body)

        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

        start = time.time()
        try:
            draft = (
                service.users()
                .drafts()
                .create(userId="me", body={"message": {"raw": raw}})
                .execute()
            )
            print(f"[TIMING] Gmail drafts.create: {time.time() - start:.2f}s")
            draft_id = draft.get("id")
            print(
                f"[GMAIL] Draft created: ID={draft_id}, "
                f"to={to_email!r}, subject={subject!r}"
            )
            
            # Add "Loss runs" label
            try:
                labels_results = service.users().labels().list(userId='me').execute()
                labels = labels_results.get('labels', [])
                target_label_id = None
                for label in labels:
                    if label['name'].lower().replace(" ", "-") == "loss-runs":
                        target_label_id = label['id']
                        break
                        
                if not target_label_id:
                    label_body = {
                        "name": "Loss runs",
                        "labelListVisibility": "labelShow",
                        "messageListVisibility": "show"
                    }
                    created_label = service.users().labels().create(userId='me', body=label_body).execute()
                    target_label_id = created_label['id']
                    
                if target_label_id:
                    msg_id = draft['message']['id']
                    service.users().messages().modify(
                        userId='me', 
                        id=msg_id, 
                        body={'addLabelIds': [target_label_id]}
                    ).execute()
                    print(f"[GMAIL] Added label 'Loss runs' to draft ID {draft_id}.")
            except Exception as label_exc:
                print(f"[GMAIL] Failed to add 'Loss runs' label to draft ID {draft_id}: {label_exc}")

            draft_ids.append(draft_id)
        except Exception as exc:
            print(f"[GMAIL] Failed to create draft (subject={subject!r}): {exc}")

    return draft_ids


# ── Autopay helpers ───────────────────────────────────────────────────────────


def _read_gmail_service(sa_info: dict):
    """Build a Gmail service with read-only scope, impersonating Tony."""
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=_GMAIL_READ_SCOPES,
        subject=_FROM_EMAIL,
    )
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def _download_attachment(service, msg_id: str, attachment_id: str) -> bytes:
    """Fetch and base64-decode a Gmail message attachment."""
    attachment = (
        service.users()
        .messages()
        .attachments()
        .get(userId="me", messageId=msg_id, id=attachment_id)
        .execute()
    )
    return base64.urlsafe_b64decode(attachment["data"])


def _collect_pdf_attachments(parts: list) -> list[tuple[str, str]]:
    """Recursively collect (filename, attachmentId) for all PDF parts."""
    found = []
    for part in parts:
        filename = part.get("filename", "")
        if filename.lower().endswith(".pdf"):
            att_id = part["body"].get("attachmentId")
            if att_id:
                found.append((filename, att_id))
        if "parts" in part:
            found.extend(_collect_pdf_attachments(part["parts"]))
    return found


def _extract_text(payload) -> str:
    if "body" in payload and "data" in payload["body"]:
        return base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="ignore")
    elif "parts" in payload:
        for part in payload["parts"]:
            text = _extract_text(part)
            if text:
                return text
    return ""


def _extract_email_address(text: str) -> str | None:
    match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    if match:
        return match.group(0)
    return None


def fetch_ach_forms(
    company_name: str, sa_info: dict
) -> tuple[list[tuple[str, bytes]], str]:
    """Download Commercial ACH Debit Authorization pages for a company.

    Searches Tony's inbox for signed documents from the company, finds all PDF
    attachments containing a "Commercial ACH Debit Authorization" page, and
    returns each such page as a single-page PDF in memory.

    Args:
        company_name: Renewal client name.
        sa_info:      Gmail service-account JSON dict.

    Returns:
        Tuple of:
          - List of (filename, single_page_pdf_bytes) for every ACH form page.
          - Client email address extracted from the email subject, or "".
    """
    service = _read_gmail_service(sa_info)
    query = f'subject:"{company_name} has been signed by" has:attachment newer_than:3m'

    start = time.time()
    try:
        resp = (
            service.users()
            .messages()
            .list(userId="me", q=query, maxResults=20)
            .execute()
        )
        messages = resp.get("messages", [])
    except Exception as exc:
        print(f"[GMAIL] fetch_ach_forms query failed for {company_name!r}: {exc}")
        return [], ""
    print(f"[TIMING] Gmail search (ACH forms) for {company_name!r}: {time.time() - start:.2f}s, {len(messages)} message(s)")

    results: list[tuple[str, bytes]] = []
    client_email = ""

    for message in messages:
        msg_id = message["id"]
        try:
            msg = (
                service.users()
                .messages()
                .get(userId="me", id=msg_id, format="full")
                .execute()
            )
        except Exception as exc:
            print(f"[GMAIL] Failed to fetch message {msg_id}: {exc}")
            continue

        # Extract client email on first match
        if not client_email:
            body_text = _extract_text(msg.get("payload", {})) or msg.get("snippet", "")
            client_email = _extract_email_address(body_text) or ""

        parts = msg.get("payload", {}).get("parts", [])
        for filename, att_id in _collect_pdf_attachments(parts):
            try:
                file_data = _download_attachment(service, msg_id, att_id)
                reader = PdfReader(BytesIO(file_data))
                for page_num, page in enumerate(reader.pages):
                    text = page.extract_text() or ""
                    if "Commercial ACH Debit Authorization" in text:
                        writer = PdfWriter()
                        writer.add_page(page)
                        buf = BytesIO()
                        writer.write(buf)
                        results.append((filename, buf.getvalue()))
                        print(
                            f"[GMAIL] ACH form page found in {filename!r} "
                            f"(page {page_num + 1}) for {company_name!r}"
                        )
            except Exception as exc:
                print(f"[GMAIL] Failed to process attachment {filename!r}: {exc}")

    print(
        f"[GMAIL] fetch_ach_forms: {len(results)} ACH form(s), "
        f"client_email={client_email!r}"
    )
    return results, client_email


def fetch_client_email(company_name: str, sa_info: dict) -> str:
    """Search for the client's email address from their signing notification email.

    Searches for the "<company> has been signed by <email>" subject pattern in
    Tony's mailbox and returns the signer's email. Used as a fallback when
    fetch_ach_forms found no matching emails (e.g. signed more than 3 months ago).

    Args:
        company_name: Insured company name.
        sa_info:      Gmail service-account JSON dict.

    Returns:
        Client email string, or "" if not found.
    """
    service = _read_gmail_service(sa_info)
    # Broaden the search — no newer_than filter, no has:attachment requirement
    query = f'subject:"{company_name} has been signed by"'

    try:
        resp = (
            service.users()
            .messages()
            .list(userId="me", q=query, maxResults=5)
            .execute()
        )
        messages = resp.get("messages", [])
    except Exception as exc:
        print(f"[GMAIL] fetch_client_email query failed for {company_name!r}: {exc}")
        return ""

    for message in messages:
        msg_id = message["id"]
        try:
            msg = (
                service.users()
                .messages()
                .get(userId="me", id=msg_id, format="full")
                .execute()
            )
        except Exception as exc:
            print(f"[GMAIL] fetch_client_email: failed to fetch message {msg_id}: {exc}")
            continue

        body_text = _extract_text(msg.get("payload", {})) or msg.get("snippet", "")
        email = _extract_email_address(body_text)
        if email:
            print(f"[GMAIL] fetch_client_email: found {email!r} for {company_name!r}")
            return email

    print(f"[GMAIL] fetch_client_email: no result for {company_name!r}")
    return ""


def fetch_notices_pdfs(company_name: str, sa_info: dict) -> list[bytes]:

    """Download FIRST Insurance Funding Notice of Acceptance PDFs.

    Only considers emails with exactly 1 PDF attachment (to avoid bulk/combined
    exports) from the last 60 days.

    Args:
        company_name: Renewal client name (added as a keyword to narrow search).
        sa_info:      Gmail service-account JSON dict.

    Returns:
        List of raw PDF bytes — one entry per matching email/loan.
    """
    service = _read_gmail_service(sa_info)
    query = (
        f'subject:"FIRST Insurance Funding - Notice of Acceptance" '
        f'{company_name} has:attachment newer_than:60d'
    )

    start = time.time()
    try:
        resp = (
            service.users()
            .messages()
            .list(userId="me", q=query, maxResults=10)
            .execute()
        )
        messages = resp.get("messages", [])
    except Exception as exc:
        print(f"[GMAIL] fetch_notices_pdfs query failed for {company_name!r}: {exc}")
        return []
    print(f"[TIMING] Gmail search (Notices) for {company_name!r}: {time.time() - start:.2f}s, {len(messages)} message(s)")

    results: list[bytes] = []

    for message in messages:
        msg_id = message["id"]
        try:
            msg = (
                service.users()
                .messages()
                .get(userId="me", id=msg_id, format="full")
                .execute()
            )
        except Exception as exc:
            print(f"[GMAIL] Failed to fetch Notices message {msg_id}: {exc}")
            continue

        parts = msg.get("payload", {}).get("parts", [])
        pdf_attachments = _collect_pdf_attachments(parts)

        # Only take emails with exactly 1 PDF attachment
        if len(pdf_attachments) != 1:
            print(
                f"[GMAIL] Notices message {msg_id}: {len(pdf_attachments)} PDF(s) "
                f"— expected exactly 1, skipping."
            )
            continue

        filename, att_id = pdf_attachments[0]
        try:
            file_data = _download_attachment(service, msg_id, att_id)
            results.append(file_data)
            print(f"[GMAIL] Notices PDF {filename!r} downloaded for {company_name!r}")
        except Exception as exc:
            print(f"[GMAIL] Failed to download Notices PDF {filename!r}: {exc}")

    print(f"[GMAIL] fetch_notices_pdfs: {len(results)} file(s) for {company_name!r}")
    return results


def fetch_quickquote_first_page(company_name: str, sa_info: dict) -> bytes | None:
    """Download the first page of a quick-quote form PDF for phone extraction.

    Searches for emails matching 'quick quote form <company_name>' that have
    exactly 1 PDF attachment, then returns the first page of that PDF only
    (so the LLM receives a minimal context).

    Args:
        company_name: Renewal client name.
        sa_info:      Gmail service-account JSON dict.

    Returns:
        Raw bytes of a single-page PDF, or None if nothing found.
    """
    service = _read_gmail_service(sa_info)
    query = f"quick quote form {company_name} has:attachment"

    start = time.time()
    try:
        resp = (
            service.users()
            .messages()
            .list(userId="me", q=query, maxResults=5)
            .execute()
        )
        messages = resp.get("messages", [])
    except Exception as exc:
        print(f"[GMAIL] fetch_quickquote query failed for {company_name!r}: {exc}")
        return None
    print(f"[TIMING] Gmail search (quick quote) for {company_name!r}: {time.time() - start:.2f}s, {len(messages)} message(s)")

    for message in messages:
        msg_id = message["id"]
        try:
            msg = (
                service.users()
                .messages()
                .get(userId="me", id=msg_id, format="full")
                .execute()
            )
        except Exception as exc:
            print(f"[GMAIL] Failed to fetch quick-quote message {msg_id}: {exc}")
            continue

        parts = msg.get("payload", {}).get("parts", [])
        pdf_attachments = _collect_pdf_attachments(parts)

        if len(pdf_attachments) != 1:
            continue

        filename, att_id = pdf_attachments[0]
        try:
            file_data = _download_attachment(service, msg_id, att_id)
            reader = PdfReader(BytesIO(file_data))
            if not reader.pages:
                continue
            # Return first page only
            writer = PdfWriter()
            writer.add_page(reader.pages[0])
            buf = BytesIO()
            writer.write(buf)
            print(f"[GMAIL] Quick-quote first page extracted from {filename!r} for {company_name!r}")
            buf.seek(0)
            return buf.read()
        except Exception as exc:
            print(f"[GMAIL] Failed to process quick-quote PDF {filename!r}: {exc}")

    print(f"[GMAIL] fetch_quickquote_first_page: no result for {company_name!r}")
    return None


def send_autopay_email(
    filled_files: list[tuple[str, bytes]],
    company_name: str,
    sa_info: dict,
) -> None:
    """Send filled autopay PDFs from no-reply alias to tony@lioninsurance.us.

    Args:
        filled_files: List of (filename, pdf_bytes) tuples to attach.
        company_name: Used in the email subject line.
        sa_info:      Gmail service-account JSON dict.
    """
    service = _gmail_service(sa_info)  # compose scope covers messages.send

    msg = EmailMessage()
    msg["From"] = _AUTOPAY_ALIAS
    msg["To"] = _FROM_EMAIL
    msg["Subject"] = f"Autopay Forms — {company_name}"
    msg.set_content(
        f"Please find the filled Commercial ACH Debit Authorization form(s) "
        f"for {company_name} attached.\n\nTotal files: {len(filled_files)}"
    )

    for filename, pdf_bytes in filled_files:
        msg.add_attachment(
            pdf_bytes,
            maintype="application",
            subtype="pdf",
            filename=filename,
        )

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

    start = time.time()
    try:
        service.users().messages().send(
            userId="me", body={"raw": raw}
        ).execute()
        print(
            f"[TIMING] Gmail send (autopay): {time.time() - start:.2f}s"
        )
        print(
            f"[GMAIL] Sent autopay email for {company_name!r} "
            f"with {len(filled_files)} attachment(s)"
        )
    except Exception as exc:
        print(f"[GMAIL] Failed to send autopay email for {company_name!r}: {exc}")
        raise

