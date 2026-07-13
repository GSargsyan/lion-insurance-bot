"""Autopay Extraction handler.

Conversation flow:
  1. User clicks "Initiate Autopay Extraction" → bot asks for company name.
  2. User types a company name → LLM fuzzy-matches it against Drive folder filenames
     (same as loss run) and returns the canonical name.
  3. Bot sends "⏳ Processing…" and runs the extraction pipeline synchronously.
  4. Pipeline steps:
       a. Fetch ACH forms + extract client email from signing notification email.
       b. Fetch FIRST Insurance Funding Notices PDFs.
       c. Parse loan numbers, address, and optional DBA from Notices via gpt-4o-mini.
       d. Fetch quick-quote form and extract phone number via LLM.
       e. Fill each ACH form × each loan entry with PyMuPDF.
       f. Send filled PDF(s) by email to tony@lioninsurance.us.
  5. Bot sends result summary and returns to main menu.

State: stored in Firestore "bot_sessions" collection, same pattern as loss_run.
    {"awaiting": "autopay_company_name"}
"""
import time

from google.cloud import firestore

from bot import drive_client, gmail_client, openai_client, pdf_filler, pdf_parser, telegram_helpers
from bot.handlers import menu

# ── Config ────────────────────────────────────────────────────────────────────
DRIVE_CERTS_FOLDER_ID = "1KIeq3LHWWklQBanmADUz6XVYodlF2id6"

# ── Firestore session helpers ─────────────────────────────────────────────────
_db: firestore.Client | None = None


def _get_db() -> firestore.Client:
    global _db
    if _db is None:
        _db = firestore.Client(database="lion-ins")
    return _db


def _session_ref(chat_id: int | str) -> firestore.DocumentReference:
    return _get_db().collection("bot_sessions").document(str(chat_id))


def _set_awaiting(chat_id: int | str, state: str | None) -> None:
    ref = _session_ref(chat_id)
    if state is None:
        ref.set({"awaiting": firestore.DELETE_FIELD}, merge=True)
    else:
        ref.set({"awaiting": state}, merge=True)


def _get_awaiting(chat_id: int | str) -> str | None:
    doc = _session_ref(chat_id).get()
    if doc.exists:
        return doc.to_dict().get("awaiting")
    return None


# ── Public entry points ───────────────────────────────────────────────────────

def start(chat_id: int | str) -> None:
    """Called when the user clicks 'Initiate Autopay Extraction'.

    Asks the user to type the insured company name.
    """
    _set_awaiting(chat_id, "autopay_company_name")
    telegram_helpers.send_message(
        chat_id,
        "🏦 *Autopay Extraction*\n\n"
        "Please type the insured company name:",
        buttons=[[{"text": "🏠 Main Menu", "callback_data": "action:menu"}]],
    )


def handle_company_name_input(
    chat_id: int | str,
    user_text: str,
    sa_info: dict,
) -> None:
    """Called when the user sends a company name while in autopay session.

    Steps:
      1. List all filenames from Drive folder.
      2. Ask LLM to fuzzy-match the user's input.
      3. If no match → ask again.
      4. If match → run the full autopay extraction pipeline.
    """
    # 1. Fetch known names from Drive
    try:
        known_names = drive_client.list_filenames_in_folder(DRIVE_CERTS_FOLDER_ID)
    except Exception as exc:
        print(f"[AUTOPAY] Drive listing failed: {exc}")
        telegram_helpers.send_message(
            chat_id,
            "❌ Failed to reach Google Drive. Please try again later.",
            buttons=[[{"text": "🏠 Main Menu", "callback_data": "action:menu"}]],
        )
        _set_awaiting(chat_id, None)
        return

    if not known_names:
        telegram_helpers.send_message(
            chat_id,
            "❌ No company files found in Drive. Please contact support.",
            buttons=[[{"text": "🏠 Main Menu", "callback_data": "action:menu"}]],
        )
        _set_awaiting(chat_id, None)
        return

    # 2. Fuzzy match via LLM
    try:
        matched_name = openai_client.match_company_name(user_text, known_names)
    except Exception as exc:
        print(f"[AUTOPAY] OpenAI matching failed: {exc}")
        telegram_helpers.send_message(
            chat_id,
            "❌ Matching service unavailable. Please try again later.",
            buttons=[[{"text": "🏠 Main Menu", "callback_data": "action:menu"}]],
        )
        _set_awaiting(chat_id, None)
        return

    # 3. No match found
    if not matched_name:
        telegram_helpers.send_message(
            chat_id,
            "Could not find a matching company for *{}*.\n\n"
            "Please try again with a different name, or return to the main menu.".format(
                user_text
            ),
            buttons=[[{"text": "🏠 Main Menu", "callback_data": "action:menu"}]],
        )
        # Keep awaiting so user can retry
        return

    display_name = matched_name.removesuffix(".pdf")
    telegram_helpers.send_message(chat_id, f"⏳ Processing autopay for *{display_name}*…")
    _set_awaiting(chat_id, None)
    _run_extraction(chat_id, display_name, sa_info)


def clear_session(chat_id: int | str) -> None:
    """Clear autopay session state from Firestore."""
    _set_awaiting(chat_id, None)


def is_awaiting_input(chat_id: int | str) -> bool:
    """Return True if this chat is waiting for an autopay company name."""
    return _get_awaiting(chat_id) == "autopay_company_name"


# ── Extraction pipeline ───────────────────────────────────────────────────────

def _run_extraction(
    chat_id: int | str,
    client: str,
    sa_info: dict,
) -> None:
    """Full autopay extraction pipeline for a single client."""
    pipeline_start = time.time()
    print(f"[AUTOPAY] Starting extraction for client: {client!r}")

    try:
        loan_numbers_found = 0
        filled_files: list[tuple[str, bytes]] = []

        # ── Step 1: Fetch FIRST Insurance Funding Notices PDFs + parse loans ───
        # Loan numbers are the most likely to be missing — check first to avoid
        # wasting further calls (including LLM tokens) if none are found.
        notices_bytes_list: list[bytes] = []
        try:
            notices_bytes_list = gmail_client.fetch_notices_pdfs(client, sa_info)
            print(f"[AUTOPAY] Notices PDFs: {len(notices_bytes_list)}")
        except Exception as exc:
            print(f"[AUTOPAY] fetch_notices_pdfs failed: {exc}")

        loan_entries: list[dict] = []
        for notices_bytes in notices_bytes_list:
            try:
                entries = pdf_parser.parse_notices_pdf(notices_bytes, client)
                loan_entries.extend(entries)
            except Exception as exc:
                print(f"[AUTOPAY] parse_notices_pdf failed: {exc}")

        loan_numbers_found = len(loan_entries)
        print(f"[AUTOPAY] Loan entries for {client!r}: {loan_entries}")

        if not loan_entries:
            elapsed = time.time() - pipeline_start
            telegram_helpers.send_message(
                chat_id,
                f"⚠️ *Autopay extraction done* ({elapsed:.0f}s)\n\n"
                f"Client: *{client}*\n"
                f"No loan numbers found — nothing to fill.",
            )
            menu.send_main_menu(chat_id)
            return

        # ── Step 2: Fetch ACH Debit Authorization forms ────────────────────────
        # Only fetched once we know there are loan numbers to fill.
        ach_forms: list[tuple[str, bytes]] = []
        client_email = ""
        try:
            ach_forms, client_email = gmail_client.fetch_ach_forms(client, sa_info)
            print(
                f"[AUTOPAY] ACH forms: {len(ach_forms)}, "
                f"client_email={client_email!r}"
            )
        except Exception as exc:
            print(f"[AUTOPAY] fetch_ach_forms failed: {exc}")

        if not ach_forms:
            elapsed = time.time() - pipeline_start
            telegram_helpers.send_message(
                chat_id,
                f"⚠️ *Autopay extraction done* ({elapsed:.0f}s)\n\n"
                f"Client: *{client}*\n"
                f"Loan numbers found: {loan_numbers_found}\n"
                f"No ACH forms found — nothing to fill.",
            )
            menu.send_main_menu(chat_id)
            return

        # ── Step 3: Fetch client email (fallback) + phone number ──────────────
        # Only reached when we have both loan entries and ACH forms.

        # If email wasn't in the ACH signing email, try fetching it separately
        if not client_email:
            try:
                client_email = gmail_client.fetch_client_email(client, sa_info)
                print(f"[AUTOPAY] client_email (fallback search): {client_email!r}")
            except Exception as exc:
                print(f"[AUTOPAY] fetch_client_email failed: {exc}")

        phone = ""
        try:
            qq_bytes = gmail_client.fetch_quickquote_first_page(client, sa_info)
            if qq_bytes:
                from io import BytesIO
                from pypdf import PdfReader
                reader = PdfReader(BytesIO(qq_bytes))
                page_text = reader.pages[0].extract_text() or "" if reader.pages else ""
                phone = openai_client.extract_phone_number(page_text) or ""
                print(f"[AUTOPAY] Phone for {client!r}: {phone!r}")
        except Exception as exc:
            print(f"[AUTOPAY] Phone extraction failed: {exc}")

        # ── Step 4: Fill ACH form × loan entry combinations ───────────────────
        if ach_forms and loan_entries:
            for ach_filename, ach_bytes in ach_forms:
                for entry in loan_entries:
                    loan_num = f"Loan Number: {entry['loan_number']}"
                    base_name = entry.get("company_name") or client
                    dba = entry.get("dba")
                    # Append DBA to company name if present
                    company = f"{base_name}, DBA: {dba}" if dba else base_name
                    address = entry.get("address") or ""

                    try:
                        filled_bytes = pdf_filler.fill_ach_form(
                            pdf_bytes=ach_bytes,
                            loan_number=loan_num,
                            company_name=company,
                            address=address,
                            phone=phone,
                            email=client_email,
                        )
                        base = ach_filename.rsplit(".", 1)[0]
                        out_name = f"{base}_{loan_num}.pdf"
                        filled_files.append((out_name, filled_bytes))
                        print(f"[AUTOPAY] Filled: {out_name}")
                    except Exception as exc:
                        print(
                            f"[AUTOPAY] fill_ach_form failed "
                            f"({ach_filename}, loan {loan_num}): {exc}"
                        )
        else:
            print(
                f"[AUTOPAY] Skipping fill step: "
                f"ach_forms={len(ach_forms)}, loan_entries={len(loan_entries)}"
            )

        # ── Step 5: Send email with all filled PDFs ────────────────────────────
        if filled_files:
            try:
                gmail_client.send_autopay_email(filled_files, client, sa_info)
            except Exception as exc:
                print(f"[AUTOPAY] send_autopay_email failed: {exc}")

        # ── Done — report back via Telegram ───────────────────────────────────
        elapsed = time.time() - pipeline_start
        if filled_files:
            result_text = (
                f"✅ *Autopay extraction complete* ({elapsed:.0f}s)\n\n"
                f"Client: *{client}*\n"
                f"Email: {client_email or '(not found)'}\n"
                f"Phone: {phone or '(not found)'}\n"
                f"Loan numbers found: {loan_numbers_found}\n"
                f"Forms filled & emailed: {len(filled_files)}"
            )
        else:
            result_text = (
                f"⚠️ *Autopay extraction done* ({elapsed:.0f}s)\n\n"
                f"Client: *{client}*\n"
                f"Email: {client_email or '(not found)'}\n"
                f"Phone: {phone or '(not found)'}\n"
                f"ACH forms downloaded: {len(ach_forms)}\n"
                f"Loan numbers found: {loan_numbers_found}\n"
            )

        telegram_helpers.send_message(chat_id, result_text)
        menu.send_main_menu(chat_id)

    except Exception as exc:
        print(f"[AUTOPAY] Unhandled error in _run_extraction: {exc}")
        telegram_helpers.send_message(
            chat_id,
            "❌ Autopay extraction failed unexpectedly. Please check Cloud Run logs.",
        )
        menu.send_main_menu(chat_id)
