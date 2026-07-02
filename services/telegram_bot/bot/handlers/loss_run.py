"""Loss Run Request handler.

Conversation flow:
  1. User clicks "Create Loss Run Request" → bot asks for company name.
  2. User types a company name → bot fuzzy-matches against Drive folder,
     downloads the cert PDF into memory (BytesIO, never persisted),
     parses policy info, fetches insurer-email lookup from Google Doc,
     asks LLM to resolve per-insurer draft specs, creates Gmail drafts,
     confirms to user, and returns to the main menu.

State is stored in Firestore under the collection "bot_sessions",
document ID = str(chat_id).  The only field we track is:
    {"awaiting": "loss_run_company_name"}  (when waiting for user text input)

Clearing awaiting (setting it to None / deleting the field) signals that the
session is idle and back at the main menu.
"""
import time

from google.cloud import firestore

from bot import drive_client, gmail_client, openai_client, pdf_parser, telegram_helpers
from bot.handlers import menu

# ── Config ────────────────────────────────────────────────────────────────────
DRIVE_CERTS_FOLDER_ID = "1KIeq3LHWWklQBanmADUz6XVYodlF2id6"
INSURER_EMAIL_DOC_ID = "1OyTBozhG484ngRDCKhrbWRc81DmH4hSyU5PFJy3tfwg"

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


def clear_session(chat_id: int | str) -> None:
    """Clear all loss-run-related session fields from Firestore."""
    _set_awaiting(chat_id, None)


def _get_awaiting(chat_id: int | str) -> str | None:
    doc = _session_ref(chat_id).get()
    if doc.exists:
        return doc.to_dict().get("awaiting")
    return None



# ── Public entry points ───────────────────────────────────────────────────────

def start(chat_id: int | str) -> None:
    """Called when the user clicks the 'Create Loss Run Request' button."""
    _set_awaiting(chat_id, "loss_run_company_name")
    telegram_helpers.send_message(
        chat_id,
        "📋 *Loss Run Request*\n\n"
        "Please type the insured company name (e.g. _aag espindola_):",
        buttons=[[{"text": "🏠 Main Menu", "callback_data": "action:menu"}]],
    )


def handle_company_name_input(
    chat_id: int | str,
    user_text: str,
    gmail_sa_info: dict,
) -> None:
    """Called when the user sends text while awaiting a company name.

    Steps:
      1. List all filenames from Drive folder.
      2. Ask LLM to fuzzy-match the user's input.
      3. If no match → ask again.
      4. If match → download PDF into memory (BytesIO, never written to disk/GCS).
      5. Parse policy info from the PDF.
      6. Fetch insurer-email lookup table from Google Doc.
      7. Ask LLM to resolve per-insurer draft specs (email + grouped policy numbers).
      8. Create one Gmail draft per insurer.
      9. Confirm to user and return to main menu.
    """
    # (no status message yet — we send one single "Processing..." only after a match is confirmed)

    # 1. Fetch known names from Drive
    try:
        known_names = drive_client.list_filenames_in_folder(DRIVE_CERTS_FOLDER_ID)
    except Exception as exc:
        print(f"[LOSS_RUN] Drive listing failed: {exc}")
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
        print(f"[LOSS_RUN] OpenAI matching failed: {exc}")
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
        # Keep awaiting state so the user can try again
        return

    display_name = matched_name.removesuffix(".pdf")
    telegram_helpers.send_message(chat_id, "⏳ Processing…")

    # 4. Download PDF into memory (BytesIO — never persisted to disk or GCS)
    pdf_bytes = None
    try:
        start = time.time()
        pdf_bytes = drive_client.download_file_by_name(
            DRIVE_CERTS_FOLDER_ID, matched_name
        )
        print(f"[TIMING] PDF download total: {time.time() - start:.2f}s")
        if not pdf_bytes:
            print(f"[LOSS_RUN] Download returned None for {matched_name!r}")
    except Exception as exc:
        print(f"[LOSS_RUN] PDF download failed: {exc}")
        # Non-fatal: proceed with empty policy info

    # 5. Extract specific AcroForm fields from the PDF
    pdf_fields = {}
    if pdf_bytes:
        try:
            pdf_fields = pdf_parser.extract_acord_fields(pdf_bytes)
        except Exception as exc:
            print(f"[LOSS_RUN] PDF form extraction failed: {exc}")
    else:
        print("[LOSS_RUN] No PDF bytes — skipping extraction")

    # 6. Fetch insurer-email lookup table from Google Doc
    insurer_email_doc_text = ""
    try:
        insurer_email_doc_text = drive_client.read_google_doc_text(INSURER_EMAIL_DOC_ID)
    except Exception as exc:
        print(f"[LOSS_RUN] Failed to fetch insurer-email doc: {exc}")
        # Non-fatal: LLM will receive empty table and leave emails blank

    # 7. Ask LLM to resolve per-insurer draft specs
    draft_specs: list[dict] = []
    if pdf_fields or insurer_email_doc_text:
        try:
            draft_specs = openai_client.resolve_loss_run_drafts(
                pdf_fields=pdf_fields,
                insurer_email_doc_text=insurer_email_doc_text,
                insured_name=display_name,
            )
        except Exception as exc:
            print(f"[LOSS_RUN] LLM draft resolution failed: {exc}")
            draft_specs = []

    # 8. Create Gmail drafts (one per insurer)
    draft_ids: list[str] = []
    if draft_specs:
        try:
            draft_ids = gmail_client.create_loss_run_drafts(
                draft_specs=draft_specs,
                insured_name=display_name,
                sa_info=gmail_sa_info,
            )
        except Exception as exc:
            print(f"[LOSS_RUN] Gmail draft creation failed: {exc}")
            draft_ids = []
    else:
        # Fallback: no policy info resolved — create a plain draft with no policy numbers
        print("[LOSS_RUN] No draft specs resolved; creating fallback draft")
        try:
            fallback_ids = gmail_client.create_loss_run_drafts(
                draft_specs=[{
                    "to_email": "",
                    "insurer_name": "",
                    "policy_numbers": [display_name],
                }],
                insured_name=display_name,
                sa_info=gmail_sa_info,
            )
            draft_ids = fallback_ids
        except Exception as exc:
            print(f"[LOSS_RUN] Fallback Gmail draft creation failed: {exc}")

    # 9. Confirm to user
    n = len(draft_ids)
    if n > 0:
        confirm_text = (
            f"✅ *{n} draft{'s' if n > 1 else ''} created for {display_name}*"
        )
        # List insurer names in confirmation if available
        insurer_lines = [
            f"  • {spec.get('insurer_name', '(unknown)')} — "
            f"{', '.join(spec.get('policy_numbers', []))}"
            for spec in draft_specs
        ]
        if insurer_lines:
            confirm_text += "\n" + "\n".join(insurer_lines)
    else:
        confirm_text = (
            f"⚠️ *Draft creation failed for {display_name}*\n"
            "Please check the logs."
        )

    _set_awaiting(chat_id, None)
    telegram_helpers.send_message(chat_id, confirm_text)
    menu.send_main_menu(chat_id)


def is_awaiting_input(chat_id: int | str) -> bool:
    """Return True if this chat is currently waiting for a company name."""
    return _get_awaiting(chat_id) == "loss_run_company_name"
