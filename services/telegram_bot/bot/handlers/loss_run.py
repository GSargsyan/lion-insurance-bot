"""Loss Run Request handler.

Conversation flow:
  1. User clicks "Create Loss Run Request" → bot asks for company name.
  2. User types a company name → bot fuzzy-matches against Drive folder,
     downloads the cert PDF to GCS, creates a Gmail draft, confirms to user,
     and returns to the main menu.

State is stored in Firestore under the collection "bot_sessions",
document ID = str(chat_id).  The only field we track is:
    {"awaiting": "loss_run_company_name"}  (when waiting for user text input)

Clearing awaiting (setting it to None / deleting the field) signals that the
session is idle and back at the main menu.
"""
import io
import os
import time

from google.cloud import firestore, storage

from bot import drive_client, gmail_client, openai_client, telegram_helpers
from bot.handlers import menu

# ── Config ────────────────────────────────────────────────────────────────────
DRIVE_CERTS_FOLDER_ID = "1KIeq3LHWWklQBanmADUz6XVYodlF2id6"
BUCKET_NAME = os.environ.get("BUCKET_NAME", "lion-insurance")
GCS_CERT_PREFIX = "loss_run_certs"

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


# ── GCS helpers ───────────────────────────────────────────────────────────────

def _upload_cert_to_gcs(filename: str, file_bytes: io.BytesIO) -> None:
    """Upload a downloaded cert PDF to GCS under loss_run_certs/<filename>."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{GCS_CERT_PREFIX}/{filename}")
    start = time.time()
    blob.upload_from_file(file_bytes, content_type="application/pdf")
    print(f"[TIMING] GCS upload ({filename}): {time.time() - start:.2f}s")
    print(f"[GCS] Uploaded gs://{BUCKET_NAME}/{GCS_CERT_PREFIX}/{filename}")


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
      4. If match → download PDF, upload to GCS, create Gmail draft, confirm.
    """
    telegram_helpers.send_message(chat_id, "🔍 Searching for the company…")

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
            "🤷 Could not find a matching company for *{}*.\n\n"
            "Please try again with a different name, or return to the main menu.".format(
                user_text
            ),
            buttons=[[{"text": "🏠 Main Menu", "callback_data": "action:menu"}]],
        )
        # Keep awaiting state so the user can try again
        return

    # 4. Match found — download PDF from Drive → upload to GCS (silent)
    try:
        file_bytes = drive_client.download_file_by_name(
            DRIVE_CERTS_FOLDER_ID, matched_name
        )
        if file_bytes:
            _upload_cert_to_gcs(matched_name, file_bytes)
        else:
            print(f"[LOSS_RUN] Download returned None for {matched_name!r}")
    except Exception as exc:
        print(f"[LOSS_RUN] PDF download/upload failed: {exc}")
        # Non-fatal: we still proceed to create the draft

    # 5. Create Gmail draft (silent)
    try:
        draft_id = gmail_client.create_loss_run_draft(matched_name, gmail_sa_info)
    except Exception as exc:
        print(f"[LOSS_RUN] Gmail draft creation failed: {exc}")
        draft_id = None

    display_name = matched_name.removesuffix(".pdf")
    if draft_id:
        confirm_text = f"✅ *Draft created for {display_name}*"
    else:
        confirm_text = (
            f"⚠️ *Draft creation failed for {display_name}*\n"
            "Please check the logs."
        )

    # Clear session and return to menu
    _set_awaiting(chat_id, None)
    telegram_helpers.send_message(chat_id, confirm_text)
    menu.send_main_menu(chat_id)


def is_awaiting_input(chat_id: int | str) -> bool:
    """Return True if this chat is currently waiting for a company name."""
    return _get_awaiting(chat_id) == "loss_run_company_name"
