"""Telegram bot — Cloud Run entry point.

Architecture overview
─────────────────────
main.py                 Flask app + Telegram webhook handler (this file)
bot/secrets.py          Secret Manager fetching
bot/telegram_helpers.py Telegram HTTP wrappers
bot/drive_client.py     Google Drive file listing & download
bot/openai_client.py    LLM helpers (matching, phone extraction, renewal clients)
bot/gmail_client.py     Gmail draft creation + autopay email helpers
bot/pdf_parser.py       PDF form field extractor + Notices PDF parser
bot/pdf_filler.py       PyMuPDF coordinate-based PDF form filler
bot/handlers/
    menu.py             Main menu builder (2 buttons)
    loss_run.py         Loss Run Request conversation handler
    autopay.py          Autopay Extraction handler (background thread)

Dispatching logic
─────────────────
POST /          Telegram webhook payload
  ├─ message.text == "/start"   → show main menu
  ├─ callback_query             → route by callback_data prefix
  │     "action:menu"           → show main menu
  │     "action:loss_run"       → loss_run.start()
  │     "action:autopay"        → autopay.start()
  └─ message.text (any)         → check active session and delegate
        loss_run awaiting       → loss_run.handle_company_name_input()
        autopay awaiting        → autopay.handle_company_name_input()
        (no active session)     → show main menu

POST /notify_coi    Legacy endpoint — kept for backward compatibility with
                    the email_watcher service that posts COI notifications.
"""
import os
import time

from flask import Flask, request

from bot import gmail_client, openai_client, secrets, telegram_helpers
from bot.handlers import autopay, loss_run, menu

app = Flask(__name__)

# ── Startup: fetch secrets and initialise SDK clients ─────────────────────────

_telegram_token = secrets.get_telegram_token()
telegram_helpers.init(_telegram_token)

_openai_key = secrets.get_openai_key()
openai_client.init(_openai_key)

# Gmail service-account info fetched once and passed through to handlers that
# need it, avoiding repeated Secret Manager round-trips per request.
_gmail_sa_info = secrets.get_gmail_service_account_info()


# ── Allowlist ─────────────────────────────────────────────────────────────────
# Add Telegram user IDs here to grant access. Find yours by messaging @userinfobot.
_ALLOWED_CHAT_IDS = {
    828259521,  # Grig
    1199848601,  # Tony
}

def _is_allowed(chat_id: int | str) -> bool:
    return int(chat_id) in _ALLOWED_CHAT_IDS


# ── Helper ────────────────────────────────────────────────────────────────────

def _handle_coi_callback(action: str, thread_id: str, chat_id: int | str) -> None:
    """Handle legacy COI send/nosend button presses from email_watcher notifications."""
    import requests as _requests
    from datetime import datetime
    from google.cloud import firestore as _fs

    db = _fs.Client(database="lion-ins")
    doc_id = f"msg_{thread_id}"
    doc_ref = db.collection("pending_requests").document(doc_id)
    doc = doc_ref.get()

    if not doc.exists:
        telegram_helpers.send_message(chat_id, "ℹ️ Request not found or already processed.")
        return

    doc_dict = doc.to_dict()
    current_status = doc_dict.get("status")
    if current_status and current_status != "pending":
        telegram_helpers.send_message(
            chat_id,
            f"ℹ️ This request is already resolved (status: *{current_status}*).",
        )
        return

    subject = doc_dict.get("subject", thread_id)
    COI_GENERATOR_URL = os.environ.get(
        "COI_GENERATOR_URL",
        "https://coi-generator-142497757030.us-west1.run.app",
    )

    if action == "coi_send":
        doc_ref.update({"status": "sent", "resolved_at": datetime.utcnow().isoformat()})
        telegram_helpers.send_message(chat_id, f"✅ Sending COI for:\n*{subject}*")
        _requests.post(
            COI_GENERATOR_URL,
            json={
                "action": "generate_coi",
                "insured_inferred": doc_dict.get("insured_inferred", False),
                "insured_name": doc_dict.get("insured_name", ""),
                "holder_inferred": doc_dict.get("holder_inferred", False),
                "holder_name": doc_dict.get("holder_name", ""),
                "holder_addr_1": doc_dict.get("holder_addr_1", ""),
                "holder_addr_2": doc_dict.get("holder_addr_2", ""),
                "send_to_emails": doc_dict.get("send_to_emails", []),
                "to_emails": doc_dict.get("to_emails", []),
                "cc_emails": doc_dict.get("cc_emails", []),
                "last_message_id": doc_dict.get("last_message_id", ""),
                "thread_id": thread_id,
                "subject_text": f"Re: {subject}",
                "body_text": "Hello,\nPlease see the COI attached.",
            },
            timeout=30,
        )

    elif action == "coi_nosend":
        doc_ref.update({"status": "skipped", "resolved_at": datetime.utcnow().isoformat()})
        telegram_helpers.send_message(chat_id, f"🚫 Skipped COI for:\n*{subject}*")


def _handle_callback(cq: dict) -> None:
    """Route a callback_query to the appropriate handler."""
    callback_id = cq.get("id")
    callback_data = cq.get("data", "")
    chat_id = cq["from"]["id"]

    telegram_helpers.answer_callback(callback_id)

    if not _is_allowed(chat_id):
        print(f"[AUTH] Blocked callback from chat_id={chat_id}")
        return

    # ── New action buttons ("action:<name>") ──────────────────────────────────
    if callback_data == "action:menu":
        loss_run.clear_session(chat_id)
        autopay.clear_session(chat_id)
        menu.send_main_menu(chat_id)

    elif callback_data == "action:loss_run":
        loss_run.start(chat_id)

    elif callback_data == "action:autopay":
        autopay.start(chat_id)

    # ── Legacy COI buttons ("coi_send:<thread_id>" / "coi_nosend:<thread_id>") ─
    elif ":" in callback_data:
        action, thread_id = callback_data.split(":", 1)
        if action in ("coi_send", "coi_nosend"):
            _handle_coi_callback(action, thread_id, chat_id)
        else:
            print(f"[DISPATCH] Unknown callback_data: {callback_data!r}")
            menu.send_main_menu(chat_id)

    else:
        print(f"[DISPATCH] Unknown callback_data: {callback_data!r}")
        menu.send_main_menu(chat_id)


def _handle_message(msg: dict) -> None:
    """Route an incoming message to the appropriate handler."""
    chat_id = msg["chat"]["id"]
    text = msg.get("text", "").strip()

    if not _is_allowed(chat_id):
        print(f"[AUTH] Blocked message from chat_id={chat_id}")
        return

    # /start command → always show main menu and reset any active session
    if text == "/start":
        loss_run.clear_session(chat_id)
        autopay.clear_session(chat_id)
        menu.send_main_menu(chat_id)
        return

    # Delegate to whichever handler is currently active for this chat
    if loss_run.is_awaiting_input(chat_id):
        loss_run.handle_company_name_input(chat_id, text, _gmail_sa_info)
        return

    if autopay.is_awaiting_input(chat_id):
        autopay.handle_company_name_input(chat_id, text, _gmail_sa_info)
        return

    # Default: no active session, show main menu
    menu.send_main_menu(chat_id)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/", methods=["POST"])
def telegram_webhook():
    """Receive Telegram webhook updates."""
    req_start = time.time()
    data = request.get_json(silent=True) or {}
    print(f"[WEBHOOK] Received update keys: {list(data.keys())}")

    if "callback_query" in data:
        _handle_callback(data["callback_query"])

    elif "message" in data:
        _handle_message(data["message"])

    else:
        print(f"[WEBHOOK] Unrecognised update type, ignoring.")

    print(f"[TIMING] Total webhook handler: {time.time() - req_start:.2f}s")
    return ("", 204)




if __name__ == "__main__":
    # Local dev only — Cloud Run uses gunicorn
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)