"""Legacy COI notification handler.

Called via POST /notify_coi by the email_watcher service when it detects a
likely COI request in Tony's inbox. The logic is preserved from the original
telegram_bot/main.py and isolated here so it doesn't pollute the new bot code.

If you need to modify or extend the COI notification flow in the future,
this is the only file you need to touch.
"""
import json
from datetime import datetime

from google.cloud import firestore

from bot import telegram_helpers

_db: firestore.Client | None = None


def _get_db() -> firestore.Client:
    global _db
    if _db is None:
        _db = firestore.Client(database="lion-ins")
    return _db


def _format_email_lists(to_emails: list[str], cc_emails: list[str]) -> str:
    parts = []
    if to_emails:
        parts.append(f"To: {', '.join(to_emails)}")
    if cc_emails:
        parts.append(f"Cc: {', '.join(cc_emails)}")
    return "\n".join(parts)


def handle(data: dict) -> None:
    """Process a COI notification payload from email_watcher."""
    if "thread_id" not in data:
        print("[COI_NOTIFY] Missing thread_id — ignoring request.")
        return

    thread_id = data["thread_id"]
    doc_id = f"msg_{thread_id}"
    db = _get_db()
    doc_ref = db.collection("pending_requests").document(doc_id)

    doc_ref.set({
        "thread_id": thread_id,
        "subject": data.get("subject"),
        "chat_id": data.get("chat_id"),
        "status": "pending",
        "insured_inferred": data.get("insured_inferred"),
        "insured_name": data.get("insured_name"),
        "holder_inferred": data.get("holder_inferred"),
        "holder_name": data.get("holder_name"),
        "holder_addr_1": data.get("holder_addr_1"),
        "holder_addr_2": data.get("holder_addr_2"),
        "send_to_emails": data.get("send_to_emails"),
        "to_emails": data.get("to_emails", []),
        "cc_emails": data.get("cc_emails", []),
        "last_message_id": data.get("last_message_id", ""),
        "timestamp": datetime.utcnow().isoformat(),
    })

    chat_id = data.get("chat_id")
    subject = data.get("subject", thread_id)
    to_emails = data.get("to_emails", [])
    cc_emails = data.get("cc_emails", [])
    email_lists_text = _format_email_lists(to_emails, cc_emails)

    if not data.get("insured_inferred"):
        text = (
            f"Email likely a COI request:\n*{subject}*\n\n"
            "🚨 Could not infer insured name, please check manually"
        )
        if email_lists_text:
            text += f"\n\n{email_lists_text}"
        telegram_helpers.send_message(chat_id, text)

    elif not data.get("holder_inferred"):
        text = (
            f"Email likely a COI request:\n*{subject}*\n\n"
            "🚨 Could not infer holder name, please check manually"
        )
        if email_lists_text:
            text += f"\n\n{email_lists_text}"
        telegram_helpers.send_message(chat_id, text)

    else:
        buttons = [[
            {"text": "✅ Send", "callback_data": f"coi_send:{thread_id}"},
            {"text": "🚫 Don't send", "callback_data": f"coi_nosend:{thread_id}"},
        ]]

        info_lines = [
            f"*Insured:* {data.get('insured_name')}",
            f"*Holder:* {data.get('holder_name')}",
            f"*Address:* {data.get('holder_addr_1')}",
            f"*{data.get('holder_addr_2')}*" if data.get("holder_addr_2") else "",
        ]
        info_text = "\n".join(line for line in info_lines if line)
        message_text = (
            f"Email likely a COI request:\n*{subject}*\n\n{info_text}"
        )
        if email_lists_text:
            message_text += f"\n\n{email_lists_text}"

        telegram_helpers.send_message(chat_id, message_text, buttons)
