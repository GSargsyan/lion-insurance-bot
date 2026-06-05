"""Telegram API helpers.

All Telegram HTTP calls go through this module so the rest of the code
never has to construct URLs or deal with raw HTTP.
"""
import time
import requests


# Populated once at startup by main.py after fetching the token.
_TELEGRAM_API_BASE: str = ""


def init(token: str) -> None:
    """Call once at startup with the bot token."""
    global _TELEGRAM_API_BASE
    _TELEGRAM_API_BASE = f"https://api.telegram.org/bot{token}"


def _base() -> str:
    if not _TELEGRAM_API_BASE:
        raise RuntimeError("telegram_helpers.init() must be called before using the API.")
    return _TELEGRAM_API_BASE


def answer_callback(callback_query_id: str) -> None:
    """Acknowledge a callback query so Telegram removes its loading spinner."""
    start = time.time()
    requests.post(
        f"{_base()}/answerCallbackQuery",
        json={"callback_query_id": callback_query_id},
        timeout=10,
    )
    print(f"[TIMING] answerCallbackQuery: {time.time() - start:.2f}s")


def send_message(
    chat_id: int | str,
    text: str,
    buttons: list[list[dict]] | None = None,
    parse_mode: str = "Markdown",
) -> None:
    """Send a text message, optionally with inline keyboard buttons.

    buttons format (Telegram inline_keyboard):
        [
            [{"text": "Label", "callback_data": "some:data"}, ...],
            ...
        ]
    """
    payload: dict = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
    }
    if buttons:
        payload["reply_markup"] = {"inline_keyboard": buttons}

    start = time.time()
    requests.post(f"{_base()}/sendMessage", json=payload, timeout=10)
    print(f"[TIMING] sendMessage: {time.time() - start:.2f}s")
