"""Main menu handler.

Sends the main menu message with all available action buttons.
Add new buttons here as the bot grows — no changes needed in other files.
"""
from bot import telegram_helpers

# ── Button definitions ────────────────────────────────────────────────────────
# Each entry is {"text": "<label>", "callback_data": "<action>"}
# Group them into rows (inner lists).  One button per row is fine.
_MENU_BUTTONS: list[list[dict]] = [
    [{"text": "Create Loss Run Request", "callback_data": "action:loss_run"}],
    [{"text": "Extract Autopay Form", "callback_data": "action:autopay"}],
]

_MENU_TEXT = (
    "*Welcome to Lion Insurance Bot*\n\n"
    "What would you like to do?"
)


def send_main_menu(chat_id: int | str) -> None:
    """Send (or re-send) the main menu to the given chat."""
    telegram_helpers.send_message(chat_id, _MENU_TEXT, buttons=_MENU_BUTTONS)

