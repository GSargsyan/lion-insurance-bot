"""Endorsement Request handler.

Conversation flow:
  1. User clicks "Create Endorsement Request" -> bot asks for type (Driver or Unit).
  2. User clicks Driver or Unit -> bot asks for action (Add, Swap, Remove).
  3. User clicks Add, Swap, or Remove -> bot asks for insured company name.
  4. User types name -> bot fuzzy-matches using Drive folders and LLM.
     - If selection is NOT Unit -> Add:
         Prints "Not implemented", clears session, and returns to main menu.
     - If selection IS Unit -> Add:
         Saves insured name, transitions to VIN collection state.
  5. User types VIN -> bot validates VIN (alphanumeric, no spaces, length 10-17, has digits).
     Transitions to Value collection state.
  6. User types Unit Value -> bot validates numeric format, displays collected details,
     prints "Not implemented" message, clears session, and returns to main menu.
"""
from google.cloud import firestore
from bot import drive_client, openai_client, telegram_helpers
from bot.handlers import menu

DRIVE_CERTS_FOLDER_ID = "1KIeq3LHWWklQBanmADUz6XVYodlF2id6"

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

def clear_session(chat_id: int | str) -> None:
    """Clear all endorsement-related session fields from Firestore."""
    ref = _session_ref(chat_id)
    ref.set({
        "awaiting": firestore.DELETE_FIELD,
        "endorsement_type": firestore.DELETE_FIELD,
        "endorsement_action": firestore.DELETE_FIELD,
        "insured_name": firestore.DELETE_FIELD,
        "vin": firestore.DELETE_FIELD,
        "unit_value": firestore.DELETE_FIELD
    }, merge=True)

def start(chat_id: int | str) -> None:
    """Step 1: Ask if they want driver or unit endorsement."""
    telegram_helpers.send_message(
        chat_id,
        "This feature is not implemented yet.",
        buttons=[[{"text": "🏠 Main Menu", "callback_data": "action:menu"}]]
    )
    return

    clear_session(chat_id)
    telegram_helpers.send_message(
        chat_id,
        "✍️ *Create Endorsement Request*\n\n"
        "Please select the endorsement type:",
        buttons=[
            [
                {"text": "👤 Driver Endorsement", "callback_data": "endorsement:type:driver"},
                {"text": "🚛 Unit Endorsement", "callback_data": "endorsement:type:unit"},
            ],
            [
                {"text": "🏠 Main Menu", "callback_data": "action:menu"}
            ]
        ]
    )

def handle_type_selection(chat_id: int | str, etype: str) -> None:
    """Step 2: Save type and ask for action (Add, Swap, Remove)."""
    ref = _session_ref(chat_id)
    ref.set({"endorsement_type": etype}, merge=True)
    
    telegram_helpers.send_message(
        chat_id,
        "Select the action for this endorsement:",
        buttons=[
            [
                {"text": "➕ Add", "callback_data": "endorsement:action:add"},
                {"text": "🔄 Swap", "callback_data": "endorsement:action:swap"},
                {"text": "➖ Remove", "callback_data": "endorsement:action:remove"},
            ],
            [
                {"text": "🏠 Main Menu", "callback_data": "action:menu"}
            ]
        ]
    )

def handle_action_selection(chat_id: int | str, eaction: str) -> None:
    """Step 3: Save action and ask for insured name."""
    ref = _session_ref(chat_id)
    ref.set({"endorsement_action": eaction}, merge=True)
    _set_awaiting(chat_id, "endorsement_company_name")
    
    telegram_helpers.send_message(
        chat_id,
        "Please type the insured company name (e.g. _aag espindola_):",
        buttons=[
            [
                {"text": "🏠 Main Menu", "callback_data": "action:menu"}
            ]
        ]
    )

def handle_company_name_input(chat_id: int | str, user_text: str) -> None:
    """Step 4: Resolve company name using Drive filenames & LLM."""
    try:
        known_names = drive_client.list_filenames_in_folder(DRIVE_CERTS_FOLDER_ID)
    except Exception as exc:
        print(f"[ENDORSEMENT] Drive listing failed: {exc}")
        telegram_helpers.send_message(
            chat_id,
            "❌ Failed to reach Google Drive. Please try again later.",
            buttons=[[{"text": "🏠 Main Menu", "callback_data": "action:menu"}]],
        )
        clear_session(chat_id)
        return

    if not known_names:
        telegram_helpers.send_message(
            chat_id,
            "❌ No company files found in Drive. Please contact support.",
            buttons=[[{"text": "🏠 Main Menu", "callback_data": "action:menu"}]],
        )
        clear_session(chat_id)
        return

    # Fuzzy match via LLM
    try:
        matched_name = openai_client.match_company_name(user_text, known_names)
    except Exception as exc:
        print(f"[ENDORSEMENT] OpenAI matching failed: {exc}")
        telegram_helpers.send_message(
            chat_id,
            "❌ Matching service unavailable. Please try again later.",
            buttons=[[{"text": "🏠 Main Menu", "callback_data": "action:menu"}]],
        )
        clear_session(chat_id)
        return

    # If no match
    if not matched_name:
        telegram_helpers.send_message(
            chat_id,
            "🤷 Could not find a matching company for *{}*.\n\n"
            "Please try again with a different name, or return to the main menu.".format(
                user_text
            ),
            buttons=[[{"text": "🏠 Main Menu", "callback_data": "action:menu"}]],
        )
        # Keep awaiting state so user can try again
        return

    display_name = matched_name.removesuffix(".pdf")
    
    # Retrieve stored endorsement options
    doc = _session_ref(chat_id).get()
    doc_dict = doc.to_dict() if doc.exists else {}
    etype = doc_dict.get("endorsement_type", "")
    eaction = doc_dict.get("endorsement_action", "")

    if etype == "unit" and eaction == "add":
        # Save matched company name and transition to VIN input
        ref = _session_ref(chat_id)
        ref.set({"insured_name": display_name}, merge=True)
        _set_awaiting(chat_id, "endorsement_vin")
        
        telegram_helpers.send_message(
            chat_id,
            "🚘 *Unit Endorsement: Add*\n\n"
            "Please enter the VIN (Vehicle Identification Number):",
            buttons=[[{"text": "🏠 Main Menu", "callback_data": "action:menu"}]],
        )
    else:
        # Not implemented case
        confirm_text = (
            f"⚠️ *Endorsement request not implemented yet*\n\n"
            f"• *Insured:* {display_name}\n"
            f"• *Type:* {etype.capitalize() if etype else '(unknown)'}\n"
            f"• *Action:* {eaction.capitalize() if eaction else '(unknown)'}\n\n"
            f"_(This feature is not implemented yet)_"
        )
        clear_session(chat_id)
        telegram_helpers.send_message(chat_id, confirm_text)
        menu.send_main_menu(chat_id)

def handle_vin_input(chat_id: int | str, user_text: str) -> None:
    """Step 5: Receive and validate VIN, then transition to unit value input."""
    cleaned_vin = user_text.strip().replace("-", "")
    
    # Basic check to filter out names or phrases:
    # A VIN is alphanumeric, contains no spaces, is between 10 and 17 characters, and contains digits.
    if (" " in cleaned_vin or 
        not cleaned_vin.isalnum() or 
        len(cleaned_vin) < 10 or 
        len(cleaned_vin) > 17 or 
        not any(char.isdigit() for char in cleaned_vin)):
        
        telegram_helpers.send_message(
            chat_id,
            "❌ *Invalid VIN format.*\n\n"
            "A VIN should be a single alphanumeric code (between 10 and 17 characters) without spaces, containing both letters and numbers.\n"
            "Please try entering the VIN again:",
            buttons=[[{"text": "🏠 Main Menu", "callback_data": "action:menu"}]],
        )
        return

    # Save VIN and transition to unit value input
    ref = _session_ref(chat_id)
    ref.set({"vin": cleaned_vin.upper()}, merge=True)
    _set_awaiting(chat_id, "endorsement_value")
    
    telegram_helpers.send_message(
        chat_id,
        "💰 *Unit Endorsement: Add*\n\n"
        "Please enter the unit value (e.g. _50000_):",
        buttons=[[{"text": "🏠 Main Menu", "callback_data": "action:menu"}]],
    )

def handle_value_input(chat_id: int | str, user_text: str) -> None:
    """Step 6: Receive unit value, validate it, print collected details, and clear session."""
    cleaned_val = user_text.strip().replace("$", "").replace(",", "")
    if not cleaned_val.isdigit():
        telegram_helpers.send_message(
            chat_id,
            "❌ *Invalid Value format.*\n\n"
            "Please enter a valid numeric value (e.g. _50000_ or _50,000_):",
            buttons=[[{"text": "🏠 Main Menu", "callback_data": "action:menu"}]],
        )
        return

    # Retrieve all saved info for display
    doc = _session_ref(chat_id).get()
    doc_dict = doc.to_dict() if doc.exists else {}
    insured_name = doc_dict.get("insured_name", "(unknown)")
    vin = doc_dict.get("vin", "(unknown)")
    
    confirm_text = (
        f"⚠️ *Endorsement request not implemented yet*\n\n"
        f"Collected details:\n"
        f"• *Insured:* {insured_name}\n"
        f"• *Type:* Unit\n"
        f"• *Action:* Add\n"
        f"• *VIN:* {vin}\n"
        f"• *Value:* ${int(cleaned_val):,}\n\n"
        f"_(This feature is not implemented yet)_"
    )
    
    clear_session(chat_id)
    telegram_helpers.send_message(chat_id, confirm_text)
    menu.send_main_menu(chat_id)

def handle_message_input(chat_id: int | str, text: str) -> None:
    """Route message to correct step depending on the current awaiting state."""
    state = _get_awaiting(chat_id)
    if state == "endorsement_company_name":
        handle_company_name_input(chat_id, text)
    elif state == "endorsement_vin":
        handle_vin_input(chat_id, text)
    elif state == "endorsement_value":
        handle_value_input(chat_id, text)

def is_awaiting_input(chat_id: int | str) -> bool:
    """Return True if this chat is currently waiting for any text input for endorsement."""
    state = _get_awaiting(chat_id)
    return state in ("endorsement_company_name", "endorsement_vin", "endorsement_value")
