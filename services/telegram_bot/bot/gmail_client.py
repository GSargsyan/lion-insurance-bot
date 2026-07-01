"""Gmail helpers for the telegram bot service.

Currently provides one function:
- create_loss_run_drafts: create Gmail drafts in Tony's mailbox, one per insurer,
  with correct subject/body format including all policy numbers for that insurer.
"""
import base64
import time
from email.message import EmailMessage

from google.oauth2 import service_account
from googleapiclient.discovery import build

_GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.compose",
    "https://www.googleapis.com/auth/gmail.labels",
    "https://www.googleapis.com/auth/gmail.modify",
]

_FROM_EMAIL = "tony@lioninsurance.us"


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
