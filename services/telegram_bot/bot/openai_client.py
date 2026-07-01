"""OpenAI helpers for the telegram bot service.

Currently provides two functions:
- match_company_name: fuzzy-match a user's free-text input against a list of known
  company names using gpt-4o-mini and return the best match (or None).
- resolve_loss_run_drafts: given COI policy info and an insurer-email lookup table,
  return per-insurer draft specs (to-email + grouped policy numbers) using gpt-5-mini.
"""
import json
import time

from openai import OpenAI

# Populated once at startup by main.py after fetching the API key.
_client: OpenAI | None = None


def init(api_key: str) -> None:
    """Call once at startup with the OpenAI API key."""
    global _client
    _client = OpenAI(api_key=api_key)


def _get_client() -> OpenAI:
    if _client is None:
        raise RuntimeError("openai_client.init() must be called before using OpenAI.")
    return _client


def match_company_name(user_input: str, known_names: list[str]) -> str | None:
    """Use gpt-4o-mini to fuzzy-match *user_input* against *known_names*.

    Returns the best-matching name from *known_names*, or None if no reasonable
    match can be found (e.g. too many typos, single character, unrecognizable input).
    """
    names_block = "\n".join(known_names)
    prompt = f"""\
You are given a user query and a list of known company file names from a Drive folder.
Your job is to identify which file name the user most likely intended.

Rules:
- Return ONLY valid JSON: {{"match": "<exact file name from the list>"}} or {{"match": null}}.
- Use "match": null if the query is too vague, has too many typos, or clearly doesn't
  correspond to any name in the list.
- The match must be an EXACT string from the provided list — do not invent names.

User query: {user_input}

Known file names:
{names_block}
"""

    start = time.time()
    print(f"[LLM:match_company_name] PROMPT:\n{'='*60}\n{prompt}\n{'='*60}")
    response = (
        _get_client()
        .chat.completions.create(
            model="gpt-4o-mini",
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a fuzzy company name matching assistant for a "
                        "commercial trucking insurance agency."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
        )
        .choices[0]
        .message.content
    )
    print(f"[LLM:match_company_name] RESPONSE: {response!r}")
    print(f"[TIMING] OpenAI match_company_name: {time.time() - start:.2f}s")

    try:
        result = json.loads(response)
        return result.get("match")  # may be None
    except Exception as exc:
        print(f"[OpenAI] Failed to parse response: {exc}  raw={response!r}")
        return None


def resolve_loss_run_drafts(
    pdf_fields: dict,
    insurer_email_doc_text: str,
    insured_name: str,
) -> list[dict]:
    """Use gpt-5-mini to extract policy data from COI form fields and resolve insurer emails.

    Receives the extracted ACORD form fields and the insurer-email lookup table,
    and returns per-insurer draft specs (one per unique insurer found).

    Returns a list like:
        [
          {
            "to_email": "lossruns@example.com",  # "" if cannot resolve
            "insurer_name": "Southlake Insurance",
            "policy_numbers": ["CUS7300-MTC-102102", "PTY57401772"]
          },
          ...
        ]

    Args:
        pdf_fields: Dictionary of extracted ACORD form fields.
        insurer_email_doc_text: Raw text of the Google Doc insurer-email lookup table.
        insured_name: Display name of the insured (for context/logging).

    Returns:
        List of draft spec dicts. Empty list on failure.
    """
    
    # Format the extracted fields nicely for the LLM
    fields_str = json.dumps(pdf_fields, indent=2)

    prompt = f"""\
You are an insurance agency assistant. You will receive a dictionary of specific extracted
ACORD form fields from a Certificate of Insurance (COI) and an insurer email lookup table.
Your job is to produce Gmail draft specifications for Loss Run Requests.

════════════════════════════════════════════════════════════════════════════════
SECTION 1 — INSURER EMAIL LOOKUP TABLE
This is from our agency Google Doc. It maps insurance company names to their
loss-run request email addresses. Columns: "Insurance Company" | "lr_request_email"
════════════════════════════════════════════════════════════════════════════════
{insurer_email_doc_text}

════════════════════════════════════════════════════════════════════════════════
SECTION 2 — EXTRACTED COI FORM FIELDS (for insured: {insured_name})
════════════════════════════════════════════════════════════════════════════════
{fields_str}

════════════════════════════════════════════════════════════════════════════════
SECTION 3 — YOUR TASK
════════════════════════════════════════════════════════════════════════════════

Step 1: Identify the insurer names from the following fields:
  Insurer_FullName_A
  Insurer_FullName_B
  Insurer_FullName_C
  Insurer_FullName_D

Step 2: Find the policies and which insurer letter (A, B, C, or D) they map to.
  - For AL (Automobile Liability):
    Policy #: Policy_AutomobileLiability_PolicyNumberIdentifier_A
    Letter code (A, B, C, or D): Vehicle_PolicyNumber_A
  
  - For MTC (Motor Truck Cargo):
    Policy #: OtherPolicy_PolicyNumberIdentifier_A
    Letter code (A, B, C, or D): OtherPolicy_InsurerLetterCode_A

  - For APD / Physical Damage:
    This is written in a text block, which will be in one of the fields containing "Physical Damage" or "Description_of_Operations". Look for a single letter followed by "PHYSICAL DAMAGE" and then the policy number. Extract that letter code and policy number.

Step 3: Map each policy to its exact insurer name based on the letter code.

Step 4: Match that insurer name to the corresponding email in the lookup table.
  IMPORTANT: The names might differ in format (e.g., "SOUTHLAKE INSURANCE" vs "Southlake Ins. Co."). Use your best judgment to match them correctly. If no match is confidently found, use "" for the email address.

Step 5: Group policies that share the same insurer company into one list.

Step 6: Return your answer as a JSON object mapping the exact insurer name (from the COI) to a list containing:
  1) A list of policy numbers
  2) The matched email address

Example format:
{{
  "Southlake Ins. Co.": [["CUS7300-MTC-102102", "PTY57401772"], "lossruns@southlake.com"],
  "Other Insurer Co.": [["AU1234567"], "claims@otherinsurerco.com"]
}}

If no policies are found at all, return an empty JSON object: {{}}
Do NOT include the insured name in the policy_numbers list.
"""

    start = time.time()
    response = (
        _get_client()
        .chat.completions.create(
            model="gpt-5-mini",
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert insurance agency assistant specializing in reading ACORD certificates of insurance. Extract policy data and map insurers to their loss-run email contacts. Always respond with a valid JSON object mapping insurer names to policies and emails.",
                },
                {"role": "user", "content": prompt},
            ],
        )
        .choices[0]
        .message.content
    )
    print(f"[TIMING] OpenAI resolve_loss_run_drafts: {time.time() - start:.2f}s")

    parsed = json.loads(response)

    # Validate and normalise each draft spec
    result = []
    for insurer_name, data in parsed.items():
        if not isinstance(data, list) or len(data) < 2:
            continue
        policy_numbers = data[0]
        to_email = data[1]
        
        if not isinstance(policy_numbers, list):
            continue
        if not policy_numbers:
            continue
            
        result.append({
            "to_email": str(to_email) if to_email else "",
            "insurer_name": str(insurer_name),
            "policy_numbers": [str(p) for p in policy_numbers],
        })

    print(f"[OpenAI] Resolved {len(result)} draft spec(s): "
          + str([{d['insurer_name']: d['policy_numbers']} for d in result]))
    return result
