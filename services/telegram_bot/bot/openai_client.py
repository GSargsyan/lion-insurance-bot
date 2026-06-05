"""OpenAI helpers for the telegram bot service.

Currently provides one function:
- match_company_name: fuzzy-match a user's free-text input against a list of known
  company names using gpt-4o-mini and return the best match (or None).
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
    print(f"[TIMING] OpenAI match_company_name: {time.time() - start:.2f}s")

    try:
        result = json.loads(response)
        return result.get("match")  # may be None
    except Exception as exc:
        print(f"[OpenAI] Failed to parse response: {exc}  raw={response!r}")
        return None
