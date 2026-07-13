"""COI PDF form field extractor + Notices PDF parser for the telegram bot service."""
import io
import time
from io import BytesIO
from pypdf import PdfReader

def extract_acord_fields(pdf_bytes: io.BytesIO) -> dict:
    """Extract relevant AcroForm fields from the COI PDF.

    Args:
        pdf_bytes: BytesIO of the PDF file, positioned at offset 0.

    Returns:
        Dictionary of extracted form fields that are relevant.
    """
    start = time.time()
    result = {}
    try:
        reader = PdfReader(pdf_bytes)
        fields = reader.get_fields()
        
        if fields:
            for k, v in fields.items():
                val = v.value if hasattr(v, 'value') else v.get('/V')
                if val is not None:
                    if hasattr(val, 'get_object'):
                        val = val.get_object()
                    if isinstance(val, bytes):
                        val = val.decode('utf-8', errors='ignore')
                    elif not isinstance(val, str):
                        val = str(val)
                    
                    val = val.strip()
                    if val:
                        result[k] = val

    except Exception as exc:
        print(f"[PDF_PARSER] pypdf form extraction failed: {exc}")

    exact_keys = {
        "Insurer_FullName_A",
        "Insurer_FullName_B",
        "Insurer_FullName_C",
        "Insurer_FullName_D",
        "Policy_AutomobileLiability_PolicyNumberIdentifier_A",
        "Vehicle_PolicyNumber_A",
        "OtherPolicy_PolicyNumberIdentifier_A",
        "OtherPolicy_InsurerLetterCode_A"
    }

    filtered = {}
    for k, v in result.items():
        if k in exact_keys:
            filtered[k] = v
        elif "PHYSICAL DAMAGE" in v.upper():
            filtered[k] = v

    print(f"[TIMING] PDF fields extraction: {time.time() - start:.2f}s")
    print(f"[PDF_PARSER] Extracted {len(filtered)} relevant fields")
    return filtered



def parse_notices_pdf(pdf_bytes: bytes, expected_company: str) -> list[dict]:
    """Parse a FIRST Insurance Funding Notice of Acceptance PDF.

    Extracts loan number, company name, optional DBA, and address for each
    loan found using an LLM call (gpt-4o-mini).  Only the tail portion of
    each page's text (from the "Loan Number" keyword onward, ≤ 800 chars) is
    sent to the LLM to keep token usage minimal.

    Only returns entries whose company name matches *expected_company* —
    this prevents accidental loan-number mix-ups when a single PDF covers
    multiple clients.

    Args:
        pdf_bytes:         Raw bytes of the Notices PDF.
        expected_company:  The company name we are currently processing
                           (used for safety cross-check).

    Returns:
        List of dicts: [{loan_number, company_name, dba, address}, ...]
        - ``dba`` is a string if the insured has a "Doing Business As" name,
          otherwise None.
        - ``address`` is "<address_line1>, <address_line2>".
        Empty list if nothing matches or parsing fails.
    """
    from bot import openai_client  # local import to avoid circular dependency at module load

    results: list[dict] = []
    try:
        reader = PdfReader(BytesIO(pdf_bytes))
    except Exception as exc:
        print(f"[PDF_PARSER] Failed to open Notices PDF: {exc}")
        return results

    for page_num, page in enumerate(reader.pages):
        text = page.extract_text() or ""

        # Skip continuation pages that don't have a new Loan Number header.
        if "Loan Number" not in text:
            continue

        # ── Extract a compact snippet starting from "Loan Number" ─────────────
        # The loan number + insured block always appears in the tail of the page.
        # Sending only this portion keeps the LLM prompt small (saves tokens).
        ln_idx = text.find("Loan Number")
        snippet = text[ln_idx : ln_idx + 800]  # ~600-700 chars is usually enough

        # ── LLM extraction ────────────────────────────────────────────────────
        info = openai_client.extract_notices_loan_info(snippet)
        if not info:
            print(f"[PDF_PARSER] Page {page_num + 1}: LLM extraction returned nothing. Skipping.")
            continue

        loan_number = info["loan_number"]
        company_name = info["company_name"]
        dba = info.get("dba")  # may be None
        address_line1 = info.get("address_line1", "")
        address_line2 = info.get("address_line2", "")
        address = f"{address_line1}, {address_line2}".strip(", ") if address_line1 or address_line2 else ""

        # ── Company name safety check ─────────────────────────────────────────
        if company_name:
            exp_upper = expected_company.upper()
            cmp_upper = company_name.upper()
            # Accept if either contains the other (handles abbreviations / word order)
            if exp_upper not in cmp_upper and cmp_upper not in exp_upper:
                print(
                    f"[PDF_PARSER] Page {page_num + 1}: loan {loan_number} "
                    f"belongs to {company_name!r}, not {expected_company!r}. Skipping."
                )
                continue

        results.append(
            {
                "loan_number": loan_number,
                "company_name": company_name,
                "dba": dba,
                "address": address,
            }
        )
        print(
            f"[PDF_PARSER] Page {page_num + 1}: loan {loan_number} "
            f"matched {company_name!r}, dba={dba!r}, address={address!r}"
        )

    return results

