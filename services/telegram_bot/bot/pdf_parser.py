"""COI PDF form field extractor + Notices PDF parser for the telegram bot service."""
import io
import re
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

    Extracts loan number, company name, and address for each loan found.
    Only returns entries whose company name matches *expected_company* —
    this prevents accidental loan-number mix-ups when a single PDF covers
    multiple clients.

    Args:
        pdf_bytes:         Raw bytes of the Notices PDF.
        expected_company:  The company name we are currently processing
                           (used for safety cross-check).

    Returns:
        List of dicts: [{loan_number, company_name, address}, ...]
        Empty list if nothing matches or parsing fails.
    """
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

        # ── Loan number ───────────────────────────────────────────────────────
        # Actual PDF text: "Loan Number\nXXX - 106980543\nRefer to this..."
        # The "XXX" is a literal fixed prefix on all FIRST Insurance Funding loans.
        # We capture only the numeric part after "XXX - ".
        loan_match = re.search(r"Loan Number\s+XXX\s*-\s*(\d+)", text)
        if not loan_match:
            continue
        loan_number = loan_match.group(1).strip()  # e.g. "106980543"


        # ── Insured block ─────────────────────────────────────────────────────
        # Pattern: "Insured\r\nCOMPANY NAME\r\nSTREET ADDRESS\r\nCITY, STATE ZIP"
        insured_match = re.search(
            r"Insured\r?\n(.+)\r?\n(.+)\r?\n([\w\s]+,\s+[A-Z]{2}\s+\d{5}[^\r\n]*)",
            text,
        )
        company_name: str | None = None
        address: str | None = None
        if insured_match:
            company_name = insured_match.group(1).strip()
            addr_line1 = insured_match.group(2).strip()
            addr_line2 = insured_match.group(3).strip()
            address = f"{addr_line1}, {addr_line2}"

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
                "address": address,
            }
        )
        print(
            f"[PDF_PARSER] Page {page_num + 1}: loan {loan_number} "
            f"matched {company_name!r}, address={address!r}"
        )

    return results


