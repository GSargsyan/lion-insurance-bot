"""PDF form filler for Commercial ACH Debit Authorization forms.

Uses PyMuPDF (fitz) to inject text at fixed coordinates — no AcroForm fields
needed. Coordinates were calibrated in scripts/test_fill_autopay.py.

Operates entirely in-memory: accepts raw PDF bytes, returns filled PDF bytes.
"""
import io
import time

import fitz  # pymupdf


# ── Coordinate map (x, y) from test_fill_autopay.py ──────────────────────────
# All values are in points from the top-left corner of page 0.
#
#   Field              x     y
#   ─────────────────────────────
#   Loan / Quote #    300   270
#   Company Name      195   312
#   Address           195   332
#   Phone Number      195   351
#   Email Address     195   370

_FONT_SIZE = 10


def fill_ach_form(
    pdf_bytes: bytes,
    loan_number: str,
    company_name: str,
    address: str,
    phone: str,
    email: str,
) -> bytes:
    """Fill a single-page ACH Debit Authorization PDF and return filled bytes.

    Args:
        pdf_bytes:    Raw bytes of the (single-page) ACH form PDF.
        loan_number:  Loan number string, e.g. "XXX-106980543".
        company_name: Insured company name.
        address:      Full address string, e.g. "123 Main St, Los Angeles, CA 90001".
        phone:        Formatted phone, e.g. "(555) 111-2222".
        email:        Client email address.

    Returns:
        Raw bytes of the filled PDF.
    """
    start = time.time()
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    page = doc[0]

    def _put(x: float, y: float, text: str) -> None:
        """Insert a single line of black text at (x, y)."""
        page.insert_text(
            fitz.Point(x, y),
            text,
            fontsize=_FONT_SIZE,
            color=(0, 0, 0),
        )

    _put(300, 270, loan_number)
    _put(195, 312, company_name)
    _put(195, 332, address)
    _put(195, 351, phone)
    _put(195, 370, email)

    buf = io.BytesIO()
    doc.save(buf)
    doc.close()

    print(f"[PDF_FILLER] Filled ACH form for loan {loan_number!r} in {time.time() - start:.2f}s")
    buf.seek(0)
    return buf.read()
