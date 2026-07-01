"""COI PDF form field extractor for the telegram bot service.

Extracts specific AcroForm fields required for Loss Run Requests.
"""
import io
import time
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

