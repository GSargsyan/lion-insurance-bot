"""Google Drive + Docs helpers for the telegram bot service.

Responsibilities:
- List all file names in a given Drive folder.
- Download a specific file by its Drive file ID into memory (BytesIO).
- Read the plain-text content of a Google Doc (for insurer-email lookup).
"""
import io
import time

from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

_DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
]


def _drive_service():
    creds, _ = default(scopes=_DRIVE_SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)



def list_filenames_in_folder(folder_id: str) -> list[str]:
    """Return all file names inside the given Drive folder (up to 1000 files)."""
    service = _drive_service()
    query = f"'{folder_id}' in parents and trashed = false"

    names: list[str] = []
    page_token = None

    while True:
        start = time.time()
        results = (
            service.files()
            .list(
                q=query,
                pageSize=1000,
                fields="nextPageToken, files(id, name)",
                pageToken=page_token,
            )
            .execute()
        )
        print(f"[TIMING] Drive files.list: {time.time() - start:.2f}s")

        for f in results.get("files", []):
            names.append(f["name"])

        page_token = results.get("nextPageToken")
        if not page_token:
            break

    return names


def download_file_by_name(folder_id: str, filename: str) -> io.BytesIO | None:
    """Download the first file whose name matches *filename* in the given folder.

    Returns a BytesIO positioned at offset 0, or None if not found.
    """
    service = _drive_service()
    query = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"

    start = time.time()
    results = (
        service.files()
        .list(q=query, pageSize=1, fields="files(id, name)")
        .execute()
    )
    print(f"[TIMING] Drive files.list (by name): {time.time() - start:.2f}s")

    items = results.get("files", [])
    if not items:
        print(f"[DRIVE] File not found: {filename!r} in folder {folder_id}")
        return None

    file_id = items[0]["id"]
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    start = time.time()
    while not done:
        status, done = downloader.next_chunk()
        if status:
            print(f"[DRIVE] Download {filename!r}: {int(status.progress() * 100)}%")
    print(f"[TIMING] Drive files.get_media ({filename}): {time.time() - start:.2f}s")

    fh.seek(0)
    return fh


def read_google_doc_text(doc_id: str) -> str:
    """Return the contents of a Google Sheets file as plain CSV text.

    Uses the Drive API's export endpoint (mimeType=text/csv) which works for
    Google Sheets and requires only the drive.readonly scope — no separate
    Sheets or Docs API needs to be enabled.

    Args:
        doc_id: The Google Sheets file ID.

    Returns:
        CSV text of the first sheet, or "" on failure.
    """
    start = time.time()
    try:
        service = _drive_service()
        response = (
            service.files()
            .export(fileId=doc_id, mimeType="text/csv")
            .execute()
        )
    except Exception as exc:
        print(f"[DRIVE] Failed to export Google Sheet {doc_id!r} as CSV: {exc}")
        return ""

    # response is raw bytes
    if isinstance(response, bytes):
        result = response.decode("utf-8", errors="replace")
    else:
        result = str(response)

    print(f"[TIMING] Drive Sheet export: {time.time() - start:.2f}s")
    print(f"[DRIVE] Google Sheet {doc_id!r}: {len(result)} chars exported")
    return result
