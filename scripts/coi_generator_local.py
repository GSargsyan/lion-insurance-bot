"""
Testing script to download certificates of insuranse from google drive, fill them with the COI holder
"""
from pypdf import PdfReader, PdfWriter
import fitz  # PyMuPDF
from fillpdf import fillpdfs

import json
import os
import io
from datetime import date
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload


# Define the scope to access to search and download files
SCOPES = ['https://www.googleapis.com/auth/drive']

def download_from_drive(insured):
  res = []
  creds = None
  # The file token.json stores the user's access and refresh tokens, and is
  # created automatically when the authorization flow completes for the first
  # time.
  if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)
  # If there are no (valid) credentials available, let the user log in.
  if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
      creds.refresh(Request())
    else:
      flow = InstalledAppFlow.from_client_secrets_file(
          "google_key.json", SCOPES
      )
      creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open("token.json", "w") as token:
      token.write(creds.to_json())

  try:
    service = build("drive", "v3", credentials=creds)
    # search the insured file name in drive 'Lion Certificates' folder
    results = (
        service.files()
        .list(
            q=f"name contains '{insured}' and '1KIeq3LHWWklQBanmADUz6XVYodlF2id6' in parents",
            pageSize=10,
            fields="nextPageToken, files(id, name)",
        )
        .execute()
    )
    items = results.get("files", [])

    if not items:
      print("No files found.")
      return

    if len(items) > 2:
        items = items[:2]

    print("Files:")
    # download the files
    for item in items:
        if not item['name'].endswith('.pdf'):
            continue

        print(f"Downloading {item['name']}")
        file_id = item['id']

        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}%.")
        fh.seek(0)
        with open(f'certificates/{item["name"]}', 'wb') as f:
            f.write(fh.read())
        res.append(item['name'])
  except HttpError as error:
    # TODO(developer) - Handle errors from drive API.
    print(f"An error occurred: {error}")

  return res


def check_coi_format(input_pdf_path):
    reader = PdfReader(input_pdf_path)
    fields = reader.get_fields()
    if 'Form_CompletionDate_A' not in fields:
        raise Exception("Certificate is old format")


def fill_pdf(input_pdf_path, output_pdf_path, coi_holder):
    reader = PdfReader(input_pdf_path)
    writer = PdfWriter()

    field_values = {
        'Form_CompletionDate_A': f'{date.today().strftime("%m/%d/%Y")}',
        'CertificateHolder_FullName_A': coi_holder[0],
        'CertificateHolder_MailingAddress_LineOne_A': coi_holder[1],
        'CertificateHolder_MailingAddress_LineTwo_A': coi_holder[2],
    }

    writer.append(reader)

    # Update the form field values
    writer.update_page_form_field_values(
        writer.pages[0],
        field_values,
        auto_regenerate=False,
    )

    with open(output_pdf_path, "wb") as output_stream:
        writer.write(output_stream)


def add_signature_and_flatten(file_name):
    # Open the PDF document
    pdf_path = f'certificates/{file_name}'
    doc = fitz.open(pdf_path)
    
    signature_path = 'signature.png'
    
    page = doc[0]
    
    page_rect = page.rect
    
    margin_right = 80
    margin_bottom = 45
    signature_width = 100
    signature_height = 50
    
    signature_rect = fitz.Rect(
        page_rect.width - signature_width - margin_right,
        page_rect.height - signature_height - margin_bottom,
        page_rect.width - margin_right,
        page_rect.height - margin_bottom
    )
    
    page.insert_image(signature_rect, filename=signature_path, keep_proportion=True)
    
    signed_pdf_path = f'certificates/{file_name.replace(".pdf", "_signed.pdf")}'

    doc.save(signed_pdf_path, incremental=False, deflate=True)

    doc.close()

    fillpdfs.flatten_pdf(signed_pdf_path, signed_pdf_path)


def find_holder(holder):
    with open("brokers.json", "r") as f:
        holders = json.load(f)

        for name, address_lines in holders.items():
            if holder.lower() in name.lower():
                return name, address_lines[0], address_lines[1]

    return None


def get_gmail_conten_by_id(email_id):
    creds = None
    if os.path.exists("gmail_token.json"):
        creds = Credentials.from_authorized_user_file("gmail_token.json", SCOPES)
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            "gmail_key.json", SCOPES
        )
        creds = flow.run_local_server(port=0)

    try:
        service = build("gmail", "v1", credentials=creds)
        message = service.users().messages().get(userId="me", id=email_id).execute()
        return message["snippet"]
    except HttpError as error:
        print(f"An error occurred: {error}")
        return None


if __name__ == '__main__':
    insured_name = input("Enter insured name: ")

    file_names = download_from_drive(insured_name)

    main_file = None
    for file_name in file_names:
        if 'additional' not in file_name.lower():
            main_file = file_name
            break

    check_coi_format(f'certificates/{main_file}')

    holder_name = input("Enter holder name: ")
    coi_holder = find_holder(holder_name)

    new_holder_name = None
    new_holder_address_1 = None
    new_holder_address_2 = None

    if not coi_holder:
        new_holder_name = input("Holder not in list. Enter full holder name: ")
        new_holder_address_1 = input("Enter holder address line 1: ")
        new_holder_address_2 = input("Enter holder address line 2: ")

        with open("brokers.json" , "r") as f:
            holders = json.load(f)
            holders[new_holder_name] = [new_holder_address_1, new_holder_address_2]

        with open("brokers.json", "w") as f:
            json.dump(holders, f, indent=4)

        coi_holder = new_holder_name, new_holder_address_1, new_holder_address_2

    fill_pdf(f'certificates/{main_file}', f'certificates/{main_file}', coi_holder)
    
    # Add signature and flatten the PDF
    add_signature_and_flatten(main_file)
