"""
Proof-of-concept: automates logging into the Trinity (TRU / tmsystems.ai)
agent portal and downloading a bound account's declaration/policy documents.

Credentials live in keys/trinity_credentials.json (gitignored), same
convention as the other secrets in keys/. Requires Playwright:
    pip install playwright && playwright install chromium

Flow: log in -> Bound Accounts -> search by insured name -> open the
matched submission -> step 7 "Policy Docs" -> expand every line-of-business
accordion (Auto Physical Damage, Motor Truck Cargo, etc. -- varies per
policy) -> for each one, download the row matching DOCUMENT_NAME.

This is a proof of concept for one company/document (hardcoded below). A
real integration would parameterize company/doc name and be wired into
send_declaration_documents.py's attachment pipeline.
"""
import os
import json

from playwright.sync_api import sync_playwright

KEYS_DIR = os.path.join(os.path.dirname(__file__), "..", "keys")
CREDS_PATH = os.path.join(KEYS_DIR, "trinity_credentials.json")
DOWNLOADS_DIR = os.path.join(os.path.dirname(__file__), "..", "downloads")

LOGIN_URL = "https://tru.tmsystems.ai/sign-in"
COMPANY_NAME = "MODIG INVESTMENTS LLC"
DOCUMENT_NAME = "Evidence of Insurance"


def load_credentials():
    with open(CREDS_PATH) as f:
        return json.load(f)


def main():
    creds = load_credentials()
    os.makedirs(DOWNLOADS_DIR, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        print("Logging into Trinity...")
        page.goto(LOGIN_URL)
        page.wait_for_selector("#email", timeout=20000)
        page.fill("#email", creds["email"])
        page.keyboard.press("Enter")
        page.wait_for_selector("#password", timeout=20000)
        page.fill("#password", creds["password"])
        page.keyboard.press("Enter")
        page.wait_for_url("https://tru.tmsystems.ai/", timeout=20000)
        page.wait_for_timeout(1500)

        print("Opening Bound Accounts...")
        page.get_by_text("Bound Accounts", exact=True).click()
        page.wait_for_url("**/bound-accounts", timeout=20000)
        page.wait_for_timeout(1500)

        print(f"Searching for '{COMPANY_NAME}'...")
        page.fill('input[placeholder="Search"]', COMPANY_NAME)
        page.wait_for_timeout(2000)

        first_row = page.locator("tr[data-row-index]").first
        if first_row.count() == 0:
            print("No matching submission found.")
            browser.close()
            return
        submission_id = first_row.locator("button").first.inner_text()
        print(f"Found submission {submission_id}, opening it...")
        first_row.locator("button").first.click()
        page.wait_for_url("**/submissions/bound/**", timeout=20000)
        page.wait_for_timeout(2000)

        print("Opening Policy Docs (step 7)...")
        page.get_by_text("Policy Docs", exact=True).first.click()
        page.wait_for_timeout(2000)

        section_headers = page.locator(".lob-specific-accordion__title")
        n_sections = section_headers.count()
        print(f"Found {n_sections} line-of-business section(s), expanding all...")
        for i in range(n_sections):
            section_headers.nth(i).click()
            page.wait_for_timeout(800)

        rows = page.locator("tr", has_text=DOCUMENT_NAME)
        n_rows = rows.count()
        if n_rows == 0:
            print(f"No '{DOCUMENT_NAME}' rows found in any section.")
            browser.close()
            return

        print(f"Found {n_rows} '{DOCUMENT_NAME}' row(s), downloading...")
        for i in range(n_rows):
            row = rows.nth(i)
            with page.expect_download(timeout=15000) as dl_info:
                row.get_by_text("Download", exact=True).click()
            download = dl_info.value
            suffix = f"_{i + 1}" if n_rows > 1 else ""
            out_path = os.path.join(DOWNLOADS_DIR, f"modig_evidence_of_insurance{suffix}.pdf")
            download.save_as(out_path)
            print(f"Saved to {out_path}")

        browser.close()


if __name__ == "__main__":
    main()
