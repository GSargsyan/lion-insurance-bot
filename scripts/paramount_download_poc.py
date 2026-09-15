"""
Proof-of-concept: automates logging into the Paramount (ISC/onlinemga) agent
portal and downloading a policy's declaration document.

Credentials live in keys/paramount_credentials.json (gitignored), same
convention as the other secrets in keys/. Requires Playwright:
    pip install playwright && playwright install chromium

Flow: log in -> click "OMGA" (bottom-left) to reach the main dashboard ->
Advanced search by Insured Company Name -> open the matched policy's detail
page -> find the named row in the Policy Documents table -> click its "eye"
action (which does a window.open to a generatePolicyDocs URL) -> fetch that
URL directly with the authenticated session and save the PDF bytes.

This is a proof of concept for one company/document (hardcoded below). A
real integration would parameterize company/doc name and be wired into
send_declaration_documents.py's attachment pipeline.
"""
import os
import json

from playwright.sync_api import sync_playwright

KEYS_DIR = os.path.join(os.path.dirname(__file__), "..", "keys")
CREDS_PATH = os.path.join(KEYS_DIR, "paramount_credentials.json")

LOGIN_URL = "https://paramount.onlinemga.com/amp/login?iss=https%3A%2F%2Fauth.onlinemga.com%2F"
COMPANY_NAME = "JUST GO TRUCKING LLC"
DOCUMENT_NAME = "AGW Domestic Policy Document"
DOWNLOADS_DIR = os.path.join(os.path.dirname(__file__), "..", "downloads")
OUT_PATH = os.path.join(DOWNLOADS_DIR, "just_go_trucking_agw.pdf")


def load_credentials():
    with open(CREDS_PATH) as f:
        return json.load(f)


def main():
    creds = load_credentials()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.add_init_script(
            "window.__openedUrls = []; "
            "const o = window.open; "
            "window.open = function(url, ...args) { window.__openedUrls.push(url); return o.apply(window, [url, ...args]); };"
        )

        print("Logging into Paramount...")
        page.goto(LOGIN_URL)
        page.get_by_role("button", name="CONTINUE TO LOG IN").wait_for(timeout=20000)
        page.get_by_role("button", name="CONTINUE TO LOG IN").click()
        page.wait_for_selector("#username", timeout=20000)
        page.fill("#username", creds["username"])
        page.fill("#password", creds["password"])
        page.get_by_role("button", name="Continue").click()
        page.wait_for_url("**/coverages", timeout=20000)
        page.wait_for_timeout(1500)

        print("Opening main dashboard via OMGA...")
        page.get_by_role("link", name="OMGA").click()
        page.wait_for_url("**/amp/list", timeout=20000)
        page.wait_for_timeout(1500)

        print(f"Searching for '{COMPANY_NAME}'...")
        page.get_by_text("Advanced", exact=True).click()
        page.wait_for_timeout(1000)
        page.fill('input[name="company_name"]', COMPANY_NAME)
        page.get_by_role("button", name="Search").click()
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(2000)

        app_id = page.locator("tr.itemRow").first.get_attribute("data-id")
        if not app_id:
            print("No matching policy found.")
            browser.close()
            return
        print(f"Found App ID {app_id}, opening detail page...")

        page.goto(f"https://paramount.onlinemga.com/amp/detail/view/{app_id}#doctab-1")
        page.wait_for_selector("tr.policyDocumentLink", timeout=20000)
        page.wait_for_timeout(1500)

        row = page.locator("tr.policyDocumentLink", has_text=DOCUMENT_NAME)
        row.locator("span.file").evaluate("e => e.click()")
        page.wait_for_timeout(2000)

        opened = page.evaluate("window.__openedUrls")
        if not opened:
            print("Document link click did not open a document URL.")
            browser.close()
            return

        doc_url = "https://paramount.onlinemga.com" + opened[-1]
        print(f"Fetching document from {doc_url}...")
        resp = context.request.get(doc_url)
        if resp.status != 200:
            print(f"Download failed with status {resp.status}")
            browser.close()
            return

        os.makedirs(DOWNLOADS_DIR, exist_ok=True)
        with open(OUT_PATH, "wb") as f:
            f.write(resp.body())
        print(f"Saved to {OUT_PATH}")
        browser.close()


if __name__ == "__main__":
    main()
