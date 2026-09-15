"""
Proof-of-concept: automates logging into the GAIG (Great American Insurance
Group) agent portal -- including its email one-time-code verification step --
and downloads a policy's declaration document.

Credentials live in keys/gaig_credentials.json (gitignored), same convention
as the other secrets in keys/. Requires Playwright:
    pip install playwright && playwright install chromium

This is a proof of concept for one company/policy (hardcoded below). A real
integration would parameterize company/policy, cache the authenticated
session (storage_state) so it doesn't request a fresh email code on every
run, and be wired into send_declaration_documents.py's attachment pipeline.
"""
import os
import re
import json
import time

from playwright.sync_api import sync_playwright

from send_declaration_documents import get_google_services, extract_text

KEYS_DIR = os.path.join(os.path.dirname(__file__), "..", "keys")
GAIG_CREDS_PATH = os.path.join(KEYS_DIR, "gaig_credentials.json")

LOGIN_URL = "https://login-registration.gaig.com/agent"
COMPANY_NAME = "REDDLINE TRANSPORTATION LLC"
OUT_PATH = os.path.join(os.path.dirname(__file__), "..", "reddline_dec_doc.pdf")


def load_gaig_credentials():
    with open(GAIG_CREDS_PATH) as f:
        return json.load(f)


def find_verification_code(gmail_service, after_ts, timeout=90):
    deadline = time.time() + timeout
    seen = set()
    while time.time() < deadline:
        resp = gmail_service.users().messages().list(
            userId="me", q='from:noreply@gaig.com subject:"One-time verification code" newer_than:1h', maxResults=5
        ).execute()
        for m in resp.get("messages", []):
            if m["id"] in seen:
                continue
            msg = gmail_service.users().messages().get(userId="me", id=m["id"], format="full").execute()
            internal_ts = int(msg.get("internalDate", 0)) / 1000
            if internal_ts < after_ts - 15:
                seen.add(m["id"])
                continue
            body = extract_text(msg.get("payload", {})) or ""
            match = re.search(r"Enter a code instead:\s*(\d{6})", body)
            if match:
                return match.group(1)
            seen.add(m["id"])
        time.sleep(3)
    return None


def main():
    creds = load_gaig_credentials()

    print("Authenticating with Gmail (reusing existing project OAuth/service-account setup)...")
    _, gmail_service = get_google_services()
    if not gmail_service:
        print("Could not get Gmail service.")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        print("Navigating to GAIG login page...")
        page.goto(LOGIN_URL)
        page.get_by_placeholder("Enter in your username").fill(creds["username"])
        page.get_by_placeholder("Enter in your password").fill(creds["password"])
        page.get_by_role("button", name="Log In").click()

        print("Waiting for email-verification screen...")
        page.wait_for_selector("text=Send me an email", timeout=20000)
        sent_at = time.time()
        page.get_by_role("button", name="Send me an email").click()
        page.get_by_text("Enter a verification code instead").click()
        page.wait_for_selector('input[name="credentials.passcode"]', timeout=10000)

        print("Verification email requested -- polling Gmail for the code (up to 90s)...")
        code = find_verification_code(gmail_service, sent_at)
        if not code:
            print("Timed out waiting for the verification code email. Aborting.")
            browser.close()
            return

        print("Got verification code, submitting...")
        page.fill('input[name="credentials.passcode"]', code)
        page.locator('input[type="submit"]').click()

        page.wait_for_url(re.compile(r"agentportal\.gaig\.com"), timeout=30000)
        print("Logged in -- on the agent portal dashboard.")

        print(f"Searching Policies for '{COMPANY_NAME}'...")
        page.goto("https://agentportal.gaig.com/policies")
        page.get_by_placeholder("Search by Policy Number or Insured Name").fill(COMPANY_NAME)
        page.get_by_test_id("policies-filter-box-search-button").click()
        page.wait_for_timeout(2000)

        row = page.locator("tr", has_text=COMPANY_NAME).first
        row.get_by_role("link").first.click()
        page.wait_for_load_state("networkidle")

        print("Opening Policy Documents tab...")
        page.get_by_text("Policy Documents", exact=True).click()
        page.wait_for_timeout(2000)

        doc_row = page.locator("tr", has_text="Insured - Renewal Policy Issue").first
        print("Downloading 'Insured - Renewal Policy Issue'...")
        with page.expect_download() as download_info:
            doc_row.get_by_role("button", name=re.compile("Download", re.I)).click()
        download = download_info.value
        download.save_as(OUT_PATH)

        print(f"Saved to {OUT_PATH}")
        browser.close()


if __name__ == "__main__":
    main()
