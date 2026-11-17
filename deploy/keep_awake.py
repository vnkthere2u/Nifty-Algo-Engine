"""Keep the Streamlit Community Cloud app awake: open it in a real (headless) browser, click
'Yes, get this app back up!' if it is asleep, and wait until the dashboard actually renders.
A plain HTTP request does not count as a visit, so a browser is required.

    python deploy/keep_awake.py https://<your-app>.streamlit.app  [expected_text]
Exit code 1 if the app could not be confirmed awake (the workflow then alerts on Telegram).
"""
import os
import re
import sys
import time

from playwright.sync_api import sync_playwright

URL = sys.argv[1]
EXPECT = sys.argv[2] if len(sys.argv) > 2 else "Algo Signals"
WAKE = re.compile(r"get this app back up|wake.*app|yes,? get this app", re.I)


def rendered(page) -> bool:
    for f in page.frames:
        try:
            if EXPECT in f.inner_text("body", timeout=2000):
                return True
        except Exception:
            pass
    return False


with sync_playwright() as p:
    exe = os.environ.get("PW_CHROMIUM")          # optional: use an existing Chromium binary
    b = p.chromium.launch(**({"executable_path": exe} if exe else {}))
    page = b.new_page()
    page.goto(URL, wait_until="domcontentloaded", timeout=90_000)
    woke = False
    deadline = time.time() + 240
    while time.time() < deadline:
        if rendered(page):
            print(f"AWAKE: dashboard rendered{' after wake-up click' if woke else ''}")
            time.sleep(20)          # stay connected briefly so the visit registers
            b.close()
            sys.exit(0)
        btn = page.get_by_role("button", name=WAKE)
        if btn.count():
            print("App was asleep -> clicking wake-up button")
            btn.first.click()
            woke = True
        time.sleep(5)
    print("FAILED: dashboard did not render within 4 minutes")
    b.close()
    sys.exit(1)
