"""Keep the Streamlit Community Cloud app awake: open it in a real (headless) browser, click
'Yes, get this app back up!' if it is asleep, and wait until the Streamlit app has rendered.
A plain HTTP request does not count as a visit, so a browser is required.

    python deploy/keep_awake.py https://<your-app>.streamlit.app
Exit code 1 if the app could not be confirmed awake (the workflow then alerts on Telegram).
Writes keep_awake.png (final screenshot) for the run's artifacts.
"""
import os
import re
import sys
import time

from playwright.sync_api import sync_playwright

URL = sys.argv[1]
WAKE = re.compile(r"back up|wake", re.I)
APP_SELECTORS = '[data-testid="stApp"], [data-testid="stAppViewContainer"], [data-testid="stMain"]'


def app_rendered(page) -> bool:
    for f in page.frames:
        try:
            el = f.query_selector(APP_SELECTORS)
            if el and len((el.inner_text() or "").strip()) > 20:
                return True
        except Exception:
            pass
    return False


def click_wake(page) -> bool:
    for f in page.frames:
        try:
            btn = f.get_by_role("button", name=WAKE)
            if btn.count():
                btn.first.click()
                return True
        except Exception:
            pass
    return False


def describe(page) -> str:
    out = [f"title={page.title()!r}"]
    for f in page.frames:
        try:
            txt = re.sub(r"\s+", " ", f.inner_text("body", timeout=2000))[:200]
        except Exception:
            txt = "?"
        out.append(f"frame {f.url[:80]} :: {txt}")
    return "\n  ".join(out)


with sync_playwright() as p:
    exe = os.environ.get("PW_CHROMIUM")          # optional: use an existing Chromium binary
    b = p.chromium.launch(**({"executable_path": exe} if exe else {}))
    page = b.new_page(viewport={"width": 1200, "height": 900})
    page.goto(URL, wait_until="domcontentloaded", timeout=90_000)
    time.sleep(8)
    print("Initial page:\n  " + describe(page))
    woke, ok = False, False
    deadline = time.time() + 300
    while time.time() < deadline:
        if app_rendered(page):
            ok = True
            break
        if not woke and click_wake(page):
            print("App was asleep -> clicked wake-up button")
            woke = True
        time.sleep(5)
    if ok:
        print(f"AWAKE: Streamlit app rendered{' after wake-up' if woke else ''}")
        time.sleep(20)            # stay connected briefly so the visit registers
    else:
        print("FAILED: app did not render within 5 minutes. Final page:\n  " + describe(page))
    page.screenshot(path="keep_awake.png")
    b.close()
    sys.exit(0 if ok else 1)
