"""Telegram notifications: bounded retries, never raises into the engine."""
from __future__ import annotations

import html
import logging
import time

import requests

log = logging.getLogger("nifty_algo.notify")


class Notifier:
    def __init__(self, token: str = "", chat_id: str = "", timeout: int = 10):
        self.token, self.chat_id, self.timeout = token, chat_id, timeout
        self.sent: list = []  # local echo, useful for tests and the journal

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.chat_id)

    def send(self, text: str) -> bool:
        self.sent.append(text)
        if not self.enabled:
            log.info("[notify-disabled] %s", text)
            return False
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        for attempt in range(3):
            try:
                r = requests.post(url, data=payload, timeout=self.timeout)
                if r.status_code == 200:
                    return True
                if r.status_code == 429:
                    time.sleep(min(30, int(r.json().get("parameters", {}).get("retry_after", 3))))
                    continue
                log.warning("telegram HTTP %s: %s", r.status_code, r.text[:200])
                return False
            except Exception as e:
                log.warning("telegram error: %s", e)
                time.sleep(1 + attempt)
        return False


def esc(x) -> str:
    return html.escape(str(x))
