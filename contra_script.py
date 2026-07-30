#!/usr/bin/env python3
"""Contra Project Monitor.

Monitors https://contra.com/jobs, extracts complete opportunity details from
JSON-LD, Contra's embedded Relay state, and semantic DOM fallbacks, stores them
in SQLite, and emails each genuinely new opportunity once.

The first successful complete scan is a baseline and sends no project emails.

Required third-party packages:
    selenium
    python-dotenv

Optional local fallback:
    webdriver-manager
"""

from __future__ import annotations

import argparse
import ast
import html as html_lib
import json
import os
import re
import shutil
import signal
import smtplib
import socket
import sqlite3
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html.parser import HTMLParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, MutableMapping, Sequence
from urllib.parse import urljoin, urlparse

from dotenv import load_dotenv

try:
    from selenium import webdriver
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    SELENIUM_AVAILABLE = True
except ImportError:  # Offline parser/DB commands remain usable without Selenium.
    webdriver = None  # type: ignore[assignment]
    TimeoutException = Exception  # type: ignore[assignment,misc]
    Options = Service = By = WebDriverWait = None  # type: ignore[assignment]
    SELENIUM_AVAILABLE = False

load_dotenv(Path(__file__).with_name(".env"))

UTC = timezone.utc
CONTRA_ORIGIN = "https://contra.com"
OPPORTUNITY_PATH_RE = re.compile(r"^/opportunity/([A-Za-z0-9_-]+)$")
SOURCE_ID_RE = re.compile(r"^[A-Za-z0-9]{3,64}$")
NOTIFICATION_STATES = {"baseline", "pending", "sent", "failed"}
BLOCKNOTE_KEYS = {
    "blocknotedescription",
    "blocknote",
    "descriptionblocks",
    "blocks",
}
UI_ONLY_LINES = {
    "apply",
    "apply now",
    "save",
    "save job",
    "share",
    "report",
    "report job",
    "back to jobs",
    "sign in",
    "log in",
    "dismiss",
    "for independents",
    "for companies",
    "use cases",
    "resources",
    "challenges",
    "hire freelancers",
    "company",
    "contact",
    "terms & conditions",
    "privacy policy",
    "cookie policy",
    "contact support",
    "hire creatives get hired",
}


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int, minimum: int | None = None) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw is not None else default
    except (TypeError, ValueError):
        value = default
    if minimum is not None:
        value = max(minimum, value)
    return value


def resolve_volume_path(filename: str, fallback: str, env_name: str) -> str:
    explicit = os.getenv(env_name, "").strip()
    if explicit:
        return explicit
    volume = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
    if volume:
        return str(Path(volume) / filename)
    return fallback


class Config:
    JOBS_URL = os.getenv("CONTRA_JOBS_URL", "https://contra.com/jobs").strip()
    SQLITE_PATH = resolve_volume_path(
        "contra_monitor.db", "data/contra_monitor.db", "SQLITE_PATH"
    )
    COOKIE_FILE = resolve_volume_path(
        "contra_cookies.json", "/tmp/contra_cookies.json", "COOKIE_FILE"
    )
    EVIDENCE_DIR = resolve_volume_path(
        "evidence", "/tmp/contra-evidence", "EVIDENCE_DIR"
    )

    CHECK_INTERVAL = env_int("CHECK_INTERVAL", 60, 10)
    PAGE_WAIT_SECONDS = env_int("PAGE_WAIT_SECONDS", 20, 5)
    DETAIL_WAIT_SECONDS = env_int("DETAIL_WAIT_SECONDS", 15, 5)
    MAX_SCROLL_ROUNDS = env_int("MAX_SCROLL_ROUNDS", 20, 1)
    STABLE_SCROLL_ROUNDS = env_int("STABLE_SCROLL_ROUNDS", 2, 1)
    SCROLL_PAUSE_SECONDS = max(0.5, float(os.getenv("SCROLL_PAUSE_SECONDS", "1.5")))
    LOGIN_RETRY_INTERVAL = env_int("LOGIN_RETRY_INTERVAL", 300, 30)
    EMAIL_RETRY_MINUTES = env_int("EMAIL_RETRY_MINUTES", 30, 1)
    MAX_EMAIL_ATTEMPTS = env_int("MAX_EMAIL_ATTEMPTS", 5, 1)
    EVIDENCE_RETENTION_HOURS = env_int("EVIDENCE_RETENTION_HOURS", 24, 1)

    HEADLESS = env_bool("HEADLESS", True)
    CHROME_BIN = os.getenv("CHROME_BIN", "").strip()
    CHROMEDRIVER_PATH = os.getenv(
        "CHROMEDRIVER_PATH", "/usr/bin/chromedriver"
    ).strip()

    SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com").strip()
    SMTP_PORT = env_int("SMTP_PORT", 587, 1)
    SENDER_EMAIL = os.getenv("SENDER_EMAIL", "").strip()
    SENDER_PASSWORD = os.getenv("SENDER_PASSWORD", "")
    RECIPIENT_EMAILS = tuple(
        item.strip()
        for item in os.getenv("RECIPIENT_EMAILS", "aliazzamdon@gmail.com").split(",")
        if item.strip()
    )
    ERROR_RECIPIENTS = tuple(
        item.strip()
        for item in (
            os.getenv("ERROR_RECIPIENTS")
            or os.getenv("ERROR_RECIPIENT")
            or os.getenv("error_recipent")
            or ""
        ).split(",")
        if item.strip()
    )
    ERROR_EMAIL_COOLDOWN_MINUTES = env_int(
        "ERROR_EMAIL_COOLDOWN_MINUTES", 30, 0
    )
    HEALTH_PORT = env_int("PORT", 8080, 1)


# ---------------------------------------------------------------------------
# Errors, logging, runtime state
# ---------------------------------------------------------------------------


class MonitorError(RuntimeError):
    """Base monitor error."""


class FeedValidationError(MonitorError):
    """The loaded page is not a valid Contra jobs feed."""


class AuthenticationRequired(MonitorError):
    """Contra requires an interactive authenticated browser session."""


class OpportunityExtractionError(MonitorError):
    """A detail page could not be extracted safely."""


class IncompleteScanError(MonitorError):
    """Not every discovered opportunity could be parsed."""


class DataIntegrityError(MonitorError):
    """Scraped identity conflicts with stored database identity."""


DEBUG = False
shutdown_event = threading.Event()
_monitor_state_lock = threading.Lock()
_monitor_state: dict[str, Any] = {
    "service": "contra-project-monitor",
    "status": "starting",
    "last_successful_scan": None,
    "last_error": None,
    "timestamp": None,
}
_health_server: ThreadingHTTPServer | None = None
_error_cooldown: dict[str, float] = {}
_sending_error_email = False


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_utc(value: datetime | None = None) -> str:
    value = value or utc_now()
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def log(level: str, event: str, **fields: Any) -> None:
    parts = [
        f"ts={iso_utc()}",
        f"level={level.upper()}",
        f"event={event}",
    ]
    for key, value in fields.items():
        if value is not None and value != "":
            parts.append(f"{key}={value}")
    print(" | ".join(parts), flush=True)


def debug(message: str) -> None:
    if DEBUG:
        print(f"DEBUG: {message}", flush=True)


def set_monitor_state(status: str, **fields: Any) -> None:
    with _monitor_state_lock:
        _monitor_state["status"] = status
        _monitor_state["timestamp"] = iso_utc()
        _monitor_state.update(fields)


def monitor_state_snapshot() -> dict[str, Any]:
    with _monitor_state_lock:
        return dict(_monitor_state)


def sanitize_error_message(value: Any, limit: int = 1000) -> str:
    text = str(value or "")
    secrets = [Config.SENDER_PASSWORD]
    for secret in secrets:
        if secret:
            text = text.replace(secret, "[REDACTED]")
    patterns = [
        r"(?i)(password|pass|passwd|token|secret|api[_-]?key|authorization)\s*[:=]\s*([^\s,;&]+)",
        r"(?i)bearer\s+[A-Za-z0-9._~+\-/=]+",
        r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b",
        r"(?i)(smtp|mongodb|postgres(?:ql)?)://[^\s/@:]+:[^\s/@]+@",
        r"(?i)(cookie|set-cookie)\s*[:=]\s*[^\r\n]+",
    ]
    replacements = [
        r"\1=[REDACTED]",
        "Bearer [REDACTED]",
        "[REDACTED_JWT]",
        r"\1://[REDACTED_CREDENTIALS]@",
        r"\1=[REDACTED]",
    ]
    for pattern, replacement in zip(patterns, replacements):
        text = re.sub(pattern, replacement, text)
    return text[:limit]


def interruptible_sleep(seconds: float) -> bool:
    return shutdown_event.wait(max(0.0, seconds))


def request_shutdown(signum: int | None = None, _frame: Any = None) -> None:
    log("INFO", "shutdown_requested", signal=signum)
    shutdown_event.set()
    set_monitor_state("shutting_down")


def install_signal_handlers() -> None:
    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(signum, request_shutdown)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------


class HealthHandler(BaseHTTPRequestHandler):
    def log_message(self, _format: str, *_args: Any) -> None:
        return

    def do_GET(self) -> None:  # noqa: N802
        if self.path.split("?", 1)[0] != "/health":
            self.send_response(404)
            self.end_headers()
            return
        payload = json.dumps(monitor_state_snapshot()).encode("utf-8")
        status = 200 if monitor_state_snapshot().get("status") != "failed" else 503
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def start_health_server() -> None:
    global _health_server
    try:
        server = ThreadingHTTPServer(("0.0.0.0", Config.HEALTH_PORT), HealthHandler)
        thread = threading.Thread(
            target=server.serve_forever, daemon=True, name="health-server"
        )
        thread.start()
        _health_server = server
        log("INFO", "health_server_started", port=Config.HEALTH_PORT)
    except OSError as exc:
        log("WARN", "health_server_not_started", error=sanitize_error_message(exc))


def stop_health_server() -> None:
    global _health_server
    if _health_server is not None:
        try:
            _health_server.shutdown()
        except Exception:
            pass
        _health_server = None


# ---------------------------------------------------------------------------
# Evidence and operational alerts
# ---------------------------------------------------------------------------


def evidence_dir() -> Path:
    path = Path(Config.EVIDENCE_DIR)
    path.mkdir(parents=True, exist_ok=True)
    return path


def cleanup_old_evidence() -> None:
    cutoff = time.time() - Config.EVIDENCE_RETENTION_HOURS * 3600
    try:
        for path in evidence_dir().iterdir():
            if path.is_file() and path.stat().st_mtime < cutoff:
                path.unlink(missing_ok=True)
    except Exception as exc:
        log("WARN", "evidence_cleanup_failed", error=sanitize_error_message(exc))


def redact_html_evidence(page_source: str) -> str:
    text = sanitize_error_message(page_source, limit=max(len(page_source), 1000))
    text = re.sub(
        r'(?is)("(?:accessToken|refreshToken|sessionToken|password)"\s*:\s*")[^"]*(")',
        r"\1[REDACTED]\2",
        text,
    )
    return text


def save_evidence(driver: webdriver.Chrome, prefix: str) -> list[str]:
    timestamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
    base = evidence_dir() / f"contra_{prefix}_{timestamp}"
    paths: list[str] = []
    png = str(base.with_suffix(".png"))
    html_path = str(base.with_suffix(".html"))
    meta_path = str(base.with_suffix(".json"))
    try:
        if driver.save_screenshot(png):
            paths.append(png)
    except Exception:
        pass
    try:
        Path(html_path).write_text(
            redact_html_evidence(driver.page_source or ""), encoding="utf-8"
        )
        paths.append(html_path)
    except Exception:
        pass
    try:
        Path(meta_path).write_text(
            json.dumps(
                {
                    "url": driver.current_url,
                    "title": driver.title,
                    "captured_at": iso_utc(),
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        paths.append(meta_path)
    except Exception:
        pass
    return paths


def smtp_configured() -> bool:
    return bool(
        Config.SMTP_SERVER
        and Config.SMTP_PORT
        and Config.SENDER_EMAIL
        and Config.SENDER_PASSWORD
        and Config.RECIPIENT_EMAILS
    )


def send_operational_error(context: str, error: Any, details: str = "") -> bool:
    global _sending_error_email
    if _sending_error_email or not Config.ERROR_RECIPIENTS or not smtp_configured():
        return False
    key = f"{context}|{type(error).__name__}|{str(error)[:200]}"
    now = time.time()
    cooldown = Config.ERROR_EMAIL_COOLDOWN_MINUTES * 60
    if now - _error_cooldown.get(key, 0) < cooldown:
        return False

    _sending_error_email = True
    try:
        error_text = sanitize_error_message(error)
        detail_text = sanitize_error_message(details, 8000)
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[Contra Monitor] Operational Error: {context}"
        msg["From"] = Config.SENDER_EMAIL
        msg["To"] = ", ".join(Config.ERROR_RECIPIENTS)
        plain = (
            f"Context: {context}\n"
            f"Time: {iso_utc()}\n"
            f"Host: {socket.gethostname()}\n"
            f"Error: {error_text}\n\n{detail_text}"
        )
        safe_context = html_lib.escape(context, quote=True)
        safe_error = html_lib.escape(error_text, quote=True)
        safe_details = html_lib.escape(detail_text, quote=True).replace("\n", "<br>")
        rich = f"""<!doctype html><html><body>
<h2>Contra Monitor operational error</h2>
<p><strong>Context:</strong> {safe_context}</p>
<p><strong>Time:</strong> {html_lib.escape(iso_utc())}</p>
<p><strong>Error:</strong> {safe_error}</p>
<p>{safe_details}</p>
</body></html>"""
        msg.attach(MIMEText(plain, "plain", "utf-8"))
        msg.attach(MIMEText(rich, "html", "utf-8"))
        context_ssl = ssl.create_default_context()
        with smtplib.SMTP(
            Config.SMTP_SERVER, Config.SMTP_PORT, timeout=30
        ) as server:
            server.ehlo()
            server.starttls(context=context_ssl)
            server.ehlo()
            server.login(Config.SENDER_EMAIL, Config.SENDER_PASSWORD)
            server.send_message(msg)
        _error_cooldown[key] = now
        return True
    except Exception as exc:
        log("ERROR", "operational_error_email_failed", error=sanitize_error_message(exc))
        return False
    finally:
        _sending_error_email = False


# ---------------------------------------------------------------------------
# URL and field normalization
# ---------------------------------------------------------------------------


def normalize_opportunity_url(value: str | None) -> str:
    if not value:
        return ""
    absolute = urljoin(CONTRA_ORIGIN, str(value).strip())
    parsed = urlparse(absolute)
    if parsed.scheme.lower() != "https":
        return ""
    if parsed.hostname not in {"contra.com", "www.contra.com"}:
        return ""
    path = parsed.path.rstrip("/")
    match = OPPORTUNITY_PATH_RE.fullmatch(path)
    if not match:
        return ""
    return f"{CONTRA_ORIGIN}/opportunity/{match.group(1)}"


def opportunity_slug(value: str | None) -> str:
    normalized = normalize_opportunity_url(value)
    if normalized:
        return normalized.rsplit("/", 1)[-1]
    raw = str(value or "").strip().strip("/")
    if raw.startswith("opportunity/"):
        raw = raw.split("/", 1)[1]
    return raw if re.fullmatch(r"[A-Za-z0-9_-]+", raw) else ""


def source_project_id(value: str | None) -> str:
    slug = opportunity_slug(value)
    if not slug:
        return ""
    candidate = slug.split("-", 1)[0]
    return candidate if SOURCE_ID_RE.fullmatch(candidate) else ""


def normalize_whitespace(value: Any) -> str:
    return re.sub(r"[ \t\r\f\v]+", " ", str(value or "")).strip()


def normalize_duration(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        amount = value.get("amount")
        interval = value.get("interval")
        if amount is not None and interval:
            return format_duration(amount, interval)
    text = normalize_whitespace(value)
    if not text:
        return None
    text = re.sub(r"(?i)\bdelivery\b", "", text).strip(" -–—•")
    if re.search(r"(?i)\bongoing\b", text):
        return "Ongoing"
    match = re.search(
        r"(?i)\b(\d+(?:\.\d+)?)\s*(day|week|month|year)s?\b", text
    )
    if not match:
        return text or None
    amount_text, unit = match.groups()
    amount = Decimal(amount_text)
    display_amount = str(int(amount)) if amount == amount.to_integral() else str(amount)
    plural = "" if amount == 1 else "s"
    return f"{display_amount} {unit.lower()}{plural}"


def format_duration(amount: Any, interval: Any) -> str | None:
    try:
        number = Decimal(str(amount))
    except (InvalidOperation, ValueError):
        return None
    interval_text = str(interval or "").strip().lower()
    unit = next(
        (unit for unit in ("day", "week", "month", "year") if unit in interval_text),
        "",
    )
    if not unit:
        return None
    display = str(int(number)) if number == number.to_integral() else str(number)
    return f"{display} {unit}{'' if number == 1 else 's'}"


def parse_money(value: Any) -> tuple[str, Decimal] | None:
    if value is None:
        return None
    if isinstance(value, (int, float, Decimal)):
        return "USD", Decimal(str(value))
    text = str(value).strip()
    match = re.fullmatch(
        r"(?i)([A-Z]{3})\s*:\s*(-?\d+(?:\.\d+)?)", text
    )
    if match:
        return match.group(1).upper(), Decimal(match.group(2))
    match = re.search(
        r"(?i)(USD|EUR|GBP|CAD|AUD)?\s*([$€£])?\s*([\d,]+(?:\.\d+)?)\s*([kKmM])?",
        text,
    )
    if not match:
        return None
    code, symbol, number, suffix = match.groups()
    currency = code.upper() if code else {"$": "USD", "€": "EUR", "£": "GBP"}.get(symbol or "", "USD")
    amount = Decimal(number.replace(",", ""))
    if suffix:
        amount *= Decimal(1000 if suffix.lower() == "k" else 1_000_000)
    return currency, amount


def currency_symbol(code: str) -> str:
    return {"USD": "$", "EUR": "€", "GBP": "£"}.get(code.upper(), f"{code.upper()} ")


def format_money(currency: str, amount: Decimal) -> str:
    quantized = amount.normalize()
    if quantized == quantized.to_integral():
        number = f"{int(quantized):,}"
    else:
        number = f"{quantized:,.2f}".rstrip("0").rstrip(".")
    return f"{currency_symbol(currency)}{number}"


def format_budget_object(value: Mapping[str, Any]) -> str | None:
    typename = str(value.get("__typename") or value.get("type") or "")
    minimum = value.get("feeMin")
    maximum = value.get("feeMax")
    if minimum is None:
        minimum = value.get("min") or value.get("minimum")
    if maximum is None:
        maximum = value.get("max") or value.get("maximum")
    if minimum is None and maximum is None:
        return None
    low = parse_money(minimum if minimum is not None else maximum)
    high = parse_money(maximum if maximum is not None else minimum)
    if not low or not high:
        return None
    currency = low[0]
    low_amount, high_amount = sorted((low[1], high[1]))
    suffix = "/hr" if "hour" in typename.lower() else ""
    if low_amount == high_amount:
        return f"{format_money(currency, low_amount)}{suffix}"
    return f"{format_money(currency, low_amount)} - {format_money(currency, high_amount)}{suffix}"


def normalize_budget(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return format_budget_object(value)
    text = normalize_whitespace(value)
    if not text:
        return None
    hourly = bool(re.search(r"(?i)(/\s*(?:hr|hour)|per\s+hour|hourly)", text))
    money_matches = list(
        re.finditer(
            r"(?i)(?:USD|EUR|GBP|CAD|AUD)?\s*[$€£]?\s*[\d,]+(?:\.\d+)?\s*[kKmM]?",
            text,
        )
    )
    parsed = [parse_money(match.group(0)) for match in money_matches]
    parsed = [item for item in parsed if item]
    if not parsed:
        return None
    first = parsed[0]
    second = parsed[1] if len(parsed) > 1 else first
    low_amount, high_amount = sorted((first[1], second[1]))
    suffix = "/hr" if hourly else ""
    if low_amount == high_amount:
        return f"{format_money(first[0], low_amount)}{suffix}"
    return f"{format_money(first[0], low_amount)} - {format_money(first[0], high_amount)}{suffix}"


def normalize_engagement(value: Any) -> str | None:
    text = normalize_whitespace(value).lower()
    if not text:
        return None
    ordered = [
        (r"\bone[- ]?time\b|\bsingle project\b", "One-time"),
        (r"\bhourly\b|\bper hour\b|/hr", "Hourly"),
        (r"\bfixed[- ]?price\b|\bfixed budget\b", "Fixed-price"),
        (r"\bfull[- ]?time\b", "Full-time"),
        (r"\bpart[- ]?time\b", "Part-time"),
        (r"\bcontractor\b|\bcontract\b", "Contractor"),
    ]
    for pattern, normalized in ordered:
        if re.search(pattern, text):
            return normalized
    return str(value).strip().title()


def parse_card_summary(text: str | None) -> dict[str, str]:
    result: dict[str, str] = {}
    if not text:
        return result
    parts = [
        normalize_whitespace(part)
        for part in re.split(r"[•·|\n]", text)
        if normalize_whitespace(part)
    ]
    for part in parts:
        if "budget" not in result:
            budget = normalize_budget(part)
            if budget and ("$" in part or re.search(r"(?i)USD|EUR|GBP|/hr|hour", part)):
                result["budget"] = budget
        if "engagement_type" not in result:
            engagement = normalize_engagement(part)
            if engagement in {"One-time", "Hourly", "Fixed-price", "Full-time", "Part-time"}:
                result["engagement_type"] = engagement
        if "project_length" not in result and re.search(
            r"(?i)\b(?:day|week|month|year)s?\b|\bongoing\b", part
        ):
            duration = normalize_duration(part)
            if duration:
                result["project_length"] = duration
    if "engagement_type" not in result:
        match = re.search(r"(?i)\b(One[- ]?time|Hourly|Fixed[- ]?price|Full[- ]?time|Part[- ]?time)\b", text)
        if match:
            result["engagement_type"] = normalize_engagement(match.group(1)) or ""
    if "project_length" not in result:
        match = re.search(r"(?i)\b(\d+(?:\.\d+)?\s*(?:day|week|month|year)s?|ongoing)\b", text)
        if match:
            result["project_length"] = normalize_duration(match.group(1)) or ""
    return {key: value for key, value in result.items() if value}


def exact_timestamp(value: Any) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return None
    if not re.search(r"[T ]\d{1,2}:\d{2}", text):
        return None
    try:
        normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC).isoformat()
    except ValueError:
        return None


def clean_description(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    if "<" in text and ">" in text:
        text = re.sub(r"(?is)<li[^>]*>", "\n- ", text)
        text = re.sub(r"(?is)</?(?:p|h[1-6]|div|section|article|br|ul|ol)[^>]*>", "\n", text)
        text = re.sub(r"(?is)<[^>]+>", "", text)
        text = html_lib.unescape(text)
    lines: list[str] = []
    seen: set[str] = set()
    for raw_line in text.splitlines():
        line = normalize_whitespace(raw_line)
        if not line or line.lower() in UI_ONLY_LINES:
            continue
        if line not in seen:
            lines.append(line)
            seen.add(line)
    return "\n".join(lines)


def blocknote_text(blocks: Any) -> str:
    if isinstance(blocks, str):
        try:
            blocks = json.loads(blocks)
        except json.JSONDecodeError:
            return clean_description(blocks)
    if not isinstance(blocks, Sequence) or isinstance(blocks, (str, bytes)):
        return ""
    output: list[str] = []
    seen: set[str] = set()

    def inline_text(content: Any) -> str:
        if isinstance(content, str):
            return content
        if isinstance(content, Mapping):
            own = str(content.get("text") or "")
            children = content.get("content") or content.get("children") or []
            return own + "".join(inline_text(item) for item in children if item is not None)
        if isinstance(content, Sequence) and not isinstance(content, (str, bytes)):
            return "".join(inline_text(item) for item in content)
        return ""

    for block in blocks:
        if isinstance(block, str):
            text = normalize_whitespace(block)
            block_type = "paragraph"
        elif isinstance(block, Mapping):
            block_type = str(block.get("type") or "paragraph").lower()
            text = normalize_whitespace(inline_text(block.get("content") or []))
        else:
            continue
        if not text:
            continue
        if "bullet" in block_type or "numberedlistitem" in block_type:
            line = f"- {text}"
        else:
            line = text
        if line.lower() in UI_ONLY_LINES or line in seen:
            continue
        output.append(line)
        seen.add(line)
    return "\n".join(output)


# ---------------------------------------------------------------------------
# HTML snapshot parsing
# ---------------------------------------------------------------------------


@dataclass
class ScriptBlock:
    attributes: dict[str, str]
    text: str


@dataclass
class HtmlSnapshot:
    canonical_url: str = ""
    og_url: str = ""
    og_title: str = ""
    title: str = ""
    h1: str = ""
    main_text: str = ""
    scripts: list[ScriptBlock] = field(default_factory=list)


class SnapshotParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.snapshot = HtmlSnapshot()
        self._script_attrs: dict[str, str] | None = None
        self._script_parts: list[str] = []
        self._capture_title = False
        self._capture_h1 = False
        self._title_parts: list[str] = []
        self._h1_parts: list[str] = []
        self._semantic_depth = 0
        self._semantic_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = {key.lower(): value or "" for key, value in attrs}
        tag = tag.lower()
        if tag == "script":
            self._script_attrs = attributes
            self._script_parts = []
        elif tag == "link":
            rel = {item.lower() for item in attributes.get("rel", "").split()}
            if "canonical" in rel:
                self.snapshot.canonical_url = attributes.get("href", "")
        elif tag == "meta":
            prop = (attributes.get("property") or attributes.get("name") or "").lower()
            if prop == "og:url":
                self.snapshot.og_url = attributes.get("content", "")
            elif prop in {"og:title", "twitter:title"} and not self.snapshot.og_title:
                self.snapshot.og_title = attributes.get("content", "")
        elif tag == "title":
            self._capture_title = True
        elif tag == "h1" and not self.snapshot.h1:
            self._capture_h1 = True
        if tag in {"main", "article"}:
            self._semantic_depth += 1
        elif self._semantic_depth:
            self._semantic_depth += 1
        if self._semantic_depth and tag in {"p", "h1", "h2", "h3", "h4", "li", "br"}:
            self._semantic_parts.append("\n- " if tag == "li" else "\n")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag == "script" and self._script_attrs is not None:
            self.snapshot.scripts.append(
                ScriptBlock(self._script_attrs, "".join(self._script_parts))
            )
            self._script_attrs = None
            self._script_parts = []
        elif tag == "title":
            self._capture_title = False
            if not self.snapshot.title:
                self.snapshot.title = normalize_whitespace("".join(self._title_parts))
        elif tag == "h1" and self._capture_h1:
            self._capture_h1 = False
            self.snapshot.h1 = normalize_whitespace("".join(self._h1_parts))
        if self._semantic_depth:
            self._semantic_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._script_attrs is not None:
            self._script_parts.append(data)
            return
        if self._capture_title:
            self._title_parts.append(data)
        if self._capture_h1:
            self._h1_parts.append(data)
        if self._semantic_depth:
            self._semantic_parts.append(data)

    def close(self) -> None:
        super().close()
        self.snapshot.main_text = clean_description("".join(self._semantic_parts))


def parse_html_snapshot(page_source: str) -> HtmlSnapshot:
    parser = SnapshotParser()
    try:
        parser.feed(page_source or "")
        parser.close()
    except Exception:
        # HTMLParser is forgiving, but a broken document should not lose regex fallbacks.
        pass
    return parser.snapshot


# ---------------------------------------------------------------------------
# JSON-LD and embedded Relay-state extraction
# ---------------------------------------------------------------------------


def walk_json(value: Any) -> Iterator[Any]:
    yield value
    if isinstance(value, Mapping):
        for child in value.values():
            yield from walk_json(child)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for child in value:
            yield from walk_json(child)


def json_type_contains(value: Mapping[str, Any], expected: str) -> bool:
    raw = value.get("@type")
    if isinstance(raw, str):
        return expected.lower() == raw.lower()
    if isinstance(raw, Sequence):
        return any(str(item).lower() == expected.lower() for item in raw)
    return False


def decode_nested_json(value: Any, depth: int = 0) -> Any:
    if depth > 4:
        return value
    if isinstance(value, str):
        stripped = html_lib.unescape(value.strip())
        if not stripped or stripped[0:1] not in {"{", "[", '"'}:
            return value
        try:
            return decode_nested_json(json.loads(stripped), depth + 1)
        except json.JSONDecodeError:
            return value
    return value


def raw_json_values(text: str, maximum: int = 50) -> Iterator[Any]:
    text = html_lib.unescape(text or "").strip()
    if not text:
        return
    seen_offsets: set[int] = set()
    decoder = json.JSONDecoder()
    starts = [0]
    marker_positions: list[int] = []
    for marker in (
        "relayRecordMap",
        "blocknoteDescription",
        "JobDuration",
        "FixedPriceJobBudget",
        "HourlyJobBudget",
    ):
        marker_positions.extend(match.start() for match in re.finditer(marker, text))
    for marker_position in marker_positions:
        starts.extend(
            index
            for index in range(max(0, marker_position - 200000), marker_position + 1)
            if text[index:index + 1] in {"{", "["}
        )
    starts.extend(
        match.start() for match in re.finditer(r"(?m)(?:^|[=(:;,])\s*([\[{])", text)
    )
    yielded = 0
    for offset in sorted(set(starts)):
        if yielded >= maximum:
            break
        if offset in seen_offsets:
            continue
        actual = offset
        if text[actual:actual + 1] not in {"{", "[", '"'}:
            brace = min(
                (idx for idx in (text.find("{", actual), text.find("[", actual)) if idx >= 0),
                default=-1,
            )
            if brace < 0:
                continue
            actual = brace
        seen_offsets.add(actual)
        try:
            parsed, _end = decoder.raw_decode(text, actual)
        except json.JSONDecodeError:
            continue
        parsed = decode_nested_json(parsed)
        if isinstance(parsed, (Mapping, list)):
            yield parsed
            yielded += 1


def script_json_values(script: ScriptBlock) -> Iterator[Any]:
    body = script.text.strip()
    if not body:
        return
    direct = decode_nested_json(body)
    direct_success = isinstance(direct, (Mapping, list))
    if direct_success:
        yield direct
    for match in re.finditer(r"JSON\.parse\((?P<quoted>['\"].*?['\"])\)", body, re.DOTALL):
        try:
            decoded_string = ast.literal_eval(match.group("quoted"))
            decoded = decode_nested_json(decoded_string)
            if isinstance(decoded, (Mapping, list)):
                yield decoded
        except (ValueError, SyntaxError):
            pass
    important = any(
        marker in body
        for marker in (
            "relayRecordMap",
            "blocknoteDescription",
            "JobDuration",
            "JobBudget",
            "jobBySlug",
        )
    )
    if important and not direct_success:
        yield from raw_json_values(body)


def all_script_json(snapshot: HtmlSnapshot) -> list[Any]:
    values: list[Any] = []
    seen_signatures: set[str] = set()
    for script in snapshot.scripts:
        for value in script_json_values(script):
            try:
                signature = json.dumps(value, sort_keys=True, default=str)[:2000]
            except Exception:
                signature = repr(type(value)) + repr(id(value))
            if signature not in seen_signatures:
                seen_signatures.add(signature)
                values.append(value)
    return values


def select_json_ld_job(snapshot: HtmlSnapshot, target_slug: str) -> dict[str, Any]:
    candidates: list[tuple[int, dict[str, Any]]] = []
    for script in snapshot.scripts:
        if script.attributes.get("type", "").lower() != "application/ld+json":
            continue
        try:
            root = json.loads(script.text)
        except json.JSONDecodeError:
            continue
        for item in walk_json(root):
            if not isinstance(item, Mapping) or not json_type_contains(item, "JobPosting"):
                continue
            candidate = dict(item)
            score = 1
            candidate_url = normalize_opportunity_url(
                str(candidate.get("url") or candidate.get("@id") or "")
            )
            if candidate_url and opportunity_slug(candidate_url) == target_slug:
                score += 100
            if candidate.get("title"):
                score += 5
            if candidate.get("description"):
                score += 5
            candidates.append((score, candidate))
    return max(candidates, key=lambda item: item[0])[1] if candidates else {}


def find_relay_record_maps(value: Any) -> Iterator[dict[str, Any]]:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if str(key).lower() == "relayrecordmap" and isinstance(child, Mapping):
                yield dict(child)
            yield from find_relay_record_maps(child)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for child in value:
            yield from find_relay_record_maps(child)


class RelayStore:
    def __init__(self, records: Mapping[str, Any]):
        self.records = dict(records)

    def resolve(self, value: Any, depth: int = 0) -> Any:
        if depth > 12:
            return value
        if isinstance(value, Mapping):
            if set(value.keys()) == {"__ref"}:
                referenced = self.records.get(str(value["__ref"]))
                return self.resolve(referenced, depth + 1) if referenced is not None else value
            return {
                key: self.resolve(child, depth + 1)
                for key, child in value.items()
            }
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            return [self.resolve(item, depth + 1) for item in value]
        return value

    def get_ref(self, value: Any) -> dict[str, Any]:
        resolved = self.resolve(value)
        return dict(resolved) if isinstance(resolved, Mapping) else {}

    def matching_job(self, slug: str) -> tuple[str, dict[str, Any]] | None:
        root = self.records.get("client:root")
        if isinstance(root, Mapping):
            for key, value in root.items():
                if "jobBySlug" in str(key) and slug in str(key):
                    ref = value.get("__ref") if isinstance(value, Mapping) else None
                    if ref and isinstance(self.records.get(str(ref)), Mapping):
                        return str(ref), dict(self.records[str(ref)])
        candidates: list[tuple[int, str, dict[str, Any]]] = []
        target_id = source_project_id(slug)
        for key, value in self.records.items():
            if not isinstance(value, Mapping):
                continue
            candidate_slug = str(value.get("slug") or "")
            if not candidate_slug:
                continue
            score = 0
            if candidate_slug == slug:
                score += 100
            elif source_project_id(candidate_slug) == target_id:
                score += 50
            if str(value.get("__typename") or "").lower() == "job":
                score += 10
            if any(str(field).lower() in BLOCKNOTE_KEYS for field in value):
                score += 10
            if score:
                candidates.append((score, str(key), dict(value)))
        if not candidates:
            return None
        _score, key, candidate = max(candidates, key=lambda item: item[0])
        return key, candidate

    def related_entity(
        self,
        job_key: str,
        job: Mapping[str, Any],
        field_names: Sequence[str],
        typenames: Sequence[str],
    ) -> dict[str, Any]:
        for field_name in field_names:
            if field_name in job:
                resolved = self.get_ref(job[field_name])
                if resolved:
                    return resolved
        base_ids = {
            job_key,
            str(job.get("__id") or ""),
            str(job.get("id") or ""),
        }
        matching: list[dict[str, Any]] = []
        for key, value in self.records.items():
            if not isinstance(value, Mapping):
                continue
            typename = str(value.get("__typename") or "")
            if typename not in typenames:
                continue
            if any(base_id and base_id in str(key) for base_id in base_ids):
                return dict(value)
            matching.append(dict(value))
        return matching[0] if len(matching) == 1 else {}


def get_case_insensitive(mapping: Mapping[str, Any], names: Iterable[str]) -> Any:
    wanted = {name.lower() for name in names}
    for key, value in mapping.items():
        if str(key).lower() in wanted:
            return value
    return None


def embedded_from_store(store: RelayStore, target_slug: str) -> dict[str, Any]:
    matched = store.matching_job(target_slug)
    if not matched:
        return {}
    job_key, job = matched
    posting = store.related_entity(
        job_key,
        job,
        ("jobPosting", "posting"),
        ("JobPosting",),
    )
    duration = store.related_entity(
        job_key,
        job,
        ("duration", "jobDuration"),
        ("JobDuration",),
    )
    budget = store.related_entity(
        job_key,
        job,
        ("budget", "jobBudget", "compensation"),
        ("FixedPriceJobBudget", "HourlyJobBudget", "JobBudget"),
    )

    blocks = get_case_insensitive(job, BLOCKNOTE_KEYS)
    description = blocknote_text(blocks)
    if not description:
        description = clean_description(job.get("description"))

    embedded_engagement = get_case_insensitive(
        job,
        (
            "engagementType",
            "jobType",
            "pricingType",
            "compensationType",
            "commitment",
        ),
    )
    if not embedded_engagement and budget:
        typename = str(budget.get("__typename") or "")
        if "Hourly" in typename:
            embedded_engagement = "Hourly"
        elif "FixedPrice" in typename:
            embedded_engagement = "Fixed-price"

    created_at = exact_timestamp(posting.get("createdAt")) or exact_timestamp(
        job.get("createdAt")
    )
    return {
        "title": normalize_whitespace(job.get("title")),
        "description": description,
        "project_length": normalize_duration(duration),
        "budget": normalize_budget(budget),
        "engagement_type": normalize_engagement(embedded_engagement),
        "created_at": created_at,
        "slug": str(job.get("slug") or target_slug),
        "diagnostics": {
            "job_record_key": job_key,
            "job_typename": job.get("__typename"),
            "posting_timestamp_found": bool(exact_timestamp(posting.get("createdAt"))),
            "job_timestamp_found": bool(exact_timestamp(job.get("createdAt"))),
            "blocknote_found": bool(blocks),
            "duration_found": bool(duration),
            "budget_found": bool(budget),
        },
    }


def generic_embedded_candidate(value: Any, target_slug: str) -> dict[str, Any]:
    candidates: list[tuple[int, dict[str, Any]]] = []
    target_id = source_project_id(target_slug)
    for item in walk_json(value):
        if not isinstance(item, Mapping):
            continue
        slug = str(item.get("slug") or "")
        score = 0
        if slug == target_slug:
            score += 100
        elif slug and source_project_id(slug) == target_id:
            score += 50
        if not score:
            continue
        blocks = get_case_insensitive(item, BLOCKNOTE_KEYS)
        candidate = {
            "title": normalize_whitespace(item.get("title")),
            "description": blocknote_text(blocks)
            or clean_description(item.get("description")),
            "created_at": exact_timestamp(item.get("createdAt")),
            "slug": slug,
            "diagnostics": {"generic_embedded_match": True},
        }
        if candidate["description"]:
            score += 10
        candidates.append((score, candidate))
    return max(candidates, key=lambda item: item[0])[1] if candidates else {}


def extract_embedded_state(snapshot: HtmlSnapshot, target_slug: str) -> dict[str, Any]:
    candidates: list[tuple[int, dict[str, Any]]] = []
    for root in all_script_json(snapshot):
        relay_maps = list(find_relay_record_maps(root))
        for records in relay_maps:
            result = embedded_from_store(RelayStore(records), target_slug)
            if result:
                score = 100
                score += 10 if result.get("description") else 0
                score += 5 if result.get("budget") else 0
                score += 5 if result.get("project_length") else 0
                score += 5 if result.get("created_at") else 0
                candidates.append((score, result))
        generic = generic_embedded_candidate(root, target_slug)
        if generic:
            candidates.append((20, generic))
    return max(candidates, key=lambda item: item[0])[1] if candidates else {}


def json_ld_location(job: Mapping[str, Any]) -> str | None:
    location_type = str(job.get("jobLocationType") or "").upper()
    if "TELECOMMUTE" in location_type:
        return "Remote"
    location = job.get("jobLocation")
    locations = location if isinstance(location, list) else [location]
    for item in locations:
        if not isinstance(item, Mapping):
            continue
        address = item.get("address")
        if isinstance(address, Mapping):
            pieces = [
                normalize_whitespace(address.get(name))
                for name in ("addressLocality", "addressRegion", "addressCountry")
            ]
            pieces = [piece for piece in pieces if piece]
            if pieces:
                return ", ".join(dict.fromkeys(pieces))
    requirements = job.get("applicantLocationRequirements")
    if isinstance(requirements, Mapping):
        name = requirements.get("name")
        if isinstance(name, str):
            return normalize_whitespace(name)
        if isinstance(name, list) and len(name) == 1:
            return normalize_whitespace(name[0])
    return None


def visible_text_from_html(page_source: str) -> str:
    cleaned = re.sub(r"(?is)<(?:script|style|noscript|svg)\b[^>]*>.*?</(?:script|style|noscript|svg)>", "\n", page_source or "")
    cleaned = re.sub(r"(?is)<li\b[^>]*>", "\n- ", cleaned)
    cleaned = re.sub(r"(?is)</?(?:p|h[1-6]|div|section|article|main|br|ul|ol)[^>]*>", "\n", cleaned)
    cleaned = re.sub(r"(?is)<[^>]+>", " ", cleaned)
    return clean_description(html_lib.unescape(cleaned))


def semantic_fallback(snapshot: HtmlSnapshot, page_source: str) -> dict[str, Any]:
    text = snapshot.main_text or visible_text_from_html(page_source)
    result: dict[str, Any] = {
        "title": snapshot.h1 or snapshot.og_title or snapshot.title,
    }
    if re.search(r"(?i)\bremote\b", text):
        result["location"] = "Remote"
    result.update(parse_card_summary(text))

    budget_line = re.search(
        r"(?i)([$€£]\s*[\d,]+(?:\.\d+)?(?:\s*[-–—]\s*[$€£]\s*[\d,]+(?:\.\d+)?)?\s*(?:/\s*(?:hr|hour))?)\s*[•·|]\s*(One[- ]?time|Hourly|Fixed[- ]?price|Full[- ]?time|Part[- ]?time)",
        text,
    )
    if budget_line:
        result["budget"] = normalize_budget(budget_line.group(1)) or result.get("budget")
        result["engagement_type"] = normalize_engagement(budget_line.group(2)) or result.get("engagement_type")

    duration_match = re.search(
        r"(?is)\b(?:delivery time|project length|duration)\b.{0,120}?\b(\d+(?:\.\d+)?\s*(?:day|week|month|year)s?|ongoing)\b",
        text,
    )
    if duration_match:
        result["project_length"] = normalize_duration(duration_match.group(1)) or result.get("project_length")

    if text:
        sections = re.split(
            r"(?i)\n(?=about the job|overview|what you(?:’|'|’)ll be doing|scope|responsibilities|deliverables|requirements)\b",
            text,
        )
        best = max(sections, key=len) if sections else ""
        if len(best) >= 100:
            result["description"] = clean_description(best)
    return result


def description_score(text: str, structured: bool = False) -> int:
    if not text:
        return 0
    lines = [line for line in text.splitlines() if line.strip()]
    bullets = sum(line.lstrip().startswith("- ") for line in lines)
    return len(text) + bullets * 120 + (10000 if (structured and len(text) >= 20) else 0)


@dataclass
class Opportunity:
    source_project_id: str
    scan_at: str
    posted_at: str
    title: str
    description: str
    location: str
    project_length: str
    url: str
    budget: str
    engagement_type: str
    extraction_diagnostics: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_project_id": self.source_project_id,
            "id": self.source_project_id,
            "scan_at": self.scan_at,
            "posted_at": self.posted_at,
            "title": self.title,
            "description": self.description,
            "location": self.location,
            "project_length": self.project_length,
            "url": self.url,
            "budget": self.budget,
            "engagement_type": self.engagement_type,
            "extraction_diagnostics": self.extraction_diagnostics,
        }


def extract_contra_opportunity_from_html(
    page_source: str,
    input_url: str,
    scan_at: datetime | None = None,
    card_metadata: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    scan_at = scan_at or utc_now()
    if scan_at.tzinfo is None:
        scan_at = scan_at.replace(tzinfo=UTC)
    scan_at_iso = scan_at.astimezone(UTC).isoformat()
    card_metadata = dict(card_metadata or {})

    snapshot = parse_html_snapshot(page_source)
    canonical_candidates = [snapshot.canonical_url, snapshot.og_url, input_url]
    canonical_url = next(
        (normalized for candidate in canonical_candidates if (normalized := normalize_opportunity_url(candidate))),
        "",
    )
    if not canonical_url:
        raise OpportunityExtractionError("No valid canonical Contra opportunity URL found")
    slug = opportunity_slug(canonical_url)
    project_id = source_project_id(slug)
    if not project_id:
        raise OpportunityExtractionError(f"Invalid source project ID in slug: {slug}")

    json_ld = select_json_ld_job(snapshot, slug)
    embedded = extract_embedded_state(snapshot, slug)
    semantic = semantic_fallback(snapshot, page_source)

    json_ld_url = normalize_opportunity_url(
        str(json_ld.get("url") or json_ld.get("@id") or "")
    )
    if json_ld_url and opportunity_slug(json_ld_url) == slug:
        canonical_url = json_ld_url
    elif normalize_opportunity_url(snapshot.canonical_url):
        canonical_url = normalize_opportunity_url(snapshot.canonical_url)

    title = (
        normalize_whitespace(json_ld.get("title"))
        or embedded.get("title")
        or semantic.get("title")
    )
    if not title:
        raise OpportunityExtractionError("Opportunity title could not be extracted")

    description_candidates = [
        (embedded.get("description") or "", True, "embedded_blocknote"),
        (clean_description(json_ld.get("description")), False, "json_ld"),
        (semantic.get("description") or "", False, "semantic_dom"),
    ]
    description, _structured, description_source = max(
        description_candidates,
        key=lambda item: description_score(item[0], item[1]),
    )
    if not description:
        raise OpportunityExtractionError("Opportunity description could not be extracted")

    exact_embedded_timestamp = embedded.get("created_at")
    exact_json_ld_timestamp = exact_timestamp(json_ld.get("datePosted"))
    if exact_embedded_timestamp:
        posted_at = exact_embedded_timestamp
        posted_source = "embedded_created_at"
    elif exact_json_ld_timestamp:
        posted_at = exact_json_ld_timestamp
        posted_source = "json_ld_exact"
    else:
        posted_at = scan_at_iso
        posted_source = "scan_at_fallback"

    location = json_ld_location(json_ld) or semantic.get("location") or "Not specified"
    project_length = (
        embedded.get("project_length")
        or card_metadata.get("project_length")
        or semantic.get("project_length")
        or "Not specified"
    )
    budget = (
        embedded.get("budget")
        or card_metadata.get("budget")
        or semantic.get("budget")
        or "Not specified"
    )
    engagement = (
        card_metadata.get("engagement_type")
        or semantic.get("engagement_type")
        or embedded.get("engagement_type")
        or normalize_engagement(json_ld.get("employmentType"))
        or "Not specified"
    )

    opportunity = Opportunity(
        source_project_id=project_id,
        scan_at=scan_at_iso,
        posted_at=posted_at,
        title=title,
        description=description,
        location=normalize_whitespace(location) or "Not specified",
        project_length=normalize_duration(project_length) or "Not specified",
        url=canonical_url,
        budget=normalize_budget(budget) or "Not specified",
        engagement_type=normalize_engagement(engagement) or "Not specified",
        extraction_diagnostics={
            "canonical_source": (
                "json_ld"
                if json_ld_url
                else "link_canonical"
                if normalize_opportunity_url(snapshot.canonical_url)
                else "input_url"
            ),
            "posted_at_source": posted_source,
            "description_source": description_source,
            "json_ld_found": bool(json_ld),
            "embedded_found": bool(embedded),
            "embedded": embedded.get("diagnostics", {}),
        },
    )
    return opportunity.as_dict()


def extract_contra_opportunity(
    driver_or_html: webdriver.Chrome | str,
    url: str,
    scan_at: datetime | None = None,
    card_metadata: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    if isinstance(driver_or_html, str):
        return extract_contra_opportunity_from_html(
            driver_or_html, url, scan_at, card_metadata
        )
    driver = driver_or_html
    driver.get(url)
    WebDriverWait(driver, Config.DETAIL_WAIT_SECONDS).until(
        lambda current: current.execute_script("return document.readyState")
        in {"interactive", "complete"}
    )
    WebDriverWait(driver, Config.DETAIL_WAIT_SECONDS).until(
        lambda current: bool(
            current.find_elements(By.CSS_SELECTOR, 'link[rel="canonical"]')
            or current.find_elements(By.CSS_SELECTOR, 'script[type="application/ld+json"]')
            or current.find_elements(By.TAG_NAME, "h1")
        )
    )
    validate_opportunity_page(driver, expected_url=url)
    return extract_contra_opportunity_from_html(
        driver.page_source or "", url, scan_at, card_metadata
    )


# ---------------------------------------------------------------------------
# Selenium feed discovery
# ---------------------------------------------------------------------------


def is_auth_path(path: str) -> bool:
    return any(
        token in path.lower()
        for token in ("/login", "/log-in", "/signin", "/sign-in", "/auth")
    )


def body_text(driver: webdriver.Chrome, limit: int = 3000) -> str:
    try:
        return (driver.find_element(By.TAG_NAME, "body").text or "")[:limit]
    except Exception:
        return ""


def validate_jobs_page(driver: webdriver.Chrome) -> None:
    parsed = urlparse(driver.current_url or "")
    host = (parsed.hostname or "").lower()
    path = parsed.path.rstrip("/") or "/"
    if host not in {"contra.com", "www.contra.com"}:
        raise FeedValidationError(f"Unexpected hostname: {host or '(empty)'}")
    if is_auth_path(path):
        raise AuthenticationRequired(
            "Contra redirected to authentication. Create a valid session interactively, "
            f"then place its cookies in {Config.COOKIE_FILE}."
        )
    if path.lower() != "/jobs":
        raise FeedValidationError(f"Expected /jobs but loaded {path}")
    page_text = body_text(driver).lower()
    access_markers = (
        "access denied",
        "forbidden",
        "verify you are human",
        "captcha",
        "something went wrong",
    )
    if any(marker in page_text for marker in access_markers):
        raise FeedValidationError("Contra jobs page shows an access or application error")


def validate_opportunity_page(
    driver: webdriver.Chrome, expected_url: str | None = None
) -> None:
    parsed = urlparse(driver.current_url or "")
    host = (parsed.hostname or "").lower()
    path = parsed.path.rstrip("/")
    if host not in {"contra.com", "www.contra.com"}:
        raise OpportunityExtractionError(f"Unexpected detail hostname: {host}")
    if is_auth_path(path):
        raise AuthenticationRequired("Contra detail page redirected to authentication")
    if not OPPORTUNITY_PATH_RE.fullmatch(path):
        raise OpportunityExtractionError(f"Unexpected detail route: {path}")
    if expected_url:
        expected = normalize_opportunity_url(expected_url)
        actual = normalize_opportunity_url(driver.current_url)
        if expected and actual and source_project_id(expected) != source_project_id(actual):
            raise OpportunityExtractionError(
                f"Detail identity changed from {expected} to {actual}"
            )


def nearest_card_text(anchor: Any) -> str:
    selectors = [
        "./ancestor::article[1]",
        "./ancestor::li[1]",
        "./ancestor::*[@role='listitem'][1]",
        "./ancestor::section[1]",
    ]
    candidates: list[str] = []
    for selector in selectors:
        try:
            text = (anchor.find_element(By.XPATH, selector).text or "").strip()
            if text:
                candidates.append(text)
        except Exception:
            pass
    try:
        parent = anchor
        for _ in range(6):
            parent = parent.find_element(By.XPATH, "..")
            text = (parent.text or "").strip()
            if 20 <= len(text) <= 3000:
                candidates.append(text)
    except Exception:
        pass
    useful = [
        candidate
        for candidate in candidates
        if re.search(r"[$€£]|\b(?:one-time|hourly|week|month|delivery)\b", candidate, re.I)
    ]
    pool = useful or candidates
    return min(pool, key=len) if pool else ""


def merge_card_metadata(existing: dict[str, str], new: Mapping[str, str]) -> dict[str, str]:
    merged = dict(existing)
    for key, value in new.items():
        if value and not merged.get(key):
            merged[key] = value
    return merged


def click_load_more(driver: webdriver.Chrome) -> bool:
    for selector in ("button", "[role='button']"):
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
        except Exception:
            continue
        for element in elements:
            try:
                if not element.is_displayed() or not element.is_enabled():
                    continue
                label = normalize_whitespace(
                    element.text or element.get_attribute("aria-label") or ""
                ).lower()
                if label not in {"load more", "show more", "see more", "view more"}:
                    continue
                driver.execute_script("arguments[0].click();", element)
                return True
            except Exception:
                continue
    return False


def expand_jobs_feed(driver: webdriver.Chrome) -> int:
    stable_rounds = 0
    previous_count = -1
    for round_number in range(Config.MAX_SCROLL_ROUNDS):
        validate_jobs_page(driver)
        anchors = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/opportunity/"]')
        current_count = len(
            {
                normalized
                for anchor in anchors
                if (normalized := normalize_opportunity_url(anchor.get_attribute("href")))
            }
        )
        if current_count == previous_count:
            stable_rounds += 1
        else:
            stable_rounds = 0
        previous_count = current_count

        clicked = click_load_more(driver)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(Config.SCROLL_PAUSE_SECONDS)
        new_height = driver.execute_script("return document.body.scrollHeight")
        debug(
            f"feed expansion round={round_number + 1} count={current_count} "
            f"stable={stable_rounds} height={new_height} clicked={clicked}"
        )
        if stable_rounds >= Config.STABLE_SCROLL_ROUNDS and not clicked:
            break
    driver.execute_script("window.scrollTo(0, 0);")
    return max(previous_count, 0)


def discover_contra_opportunities(
    driver: webdriver.Chrome,
) -> dict[str, dict[str, str]]:
    target = Config.JOBS_URL
    last_error: Exception | None = None
    for attempt in range(2):
        try:
            driver.get(target)
            WebDriverWait(driver, Config.PAGE_WAIT_SECONDS).until(
                lambda current: current.execute_script("return document.readyState")
                in {"interactive", "complete"}
            )
            validate_jobs_page(driver)
            expand_jobs_feed(driver)
            anchors = WebDriverWait(driver, Config.PAGE_WAIT_SECONDS).until(
                lambda current: current.find_elements(
                    By.CSS_SELECTOR, 'a[href*="/opportunity/"]'
                )
            )
            discovered: dict[str, dict[str, str]] = {}
            for anchor in anchors:
                try:
                    normalized = normalize_opportunity_url(anchor.get_attribute("href"))
                    if not normalized:
                        continue
                    metadata = parse_card_summary(nearest_card_text(anchor))
                    discovered[normalized] = merge_card_metadata(
                        discovered.get(normalized, {}), metadata
                    )
                except Exception as exc:
                    debug(f"Feed anchor ignored: {exc}")
            if not discovered:
                raise FeedValidationError("No valid opportunity URLs were extracted")
            log("INFO", "feed_discovered", count=len(discovered), attempt=attempt + 1)
            return discovered
        except (TimeoutException, FeedValidationError, AuthenticationRequired) as exc:
            last_error = exc
            if isinstance(exc, AuthenticationRequired):
                raise
            if attempt == 0:
                debug(f"Feed attempt failed, refreshing once: {exc}")
                try:
                    driver.refresh()
                except Exception:
                    pass
                time.sleep(2)
                continue
            break
    raise FeedValidationError(str(last_error or "Contra feed discovery failed"))


@dataclass
class ScanResult:
    projects: list[dict[str, Any]]
    discovered_count: int
    failures: list[dict[str, str]]
    scan_at: str

    @property
    def complete(self) -> bool:
        return not self.failures and len(self.projects) == self.discovered_count


def scan_for_projects(driver: webdriver.Chrome) -> ScanResult:
    scan_time = utc_now()
    discovered = discover_contra_opportunities(driver)
    projects_by_id: dict[str, dict[str, Any]] = {}
    urls_by_id: dict[str, str] = {}
    failures: list[dict[str, str]] = []
    for url, metadata in discovered.items():
        try:
            project = extract_contra_opportunity(
                driver, url, scan_at=scan_time, card_metadata=metadata
            )
            project_id = project["source_project_id"]
            existing_url = urls_by_id.get(project_id)
            if existing_url and existing_url != project["url"]:
                raise DataIntegrityError(
                    f"Source ID {project_id} maps to two URLs in one scan"
                )
            projects_by_id[project_id] = project
            urls_by_id[project_id] = project["url"]
        except Exception as exc:
            failures.append(
                {
                    "url": url,
                    "error": sanitize_error_message(exc),
                    "type": type(exc).__name__,
                }
            )
            log(
                "ERROR",
                "opportunity_extraction_failed",
                url=url,
                error=sanitize_error_message(exc),
            )
    return ScanResult(
        projects=list(projects_by_id.values()),
        discovered_count=len(discovered),
        failures=failures,
        scan_at=scan_time.isoformat(),
    )


# ---------------------------------------------------------------------------
# SQLite persistence
# ---------------------------------------------------------------------------


REQUIRED_COLUMNS = {
    "id",
    "source_project_id",
    "scan_at",
    "posted_at",
    "title",
    "description",
    "location",
    "project_length",
    "url",
    "budget",
    "engagement_type",
    "notification_status",
    "first_seen_at",
    "last_seen_at",
    "email_sent_at",
    "email_attempts",
    "last_email_attempt_at",
    "last_email_error",
    "created_at",
    "updated_at",
}


@contextmanager
def db_connection(db_path: str | None = None) -> Iterator[sqlite3.Connection]:
    path = Path(db_path or Config.SQLITE_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path), timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=5000")
    connection.execute("PRAGMA foreign_keys=ON")
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def table_columns(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row["name"])
        for row in connection.execute("PRAGMA table_info(projects)").fetchall()
    }


def init_db(db_path: str | None = None) -> None:
    with db_connection(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_project_id TEXT NOT NULL UNIQUE,
                scan_at TEXT NOT NULL,
                posted_at TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                location TEXT NOT NULL DEFAULT 'Not specified',
                project_length TEXT NOT NULL DEFAULT 'Not specified',
                url TEXT NOT NULL UNIQUE,
                budget TEXT NOT NULL DEFAULT 'Not specified',
                engagement_type TEXT NOT NULL DEFAULT 'Not specified',
                notification_status TEXT NOT NULL DEFAULT 'pending',
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                email_sent_at TEXT,
                email_attempts INTEGER NOT NULL DEFAULT 0,
                last_email_attempt_at TEXT,
                last_email_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        columns = table_columns(connection)
        added_notification_status = "notification_status" not in columns
        migrations = {
            "notification_status": "TEXT NOT NULL DEFAULT 'pending'",
            "last_email_attempt_at": "TEXT",
        }
        for name, declaration in migrations.items():
            if name not in columns:
                connection.execute(
                    f"ALTER TABLE projects ADD COLUMN {name} {declaration}"
                )
        columns = table_columns(connection)
        if "notification_status" in columns:
            where_clause = "" if added_notification_status else "WHERE notification_status IS NULL OR notification_status NOT IN ('baseline','pending','sent','failed')"
            connection.execute(
                f"""
                UPDATE projects
                SET notification_status = CASE
                    WHEN email_sent_at IS NOT NULL THEN 'sent'
                    WHEN email_attempts > 0 THEN 'failed'
                    ELSE 'baseline'
                END
                {where_clause}
                """
            )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_projects_notification ON projects(notification_status, email_attempts)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_projects_last_seen ON projects(last_seen_at)"
        )


def db_is_empty(db_path: str | None = None) -> bool:
    with db_connection(db_path) as connection:
        return connection.execute("SELECT COUNT(*) FROM projects").fetchone()[0] == 0


def get_project(source_id: str, db_path: str | None = None) -> dict[str, Any] | None:
    with db_connection(db_path) as connection:
        row = connection.execute(
            "SELECT * FROM projects WHERE source_project_id = ?", (source_id,)
        ).fetchone()
        return dict(row) if row else None


def project_exists(source_id: str, db_path: str | None = None) -> bool:
    return get_project(source_id, db_path) is not None


def validate_project_identity(
    connection: sqlite3.Connection, project: Mapping[str, Any]
) -> None:
    source_id = str(project["source_project_id"])
    url = str(project["url"])
    row = connection.execute(
        "SELECT source_project_id, url FROM projects WHERE url = ? OR source_project_id = ?",
        (url, source_id),
    ).fetchone()
    if row and (row["source_project_id"] != source_id or row["url"] != url):
        raise DataIntegrityError(
            f"Identity collision: {source_id}/{url} conflicts with "
            f"{row['source_project_id']}/{row['url']}"
        )


def project_values(project: Mapping[str, Any], now: str) -> tuple[Any, ...]:
    return (
        project["source_project_id"],
        project["scan_at"],
        project["posted_at"],
        project["title"],
        project.get("description") or "",
        project.get("location") or "Not specified",
        project.get("project_length") or "Not specified",
        project["url"],
        project.get("budget") or "Not specified",
        project.get("engagement_type") or "Not specified",
        now,
        now,
        now,
        now,
    )


def seed_baseline(projects: Sequence[Mapping[str, Any]], db_path: str | None = None) -> int:
    now = iso_utc()
    inserted = 0
    with db_connection(db_path) as connection:
        if connection.execute("SELECT COUNT(*) FROM projects").fetchone()[0] != 0:
            raise DataIntegrityError("Baseline can only be seeded into an empty projects table")
        for project in projects:
            validate_project_identity(connection, project)
            connection.execute(
                """
                INSERT INTO projects (
                    source_project_id, scan_at, posted_at, title, description,
                    location, project_length, url, budget, engagement_type,
                    notification_status, first_seen_at, last_seen_at,
                    email_sent_at, email_attempts, last_email_attempt_at,
                    last_email_error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'baseline', ?, ?, NULL, 0, NULL, NULL, ?, ?)
                """,
                project_values(project, now),
            )
            inserted += 1
    return inserted


def insert_new_project(project: Mapping[str, Any], db_path: str | None = None) -> bool:
    now = iso_utc()
    with db_connection(db_path) as connection:
        validate_project_identity(connection, project)
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO projects (
                source_project_id, scan_at, posted_at, title, description,
                location, project_length, url, budget, engagement_type,
                notification_status, first_seen_at, last_seen_at,
                email_sent_at, email_attempts, last_email_attempt_at,
                last_email_error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, NULL, 0, NULL, NULL, ?, ?)
            """,
            project_values(project, now),
        )
        return cursor.rowcount == 1


def update_seen_project(project: Mapping[str, Any], db_path: str | None = None) -> None:
    now = iso_utc()
    with db_connection(db_path) as connection:
        validate_project_identity(connection, project)
        connection.execute(
            """
            UPDATE projects
            SET scan_at = ?, posted_at = ?, title = ?, description = ?,
                location = ?, project_length = ?, budget = ?, engagement_type = ?,
                last_seen_at = ?, updated_at = ?
            WHERE source_project_id = ?
            """,
            (
                project["scan_at"],
                project["posted_at"],
                project["title"],
                project.get("description") or "",
                project.get("location") or "Not specified",
                project.get("project_length") or "Not specified",
                project.get("budget") or "Not specified",
                project.get("engagement_type") or "Not specified",
                now,
                now,
                project["source_project_id"],
            ),
        )


def mark_email_sent(source_id: str, db_path: str | None = None) -> None:
    now = iso_utc()
    with db_connection(db_path) as connection:
        connection.execute(
            """
            UPDATE projects
            SET notification_status = 'sent', email_sent_at = ?,
                last_email_attempt_at = ?, last_email_error = NULL, updated_at = ?
            WHERE source_project_id = ?
            """,
            (now, now, now, source_id),
        )


def record_email_failure(
    source_id: str, error: Any, db_path: str | None = None
) -> None:
    now = iso_utc()
    with db_connection(db_path) as connection:
        connection.execute(
            """
            UPDATE projects
            SET notification_status = 'failed',
                email_attempts = email_attempts + 1,
                last_email_attempt_at = ?, last_email_error = ?, updated_at = ?
            WHERE source_project_id = ?
            """,
            (now, sanitize_error_message(error, 500), now, source_id),
        )


def pending_notifications(db_path: str | None = None) -> list[dict[str, Any]]:
    cutoff = iso_utc(utc_now() - timedelta(minutes=Config.EMAIL_RETRY_MINUTES))
    with db_connection(db_path) as connection:
        rows = connection.execute(
            """
            SELECT * FROM projects
            WHERE notification_status IN ('pending', 'failed')
              AND email_sent_at IS NULL
              AND email_attempts < ?
              AND (last_email_attempt_at IS NULL OR last_email_attempt_at <= ?)
            ORDER BY first_seen_at ASC
            """,
            (Config.MAX_EMAIL_ATTEMPTS, cutoff),
        ).fetchall()
        return [dict(row) for row in rows]


def list_projects(limit: int = 100, db_path: str | None = None) -> list[dict[str, Any]]:
    with db_connection(db_path) as connection:
        rows = connection.execute(
            "SELECT * FROM projects ORDER BY id DESC LIMIT ?", (max(1, limit),)
        ).fetchall()
        return [dict(row) for row in rows]


def run_db_check(db_path: str | None = None) -> bool:
    path = db_path or Config.SQLITE_PATH
    try:
        init_db(path)
        with db_connection(path) as connection:
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            columns = table_columns(connection)
            missing = REQUIRED_COLUMNS - columns
            invalid_states = connection.execute(
                """
                SELECT COUNT(*) FROM projects
                WHERE notification_status NOT IN ('baseline','pending','sent','failed')
                """
            ).fetchone()[0]
        print(f"Database: {path}")
        print(f"Integrity: {integrity}")
        print(f"Columns: {len(columns)}")
        print(f"Missing columns: {sorted(missing)}")
        print(f"Invalid notification states: {invalid_states}")
        return integrity == "ok" and not missing and invalid_states == 0
    except Exception as exc:
        print(f"Database check failed: {sanitize_error_message(exc)}")
        return False


# ---------------------------------------------------------------------------
# Email notifications
# ---------------------------------------------------------------------------


def project_email_text(project: Mapping[str, Any]) -> str:
    return f"""[Contra] New Project: {project.get('title', 'Untitled Opportunity')}

Title: {project.get('title', 'Untitled Opportunity')}
Scan At: {project.get('scan_at', '')}
Posted At: {project.get('posted_at', '')}
Location: {project.get('location', 'Not specified')}
Project Length: {project.get('project_length', 'Not specified')}
Budget: {project.get('budget', 'Not specified')}
Engagement Type: {project.get('engagement_type', 'Not specified')}
Exact Project URL: {project.get('url', '')}

Description:
{project.get('description', '')}

View Project on Contra:
{project.get('url', '')}
"""


def project_email_html(project: Mapping[str, Any]) -> str:
    def esc(name: str, default: str = "") -> str:
        return html_lib.escape(str(project.get(name) or default), quote=True)

    url = normalize_opportunity_url(str(project.get("url") or ""))
    if not url:
        raise ValueError("Project email contains an invalid Contra opportunity URL")
    safe_url = html_lib.escape(url, quote=True)
    description = html_lib.escape(str(project.get("description") or ""), quote=True)
    description = description.replace("\n", "<br>")
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"></head>
<body style="margin:0;background:#f4f5f7;font-family:Arial,Helvetica,sans-serif;color:#172033">
<div style="max-width:720px;margin:28px auto;background:#fff;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden">
  <div style="background:#111827;color:#fff;padding:24px 30px">
    <div style="font-size:12px;letter-spacing:1.2px;text-transform:uppercase;color:#cbd5e1">Contra Project Monitor</div>
    <h1 style="font-size:24px;line-height:1.35;margin:8px 0 0">{esc('title', 'Untitled Opportunity')}</h1>
  </div>
  <div style="padding:26px 30px">
    <table style="width:100%;border-collapse:collapse;font-size:14px">
      <tr><td style="padding:9px;border-bottom:1px solid #e5e7eb"><strong>Scan At</strong></td><td style="padding:9px;border-bottom:1px solid #e5e7eb">{esc('scan_at')}</td></tr>
      <tr><td style="padding:9px;border-bottom:1px solid #e5e7eb"><strong>Posted At</strong></td><td style="padding:9px;border-bottom:1px solid #e5e7eb">{esc('posted_at')}</td></tr>
      <tr><td style="padding:9px;border-bottom:1px solid #e5e7eb"><strong>Location</strong></td><td style="padding:9px;border-bottom:1px solid #e5e7eb">{esc('location', 'Not specified')}</td></tr>
      <tr><td style="padding:9px;border-bottom:1px solid #e5e7eb"><strong>Project Length</strong></td><td style="padding:9px;border-bottom:1px solid #e5e7eb">{esc('project_length', 'Not specified')}</td></tr>
      <tr><td style="padding:9px;border-bottom:1px solid #e5e7eb"><strong>Budget</strong></td><td style="padding:9px;border-bottom:1px solid #e5e7eb">{esc('budget', 'Not specified')}</td></tr>
      <tr><td style="padding:9px"><strong>Engagement Type</strong></td><td style="padding:9px">{esc('engagement_type', 'Not specified')}</td></tr>
    </table>
    <h2 style="font-size:17px;margin:26px 0 10px">Description</h2>
    <div style="font-size:14px;line-height:1.65;background:#f8fafc;border:1px solid #e5e7eb;border-radius:8px;padding:16px">{description}</div>
    <p style="text-align:center;margin:28px 0 4px">
      <a href="{safe_url}" style="display:inline-block;background:#2563eb;color:white;text-decoration:none;padding:13px 24px;border-radius:7px;font-weight:bold">View Project on Contra</a>
    </p>
    <p style="font-size:12px;color:#64748b;word-break:break-all;text-align:center">{safe_url}</p>
  </div>
</div>
</body></html>"""


def send_project_email(project: Mapping[str, Any]) -> tuple[bool, str]:
    if not smtp_configured():
        return False, "SMTP configuration is incomplete"
    try:
        msg = MIMEMultipart("alternative")
        safe_title = normalize_whitespace(project.get("title") or "Untitled Opportunity")
        msg["Subject"] = f"[Contra] New Project: {safe_title}"
        msg["From"] = Config.SENDER_EMAIL
        msg["To"] = ", ".join(Config.RECIPIENT_EMAILS)
        msg.attach(MIMEText(project_email_text(project), "plain", "utf-8"))
        msg.attach(MIMEText(project_email_html(project), "html", "utf-8"))
        tls_context = ssl.create_default_context()
        with smtplib.SMTP(
            Config.SMTP_SERVER, Config.SMTP_PORT, timeout=30
        ) as server:
            server.ehlo()
            server.starttls(context=tls_context)
            server.ehlo()
            server.login(Config.SENDER_EMAIL, Config.SENDER_PASSWORD)
            server.send_message(msg)
        log(
            "INFO",
            "project_email_accepted",
            source_project_id=project.get("source_project_id"),
            recipients=len(Config.RECIPIENT_EMAILS),
        )
        return True, ""
    except Exception as exc:
        message = sanitize_error_message(exc)
        log(
            "ERROR",
            "project_email_failed",
            source_project_id=project.get("source_project_id"),
            error=message,
        )
        # Deliberately do not call the operational-error email path here; the same
        # broken SMTP service would otherwise recursively fail.
        return False, message


# ---------------------------------------------------------------------------
# Chrome and cookie session
# ---------------------------------------------------------------------------


def find_executable(explicit: str, candidates: Sequence[str]) -> str:
    if explicit and Path(explicit).exists():
        return explicit
    for candidate in candidates:
        if Path(candidate).exists():
            return candidate
        found = shutil.which(Path(candidate).name)
        if found:
            return found
    return ""


def initialize_driver() -> webdriver.Chrome:
    if not SELENIUM_AVAILABLE:
        raise RuntimeError("Selenium is required for live monitoring. Install requirements.txt first.")
    options = Options()
    if Config.HEADLESS:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=en-US")
    profile_dir = tempfile.mkdtemp(prefix="contra-chrome-")
    options.add_argument(f"--user-data-dir={profile_dir}")
    setattr(options, "_contra_profile_dir", profile_dir)

    chrome = find_executable(
        Config.CHROME_BIN,
        (
            "/usr/bin/google-chrome",
            "/usr/bin/google-chrome-stable",
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
        ),
    )
    if chrome:
        options.binary_location = chrome

    driver_path = find_executable(
        Config.CHROMEDRIVER_PATH,
        (
            "/usr/bin/chromedriver",
            "/usr/lib/chromium/chromedriver",
            "/usr/lib/chromium-browser/chromedriver",
        ),
    )
    service: Service
    if driver_path:
        service = Service(driver_path)
    else:
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            from webdriver_manager.core.os_manager import ChromeType

            installed = ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
            service = Service(installed)
        except Exception:
            service = Service()
    try:
        driver = webdriver.Chrome(service=service, options=options)
        setattr(driver, "_contra_profile_dir", profile_dir)
        return driver
    except Exception:
        shutil.rmtree(profile_dir, ignore_errors=True)
        raise


def safe_quit(driver: webdriver.Chrome | None) -> None:
    if driver is None:
        return
    profile = getattr(driver, "_contra_profile_dir", "")
    try:
        driver.quit()
    except Exception:
        pass
    if profile:
        shutil.rmtree(profile, ignore_errors=True)


def cookie_path() -> Path:
    path = Path(Config.COOKIE_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def load_cookies(driver: webdriver.Chrome) -> bool:
    path = cookie_path()
    if not path.exists():
        return False
    try:
        cookies = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(cookies, list):
            return False
        driver.get(CONTRA_ORIGIN)
        WebDriverWait(driver, Config.PAGE_WAIT_SECONDS).until(
            lambda current: current.execute_script("return document.readyState")
            in {"interactive", "complete"}
        )
        driver.delete_all_cookies()
        loaded = 0
        for cookie in cookies:
            if not isinstance(cookie, MutableMapping):
                continue
            sanitized = dict(cookie)
            if sanitized.get("sameSite") not in {"Strict", "Lax", "None"}:
                sanitized.pop("sameSite", None)
            for unsupported in ("hostOnly", "session", "storeId", "id"):
                sanitized.pop(unsupported, None)
            if "expiry" in sanitized:
                try:
                    sanitized["expiry"] = int(sanitized["expiry"])
                except (TypeError, ValueError):
                    sanitized.pop("expiry", None)
            try:
                driver.add_cookie(sanitized)
                loaded += 1
            except Exception:
                pass
        return loaded > 0
    except Exception as exc:
        log("WARN", "cookie_load_failed", error=sanitize_error_message(exc))
        return False


def save_cookies(driver: webdriver.Chrome) -> bool:
    try:
        path = cookie_path()
        temporary = path.with_name(path.name + ".tmp")
        payload = json.dumps(driver.get_cookies(), indent=2).encode("utf-8")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
        os.replace(temporary, path)
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass
        return True
    except Exception as exc:
        log("WARN", "cookie_save_failed", error=sanitize_error_message(exc))
        return False


def setup_session(driver: webdriver.Chrome) -> None:
    load_cookies(driver)
    driver.get(Config.JOBS_URL)
    WebDriverWait(driver, Config.PAGE_WAIT_SECONDS).until(
        lambda current: current.execute_script("return document.readyState")
        in {"interactive", "complete"}
    )
    validate_jobs_page(driver)
    save_cookies(driver)


# ---------------------------------------------------------------------------
# Monitoring behavior
# ---------------------------------------------------------------------------


def attempt_notification(project: Mapping[str, Any], db_path: str | None = None) -> bool:
    source_id = str(project["source_project_id"])
    sent, error = send_project_email(project)
    if sent:
        mark_email_sent(source_id, db_path)
        return True
    record_email_failure(source_id, error, db_path)
    return False


def process_scan(result: ScanResult) -> tuple[int, int, int]:
    if not result.projects:
        raise FeedValidationError("A successful scan returned zero parsed opportunities")
    cold_start = db_is_empty()
    if cold_start:
        if not result.complete:
            raise IncompleteScanError(
                f"Baseline aborted: parsed {len(result.projects)} of "
                f"{result.discovered_count}; failures={len(result.failures)}"
            )
        inserted = seed_baseline(result.projects)
        log("INFO", "baseline_seeded", count=inserted)
        return inserted, 0, 0

    new_count = 0
    sent_count = 0
    failed_count = 0
    attempted_ids: set[str] = set()
    for project in result.projects:
        source_id = str(project["source_project_id"])
        existing = get_project(source_id)
        if existing:
            update_seen_project(project)
            continue
        if insert_new_project(project):
            new_count += 1
            attempted_ids.add(source_id)
            if attempt_notification(project):
                sent_count += 1
            else:
                failed_count += 1

    for pending in pending_notifications():
        source_id = str(pending["source_project_id"])
        if source_id in attempted_ids:
            continue
        attempted_ids.add(source_id)
        if attempt_notification(pending):
            sent_count += 1
        else:
            failed_count += 1
    return new_count, sent_count, failed_count


def run_one_scan(driver: webdriver.Chrome) -> bool:
    set_monitor_state("scanning")
    result = scan_for_projects(driver)
    if result.failures:
        log(
            "WARN",
            "scan_partial",
            discovered=result.discovered_count,
            parsed=len(result.projects),
            failures=len(result.failures),
        )
    new_count, sent_count, failed_count = process_scan(result)
    save_cookies(driver)
    scan_status = "degraded" if result.failures or failed_count else "ok"
    set_monitor_state(
        scan_status,
        last_successful_scan=result.scan_at if not result.failures else None,
        last_error=(f"{len(result.failures)} opportunity extraction failure(s)" if result.failures else None),
        visible_projects=len(result.projects),
        new_projects=new_count,
        emails_accepted=sent_count,
        email_failures=failed_count,
        extraction_failures=len(result.failures),
    )
    log(
        "INFO",
        "scan_complete",
        discovered=result.discovered_count,
        parsed=len(result.projects),
        new=new_count,
        emailed=sent_count,
        email_failures=failed_count,
    )
    return failed_count == 0 and not result.failures


def run_monitor(once: bool = False) -> int:
    init_db()
    driver: webdriver.Chrome | None = None
    try:
        driver = initialize_driver()
        setup_session(driver)
        while not shutdown_event.is_set():
            try:
                success = run_one_scan(driver)
                if once:
                    return 0 if success else 1
            except AuthenticationRequired as exc:
                paths = save_evidence(driver, "authentication_required")
                set_monitor_state("failed", last_error=sanitize_error_message(exc))
                send_operational_error(
                    "CONTRA_AUTHENTICATION_REQUIRED", exc, "\n".join(paths)
                )
                return 1 if once else 2
            except Exception as exc:
                paths = save_evidence(driver, "scan_failure")
                error = sanitize_error_message(exc)
                set_monitor_state("failed", last_error=error)
                log("ERROR", "scan_failed", error=error)
                send_operational_error(
                    type(exc).__name__.upper(),
                    exc,
                    f"Evidence: {paths}\n{sanitize_error_message(traceback.format_exc(), 8000)}",
                )
                if once:
                    return 1
                safe_quit(driver)
                driver = None
                if interruptible_sleep(Config.LOGIN_RETRY_INTERVAL):
                    break
                driver = initialize_driver()
                setup_session(driver)
                continue
            cleanup_old_evidence()
            if interruptible_sleep(Config.CHECK_INTERVAL):
                break
        return 0
    finally:
        safe_quit(driver)


# ---------------------------------------------------------------------------
# Self-test and CLI
# ---------------------------------------------------------------------------


def self_test() -> bool:
    slug = "RrWbgmE2-framer-specialist-for-technical-seo-and-final-touches"
    url = f"https://contra.com/opportunity/{slug}"
    fixture_state = {
        "routeParams": {"slug": slug},
        "publicAppConfiguration": {
            "relayRecordMap": {
                "client:root": {
                    '__typename': '__Root',
                    f'jobBySlug(slug:"{slug}")': {"__ref": "job:1"},
                },
                "job:1": {
                    "__typename": "Job",
                    "id": "job:1",
                    "title": "Framer Specialist for Technical SEO and Final Touches",
                    "slug": slug,
                    "jobPosting": {"__ref": "posting:1"},
                    "duration": {"__ref": "client:job:1:duration"},
                    "budget": {"__ref": "client:job:1:budget"},
                    "blocknoteDescription": [
                        {"type": "heading", "content": [{"text": "About the job:"}]},
                        {
                            "type": "paragraph",
                            "content": [{"text": "Brandbeet is seeking a skilled Framer specialist."}],
                        },
                        {"type": "heading", "content": [{"text": "What you’ll be doing:"}]},
                        {
                            "type": "bulletListItem",
                            "content": [{"text": "Implementing technical SEO best practices to enhance site visibility."}],
                        },
                        {
                            "type": "bulletListItem",
                            "content": [{"text": "Conducting thorough site audits to identify areas for improvement."}],
                        },
                        {
                            "type": "bulletListItem",
                            "content": [{"text": "Collaborating with the development team to ensure seamless integration of SEO enhancements."}],
                        },
                        {
                            "type": "bulletListItem",
                            "content": [{"text": "Testing the site post-implementation to confirm readiness for launch."}],
                        },
                    ],
                },
                "posting:1": {
                    "__typename": "JobPosting",
                    "createdAt": "2026-07-29T09:35:19.920Z",
                },
                "client:job:1:duration": {
                    "__id": "client:job:1:duration",
                    "__typename": "JobDuration",
                    "amount": 1,
                    "interval": "WEEK",
                },
                "client:job:1:budget": {
                    "__id": "client:job:1:budget",
                    "__typename": "FixedPriceJobBudget",
                    "feeMax": "USD:1000.00000000",
                    "feeMin": "USD:500.00000000",
                },
            }
        },
    }
    json_ld = {
        "@context": "https://schema.org",
        "@type": "JobPosting",
        "title": "Framer Specialist for Technical SEO and Final Touches",
        "description": "About the job:\nBrandbeet is seeking a skilled Framer specialist.",
        "datePosted": "2026-07-29",
        "employmentType": "CONTRACTOR",
        "jobLocationType": "TELECOMMUTE",
        "url": url,
    }
    fixture = f"""<html><head>
<link rel="canonical" href="{url}">
<script type="application/ld+json">{json.dumps(json_ld)}</script>
<script id="vike_pageContext" type="application/json">{json.dumps(fixture_state)}</script>
</head><body><main><h1>{json_ld['title']}</h1></main></body></html>"""
    scan_at = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)
    parsed = extract_contra_opportunity_from_html(
        fixture,
        url,
        scan_at,
        {"engagement_type": "One-time", "budget": "$500 - $1,000", "project_length": "1 week delivery"},
    )
    assertions = [
        parsed["source_project_id"] == "RrWbgmE2",
        parsed["title"] == json_ld["title"],
        parsed["location"] == "Remote",
        parsed["project_length"] == "1 week",
        parsed["budget"] == "$500 - $1,000",
        parsed["engagement_type"] == "One-time",
        parsed["posted_at"] == "2026-07-29T09:35:19.920000+00:00",
        all(
            responsibility in parsed["description"]
            for responsibility in (
                "Implementing technical SEO best practices",
                "Conducting thorough site audits",
                "Collaborating with the development team",
                "Testing the site post-implementation",
            )
        ),
        "&lt;script&gt;" in project_email_html({**parsed, "title": "<script>"}),
    ]

    temporary = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    temporary.close()
    try:
        init_db(temporary.name)
        assertions.append(db_is_empty(temporary.name))
        assertions.append(seed_baseline([parsed], temporary.name) == 1)
        row = get_project(parsed["source_project_id"], temporary.name)
        assertions.append(bool(row and row["notification_status"] == "baseline"))
        assertions.append(not pending_notifications(temporary.name))

        second = {
            **parsed,
            "source_project_id": "e896bbaf53c5bc9e",
            "id": "e896bbaf53c5bc9e",
            "url": "https://contra.com/opportunity/e896bbaf53c5bc9e-freelance-designer",
            "budget": "$60 - $90/hr",
            "engagement_type": "Hourly",
            "project_length": "Ongoing",
        }
        assertions.append(insert_new_project(second, temporary.name))
        pending = pending_notifications(temporary.name)
        assertions.append(len(pending) == 1 and pending[0]["notification_status"] == "pending")
        record_email_failure(second["source_project_id"], "password=secret token=abc", temporary.name)
        failed = get_project(second["source_project_id"], temporary.name)
        assertions.append(
            bool(
                failed
                and failed["notification_status"] == "failed"
                and failed["email_attempts"] == 1
                and "secret" not in (failed["last_email_error"] or "")
            )
        )
        mark_email_sent(second["source_project_id"], temporary.name)
        sent_row = get_project(second["source_project_id"], temporary.name)
        assertions.append(bool(sent_row and sent_row["notification_status"] == "sent"))
        update_seen_project({**second, "title": "Updated title"}, temporary.name)
        preserved = get_project(second["source_project_id"], temporary.name)
        assertions.append(bool(preserved and preserved["notification_status"] == "sent"))
        assertions.append(normalize_budget({"__typename": "HourlyJobBudget", "feeMin": "USD:60", "feeMax": "USD:90"}) == "$60 - $90/hr")
    finally:
        for suffix in ("", "-wal", "-shm"):
            Path(temporary.name + suffix).unlink(missing_ok=True)

    legacy = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    legacy.close()
    try:
        connection = sqlite3.connect(legacy.name)
        connection.execute(
            """
            CREATE TABLE projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_project_id TEXT NOT NULL UNIQUE,
                scan_at TEXT NOT NULL,
                posted_at TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                location TEXT NOT NULL DEFAULT 'Not specified',
                project_length TEXT NOT NULL DEFAULT 'Not specified',
                url TEXT NOT NULL UNIQUE,
                budget TEXT NOT NULL DEFAULT 'Not specified',
                engagement_type TEXT NOT NULL DEFAULT 'Not specified',
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                email_sent_at TEXT,
                email_attempts INTEGER NOT NULL DEFAULT 0,
                last_email_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        now = iso_utc()
        connection.execute(
            """
            INSERT INTO projects (
                source_project_id, scan_at, posted_at, title, description,
                location, project_length, url, budget, engagement_type,
                first_seen_at, last_seen_at, email_sent_at, email_attempts,
                last_email_error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, '', 'Remote', '1 week', ?, '$500', 'One-time', ?, ?, NULL, 0, NULL, ?, ?)
            """,
            ("legacy1", now, now, "Legacy baseline", "https://contra.com/opportunity/legacy1-project", now, now, now, now),
        )
        connection.commit()
        connection.close()
        init_db(legacy.name)
        legacy_row = get_project("legacy1", legacy.name)
        assertions.append(bool(legacy_row and legacy_row["notification_status"] == "baseline"))
        assertions.append(not pending_notifications(legacy.name))
    finally:
        for suffix in ("", "-wal", "-shm"):
            Path(legacy.name + suffix).unlink(missing_ok=True)

    for index, passed in enumerate(assertions, 1):
        print(f"self-test {index}: {'PASS' if passed else 'FAIL'}")
    return all(assertions)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Contra Project Monitor")
    parser.add_argument("--once", action="store_true", help="Run one scan and exit")
    parser.add_argument("--test", action="store_true", help="Send one explicit SMTP test email")
    parser.add_argument("--test-error-email", action="store_true", help="Send one operational error test email")
    parser.add_argument("--db-check", action="store_true", help="Check SQLite schema and integrity")
    parser.add_argument("--self-test", action="store_true", help="Run offline parser/database self-tests")
    parser.add_argument("--parse-html", metavar="PATH", help="Parse a saved Contra opportunity HTML file")
    parser.add_argument("--url", help="Opportunity URL for --parse-html")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    return parser


def smtp_test_project() -> dict[str, Any]:
    now = iso_utc()
    return {
        "source_project_id": "test-sample-id",
        "scan_at": now,
        "posted_at": now,
        "title": "Test Contra Opportunity",
        "description": "This email was generated by the explicit --test command.",
        "location": "Remote",
        "project_length": "2 weeks",
        "url": "https://contra.com/opportunity/test-sample-id",
        "budget": "$1,000 - $2,500",
        "engagement_type": "One-time",
    }


def main(argv: Sequence[str] | None = None) -> int:
    global DEBUG
    args = build_parser().parse_args(argv)
    DEBUG = args.debug
    install_signal_handlers()

    if args.self_test:
        return 0 if self_test() else 1
    if args.db_check:
        return 0 if run_db_check() else 1
    if args.parse_html:
        if not args.url:
            print("--parse-html requires --url", file=sys.stderr)
            return 2
        source = Path(args.parse_html).read_text(encoding="utf-8", errors="replace")
        parsed = extract_contra_opportunity_from_html(source, args.url)
        print(json.dumps(parsed, indent=2, ensure_ascii=False))
        return 0
    if args.test:
        sent, error = send_project_email(smtp_test_project())
        if not sent:
            print(f"Test email failed: {error}")
            return 1
        print(f"Test email accepted for {', '.join(Config.RECIPIENT_EMAILS)}")
        return 0
    if args.test_error_email:
        return 0 if send_operational_error("TEST_ERROR_EMAIL", "Forced test") else 1

    if not args.once:
        start_health_server()
    try:
        return run_monitor(once=args.once)
    finally:
        stop_health_server()


if __name__ == "__main__":
    raise SystemExit(main())