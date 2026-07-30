# Contra Project Monitor — Railway Deployment Guide

Production guide for running the Selenium Contra Project Monitor as a **single-replica** Railway background worker.

---

## Repository files

| File | Purpose |
|------|---------|
| `Dockerfile` | Python 3.12 slim + Debian Chromium/ChromeDriver image |
| `requirements.txt` | Python dependencies (`selenium`, `requests`, …) |
| `start.sh` | Startup banner + `exec python -u contra_script.py` |
| `railway.toml` | Dockerfile build, `/health` healthcheck, restart policy |
| `.env.example` | Safe variable template (no secrets) |
| `.dockerignore` | Keeps secrets and junk out of the image |
| `contra_script.py` | Full monitor (scrape, SQLite, cookies, emails, health) |

**Use exactly one Railway replica.** Multiple replicas can scan the same projects and send duplicate emails.

---

## Railway deployment

1. Push the project to a **private** GitHub repository.
2. Create a Railway project.
3. Add a service from that GitHub repository.
4. Confirm Railway detects the `Dockerfile` (`railway.toml` sets `builder = "DOCKERFILE"`).
5. Add required variables (see checklist below). Copy names from `.env.example`.
6. Set **replicas = 1**.
7. Deploy.

---

## Healthcheck

`GET /health` on `0.0.0.0:$PORT` (Railway sets `PORT`).

Returns HTTP **200** JSON while the process is alive.

---

## Railway variables

### Required

```env
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SENDER_EMAIL=your-sender@example.com
SENDER_PASSWORD=your-gmail-app-password
RECIPIENT_EMAILS=aliazzamdon@gmail.com
```

### Recommended production

```env
CONTRA_EMAIL=your-contra-email@example.com
CONTRA_JOBS_URL=https://contra.com/jobs
SQLITE_PATH=data/contra_monitor.db

error_recipent=operations@example.com
ERROR_EMAIL_COOLDOWN_MINUTES=30
CHECK_INTERVAL=60

CHROME_BIN=/usr/bin/chromium
CHROMEDRIVER_PATH=/usr/bin/chromedriver

TZ=Asia/Karachi
EVIDENCE_DIR=/tmp/contra-evidence
EVIDENCE_RETENTION_HOURS=24
COOKIE_FILE=/tmp/contra_cookies.json
```
