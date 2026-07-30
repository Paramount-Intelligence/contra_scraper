# Contra Scraper

Automated Contra opportunity monitor — scrapes [contra.com/jobs](https://contra.com/jobs), extracts full project details (budget, duration, engagement type, description), stores them in SQLite, and sends email notifications for every new listing.

Built for **24/7 unattended deployment** on Railway, Coolify, or any Docker host.

---

## Features

- **Full opportunity extraction** — JSON-LD, embedded Relay state, and semantic DOM parsing
- **SQLite persistence** — tracks seen projects, notification state, and scan history
- **Email alerts** — SMTP-based notifications with HTML + plain-text fallback
- **Session management** — cookie persistence with automatic re-login
- **Health check endpoint** — `GET /health` for container orchestrators
- **Evidence capture** — saves page snapshots for debugging failed parses
- **Single-replica safe** — designed to run as one instance to avoid duplicate sends

---

## Project Structure

```
contra_scraper/
├── contra_script.py          # Main monitor (scrape, DB, email, health server)
├── test_integration.py       # Integration test suite
├── fixtures/                 # HTML fixtures for testing
├── Dockerfile                # Python 3.12 + Chromium image
├── docker-compose.yml        # Local development (optional)
├── start.sh                  # Container entrypoint
├── requirements.txt          # Python dependencies
├── railway.toml              # Railway deployment config
├── .env.example              # Environment variable template
├── .github/workflows/        # CI/CD (Coolify deploy trigger)
└── README.md
```

---

## Quick Start

### Local Development

```bash
# Clone
git clone https://github.com/Paramount-Intelligence/contra_scraper.git
cd contra_scraper

# Install dependencies
pip install -r requirements.txt

# Configure
cp .env.example .env
# Edit .env with your credentials

# Run
python contra_script.py
```

### Docker

```bash
docker build -t contra-scraper .
docker run --env-file .env -p 8080:8080 contra-scraper
```

### Railway / Coolify

1. Push to a **private** GitHub repository
2. Create a new service from the repo
3. Railway auto-detects `Dockerfile` via `railway.toml`
4. Add environment variables (see [Configuration](#configuration))
5. Set **replicas = 1**
6. Deploy

---

## Configuration

Copy `.env.example` to `.env` and fill in:

| Variable | Required | Description |
|----------|----------|-------------|
| `CONTRA_EMAIL` | Yes | Contra account email |
| `CONTRA_PASSWORD` | Yes | Contra account password |
| `SMTP_SERVER` | Yes | SMTP host (e.g. `smtp.gmail.com`) |
| `SMTP_PORT` | Yes | SMTP port (e.g. `587`) |
| `SENDER_EMAIL` | Yes | Notification sender email |
| `SENDER_PASSWORD` | Yes | SMTP app password |
| `RECIPIENT_EMAILS` | Yes | Comma-separated recipient list |
| `CHECK_INTERVAL` | No | Seconds between scans (default: `60`) |
| `HEADLESS` | No | Run Chromium headless (default: `true`) |
| `SQLITE_PATH` | No | Database path (default: `data/contra_monitor.db`) |
| `EVIDENCE_DIR` | No | Snapshot storage (default: `/tmp/contra-evidence`) |
| `TZ` | No | Timezone (default: `Asia/Karachi`) |

---

## Health Check

The script runs an HTTP server on `0.0.0.0:$PORT`:

```
GET /health → 200 {"status": "ok", "uptime": "..."}
```

Railway and Coolify use this for automatic restart on failure.

---

## Important Notes

- **Run exactly one replica.** Multiple instances scanning simultaneously will send duplicate emails.
- The first successful scan is a **baseline** — no emails are sent until the second scan detects new projects.
- Cookies are persisted to survive container restarts and avoid repeated logins.

---

## License

Private — Paramount Intelligence. All rights reserved.
