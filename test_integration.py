import os
import json
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest
import smtplib
import socket

from contra_script import (
    init_db,
    db_connection,
    db_is_empty,
    get_project,
    seed_baseline,
    insert_new_project,
    update_seen_project,
    pending_notifications,
    mark_email_sent,
    record_email_failure,
    send_project_email,
    attempt_notification,
    normalize_opportunity_url,
    opportunity_slug,
    source_project_id,
    extract_contra_opportunity_from_html,
    parse_card_summary,
    normalize_budget,
    normalize_engagement,
    normalize_duration,
    sanitize_error_message,
    project_email_html,
    project_email_text,
    process_scan,
    ScanResult,
    IncompleteScanError,
    Config,
    main,
)


@pytest.fixture
def tmp_db(tmp_path):
    db_file = str(tmp_path / "test_contra.db")
    init_db(db_file)
    return db_file


def test_e2e_database_creation_and_integrity(tmp_db):
    assert os.path.exists(tmp_db)
    assert db_is_empty(tmp_db) is True
    with db_connection(tmp_db) as conn:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='projects';")
        assert cursor.fetchone() is not None


def test_e2e_source_id_and_url_normalization():
    slug = "RrWbgmE2-framer-specialist-for-technical-seo"
    url = f"https://contra.com/opportunity/{slug}"
    assert source_project_id(slug) == "RrWbgmE2"
    assert opportunity_slug(url) == slug
    assert normalize_opportunity_url(f"{url}?ref=feed#section") == url


def test_e2e_baseline_seeding_suppresses_emails(tmp_db):
    baseline_projects = [
        {
            "source_project_id": "sp-base-1",
            "id": "sp-base-1",
            "scan_at": "2026-07-30T12:00:00+00:00",
            "posted_at": "2026-07-30T12:00:00+00:00",
            "title": "Baseline Project 1",
            "description": "Pre-existing job",
            "location": "Remote",
            "project_length": "1 week",
            "url": "https://contra.com/opportunity/sp-base-1-job",
            "budget": "$500 - $1,000",
            "engagement_type": "One-time",
        }
    ]

    inserted = seed_baseline(baseline_projects, tmp_db)
    assert inserted == 1
    assert db_is_empty(tmp_db) is False

    row = get_project("sp-base-1", tmp_db)
    assert row is not None
    assert row["notification_status"] == "baseline"
    assert row["email_sent_at"] is None

    # Pending retry list MUST be empty (baseline projects are NOT emailed)
    assert len(pending_notifications(tmp_db)) == 0


def test_e2e_new_project_flow_and_email_state_transition(tmp_db):
    # Seed baseline
    seed_baseline([{
        "source_project_id": "base-1",
        "id": "base-1",
        "scan_at": "2026-07-30T12:00:00+00:00",
        "posted_at": "2026-07-30T12:00:00+00:00",
        "title": "Old Project",
        "description": "Old",
        "location": "Remote",
        "project_length": "1 week",
        "url": "https://contra.com/opportunity/base-1",
        "budget": "$100",
        "engagement_type": "One-time",
    }], tmp_db)

    # Insert new project
    new_opp = {
        "source_project_id": "new-1",
        "id": "new-1",
        "scan_at": "2026-07-30T12:05:00+00:00",
        "posted_at": "2026-07-30T12:05:00+00:00",
        "title": "Brand New Framer Project",
        "description": "New SEO project",
        "location": "Remote",
        "project_length": "2 weeks",
        "url": "https://contra.com/opportunity/new-1-framer",
        "budget": "$1,000",
        "engagement_type": "Fixed-price",
    }
    ok = insert_new_project(new_opp, tmp_db)
    assert ok is True

    # Check pending state
    pending = pending_notifications(tmp_db)
    assert len(pending) == 1
    assert pending[0]["source_project_id"] == "new-1"
    assert pending[0]["notification_status"] == "pending"

    # Mark email sent
    mark_email_sent("new-1", tmp_db)

    # Verify state transition to 'sent'
    sent_row = get_project("new-1", tmp_db)
    assert sent_row["notification_status"] == "sent"
    assert sent_row["email_sent_at"] is not None
    assert len(pending_notifications(tmp_db)) == 0


def test_e2e_email_failure_sanitization_and_retry(tmp_db):
    new_opp = {
        "source_project_id": "fail-1",
        "id": "fail-1",
        "scan_at": "2026-07-30T12:10:00+00:00",
        "posted_at": "2026-07-30T12:10:00+00:00",
        "title": "Failing Email Project",
        "description": "Desc",
        "location": "Remote",
        "project_length": "1 month",
        "url": "https://contra.com/opportunity/fail-1",
        "budget": "$500",
        "engagement_type": "One-time",
    }
    insert_new_project(new_opp, tmp_db)

    # Record failure with sensitive string
    record_email_failure("fail-1", "SMTP auth error password=secret_pass token=12345", tmp_db)

    failed_row = get_project("fail-1", tmp_db)
    assert failed_row["notification_status"] == "failed"
    assert failed_row["email_attempts"] == 1
    # Secrets MUST be redacted
    assert "secret_pass" not in failed_row["last_email_error"]
    assert "REDACTED" in failed_row["last_email_error"]


def test_e2e_html_email_generation_escaping():
    project = {
        "source_project_id": "xss-1",
        "title": "Title <script>alert(1)</script>",
        "description": "Desc <b>Bold</b>",
        "url": "https://contra.com/opportunity/xss-1",
        "budget": "$500",
        "location": "Remote",
        "project_length": "1 week",
        "engagement_type": "One-time",
        "posted_at": "2026-07-30T12:00:00+00:00",
    }
    html = project_email_html(project)
    assert "<script>" not in html
    assert "&lt;script&gt;" in html
    assert "https://contra.com/opportunity/xss-1" in html


def test_framer_fixture_extraction():
    fixture_path = Path(__file__).parent / "fixtures" / "framer_specialist.html"
    source = fixture_path.read_text(encoding="utf-8")
    url = "https://contra.com/opportunity/RrWbgmE2-framer-specialist-for-technical-seo-and-final-touches"
    parsed = extract_contra_opportunity_from_html(source, url)

    assert parsed["source_project_id"] == "RrWbgmE2"
    assert parsed["title"] == "Framer Specialist for Technical SEO and Final Touches"
    assert parsed["location"] == "Remote"
    assert parsed["project_length"] == "1 week"
    assert parsed["budget"] == "$500 - $1,000"
    assert parsed["engagement_type"] == "One-time"
    assert parsed["url"] == url
    assert parsed["posted_at"] == "2026-07-29T09:35:19.920000+00:00"

    responsibilities = [
        "Implementing technical SEO best practices",
        "Conducting comprehensive site audits",
        "Collaborating with the development team",
        "Testing the site post-implementation",
    ]
    for resp in responsibilities:
        assert resp in parsed["description"]


def test_duplicate_scan_behavior(tmp_db):
    project = {
        "source_project_id": "dup-1",
        "id": "dup-1",
        "scan_at": "2026-07-30T12:00:00+00:00",
        "posted_at": "2026-07-30T12:00:00+00:00",
        "title": "Duplicate Target",
        "description": "Original description",
        "location": "Remote",
        "project_length": "1 week",
        "url": "https://contra.com/opportunity/dup-1",
        "budget": "$500",
        "engagement_type": "One-time",
    }
    seed_baseline([project], tmp_db)

    # Verify baseline status
    row_before = get_project("dup-1", tmp_db)
    assert row_before["notification_status"] == "baseline"
    first_seen = row_before["first_seen_at"]

    # Re-scan duplicate project with updated details
    updated_project = {
        **project,
        "scan_at": "2026-07-30T13:00:00+00:00",
        "title": "Duplicate Target Updated",
    }
    update_seen_project(updated_project, tmp_db)

    # Verify row count is still 1
    with db_connection(tmp_db) as conn:
        count = conn.execute("SELECT COUNT(*) FROM projects WHERE source_project_id = 'dup-1'").fetchone()[0]
        assert count == 1

    row_after = get_project("dup-1", tmp_db)
    assert row_after["notification_status"] == "baseline"
    assert row_after["title"] == "Duplicate Target Updated"
    assert row_after["first_seen_at"] == first_seen


def test_partial_cold_start_scan_behavior(tmp_db, monkeypatch):
    # Override default DB path for process_scan
    monkeypatch.setattr("contra_script.Config.SQLITE_PATH", tmp_db)

    # Simulate 5 links discovered, 4 parsed, 1 failed
    parsed_projects = [
        {
            "source_project_id": f"cold-{i}",
            "id": f"cold-{i}",
            "scan_at": "2026-07-30T12:00:00+00:00",
            "posted_at": "2026-07-30T12:00:00+00:00",
            "title": f"Cold Project {i}",
            "description": f"Desc {i}",
            "location": "Remote",
            "project_length": "1 week",
            "url": f"https://contra.com/opportunity/cold-{i}",
            "budget": "$500",
            "engagement_type": "One-time",
        }
        for i in range(1, 5)
    ]
    scan_res = ScanResult(
        projects=parsed_projects,
        discovered_count=5,
        failures=[{"url": "https://contra.com/opportunity/cold-5", "error": "Extraction failure"}],
        scan_at="2026-07-30T12:00:00+00:00",
    )

    with pytest.raises(IncompleteScanError):
        process_scan(scan_res)

    # Database MUST remain empty (cold-start baseline was aborted)
    assert db_is_empty(tmp_db) is True


def test_smtp_call_sequence_and_message_content(monkeypatch):
    monkeypatch.setattr(Config, "SENDER_EMAIL", "test_sender@example.com")
    monkeypatch.setattr(Config, "SENDER_PASSWORD", "super_secret_password_123")
    monkeypatch.setattr(Config, "RECIPIENT_EMAILS", ("recip1@example.com", "recip2@example.com"))
    monkeypatch.setattr(Config, "SMTP_SERVER", "smtp.test.com")
    monkeypatch.setattr(Config, "SMTP_PORT", 587)

    project = {
        "source_project_id": "seq-1",
        "title": "Framer Specialist for Technical SEO",
        "scan_at": "2026-07-30T12:00:00+00:00",
        "posted_at": "2026-07-29T09:35:19.920000+00:00",
        "description": "About the job:\nBrandbeet is seeking a specialist.\n- Bullet 1",
        "location": "Remote",
        "project_length": "1 week",
        "budget": "$500 - $1,000",
        "engagement_type": "One-time",
        "url": "https://contra.com/opportunity/seq-1-framer-specialist",
    }

    mock_server = MagicMock()
    with patch("smtplib.SMTP", return_value=mock_server) as mock_smtp_cls:
        mock_server.__enter__.return_value = mock_server

        success, error = send_project_email(project)
        assert success is True
        assert error == ""

        # Verify SMTP instantiation arguments
        mock_smtp_cls.assert_called_once_with("smtp.test.com", 587, timeout=30)

        # Verify call sequence
        mock_server.ehlo.assert_called()
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with("test_sender@example.com", "super_secret_password_123")
        mock_server.send_message.assert_called_once()

        # Inspect generated MIMEMultipart message
        sent_msg = mock_server.send_message.call_args[0][0]
        assert sent_msg["Subject"] == "[Contra] New Project: Framer Specialist for Technical SEO"
        assert sent_msg["From"] == "test_sender@example.com"
        assert sent_msg["To"] == "recip1@example.com, recip2@example.com"

        # Check payload parts
        parts = sent_msg.get_payload()
        assert len(parts) == 2
        plain_body = parts[0].get_payload(decode=True).decode("utf-8")
        html_body = parts[1].get_payload(decode=True).decode("utf-8")

        # Verify required fields in plain text
        for text_field in (
            "Framer Specialist for Technical SEO",
            "2026-07-30T12:00:00+00:00",
            "2026-07-29T09:35:19.920000+00:00",
            "Remote",
            "1 week",
            "$500 - $1,000",
            "One-time",
            "https://contra.com/opportunity/seq-1-framer-specialist",
            "Brandbeet is seeking a specialist.",
        ):
            assert text_field in plain_body

        # Verify HTML escaping and Contra link
        assert 'href="https://contra.com/opportunity/seq-1-framer-specialist"' in html_body
        assert "super_secret_password_123" not in plain_body
        assert "super_secret_password_123" not in html_body
        assert "super_secret_password_123" not in str(sent_msg.items())


def test_db_state_transitions_all_cases(tmp_db):
    # 1. baseline -> baseline when seen again
    b_proj = {
        "source_project_id": "b-1",
        "scan_at": "2026-07-30T12:00:00+00:00",
        "posted_at": "2026-07-30T12:00:00+00:00",
        "title": "Base Proj",
        "description": "Desc",
        "location": "Remote",
        "project_length": "1 week",
        "url": "https://contra.com/opportunity/b-1",
        "budget": "$100",
        "engagement_type": "One-time",
    }
    seed_baseline([b_proj], tmp_db)
    row = get_project("b-1", tmp_db)
    assert row["notification_status"] == "baseline"
    assert row["email_sent_at"] is None

    update_seen_project({**b_proj, "title": "Base Proj Updated"}, tmp_db)
    row_reseen = get_project("b-1", tmp_db)
    assert row_reseen["notification_status"] == "baseline"

    # 2. new opportunity -> pending
    n_proj = {
        "source_project_id": "n-1",
        "scan_at": "2026-07-30T12:05:00+00:00",
        "posted_at": "2026-07-30T12:05:00+00:00",
        "title": "New Proj",
        "description": "Desc",
        "location": "Remote",
        "project_length": "2 weeks",
        "url": "https://contra.com/opportunity/n-1",
        "budget": "$500",
        "engagement_type": "Fixed-price",
    }
    insert_new_project(n_proj, tmp_db)
    row_new = get_project("n-1", tmp_db)
    assert row_new["notification_status"] == "pending"
    assert row_new["email_sent_at"] is None
    assert row_new["email_attempts"] == 0

    # 3. pending + SMTP success -> sent
    mark_email_sent("n-1", tmp_db)
    row_sent = get_project("n-1", tmp_db)
    assert row_sent["notification_status"] == "sent"
    assert row_sent["email_sent_at"] is not None
    assert row_sent["last_email_error"] is None

    # 4. pending + SMTP failure -> failed
    f_proj = {
        "source_project_id": "f-1",
        "scan_at": "2026-07-30T12:10:00+00:00",
        "posted_at": "2026-07-30T12:10:00+00:00",
        "title": "Fail Proj",
        "description": "Desc",
        "location": "Remote",
        "project_length": "1 month",
        "url": "https://contra.com/opportunity/f-1",
        "budget": "$1,000",
        "engagement_type": "One-time",
    }
    insert_new_project(f_proj, tmp_db)
    record_email_failure("f-1", "Connection timed out password=secret", tmp_db)
    row_fail = get_project("f-1", tmp_db)
    assert row_fail["notification_status"] == "failed"
    assert row_fail["email_attempts"] == 1
    assert row_fail["email_sent_at"] is None
    assert "secret" not in row_fail["last_email_error"]

    # 5. failed + retry success -> sent
    mark_email_sent("f-1", tmp_db)
    row_retry_sent = get_project("f-1", tmp_db)
    assert row_retry_sent["notification_status"] == "sent"
    assert row_retry_sent["email_sent_at"] is not None
    assert row_retry_sent["last_email_error"] is None

    # 6. failed + retry failure -> failed (attempts incremented)
    f2_proj = {
        "source_project_id": "f-2",
        "scan_at": "2026-07-30T12:15:00+00:00",
        "posted_at": "2026-07-30T12:15:00+00:00",
        "title": "Fail Proj 2",
        "description": "Desc",
        "location": "Remote",
        "project_length": "1 month",
        "url": "https://contra.com/opportunity/f-2",
        "budget": "$1,000",
        "engagement_type": "One-time",
    }
    insert_new_project(f2_proj, tmp_db)
    record_email_failure("f-2", "Attempt 1 failed", tmp_db)
    record_email_failure("f-2", "Attempt 2 failed", tmp_db)
    row_fail2 = get_project("f-2", tmp_db)
    assert row_fail2["notification_status"] == "failed"
    assert row_fail2["email_attempts"] == 2

    # 7. sent -> sent when seen again
    update_seen_project({**n_proj, "title": "New Proj Title Change"}, tmp_db)
    row_sent_reseen = get_project("n-1", tmp_db)
    assert row_sent_reseen["notification_status"] == "sent"
    assert row_sent_reseen["title"] == "New Proj Title Change"

    # Exclusions check: pending list must exclude baseline and sent rows
    pending = pending_notifications(tmp_db)
    pending_ids = {p["source_project_id"] for p in pending}
    assert "b-1" not in pending_ids
    assert "n-1" not in pending_ids
    assert "f-1" not in pending_ids


def test_smtp_failure_modes(monkeypatch, tmp_db):
    monkeypatch.setattr("contra_script.Config.SQLITE_PATH", tmp_db)
    monkeypatch.setattr(Config, "SENDER_EMAIL", "test@example.com")
    monkeypatch.setattr(Config, "SENDER_PASSWORD", "super_secret_password_123")
    monkeypatch.setattr(Config, "RECIPIENT_EMAILS", ("recip@example.com",))
    monkeypatch.setattr(Config, "SMTP_SERVER", "smtp.test.com")

    project = {
        "source_project_id": "failmode-1",
        "title": "Failure Mode Opportunity",
        "scan_at": "2026-07-30T12:00:00+00:00",
        "posted_at": "2026-07-30T12:00:00+00:00",
        "description": "Desc",
        "location": "Remote",
        "project_length": "1 week",
        "budget": "$500",
        "engagement_type": "One-time",
        "url": "https://contra.com/opportunity/failmode-1",
    }
    insert_new_project(project, tmp_db)

    failures = [
        ("connection", socket.gaierror("Name resolution failed")),
        ("tls", smtplib.SMTPException("TLS handshake failed")),
        ("auth", smtplib.SMTPAuthenticationError(535, b"Auth failed password=super_secret_password_123")),
        ("data", smtplib.SMTPDataError(554, b"Transaction failed")),
        ("timeout", socket.timeout("SMTP connection timed out")),
    ]

    for label, exc in failures:
        mock_server = MagicMock()
        mock_server.__enter__.return_value = mock_server

        if label == "connection":
            smtp_mock = patch("smtplib.SMTP", side_effect=exc)
        elif label == "tls":
            mock_server.starttls.side_effect = exc
            smtp_mock = patch("smtplib.SMTP", return_value=mock_server)
        elif label == "auth":
            mock_server.login.side_effect = exc
            smtp_mock = patch("smtplib.SMTP", return_value=mock_server)
        elif label == "data":
            mock_server.send_message.side_effect = exc
            smtp_mock = patch("smtplib.SMTP", return_value=mock_server)
        elif label == "timeout":
            mock_server.ehlo.side_effect = exc
            smtp_mock = patch("smtplib.SMTP", return_value=mock_server)

        with smtp_mock:
            success, err_msg = send_project_email(project)
            assert success is False
            assert len(err_msg) > 0
            assert "super_secret_password_123" not in err_msg

            # Execute attempt_notification to update DB
            attempt_notification(project, tmp_db)
            db_row = get_project("failmode-1", tmp_db)
            assert db_row["notification_status"] == "failed"
            assert "super_secret_password_123" not in (db_row["last_email_error"] or "")


def test_exact_once_behavior_and_restart(tmp_db, monkeypatch):
    monkeypatch.setattr("contra_script.Config.SQLITE_PATH", tmp_db)
    monkeypatch.setattr(Config, "SENDER_EMAIL", "sender@example.com")
    monkeypatch.setattr(Config, "SENDER_PASSWORD", "secret")
    monkeypatch.setattr(Config, "RECIPIENT_EMAILS", ("recip@example.com",))
    monkeypatch.setattr(Config, "SMTP_SERVER", "smtp.example.com")

    # Seed baseline first
    base_project = {
        "source_project_id": "b-init",
        "id": "b-init",
        "scan_at": "2026-07-30T12:00:00+00:00",
        "posted_at": "2026-07-30T12:00:00+00:00",
        "title": "Baseline Initial",
        "description": "Baseline",
        "location": "Remote",
        "project_length": "1 week",
        "url": "https://contra.com/opportunity/b-init",
        "budget": "$100",
        "engagement_type": "One-time",
    }
    seed_baseline([base_project], tmp_db)

    new_project = {
        "source_project_id": "once-1",
        "id": "once-1",
        "scan_at": "2026-07-30T12:05:00+00:00",
        "posted_at": "2026-07-30T12:05:00+00:00",
        "title": "Exact Once Target",
        "description": "Test description",
        "location": "Remote",
        "project_length": "2 weeks",
        "url": "https://contra.com/opportunity/once-1-target",
        "budget": "$1,500",
        "engagement_type": "Fixed-price",
    }

    send_count = 0

    def mock_send(proj):
        nonlocal send_count
        send_count += 1
        return True, ""

    monkeypatch.setattr("contra_script.send_project_email", mock_send)

    # Cycle 1: First scan with new project
    scan_res1 = ScanResult(
        projects=[base_project, new_project],
        discovered_count=2,
        failures=[],
        scan_at="2026-07-30T12:05:00+00:00",
    )
    new1, sent1, fail1 = process_scan(scan_res1)

    assert new1 == 1
    assert sent1 == 1
    assert fail1 == 0
    assert send_count == 1

    row1 = get_project("once-1", tmp_db)
    assert row1["notification_status"] == "sent"

    # Cycle 2: Second scan with same project
    scan_res2 = ScanResult(
        projects=[base_project, new_project],
        discovered_count=2,
        failures=[],
        scan_at="2026-07-30T12:10:00+00:00",
    )
    new2, sent2, fail2 = process_scan(scan_res2)

    assert new2 == 0
    assert sent2 == 0
    assert fail2 == 0
    assert send_count == 1  # STILL 1, no duplicate email sent!

    # Simulate Process Restart: Re-initialize DB context
    init_db(tmp_db)

    # Cycle 3: Post-restart scan with same project
    scan_res3 = ScanResult(
        projects=[base_project, new_project],
        discovered_count=2,
        failures=[],
        scan_at="2026-07-30T12:15:00+00:00",
    )
    new3, sent3, fail3 = process_scan(scan_res3)

    assert new3 == 0
    assert sent3 == 0
    assert fail3 == 0
    assert send_count == 1  # STILL 1, exact-once delivery guaranteed!


def test_cli_test_command(monkeypatch, tmp_db):
    monkeypatch.setattr("contra_script.Config.SQLITE_PATH", tmp_db)
    monkeypatch.setattr(Config, "SENDER_EMAIL", "sender@example.com")
    monkeypatch.setattr(Config, "SENDER_PASSWORD", "secret")
    monkeypatch.setattr(Config, "RECIPIENT_EMAILS", ("recip@example.com",))
    monkeypatch.setattr(Config, "SMTP_SERVER", "smtp.example.com")

    with patch("contra_script.send_project_email", return_value=(True, "")) as mock_send:
        ret = main(["--test"])
        assert ret == 0
        mock_send.assert_called_once()

    # DB should remain empty (no production opportunity row created by CLI test)
    assert db_is_empty(tmp_db) is True
