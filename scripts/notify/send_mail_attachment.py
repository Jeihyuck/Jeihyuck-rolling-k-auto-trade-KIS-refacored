#!/usr/bin/env python3
import os
import sys
import smtplib
import argparse
import tarfile
import uuid
from email.message import EmailMessage
from pathlib import Path


def env_first(*names, default=None):
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def main():
    parser = argparse.ArgumentParser(description="Send email with a required, verified attachment via SMTP.")
    parser.add_argument("--to", default=env_first("MAIL_TO", "NAVER_MAIL_TO", "REPORT_MAIL_TO"))
    parser.add_argument("--subject", required=True)
    parser.add_argument("--body", default="")
    parser.add_argument("--attach", action="append", default=[])
    parser.add_argument("--attempt-id", default=os.getenv("NULLIM_MAIL_ATTEMPT_ID") or str(uuid.uuid4()))
    parser.add_argument("--from-addr", default=env_first("MAIL_FROM", "SMTP_FROM", "NAVER_SMTP_USER", "SMTP_USER"))
    parser.add_argument("--smtp-host", default=env_first("SMTP_HOST", "NAVER_SMTP_HOST", default="smtp.naver.com"))
    parser.add_argument("--smtp-port", type=int, default=int(env_first("SMTP_PORT", "NAVER_SMTP_PORT", default="587")))
    parser.add_argument("--smtp-user", default=env_first("SMTP_USER", "NAVER_SMTP_USER", "MAIL_USER"))
    parser.add_argument("--smtp-pass", default=env_first("SMTP_PASS", "NAVER_SMTP_PASS", "MAIL_PASS"))
    args = parser.parse_args()

    if not args.attach:
        print("[MAIL][ERROR] at least one attachment is required", file=sys.stderr)
        return 2

    if not args.to:
        print("[MAIL][ERROR] missing recipient: set MAIL_TO or pass --to", file=sys.stderr)
        return 2
    if not args.from_addr:
        print("[MAIL][ERROR] missing sender: set MAIL_FROM or SMTP_USER", file=sys.stderr)
        return 2
    if not args.smtp_user:
        print("[MAIL][ERROR] missing SMTP user: set SMTP_USER or NAVER_SMTP_USER", file=sys.stderr)
        return 2
    if not args.smtp_pass:
        print("[MAIL][ERROR] missing SMTP password: set SMTP_PASS or NAVER_SMTP_PASS", file=sys.stderr)
        return 2

    msg = EmailMessage()
    msg["From"] = args.from_addr
    msg["To"] = args.to
    msg["Subject"] = args.subject
    msg.set_content(args.body or "")

    max_bytes = int(os.getenv("NULLIM_MAIL_MAX_ATTACHMENT_MB", "15")) * 1024 * 1024
    for item in args.attach:
        path = Path(item)
        if not path.is_file() or path.stat().st_size == 0:
            print(f"[MAIL][ERROR] attachment missing or empty: {path}", file=sys.stderr)
            return 2
        if path.stat().st_size > max_bytes:
            print(f"[MAIL][ERROR] attachment exceeds {max_bytes} bytes: {path}", file=sys.stderr)
            return 2
        if path.name.endswith(".tar.gz"):
            try:
                with tarfile.open(path, "r:gz") as archive:
                    if not archive.getmembers():
                        raise tarfile.ReadError("empty archive")
            except (tarfile.TarError, OSError) as exc:
                print(f"[MAIL][ERROR] invalid tar.gz attachment: {path}: {exc}", file=sys.stderr)
                return 2
        data = path.read_bytes()
        msg.add_attachment(
            data,
            maintype="application",
            subtype="octet-stream",
            filename=path.name,
        )
        print(f"[MAIL][ATTACH] {path} bytes={len(data)}")

    print(f"[MAIL][SEND][START] host={args.smtp_host} port={args.smtp_port} to={args.to} subject={args.subject}")

    accepted = False
    try:
        with smtplib.SMTP(args.smtp_host, args.smtp_port, timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(args.smtp_user, args.smtp_pass)
            smtp.send_message(msg)
            accepted = True
    except (smtplib.SMTPServerDisconnected, TimeoutError, OSError) as exc:
        if accepted:
            print(f"[MAIL][UNKNOWN_AFTER_SEND] {exc}", file=sys.stderr)
            return 75
        raise

    print("[MAIL][SEND][OK]")
    print(f"[MAIL][MESSAGE_ID] {args.attempt_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
