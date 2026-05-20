#!/usr/bin/env python3
"""Inspect Jarvis engine session context size."""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from engines.session_usage import SessionUsage, inspect_session_usage  # noqa: E402


def _db_path() -> Path:
    return Path(os.environ.get("JARVIS_MCP_DB", ROOT / "bot_state.db"))


def _session_from_topic(chat_id: int, thread_id: int) -> tuple[str, str, str | None]:
    db = _db_path()
    con = sqlite3.connect(db)
    row = con.execute(
        "SELECT engine, session_id, cwd FROM sessions WHERE chat_id = ? AND thread_id = ?",
        (chat_id, thread_id),
    ).fetchone()
    con.close()
    if row is None:
        raise SystemExit(f"topic not found in {db}: chat_id={chat_id} thread_id={thread_id}")
    return row[0], row[1], row[2]


def _fmt_int(value: int | None) -> str:
    if value is None:
        return "unknown"
    return f"{value:,}".replace(",", " ")


def print_usage(usage: SessionUsage) -> None:
    print(f"engine:       {usage.engine}")
    print(f"session_id:   {usage.session_id}")
    print(f"source:       {usage.source}")
    print(f"context:      {_fmt_int(usage.threshold_tokens)}" + (" (estimate)" if usage.is_estimate else ""))
    print(f"input:        {_fmt_int(usage.input_tokens)}")
    print(f"cache_read:   {_fmt_int(usage.cache_read_tokens)}")
    print(f"cache_write:  {_fmt_int(usage.cache_write_tokens)}")
    print(f"output:       {_fmt_int(usage.output_tokens)}")
    print(f"bytes:        {_fmt_int(usage.bytes_size)}")
    if usage.path:
        print(f"path:         {usage.path}")
    if usage.note:
        print(f"note:         {usage.note}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chat-id", type=int, help="Telegram chat_id from bot_state.db")
    parser.add_argument("--thread-id", type=int, default=0, help="Telegram thread_id")
    parser.add_argument("--engine", choices=["claude", "codex", "opencode"])
    parser.add_argument("--session-id")
    parser.add_argument("--cwd")
    args = parser.parse_args()

    if args.chat_id is not None:
        engine, session_id, cwd = _session_from_topic(args.chat_id, args.thread_id)
    else:
        if not args.engine or not args.session_id:
            parser.error("use either --chat-id [--thread-id] or --engine + --session-id")
        engine, session_id, cwd = args.engine, args.session_id, args.cwd

    usage = inspect_session_usage(engine, session_id, cwd)
    print_usage(usage)


if __name__ == "__main__":
    main()
