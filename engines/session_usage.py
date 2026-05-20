"""Best-effort session context usage inspection for Jarvis engines."""

from __future__ import annotations

import json
import os
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class SessionUsage:
    engine: str
    session_id: str
    source: str
    context_tokens: int | None = None
    input_tokens: int | None = None
    cache_read_tokens: int | None = None
    cache_write_tokens: int | None = None
    output_tokens: int | None = None
    estimated_tokens: int | None = None
    bytes_size: int | None = None
    path: str | None = None
    note: str | None = None

    @property
    def threshold_tokens(self) -> int | None:
        return self.context_tokens or self.estimated_tokens

    @property
    def is_estimate(self) -> bool:
        return self.context_tokens is None and self.estimated_tokens is not None


def inspect_session_usage(engine: str, session_id: str, cwd: str | None) -> SessionUsage:
    engine = (engine or "").strip().lower()
    if engine == "claude":
        return _inspect_claude(session_id, cwd)
    if engine == "codex":
        return _inspect_codex(session_id)
    if engine == "opencode":
        return _inspect_opencode(session_id)
    return SessionUsage(engine=engine, session_id=session_id, source="unknown")


def _estimate_tokens_from_bytes(size: int | None) -> int | None:
    if not size:
        return None
    # Conservative approximation for mixed RU/EN/code JSONL transcripts.
    return max(1, size // 4)


def _claude_sessions_dir_for(cwd: str) -> Path:
    encoded = re.sub(r"[/.]+", "-", cwd)
    return Path.home() / ".claude" / "projects" / encoded


def _inspect_claude(session_id: str, cwd: str | None) -> SessionUsage:
    effective_cwd = cwd or os.environ.get("CLAUDE_CWD", str(Path.home()))
    path = _claude_sessions_dir_for(effective_cwd) / f"{session_id}.jsonl"
    usage = SessionUsage(
        engine="claude",
        session_id=session_id,
        source="claude-jsonl",
        path=str(path),
    )
    if not path.is_file():
        usage.note = "session file not found"
        return usage
    try:
        usage.bytes_size = path.stat().st_size
        last_usage: dict[str, Any] | None = None
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                msg = row.get("message")
                if isinstance(msg, dict) and isinstance(msg.get("usage"), dict):
                    last_usage = msg["usage"]
        if last_usage:
            inp = _as_int(last_usage.get("input_tokens"))
            cache_read = _as_int(last_usage.get("cache_read_input_tokens"))
            cache_write = _as_int(last_usage.get("cache_creation_input_tokens"))
            out = _as_int(last_usage.get("output_tokens"))
            usage.input_tokens = inp
            usage.cache_read_tokens = cache_read
            usage.cache_write_tokens = cache_write
            usage.output_tokens = out
            usage.context_tokens = sum(v or 0 for v in (inp, cache_read, cache_write))
        else:
            usage.estimated_tokens = _estimate_tokens_from_bytes(usage.bytes_size)
            usage.source = "claude-jsonl-estimate"
            usage.note = "usage block not found; estimated from file size"
    except OSError as exc:
        usage.note = f"read failed: {exc}"
    return usage


def _codex_sessions_root() -> Path:
    return Path(os.environ.get("CODEX_SESSIONS_DIR", Path.home() / ".codex" / "sessions"))


def _inspect_codex(session_id: str) -> SessionUsage:
    usage = SessionUsage(engine="codex", session_id=session_id, source="codex-jsonl-estimate")
    if session_id.startswith("placeholder-"):
        usage.note = "placeholder session; codex has not returned real thread_id yet"
        return usage
    root = _codex_sessions_root()
    if not root.is_dir():
        usage.note = "sessions root not found"
        return usage
    path = None
    try:
        for match in root.rglob(f"rollout-*-{session_id}.jsonl"):
            path = match
            break
    except OSError as exc:
        usage.note = f"scan failed: {exc}"
        return usage
    if path is None or not path.is_file():
        usage.note = "session file not found"
        return usage
    usage.path = str(path)
    try:
        usage.bytes_size = path.stat().st_size
        usage.estimated_tokens = _estimate_tokens_from_bytes(usage.bytes_size)
        usage.note = "codex local session log does not expose stable usage; estimated from file size"
    except OSError as exc:
        usage.note = f"stat failed: {exc}"
    return usage


def _inspect_opencode(session_id: str) -> SessionUsage:
    usage = SessionUsage(engine="opencode", session_id=session_id, source="opencode-db")
    db_path = Path(os.environ.get("OPENCODE_DB", Path.home() / ".local/share/opencode/opencode.db"))
    usage.path = str(db_path)
    if session_id.startswith("placeholder-"):
        usage.note = "placeholder session; opencode has not returned real session id yet"
        return usage
    if not db_path.is_file():
        usage.note = "opencode db not found"
        return usage
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT tokens_input, tokens_output, tokens_cache_read, "
            "tokens_cache_write FROM session WHERE id = ?",
            (session_id,),
        ).fetchone()
        if row:
            usage.input_tokens = _as_int(row["tokens_input"])
            usage.output_tokens = _as_int(row["tokens_output"])
            usage.cache_read_tokens = _as_int(row["tokens_cache_read"])
            usage.cache_write_tokens = _as_int(row["tokens_cache_write"])
        msg = conn.execute(
            "SELECT data FROM message WHERE session_id = ? "
            "ORDER BY time_updated DESC LIMIT 20",
            (session_id,),
        ).fetchall()
        for item in msg:
            try:
                data = json.loads(item["data"])
            except (TypeError, json.JSONDecodeError):
                continue
            tokens = data.get("tokens")
            if isinstance(tokens, dict):
                usage.context_tokens = _as_int(tokens.get("total"))
                usage.input_tokens = _as_int(tokens.get("input")) or usage.input_tokens
                usage.output_tokens = _as_int(tokens.get("output")) or usage.output_tokens
                cache = tokens.get("cache")
                if isinstance(cache, dict):
                    usage.cache_read_tokens = _as_int(cache.get("read")) or usage.cache_read_tokens
                    usage.cache_write_tokens = _as_int(cache.get("write")) or usage.cache_write_tokens
                break
        if row is None and usage.context_tokens is None:
            usage.note = "session row not found"
    except sqlite3.Error as exc:
        usage.note = f"db read failed: {exc}"
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return usage


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
