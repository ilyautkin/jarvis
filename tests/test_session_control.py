from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from engines.session_usage import aggregate_claude_usage
from bot import db as bot_db
from bot import workers as bot_workers
from bot.handlers.toggles import (
    _done_confirm_keyboard,
    _looks_like_task_done,
    _looks_like_waiting_for_user,
    _session_confirm_token,
)


class DoneDetectorTest(unittest.TestCase):
    def test_done_phrases(self) -> None:
        samples = [
            "Готово. Изменения закоммитил, проверки прошли.",
            "Итог: фикс задеплоен и проверен.",
            "Done: implemented session control.",
        ]
        for text in samples:
            with self.subTest(text=text):
                self.assertTrue(_looks_like_task_done(text))

    def test_waiting_phrases_block_autoclose(self) -> None:
        samples = [
            "План готов, жду подтверждение.",
            "Можно продолжать?",
            "Нужно согласовать деплой.",
            "Готово ли отправлять?",
            "#ask_563 Нужно уточнение по окружению.",
        ]
        for text in samples:
            with self.subTest(text=text):
                self.assertTrue(_looks_like_waiting_for_user(text))
                self.assertFalse(_looks_like_task_done(text))

    def test_negative_done_phrases_block_autoclose(self) -> None:
        samples = [
            "Не готово: тесты упали.",
            "Not done: waiting for CI.",
        ]
        for text in samples:
            with self.subTest(text=text):
                self.assertFalse(_looks_like_task_done(text))

    def test_done_confirm_keyboard_uses_short_session_token(self) -> None:
        session_id = "placeholder-36e972de-0504-4d32-9c19-6eb67879d72c"
        token = _session_confirm_token(session_id)
        self.assertEqual(len(token), 12)

        keyboard = _done_confirm_keyboard(session_id)
        buttons = keyboard.inline_keyboard[0]
        callback_data = [button.callback_data for button in buttons]

        self.assertEqual(callback_data[0], f"done_confirm:{token}:yes")
        self.assertEqual(callback_data[1], f"done_confirm:{token}:no")
        self.assertLessEqual(max(len(item) for item in callback_data), 64)


class ClaudeUsageAggregationTest(unittest.TestCase):
    def test_deduplicates_repeated_request_id_rows(self) -> None:
        session_id = "session-1"
        cwd = "/tmp/jarvis-session-control-test"
        encoded_cwd = "-tmp-jarvis-session-control-test"
        ts = datetime(2026, 7, 19, 12, 0, tzinfo=timezone.utc).isoformat()

        duplicate_a = {
            "timestamp": ts,
            "sessionId": session_id,
            "requestId": "req_same",
            "uuid": "uuid-a",
            "message": {
                "id": "msg_same",
                "role": "assistant",
                "model": "claude-sonnet-5",
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 20,
                    "cache_creation_input_tokens": 30,
                    "cache_read_input_tokens": 40,
                },
            },
        }
        duplicate_b = {
            **duplicate_a,
            "uuid": "uuid-b",
        }
        unique = {
            "timestamp": ts,
            "sessionId": session_id,
            "requestId": "req_unique",
            "uuid": "uuid-c",
            "message": {
                "id": "msg_unique",
                "role": "assistant",
                "model": "claude-sonnet-5",
                "usage": {
                    "input_tokens": 1,
                    "output_tokens": 2,
                    "cache_creation_input_tokens": 3,
                    "cache_read_input_tokens": 4,
                },
            },
        }

        with tempfile.TemporaryDirectory() as tmp:
            transcript_dir = Path(tmp) / ".claude" / "projects" / encoded_cwd
            transcript_dir.mkdir(parents=True)
            transcript = transcript_dir / f"{session_id}.jsonl"
            transcript.write_text(
                "\n".join(json.dumps(row) for row in (duplicate_a, duplicate_b, unique)),
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"HOME": tmp}):
                usage = aggregate_claude_usage(session_id, cwd)

        totals = usage.by_model["claude-sonnet-5"]
        self.assertEqual(totals.n_messages, 2)
        self.assertEqual(totals.input_tokens, 11)
        self.assertEqual(totals.output_tokens, 22)
        self.assertEqual(totals.cache_write_tokens, 33)
        self.assertEqual(totals.cache_read_tokens, 44)
        self.assertIn("deduplicated 1", usage.note or "")


def _load_mcp_server():
    """scripts/ не пакет — грузим MCP-сервер по пути."""
    path = Path(__file__).resolve().parent.parent / "scripts" / "jarvis_mcp_server.py"
    spec = importlib.util.spec_from_file_location("jarvis_mcp_server_under_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class _FakeChat:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict]] = []

    async def send_message(self, text: str, **kwargs):
        self.sent.append((text, kwargs))
        return SimpleNamespace(message_id=4242)


class _FakeApp:
    def __init__(self, chat: _FakeChat) -> None:
        self.chat = chat
        self.bot = SimpleNamespace(get_chat=self._get_chat)

    async def _get_chat(self, chat_id: int):
        return self.chat


class ManagerCloseSessionTest(unittest.TestCase):
    """manager_close_session (MCP) → close_requests_worker (бот): закрытие
    сеанса чужого топика через БД, потому что процессы топика видит только бот."""

    CHAT_ID = -100500
    THREAD_ID = 77

    def _seed(self, db_path: str) -> None:
        now = datetime.utcnow().isoformat()
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, "
                "model, updated_at, last_activity_at, session_started_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (self.CHAT_ID, self.THREAD_ID, "sid-1", "/tmp/topic", "claude",
                 None, now, now, now),
            )
            conn.execute(
                "INSERT INTO jobs(chat_id, thread_id, text, status, created_at) "
                "VALUES (?, ?, ?, 'in_progress', ?)",
                (self.CHAT_ID, self.THREAD_ID, "долгая задача", now),
            )

    def test_close_request_flows_from_mcp_to_bot(self) -> None:
        mcp_server = _load_mcp_server()
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "bot_state.db")
            with patch.object(bot_db, "DB_PATH", db_path):
                bot_db.init_db()
                self._seed(db_path)

                mcp_server._DB_PATH = Path(db_path)
                result = mcp_server.manager_close_session(
                    thread_id=self.THREAD_ID, chat_id=self.CHAT_ID,
                )

                # MCP: сеанс закрыт сразу, активный job помечен на прерывание,
                # для бота выставлен close_requested.
                self.assertTrue(result["was_open"])
                self.assertEqual(len(result["interrupted_jobs"]), 1)
                self.assertEqual(result["engine"], "claude")
                with sqlite3.connect(db_path) as conn:
                    row = conn.execute(
                        "SELECT last_activity_at, close_requested FROM sessions "
                        "WHERE chat_id = ? AND thread_id = ?",
                        (self.CHAT_ID, self.THREAD_ID),
                    ).fetchone()
                    cancel = conn.execute(
                        "SELECT cancel_requested FROM jobs"
                    ).fetchone()[0]
                self.assertIsNone(row[0])
                self.assertIsNotNone(row[1])
                self.assertIsNotNone(cancel)

                # Бот: добивает процессы топика, гасит флаг, пишет в топик.
                chat = _FakeChat()
                app = _FakeApp(chat)
                key = (self.CHAT_ID, self.THREAD_ID)
                with patch.object(
                    bot_workers, "_kill_persistent_worker", new=AsyncMock(return_value=True)
                ) as kill:
                    asyncio.run(bot_workers._apply_close_request(app, key))
                kill.assert_awaited_once()
                self.assertEqual(kill.await_args.args[0], key)

                with sqlite3.connect(db_path) as conn:
                    flag = conn.execute(
                        "SELECT close_requested FROM sessions "
                        "WHERE chat_id = ? AND thread_id = ?",
                        (self.CHAT_ID, self.THREAD_ID),
                    ).fetchone()[0]
                    logged = conn.execute(
                        "SELECT kind, telegram_message_id FROM messages_log"
                    ).fetchone()
                self.assertIsNone(flag)
                self.assertEqual(len(chat.sent), 1)
                self.assertIn("Сеанс закрыт Менеджером", chat.sent[0][0])
                self.assertEqual(chat.sent[0][1]["message_thread_id"], self.THREAD_ID)
                self.assertEqual(logged, ("session_closed", 4242))

                # Повтор на закрытом сеансе идемпотентен (job к этому моменту
                # уже завершён — прерывать нечего).
                with sqlite3.connect(db_path) as conn:
                    conn.execute("UPDATE jobs SET status = 'done'")
                repeat = mcp_server.manager_close_session(
                    thread_id=self.THREAD_ID, chat_id=self.CHAT_ID,
                )
                self.assertFalse(repeat["was_open"])
                self.assertEqual(repeat["interrupted_jobs"], [])

    def test_unknown_topic_is_rejected(self) -> None:
        mcp_server = _load_mcp_server()
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "bot_state.db")
            with patch.object(bot_db, "DB_PATH", db_path):
                bot_db.init_db()
            mcp_server._DB_PATH = Path(db_path)
            with self.assertRaises(RuntimeError):
                mcp_server.manager_close_session(thread_id=1, chat_id=self.CHAT_ID)

    def test_mcp_adds_close_requested_column_on_old_db(self) -> None:
        """MCP-сервер может подняться раньше бота новой версии — колонку
        заводит сам, иначе инструмент падал бы на 'no such column'."""
        mcp_server = _load_mcp_server()
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "old.db")
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "CREATE TABLE sessions (chat_id INTEGER NOT NULL, "
                    "thread_id INTEGER NOT NULL, session_id TEXT NOT NULL)"
                )
                mcp_server._ensure_close_requested_column(conn)
                cols = [r[1] for r in conn.execute(
                    "PRAGMA table_info(sessions)"
                ).fetchall()]
        self.assertIn("close_requested", cols)


if __name__ == "__main__":
    unittest.main()
