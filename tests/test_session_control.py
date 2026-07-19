from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from engines.session_usage import aggregate_claude_usage
from telegram_bot import (
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


if __name__ == "__main__":
    unittest.main()
