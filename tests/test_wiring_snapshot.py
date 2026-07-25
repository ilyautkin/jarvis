"""Страховочная сеть рефакторинга: снапшот проводки бота.

`telegram_bot.py` был монолитом на 4924 строки, и юнит-тестов на подавляющую
часть его кода нет. Разбирать такой файл на пакет `bot/` без сети — значит
двигать код вслепую: потерянный хендлер или забытая функция не упадут ни на
одном тесте, а проявятся в Telegram отказом одной команды.

Эти два теста дают минимальную, но точную гарантию для ПЕРЕМЕЩЕНИЙ:

* `HandlerWiringTest` — каждая команда и каждый callback-паттерн по-прежнему
  ведут в функцию с тем же именем. Ловит потерянную регистрацию, разъехавшийся
  паттерн, перепутанные местами колбэки.
* `PublicSurfaceTest` — имена, на которые опираются `scripts/jarvis_mcp_server.py`
  и остальные тесты, остаются доступны как `telegram_bot.<name>`.

Эталоны снимались с монолита ДО первого переноса (2026-07-25). Менять их в
одном коммите с переездом кода нельзя — это ровно та ошибка, от которой они
защищают. Меняем осознанно и отдельно, когда проводка правда должна измениться.
"""

from __future__ import annotations

import unittest

import telegram_bot


# (group, тип хендлера, команда или regex, имя колбэка). Порядок значим:
# PTB проверяет хендлеры в порядке регистрации, поэтому перестановка
# unauthorized_handler выше остальных отключила бы бота целиком.
EXPECTED_HANDLERS = [
    (0, 'CommandHandler', 'start', 'cmd_start'),
    (0, 'CommandHandler', 'new', 'cmd_reset'),
    (0, 'CommandHandler', 'reset', 'cmd_reset'),
    (0, 'CommandHandler', 'stop', 'cmd_stop'),
    (0, 'CommandHandler', 'spawn', 'cmd_spawn'),
    (0, 'CommandHandler', 'session', 'cmd_session'),
    (0, 'CommandHandler', 'tokens', 'cmd_tokens'),
    (0, 'CommandHandler', 'close', 'cmd_close'),
    (0, 'CommandHandler', 'engine', 'cmd_engine'),
    (0, 'CommandHandler', 'browser', 'cmd_browser'),
    (0, 'CommandHandler', 'persistent', 'cmd_persistent'),
    (0, 'CommandHandler', 'bind', 'cmd_bind'),
    (0, 'CommandHandler', 'unbind', 'cmd_unbind'),
    (0, 'CommandHandler', 'where', 'cmd_where'),
    (0, 'MessageHandler', '-', 'handle_photo'),
    (0, 'MessageHandler', '-', 'handle_document'),
    (0, 'MessageHandler', '-', 'handle_text'),
    (0, 'CallbackQueryHandler', '^cancel_queue:', 'on_cancel_queue'),
    (0, 'CallbackQueryHandler', '^engine_select:', 'on_engine_select'),
    (0, 'CallbackQueryHandler', '^model_select:', 'on_model_select'),
    (0, 'CallbackQueryHandler', '^engine_carry:', 'on_engine_carry'),
    (0, 'CallbackQueryHandler', '^ask:', 'on_ask_answer'),
    (0, 'CallbackQueryHandler', '^browser_toggle:', 'on_browser_toggle'),
    (0, 'CallbackQueryHandler', '^persistent_toggle:', 'on_persistent_toggle'),
    (0, 'CallbackQueryHandler', '^done_confirm:', 'on_done_confirm'),
    (0, 'MessageHandler', '-', 'unauthorized_handler'),
]

# Имена, которые обязаны остаться доступны через `telegram_bot.<name>`.
# Внешние потребители: scripts/jarvis_mcp_server.py импортирует
# parse_reminder_schedule/compute_next_fire, тесты — остальное.
EXPECTED_PUBLIC_NAMES = [
    # напоминания — единственный импорт из MCP-сервера
    "parse_reminder_schedule",
    "compute_next_fire",
    # схема и служебное
    "init_db",
    "DB_PATH",
    "build_application",
    "main",
    # сессии и сеансы
    "get_session",
    "reset_session",
    "update_session_id",
    "close_session",
    "touch_session",
    "mark_session_start",
    "ensure_active_session",
    "clear_close_request",
    "set_engine",
    "get_model",
    "update_model_only",
    "get_mcp_playwright",
    "set_mcp_playwright",
    "get_persistent_for_engine",
    "set_persistent_for_engine",
    "update_actual_model",
    "get_actual_model",
    "set_cwd",
    "clear_cwd",
    # топики и роли
    "resolve_manager_topic",
    "resolve_topic_role",
    "save_message_context",
    "load_message_context",
    # очереди
    "claim_next_job",
    "finish_job",
    "claim_next_agent_trigger",
    "finish_agent_trigger",
    "cleanup_old_log_entries",
    "log_message",
    # промпт и вызов движка
    "build_system_prefix",
    "call_llm_stream",
    # разметка и отправка
    "md_to_html",
    "split_html_for_telegram",
    "send_to_topic",
    "send_document_to_topic",
    "send_claude_reply",
    "extract_file_markers",
    "deliver_file_markers",
    "ProgressJournal",
    # ask_user
    "get_pending_ask",
    "answer_ask",
    "ask_question_text",
    # handoff между движками
    "set_pending_summary",
    "get_pending_summary",
    "clear_pending_summary",
    "build_context_handoff",
    # разделяемое состояние: контейнеры, а не переприсваиваемые значения,
    # поэтому переезд в другой модуль их не ломает — но исчезнуть они не должны
    "chat_locks",
    "active_procs",
    "spawn_procs",
    "persistent_workers",
    "pending_queue",
]


def _wiring() -> list[tuple[int, str, str, str]]:
    app = telegram_bot.build_application(
        token="123456:fake-token-for-tests", allowed_user_ids={1},
    )
    rows: list[tuple[int, str, str, str]] = []
    for group, handlers in sorted(app.handlers.items()):
        for handler in handlers:
            commands = getattr(handler, "commands", None)
            pattern = getattr(getattr(handler, "pattern", None), "pattern", None)
            key = ",".join(sorted(commands)) if commands else (pattern or "-")
            rows.append((group, type(handler).__name__, key, handler.callback.__name__))
    return rows


class HandlerWiringTest(unittest.TestCase):
    def test_every_command_and_callback_routes_to_the_same_function(self) -> None:
        self.assertEqual(_wiring(), EXPECTED_HANDLERS)

    def test_unauthorized_handler_stays_last(self) -> None:
        """Он ловит всё, что не прошло whitelist. Уехав вверх по списку, он
        начал бы перехватывать сообщения разрешённых пользователей."""
        self.assertEqual(_wiring()[-1][3], "unauthorized_handler")

    def test_application_builds_without_network(self) -> None:
        app = telegram_bot.build_application(token="123456:fake", allowed_user_ids={1})
        self.assertTrue(app.handlers[0])


class PublicSurfaceTest(unittest.TestCase):
    def test_public_names_are_still_importable(self) -> None:
        missing = [n for n in EXPECTED_PUBLIC_NAMES if not hasattr(telegram_bot, n)]
        self.assertEqual(missing, [], f"пропали из telegram_bot: {missing}")

    def test_shared_state_containers_are_mutable_singletons(self) -> None:
        """Состояние делится по ссылке. Если контейнер подменят на новый объект
        (например пересоздадут при импорте), топики разойдутся по разным
        словарям и per-topic локи перестанут защищать."""
        for name in ("chat_locks", "active_procs", "spawn_procs",
                     "persistent_workers", "pending_queue"):
            with self.subTest(name=name):
                first = getattr(telegram_bot, name)
                self.assertIsInstance(first, dict)
                self.assertIs(first, getattr(telegram_bot, name))


if __name__ == "__main__":
    unittest.main()
