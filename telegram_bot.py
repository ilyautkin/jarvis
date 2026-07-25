#!/usr/bin/env python3
"""Jarvis — тонкая Telegram-обёртка над LLM-CLI (claude, codex или opencode).

Модель: «один топик Telegram = один проект = одна постоянная LLM-сессия».
- Ключ сессии — (chat_id, message_thread_id). В не-форумных чатах thread_id=0.
- Каждый топик может быть привязан к своей рабочей директории (cwd) командой /bind.
- Внутри ключа вызовы сериализуются через asyncio.Lock; разные ключи работают параллельно.
- Используется stream-json: промежуточные сообщения (tool_use/exec, рассуждения)
  показываются пользователю.
- Движок выбирается per-topic: env JARVIS_ENGINE задаёт дефолт для новых топиков,
  команда /engine — переключает движок текущего топика (новый session_id, cwd
  сохраняется; контекст прежнего движка не переносится).
"""

import os
import re
import json
import hashlib
import shutil
import uuid
import secrets
import sqlite3
import asyncio
import logging
import tempfile
import time
from datetime import datetime, timedelta, timezone

from telegram import (
    BotCommand,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    MenuButtonCommands,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, RetryAfter
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

from config import TELEGRAM_TOKEN, ALLOWED_USER_IDS, BASE_DIR
from webhook_server import run_webhook_server
from imap_watcher import run_imap_watcher
from engines import (
    SUPPORTED_ENGINES,
    Engine,
    default_engine_name,
    engine_model_scope,
    ensure_engine_tools,
    get_engine_by_name,
    prewarm_models,
)
from engines.process_control import terminate_process_tree
from engines.session_usage import SessionUsage, inspect_session_usage
from engines.claude_engine import (
    CLAUDE_TIMEOUT,
    start_persistent as start_persistent_claude,
)
from engines.codex_engine import CODEX_TIMEOUT
from engines.persistent_codex import start_persistent as start_persistent_codex

# ---------- Константы и реэкспорт слоёв пакета ----------

from bot.settings import (
    CLAUDE_CWD,
    CONTEXT_WARN_TOKENS,
    DEFAULT_ENGINE,
    DEFAULT_ENGINE_NAME,
    DONE_CONFIRM_ON_DONE,
    FILE_MARKER_RE,
    MEDIA_DIR,
    MSG_LIMIT,
    SESSION_IDLE_MINUTES,
    TG_FILE_LIMIT_MB,
    TG_HARD_LIMIT,
    bool_env as _bool_env_raw,
    int_env as _int_env_raw,
)
from bot.db import DB_PATH, _backup_db_once, _db, init_db, log_message
from bot.queues import (
    _log_ttl_days,
    claim_next_agent_trigger,
    claim_next_job,
    cleanup_old_log_entries,
    finish_agent_trigger,
    finish_job,
)
from bot.topics import (
    PERSISTENT_IDLE_MINUTES,
    _key,
    _lock_for,
    active_procs,
    chat_locks,
    load_message_context,
    pending_queue,
    persistent_workers,
    resolve_manager_topic,
    resolve_topic_role,
    save_message_context,
    spawn_procs,
)
from bot.formatting import _html_escape, md_to_html, split_html_for_telegram
from bot.sessions import (
    INSTRUCTION_FILES,
    _instructions_changed,
    _parse_transfer_marker,
    _persistent_column_for_engine,
    _session_is_stale,
    _session_state_line,
    _transfer_marker,
    build_context_handoff,
    clear_close_request,
    clear_cwd,
    clear_pending_summary,
    close_session,
    ensure_active_session,
    get_actual_model,
    get_mcp_playwright,
    get_model,
    get_pending_summary,
    get_persistent_claude,
    get_persistent_for_engine,
    get_session,
    mark_session_start,
    reset_session,
    set_cwd,
    set_engine,
    set_mcp_playwright,
    set_pending_summary,
    set_persistent_claude,
    set_persistent_for_engine,
    touch_session,
    update_actual_model,
    update_model_only,
    update_session_id,
)
from bot.asks import (
    _mark_ask_answered,
    answer_ask,
    ask_question_text,
    get_pending_ask,
    on_ask_answer,
)
from bot.delivery import (
    JOURNAL_LINE_CHARS,
    JOURNAL_MAX_CHARS,
    ProgressJournal,
    _send_manager_notice,
    _send_with_html_fallback,
    deliver_file_markers,
    extract_file_markers,
    send_claude_reply,
    send_document_to_topic,
    send_to_topic,
)
from bot.llm import _build_reply_context_prefix, build_system_prefix, call_llm_stream
from bot.reminders import _DAY_NAMES, _reminders_tz, compute_next_fire, parse_reminder_schedule

logger = logging.getLogger(__name__)


async def cleanup_worker(app: Application) -> None:
    """Long-running background task: hourly sweep of stale log/jobs rows.

    Honors JARVIS_LOG_TTL_DAYS env (defaults to 30). Set to 0/none/off
    to disable cleanup entirely.
    """
    ttl = _log_ttl_days()
    if ttl <= 0:
        logger.info("cleanup_worker: TTL disabled (JARVIS_LOG_TTL_DAYS=%s)",
                    os.environ.get("JARVIS_LOG_TTL_DAYS"))
        return
    logger.info("cleanup_worker started (TTL=%dd)", ttl)
    while True:
        try:
            stats = cleanup_old_log_entries(ttl)
            if stats["messages_log"] or stats["jobs"] or stats["agent_triggers"]:
                logger.info(
                    "cleanup_worker: pruned messages_log=%d jobs=%d "
                    "agent_triggers=%d (TTL=%dd)",
                    stats["messages_log"], stats["jobs"], stats["agent_triggers"], ttl,
                )
            await asyncio.sleep(3600.0)
        except asyncio.CancelledError:
            logger.info("cleanup_worker cancelled")
            raise
        except Exception:
            logger.exception("cleanup_worker loop crashed; sleeping 5min")
            await asyncio.sleep(300.0)




async def reminders_worker(app: Application) -> None:
    """Раз в N секунд сканирует reminders и шлёт сработавшие в Менеджера."""
    interval = _env_int("JARVIS_REMINDERS_INTERVAL", 60, 10)
    logger.info("reminders_worker started (interval=%ds)", interval)
    while True:
        try:
            now = datetime.utcnow()
            now_iso = now.isoformat()
            with _db() as conn:
                due_rows = conn.execute(
                    "SELECT id, chat_id, thread_id, text, schedule, next_fire_at "
                    "FROM reminders WHERE enabled = 1 AND next_fire_at <= ? "
                    "ORDER BY next_fire_at ASC",
                    (now_iso,),
                ).fetchall()
            for r in due_rows:
                rid, rchat, rthread, rtext, rschedule, _ = r
                notice = f"🔔 Напоминание #{rid}: {rtext}"
                # Используем _send_manager_notice не получится — нотис для
                # конкретного thread_id, а helper жёстко идёт в Менеджера.
                # Но reminders сейчас работают только в Менеджеров топик
                # (по умолчанию), так что helper подходит, если thread_id
                # совпадает с manager-target. Универсально — отправим
                # напрямую как plain notice, плюс auto-kick если это
                # Менеджеров топик.
                mgr_target = resolve_manager_topic()
                try:
                    chat = await app.bot.get_chat(rchat)
                    sent = await send_to_topic(chat, rthread, notice)
                    msg_id = sent.message_id if sent is not None else None
                    log_message(rchat, rthread, "out", "reminder", notice, msg_id)
                except Exception:
                    logger.exception("reminders_worker: send failed id=%s", rid)
                    # Не пересчитываем next_fire_at — попробуем в следующем цикле.
                    continue

                # Auto-kick если напоминание в топик Менеджера.
                if mgr_target and (rchat, rthread) == mgr_target:
                    try:
                        with _db() as conn:
                            existing = conn.execute(
                                "SELECT COUNT(*) FROM jobs "
                                "WHERE chat_id=? AND thread_id=? "
                                "AND status IN ('pending','in_progress')",
                                (rchat, rthread),
                            ).fetchone()[0]
                            if existing == 0:
                                conn.execute(
                                    "INSERT INTO jobs(chat_id, thread_id, text, "
                                    "source, status, created_at) VALUES "
                                    "(?, ?, ?, 'self_notice', 'pending', ?)",
                                    (
                                        rchat, rthread,
                                        f"[REMINDER] 🔔 Сработало напоминание "
                                        f"#{rid}: {rtext}",
                                        now_iso,
                                    ),
                                )
                    except Exception:
                        logger.exception(
                            "reminders_worker: auto-kick failed id=%s", rid,
                        )

                # Пересчитать next_fire_at.
                try:
                    parsed = parse_reminder_schedule(rschedule)
                    next_fire = compute_next_fire(parsed, now)
                except Exception:
                    logger.exception(
                        "reminders_worker: failed to recompute next_fire id=%s "
                        "schedule=%r → disabling", rid, rschedule,
                    )
                    next_fire = None
                with _db() as conn:
                    if next_fire is None:
                        # once отработал, либо schedule сломался → выключаем.
                        conn.execute(
                            "UPDATE reminders SET enabled=0, last_fired_at=? "
                            "WHERE id=?",
                            (now_iso, rid),
                        )
                    else:
                        conn.execute(
                            "UPDATE reminders SET next_fire_at=?, last_fired_at=? "
                            "WHERE id=?",
                            (next_fire.isoformat(), now_iso, rid),
                        )
                logger.info(
                    "reminders_worker: fired id=%s next=%s",
                    rid, next_fire.isoformat() if next_fire else "DISABLED",
                )
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info("reminders_worker cancelled")
            raise
        except Exception:
            logger.exception("reminders_worker loop crashed; sleeping 60s")
            await asyncio.sleep(60.0)


def _env_int(name: str, default: int, min_val: int = 1) -> int:
    """Parse positive int from env with fallback. Used for heartbeat thresholds."""
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        n = int(raw)
        return max(min_val, n)
    except ValueError:
        logger.warning("%s=%r is not int, defaulting to %d", name, raw, default)
        return default


#
# Сеанс — это окно терминала: открывается первым сообщением, закрывается /close
# или по простою. Топик (cwd, движок, модель) переживает закрытие, контекст
# сессии — нет; переписка остаётся в messages_log и доступна движку через
# manager_inbox.
#
# Признак закрытого сеанса — last_activity_at IS NULL (session_id объявлен
# NOT NULL, обнулить его нельзя). Новый session_id создаётся лениво, при
# следующем сообщении, чтобы не плодить пустые сессии.


#
# Агент зовёт MCP-инструмент ask_user; тот пишет вопрос в ask_requests, шлёт его
# в топик (с кнопками, если заданы варианты) и БЛОКИРУЕТСЯ, полля таблицу.
# Ответ кладёт сюда бот — из нажатия кнопки или из обычного сообщения в топик.


async def _resolve_pending_summary(
    key: tuple[int, int], pending: str,
) -> str | None:
    """Маркер transfer_requested → указание новому движку прочитать историю
    топика. Легаси-строки (готовое summary из старых версий) отдаём как есть."""
    marker = _parse_transfer_marker(pending)
    if marker is None:
        return pending  # легаси: реальное summary, уже лежит в БД

    old_engine_name = marker["old_engine"]
    logger.info("resolving transfer marker: key=%s old_engine=%s",
                key, old_engine_name)
    return build_context_handoff(key, old_engine_name)


async def _kill_persistent_worker(key: tuple[int, int], reason: str) -> bool:
    """Убить живой процесс топика, если есть. Будит того, кто ждёт
    результата текущего хода (не вешает его до CLAUDE_TIMEOUT). Возвращает
    True, если воркер был и его убили."""
    worker = persistent_workers.pop(key, None)
    if worker is None:
        return False
    logger.info("killing persistent worker key=%s reason=%s", key, reason)
    worker.dead = True
    if worker.pending_future is not None and not worker.pending_future.done():
        worker.pending_future.set_result((False, reason))
    if worker.reader_task is not None:
        worker.reader_task.cancel()
    stderr_task = getattr(worker, "stderr_task", None)
    if stderr_task is not None:
        stderr_task.cancel()
    await terminate_process_tree(worker.proc)
    return True


async def persistent_reaper(app: Application) -> None:
    """Убивает простаивающие живые процессы (свободные между ходами)
    после PERSISTENT_IDLE_MINUTES простоя. Живой процесс не экономит токены
    (сессия и так резюмируется с диска), только задержку на старте — вечно
    держать subprocess смысла нет. Активные (busy) воркеры не трогает."""
    if PERSISTENT_IDLE_MINUTES <= 0:
        logger.info("persistent_reaper: disabled (JARVIS_PERSISTENT_IDLE_MINUTES<=0)")
        return
    logger.info("persistent_reaper started (idle=%dmin)", PERSISTENT_IDLE_MINUTES)
    while True:
        try:
            await asyncio.sleep(60.0)
            now = time.monotonic()
            stale = [
                key for key, w in list(persistent_workers.items())
                if not w.busy and (now - w.last_activity) > PERSISTENT_IDLE_MINUTES * 60
            ]
            for key in stale:
                await _kill_persistent_worker(key, "")
                logger.info("persistent_reaper: killed idle worker key=%s", key)
        except asyncio.CancelledError:
            logger.info("persistent_reaper cancelled")
            raise
        except Exception:
            logger.exception("persistent_reaper loop crashed; continuing")


# Закрытия сеансов, запрошенные Менеджером через MCP. Опрос частый (как у
# interrupt-вотчера): между запросом и смертью живого процесса топик ещё
# отвечает старым контекстом, поэтому окно держим коротким.
CLOSE_REQUEST_POLL_SECONDS = 2.0


async def _apply_close_request(app: Application, key: tuple[int, int]) -> None:
    """Доделать закрытие сеанса, помеченное Менеджером: убить процессы топика
    (их видит только бот), закрыть сеанс, погасить флаг и сказать об этом в
    топик. Порядок и эффект — те же, что у /close."""
    proc = active_procs.get(key)
    if proc is not None:
        await terminate_process_tree(proc)
        active_procs.pop(key, None)
        logger.info("manager close: killed active proc for key=%s", key)
    await _kill_persistent_worker(key, "сеанс закрыт Менеджером")

    close_session(*key)
    clear_close_request(*key)

    _sid, cwd, engine_name = get_session(*key)
    logger.info("session closed by manager: key=%s engine=%s", key, engine_name)

    notice = (
        "🚪 Сеанс закрыт Менеджером. Контекст сброшен — следующее сообщение "
        "начнёт новый.\n"
        f"Топик сохранён: {engine_name}, {cwd or CLAUDE_CWD}"
    )
    try:
        chat = await app.bot.get_chat(key[0])
        sent = await send_to_topic(chat, key[1], notice)
        log_message(key[0], key[1], "out", "session_closed", notice,
                    sent.message_id if sent is not None else None)
    except Exception:
        # Сеанс уже закрыт — нотис вторичен, повторять цикл из-за него нельзя.
        logger.exception("manager close: notice failed key=%s", key)


async def close_requests_worker(app: Application) -> None:
    """Исполняет закрытия сеансов, запрошенные Менеджером через MCP
    (manager_close_session).

    MCP-сервер — отдельный процесс: он умеет только пометить строку в
    sessions. Убить активный subprocess и живой процесс /persistent может
    лишь бот — они лежат в его памяти (active_procs / persistent_workers), а
    persistent-путь берёт воркера ДО ensure_active_session, т.е. без этого
    добивания топик продолжил бы отвечать из старого контекста."""
    logger.info("close_requests_worker started (poll=%.1fs)",
                CLOSE_REQUEST_POLL_SECONDS)
    while True:
        try:
            await asyncio.sleep(CLOSE_REQUEST_POLL_SECONDS)
            with _db() as conn:
                rows = conn.execute(
                    "SELECT chat_id, thread_id FROM sessions "
                    "WHERE close_requested IS NOT NULL"
                ).fetchall()
            for chat_id, thread_id in rows:
                await _apply_close_request(app, (chat_id, thread_id))
        except asyncio.CancelledError:
            logger.info("close_requests_worker cancelled")
            raise
        except Exception:
            logger.exception("close_requests_worker loop crashed; continuing")


# ```lang\n...\n```  (multiline) или ```...```










# ---------- Handlers: команды ----------

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    key = _key(update)
    session_id, cwd, engine_name = get_session(*key)
    model = get_model(*key)
    effective = cwd or CLAUDE_CWD
    model_line = f"Модель: `{model}`\n" if model else ""
    text = (
        f"Привет! Я Jarvis — Telegram-обёртка над LLM CLI.\n\n"
        f"Движок этого топика: `{engine_name}` (дефолт: `{DEFAULT_ENGINE_NAME}`)\n"
        f"{model_line}"
        f"session-id: `{session_id}`\n"
        f"Рабочая директория: `{effective}`" + (" (дефолт)" if not cwd else "") + "\n\n"
        "Команды:\n"
        "/engine [name] — показать/переключить движок (claude|codex|opencode)\n"
        "/browser [on|off] — браузер (Playwright MCP) для топика, on-demand\n"
        "/persistent [on|off] — живой процесс claude/codex: сообщения на лету, без очереди\n"
        "/tokens — оценка размера текущей сессии\n"
        f"/close — закрыть сеанс (сам закроется после {SESSION_IDLE_MINUTES} мин простоя)\n"
        "/new, /reset — закрыть сеанс и сразу открыть новый\n"
        "/stop — прервать текущий запрос (сеанс сохраняется)\n"
        "/session — показать session-id, cwd и движок\n"
        "/bind <path> — привязать топик к директории\n"
        "/unbind — сбросить привязку к дефолту\n"
        "/where — эффективный cwd"
    )
    await update.message.reply_text(text)


async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    key = _key(update)
    # Прерываем активный процесс
    proc = active_procs.get(key)
    if proc is not None:
        await terminate_process_tree(proc)
        logger.info("reset: killed active proc for key=%s", key)
    await _kill_persistent_worker(key, "сеанс сброшен через /new или /reset")
    new_id, cwd, engine_name = reset_session(*key)
    touch_session(*key)  # сеанс сразу открыт — /new это «закрыть и открыть»
    effective = cwd or CLAUDE_CWD
    logger.info("reset: key=%s engine=%s new_session=%s cwd=%s",
                key, engine_name, new_id, effective)
    await update.message.reply_text(
        f"🆕 Новый сеанс ({engine_name}), id: {new_id}\nCwd: {effective}"
    )


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    key = _key(update)
    args = context.args or []
    # /stop <spawn_id> — прибить конкретный spawn в этом топике.
    if args:
        spawn_id = args[0].strip().lower().lstrip("#")
        skey = (key[0], key[1], spawn_id)
        sproc = spawn_procs.get(skey)
        if sproc is None or sproc.returncode is not None:
            await update.message.reply_text(f"Spawn [#{spawn_id}] не найден или уже завершён.")
            return
        await terminate_process_tree(sproc)
        spawn_procs.pop(skey, None)
        await update.message.reply_text(f"⛔ Spawn [#{spawn_id}] прерван.")
        return

    worker = persistent_workers.get(key)
    if worker is not None and worker.busy and not worker.dead:
        _sid, _, engine_name = get_session(*key)
        await _kill_persistent_worker(key, "прервано через /stop")
        await update.message.reply_text(
            f"⛔ Живой процесс {engine_name} прерван. Сессия сохранена — следующее "
            "сообщение поднимет новый живой процесс."
        )
        return

    proc = active_procs.get(key)
    if proc is None or proc.returncode is not None:
        await update.message.reply_text(
            "Нет активного запроса в этом топике."
            + (f"\nАктивные spawn'ы: {', '.join('#' + s[2] for s in spawn_procs if s[:2] == key)}"
               if any(s[:2] == key for s in spawn_procs) else "")
        )
        return
    pid = proc.pid
    await terminate_process_tree(proc)
    # Достаём session_id и engine из БД, чистим pidfile, чтобы следующий запрос
    # не упёрся в "session in use".
    session_id, _, engine_name = get_session(*key)
    engine = get_engine_by_name(engine_name)
    engine.clear_stale_session_pidfile(session_id)
    active_procs.pop(key, None)
    logger.info("stop: killed proc pid=%s for key=%s, cleaned pidfile for %s (engine=%s)",
                pid, key, session_id, engine_name)
    await update.message.reply_text("⛔ Текущий запрос прерван. Сессия сохранена.")


def _topic_status_block(key: tuple[int, int]) -> str:
    """HTML-блок со статусом топика для /session и /engine.

    Pre-форматированный, моноширинный. Показывает engine, выбранную модель
    (что хотим) и реально использованную (что CLI сообщил последний раз),
    session-id, cwd.
    """
    session_id, cwd, engine_name = get_session(*key)
    model = get_model(*key)
    actual = get_actual_model(*key)
    effective_cwd = cwd or CLAUDE_CWD
    cwd_suffix = "" if cwd else " (дефолт)"
    if model and actual and model.lower() == actual.lower():
        model_line = actual
    elif model and actual:
        model_line = f"{actual}  (выбрано: {model})"
    elif actual:
        model_line = f"{actual}  (дефолт движка)"
    elif model:
        model_line = f"{model}  (ожидается, ещё не отвечал)"
    else:
        model_line = "(дефолт движка; ещё ни разу не отвечал)"
    browser_state = "on" if get_mcp_playwright(*key) else "off"
    persistent_state = "on" if get_persistent_for_engine(*key, engine_name) else "off"
    persistent_desc = (
        f"живой процесс {engine_name}"
        if _persistent_column_for_engine(engine_name)
        else "не поддерживается для этого движка"
    )
    body = (
        f"engine     : {engine_name}\n"
        f"model      : {model_line}\n"
        f"session-id : {session_id}\n"
        f"cwd        : {effective_cwd}{cwd_suffix}\n"
        f"browser    : {browser_state}  (Playwright MCP, on-demand)\n"
        f"persistent : {persistent_state}  ({persistent_desc})\n"
        f"сеанс      : {_session_state_line(key)}"
    )
    return "<pre>" + _html_escape(body) + "</pre>"


async def cmd_session(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    key = _key(update)
    await update.message.reply_text(
        _topic_status_block(key), parse_mode=ParseMode.HTML,
    )


async def cmd_tokens(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    key = _key(update)
    session_id, cwd, engine_name = get_session(*key)
    usage = inspect_session_usage(engine_name, session_id, cwd or CLAUDE_CWD)
    body = (
        f"engine      : {engine_name}\n"
        f"session-id  : {session_id}\n"
        f"сеанс       : {_session_state_line(key)}\n"
        f"usage       : {_usage_line(usage)}"
    )
    if usage.path:
        body += f"\npath        : {usage.path}"
    await update.message.reply_text("<pre>" + _html_escape(body) + "</pre>", parse_mode=ParseMode.HTML)


async def cmd_close(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Закрыть сеанс — как закрыть окно терминала. Топик (cwd, движок, модель)
    остаётся, контекст сессии — нет. Следующее сообщение откроет новый сеанс."""
    key = _key(update)

    proc = active_procs.get(key)
    if proc is not None:
        await terminate_process_tree(proc)
        active_procs.pop(key, None)
        logger.info("session close: killed active proc for key=%s", key)
    await _kill_persistent_worker(key, "сеанс закрыт через /close")

    was_open = close_session(*key)
    if not was_open:
        await update.message.reply_text(
            "Сеанс и так закрыт. Следующее сообщение откроет новый."
        )
        return

    _sid, cwd, engine_name = get_session(*key)
    logger.info("session closed: key=%s engine=%s", key, engine_name)
    await update.message.reply_text(
        "🚪 Сеанс закрыт. Контекст сброшен — следующее сообщение начнёт новый.\n"
        f"Топик сохранён: {engine_name}, {cwd or CLAUDE_CWD}"
    )


def _browser_keyboard(enabled: bool) -> InlineKeyboardMarkup:
    """Кнопка-тоггл браузера для /browser."""
    target = "off" if enabled else "on"
    label = "🚫 Выключить браузер" if enabled else "🌐 Включить браузер"
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(label, callback_data=f"browser_toggle:{target}")]]
    )


def _browser_precheck(enable: bool) -> tuple[bool, str]:
    """Можно ли включить браузер: npx есть и Playwright не выключен глобально."""
    if not enable:
        return True, ""
    from engines.playwright_mcp import playwright_command_args

    try:
        spec = playwright_command_args()
    except Exception as exc:
        return False, f"⚠️ Playwright недоступен: {exc}"
    if spec is None:
        return False, "⚠️ Playwright выключен глобально (JARVIS_PLAYWRIGHT_MCP=0)."
    return True, ""


async def _apply_browser(key: tuple[int, int], enable: bool) -> str:
    """Применить флаг браузера (с pre-check) и вернуть текст ответа."""
    ok, msg = _browser_precheck(enable)
    if not ok:
        return msg
    set_mcp_playwright(key[0], key[1], enable)
    logger.info("browser toggled for key=%s: %s", key, "on" if enable else "off")
    if enable:
        return (
            "🌐 Браузер включён для топика. Playwright MCP подключится со "
            "СЛЕДУЮЩЕГО сообщения (≈30 browser_* тулов в контексте). Контекст "
            "сессии сохраняется. Выключай через /browser off, когда закончишь "
            "— это экономит токены."
        )
    return (
        "🚫 Браузер выключен. Playwright больше не грузится в контекст этого "
        "топика (со следующего сообщения). Контекст сессии сохранён."
    )


async def cmd_browser(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/browser — статус + кнопка; /browser on|off — включить/выключить браузер
    (Playwright MCP) для текущего топика. On-demand: дефолт off."""
    key = _key(update)
    args = [a.strip().lower() for a in (context.args or [])]
    current = get_mcp_playwright(*key)

    if not args:
        state = "включён" if current else "выключен"
        await update.message.reply_text(
            f"Браузер (Playwright MCP) сейчас {state} для этого топика.\n\n"
            "On-demand: по умолчанию выключен, чтобы не держать ~30 browser_* "
            "тулов в каждом запросе. Включай только под браузерные задачи.",
            reply_markup=_browser_keyboard(current),
        )
        return

    arg = args[0]
    if arg in {"on", "вкл", "1", "true", "yes"}:
        enable = True
    elif arg in {"off", "выкл", "0", "false", "no"}:
        enable = False
    else:
        await update.message.reply_text("Использование: /browser [on|off]")
        return

    if enable == current:
        await update.message.reply_text(
            f"Браузер уже {'включён' if current else 'выключен'}."
        )
        return
    await update.message.reply_text(await _apply_browser(key, enable))


async def on_browser_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Колбэк кнопки browser_toggle:<on|off>."""
    query = update.callback_query
    data = query.data or ""
    if not data.startswith("browser_toggle:"):
        return
    try:
        await query.answer()
    except Exception:
        pass
    enable = data.split(":", 1)[1] == "on"
    key = _key(update)
    text = await _apply_browser(key, enable)
    try:
        await query.edit_message_text(
            text, reply_markup=_browser_keyboard(get_mcp_playwright(*key)),
        )
    except BadRequest:
        await send_to_topic(update.effective_chat, key[1], text)


def _persistent_keyboard(enabled: bool) -> InlineKeyboardMarkup:
    target = "off" if enabled else "on"
    label = "🚫 Выключить живой процесс" if enabled else "⚡ Включить живой процесс"
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(label, callback_data=f"persistent_toggle:{target}")]]
    )


async def _apply_persistent(key: tuple[int, int], enable: bool) -> str:
    _session_id, _cwd, engine_name = get_session(*key)
    if enable and _persistent_column_for_engine(engine_name) is None:
        return (
            f"⚠️ Живой процесс поддержан для claude и codex, а у топика "
            f"движок `{engine_name}`. Переключи `/engine claude` или "
            "`/engine codex` и включай после."
        )
    set_persistent_for_engine(key[0], key[1], engine_name, enable)
    logger.info(
        "persistent toggled for key=%s engine=%s: %s",
        key, engine_name, "on" if enable else "off",
    )
    if enable:
        if engine_name == "codex":
            transport = "codex app-server"
            append = "через turn/steer"
        else:
            transport = engine_name
            append = "через stdin stream-json"
        return (
            f"⚡ Живой процесс {engine_name} включён для топика. Со следующего "
            f"сообщения {transport} поднимается один раз на весь сеанс: то, что "
            "прилетит, пока он ещё работает над предыдущим, допишется ему "
            f"прямо во время работы ({append}), а не будет ждать своей очереди. "
            "Уже начатую команду это не остановит — только подхватится, как "
            "только он освободится от неё. Выключай через /persistent off, "
            "когда не нужно — простаивающий процесс просто занимает память."
        )
    await _kill_persistent_worker(key, "выключено через /persistent off")
    return "🚫 Живой процесс выключен. Дальше — как обычно, процесс на сообщение."


async def cmd_persistent(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/persistent — статус + кнопка; /persistent on|off — включить/выключить
    живой процесс claude/codex для топика (сообщения во время работы агента
    подхватываются на лету, а не ждут своей очереди)."""
    key = _key(update)
    args = [a.strip().lower() for a in (context.args or [])]
    _sid, _cwd, engine_name = get_session(*key)
    current = get_persistent_for_engine(*key, engine_name)

    if not args:
        state = "включён" if current else "выключен"
        if _persistent_column_for_engine(engine_name) is None:
            await update.message.reply_text(
                f"Живой процесс не поддержан для `{engine_name}`. "
                "Доступно для `claude` и `codex`."
            )
            return
        await update.message.reply_text(
            f"Живой процесс {engine_name} сейчас {state} для этого топика.\n\n"
            "Пока выключен (дефолт) — на каждое сообщение новый процесс, а "
            "то, что прилетает во время работы, ждёт своей очереди. Включи, "
            "если хочешь на лету дописывать задачу агенту, пока он работает.",
            reply_markup=_persistent_keyboard(current),
        )
        return

    arg = args[0]
    if arg in {"on", "вкл", "1", "true", "yes"}:
        enable = True
    elif arg in {"off", "выкл", "0", "false", "no"}:
        enable = False
    else:
        await update.message.reply_text("Использование: /persistent [on|off]")
        return

    if enable == current:
        await update.message.reply_text(
            f"Живой процесс уже {'включён' if current else 'выключен'}."
        )
        return
    await update.message.reply_text(await _apply_persistent(key, enable))


async def on_persistent_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Колбэк кнопки persistent_toggle:<on|off>."""
    query = update.callback_query
    data = query.data or ""
    if not data.startswith("persistent_toggle:"):
        return
    try:
        await query.answer()
    except Exception:
        pass
    enable = data.split(":", 1)[1] == "on"
    key = _key(update)
    text = await _apply_persistent(key, enable)
    try:
        await query.edit_message_text(
            text,
            reply_markup=_persistent_keyboard(
                get_persistent_for_engine(*key, get_session(*key)[2])
            ),
        )
    except BadRequest:
        await send_to_topic(update.effective_chat, key[1], text)


async def on_done_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Колбэк кнопки done_confirm:<session_token>:<yes|no>."""
    query = update.callback_query
    data = query.data or ""
    if not data.startswith("done_confirm:"):
        return
    try:
        _prefix, token, action = data.split(":", 2)
    except ValueError:
        try:
            await query.answer("Некорректная кнопка", show_alert=True)
        except Exception:
            pass
        return

    key = _key(update)
    session_id, cwd, engine_name = get_session(*key)
    if token != _session_confirm_token(session_id):
        text = "Эта кнопка относится к старой сессии. Текущую сессию не трогаю."
        try:
            await query.answer(text, show_alert=True)
        except Exception:
            pass
        try:
            await query.edit_message_text(text)
        except BadRequest:
            await send_to_topic(update.effective_chat, key[1], text)
        return

    if action != "yes":
        text = "Ок, продолжаем в текущей сессии."
        try:
            await query.answer("Продолжаем")
        except Exception:
            pass
        try:
            await query.edit_message_text(text)
        except BadRequest:
            await send_to_topic(update.effective_chat, key[1], text)
        return

    try:
        await query.answer("Закрываю сессию")
    except Exception:
        pass
    await _kill_persistent_worker(key, "сессия закрыта по подтверждению завершения задачи")
    was_open = close_session(key[0], key[1])
    if was_open:
        text = (
            "✅ Сессия закрыта после подтверждения завершения задачи. "
            "Следующее сообщение откроет новую."
        )
    else:
        text = "Сессия уже закрыта. Следующее сообщение откроет новую."
    logger.info(
        "done confirmation: key=%s engine=%s cwd=%s action=yes closed=%s",
        key, engine_name, cwd or CLAUDE_CWD, was_open,
    )
    try:
        await query.edit_message_text(text)
    except BadRequest:
        await send_to_topic(update.effective_chat, key[1], text)


def _engine_keyboard(current_engine: str) -> InlineKeyboardMarkup:
    """Inline-клавиатура с кнопками выбора движка. Текущий помечается ✓."""
    row = []
    for name in SUPPORTED_ENGINES:
        label = f"✓ {name}" if name == current_engine else name
        row.append(InlineKeyboardButton(label, callback_data=f"engine_select:{name}"))
    return InlineKeyboardMarkup([row])


def _model_label(model: str) -> str:
    """Сокращение для отображения: 'deepseek/deepseek-v4-flash' → 'deepseek-v4-flash'.

    Провайдера прячем, только если он и так дублируется в имени модели: в списке
    opencode рядом живут 'deepseek/deepseek-chat' и 'opencode/hy3-free', и у
    второго провайдер — единственное, что говорит, чья это модель."""
    provider, _, short = model.partition("/")
    if short and short.startswith(provider):
        return short
    return model


def _model_keyboard(engine_name: str, models: list[str]) -> InlineKeyboardMarkup:
    """Список моделей движка — по одной в строке, callback_data использует
    индекс модели в списке (не имя), чтобы не упереться в 64-байтный лимит
    callback_data при длинных идентификаторах."""
    rows = []
    for idx, model in enumerate(models):
        rows.append([
            InlineKeyboardButton(
                _model_label(model),
                callback_data=f"model_select:{engine_name}:{idx}",
            )
        ])
    return InlineKeyboardMarkup(rows)


def _carry_keyboard(
    old_engine: str, new_engine: str, model_idx: int | None = None,
) -> InlineKeyboardMarkup:
    """Inline-клавиатура «перенести контекст?». В callback_data зашивается
    выбранная модель целевого движка (индексом) — чтобы переключение и выбор
    модели атомарно прилетели в `on_engine_carry`. Для движков без моделей —
    `-` вместо индекса."""
    mtoken = "-" if model_idx is None else str(model_idx)
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "✅ Да, с резюме",
            callback_data=f"engine_carry:{old_engine}:{new_engine}:{mtoken}:y",
        ),
        InlineKeyboardButton(
            "🚫 Нет, чисто",
            callback_data=f"engine_carry:{old_engine}:{new_engine}:{mtoken}:n",
        ),
    ]])


def _engine_precheck(key: tuple[int, int], target: str) -> tuple[bool, str, str | None]:
    """Проверяет переключение ДО действий. Возвращает (ok, message, current_engine).
    current_engine != None даже при ok=False (если запись в БД есть)."""
    available = ", ".join(SUPPORTED_ENGINES)
    if target not in SUPPORTED_ENGINES:
        return False, f"Неизвестный движок: {target!r}. Доступны: {available}.", None

    _, _, current_engine = get_session(*key)
    if target == current_engine:
        return False, (
            f"Этот топик уже на движке `{target}`. /new — если нужна свежая сессия."
        ), current_engine

    target_engine = get_engine_by_name(target)
    if shutil.which(target_engine.bin_path) is None:
        return False, (
            f"⚠️ Бинарь `{target_engine.bin_path}` не найден в PATH. "
            f"Установи {target!r} CLI или задай путь через "
            f"{target.upper()}_BIN, перезапусти бота."
        ), current_engine

    return True, "", current_engine


def _resolve_target_model(target: str, model_idx: int | None) -> str | None:
    """По индексу из callback_data выдаёт реальное имя модели целевого движка.
    Контракт: если у движка нет моделей — None; если одна — она; иначе — по idx."""
    target_engine = get_engine_by_name(target)
    models = list(target_engine.models)
    if not models:
        return None
    if len(models) == 1:
        return models[0]
    if model_idx is None or model_idx < 0 or model_idx >= len(models):
        return None
    return models[model_idx]


async def _do_engine_switch(
    key: tuple[int, int], target: str, model: str | None = None,
) -> str:
    """Финальное действие переключения (без pre-check, который уже сделан вызывающим).
    Прерывает активный процесс, создаёт новый session_id, сохраняет model (или
    NULL для движков без моделей), возвращает текст ответа."""
    _, _, current_engine = get_session(*key)
    target_engine = get_engine_by_name(target)
    mcp_ok, mcp_status = ensure_engine_tools(target_engine)

    proc = active_procs.get(key)
    if proc is not None:
        await terminate_process_tree(proc)
        active_procs.pop(key, None)
        logger.info("engine switch: killed active proc for key=%s", key)
    await _kill_persistent_worker(key, "движок переключён через /engine")

    new_id, cwd = set_engine(key[0], key[1], target, model=model)
    effective = cwd or CLAUDE_CWD
    logger.info("engine switched for key=%s: %s -> %s (new sid=%s, model=%s)",
                key, current_engine, target, new_id, model)
    mcp_line = f"\n{mcp_status}" if mcp_ok else f"\n⚠️ {mcp_status}"
    model_line = f"\nМодель: {model}" if model else ""
    return (
        f"🔁 Движок переключён: {current_engine} → {target}"
        f"{model_line}\n"
        f"Новая сессия: {new_id}\n"
        f"Cwd сохранён: {effective}"
        f"{mcp_line}"
    )


def _usage_line(usage: SessionUsage) -> str:
    token_value = usage.threshold_tokens
    token_part = "unknown" if token_value is None else f"{token_value:,}".replace(",", " ")
    suffix = " (estimate)" if usage.is_estimate else ""
    bits = [f"context: {token_part}{suffix}", f"source: {usage.source}"]
    if usage.cache_read_tokens is not None:
        bits.append(f"cache_read: {usage.cache_read_tokens:,}".replace(",", " "))
    if usage.cache_write_tokens is not None:
        bits.append(f"cache_write: {usage.cache_write_tokens:,}".replace(",", " "))
    if usage.output_tokens is not None:
        bits.append(f"last_output: {usage.output_tokens:,}".replace(",", " "))
    if usage.bytes_size is not None:
        bits.append(f"file: {usage.bytes_size / 1024 / 1024:.1f} MB")
    if usage.note:
        bits.append(f"note: {usage.note}")
    return "; ".join(bits)


_DONE_RE = re.compile(
    r"\b("
    r"готово|итог|выполнено|закрыто|задеплоено|задеплоил|деплой\s+выполнен|"
    r"проверено|проверил|закоммитил|коммит|commit|deploy(?:ed|ment)?|"
    r"implemented|done|fixed"
    r")\b",
    re.IGNORECASE,
)
_WAIT_RE = re.compile(
    r"("
    r"жду|подтверди|подтвердите|можно(?:\s+[^?\n]{1,40})?\?|что\s+дальше\?|отправлять\?|"
    r"согласовать|согласуй|нужно\s+подтверждение|нужен\s+ответ|"
    r"уточни|уточните|нужно\s+уточнить|ожидаю|#ask_\d+|waiting|confirm|approve"
    r")",
    re.IGNORECASE,
)
_NOT_DONE_RE = re.compile(r"\b(не\s+готово|не\s+выполнено|не\s+закрыто|not\s+done)\b", re.IGNORECASE)


def _looks_like_waiting_for_user(text: str) -> bool:
    return bool(_WAIT_RE.search(text or ""))


def _looks_like_task_done(text: str) -> bool:
    if _looks_like_waiting_for_user(text):
        return False
    if _NOT_DONE_RE.search(text or ""):
        return False
    return bool(_DONE_RE.search(text or ""))


def _session_confirm_token(session_id: str) -> str:
    return hashlib.sha256(session_id.encode("utf-8", errors="replace")).hexdigest()[:12]


def _done_confirm_keyboard(session_id: str) -> InlineKeyboardMarkup:
    token = _session_confirm_token(session_id)
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("Да, закрыть", callback_data=f"done_confirm:{token}:yes"),
        InlineKeyboardButton("Нет", callback_data=f"done_confirm:{token}:no"),
    ]])


async def _warn_large_context_if_needed(
    chat, thread_id: int, engine_name: str, session_id: str, cwd: str | None, key: tuple[int, int],
) -> None:
    if CONTEXT_WARN_TOKENS <= 0:
        return
    try:
        usage = inspect_session_usage(engine_name, session_id, cwd or CLAUDE_CWD)
    except Exception:
        logger.exception("session usage inspection failed: key=%s engine=%s session=%s",
                         key, engine_name, session_id)
        return
    tokens = usage.threshold_tokens
    if tokens is None or tokens < CONTEXT_WARN_TOKENS:
        return
    try:
        await send_to_topic(
            chat, thread_id,
            "⚠️ Большой контекст: "
            f"{_usage_line(usage)}. "
            "Если задача завершена, используй /new или /close, чтобы следующий ход не тянул старую историю.",
        )
    except Exception:
        logger.exception("failed to send context warning: key=%s", key)


async def _ask_done_confirmation_if_needed(
    chat, thread_id: int, ok: bool, final_text: str, session_id: str, key: tuple[int, int],
) -> None:
    if not (DONE_CONFIRM_ON_DONE and ok and _looks_like_task_done(final_text)):
        return
    try:
        await send_to_topic(
            chat, thread_id,
            "Задача завершена? Закрыть сессию, чтобы следующий ход начал новый контекст?",
            reply_markup=_done_confirm_keyboard(session_id),
        )
    except Exception:
        logger.exception("failed to send done confirmation: key=%s", key)


def _inspect_topic_usage(key: tuple[int, int]) -> SessionUsage:
    session_id, cwd, engine_name = get_session(*key)
    return inspect_session_usage(engine_name, session_id, cwd or CLAUDE_CWD)


async def _do_engine_handoff(
    key: tuple[int, int],
    old_engine_name: str,
    new_engine_name: str,
    progress_edit,
    model: str | None = None,
) -> str:
    """Сценарий «с переносом контекста»: переключить движок и велеть новому
    поднять историю топика самому (через manager_inbox).

    Раньше здесь старый движок гонялся за резюме — полный проход по всей его
    истории, самый дорогой вызов из возможных, да ещё и до переключения. Теперь
    переключение мгновенное и бесплатное: новый движок читает ровно столько,
    сколько ему нужно, и только когда ему нужно.
    """
    chat_id, thread_id = key

    lock = _lock_for(key)
    if lock.locked():
        return (
            "⚠️ Топик занят активным запросом. Дождись завершения или /stop, "
            "потом повтори переключение."
        )

    await lock.acquire()
    try:
        await progress_edit("🔁 Переключаю движок...")
        switch_text = await _do_engine_switch(key, new_engine_name, model=model)
        set_pending_summary(
            chat_id, thread_id, _transfer_marker(old_engine_name),
        )
        logger.info("handoff: stored transfer marker for key=%s (old=%s)",
                    key, old_engine_name)
        return (
            f"{switch_text}\n\n"
            f"📖 Новый движок сам поднимет историю топика через manager_inbox "
            f"при первом сообщении — резюме у {old_engine_name} не запрашиваем."
        )
    finally:
        try:
            lock.release()
        except RuntimeError:
            pass


async def cmd_engine(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/engine — показать движок топика с кнопками переключения;
    /engine <name> [model-substring] [--keep-context] — переключить движок.
    С флагом --keep-context: summary-based handoff (старый движок пишет резюме,
    новый получает его в первый prompt). Без флага: чистый старт новой сессии."""
    key = _key(update)
    args = list(context.args or [])

    # Вытащим --keep-context из аргументов
    keep_context = False
    filtered: list[str] = []
    for a in args:
        if a == "--keep-context":
            keep_context = True
        else:
            filtered.append(a)
    args = filtered

    if not args:
        _, _, engine_name = get_session(*key)
        footer = _html_escape(
            f"\n\nДефолт (для новых топиков): {DEFAULT_ENGINE_NAME}\n"
            "Выбери новый движок ниже или введи /engine <name> [--keep-context]."
        )
        await update.message.reply_text(
            _topic_status_block(key) + footer,
            parse_mode=ParseMode.HTML,
            reply_markup=_engine_keyboard(engine_name),
        )
        return

    target = args[0].strip().lower()

    # Same-engine: текстовое /engine <current> <model> меняет только модель,
    # не пересоздаёт сессию. Контекст сохраняется. --keep-context не нужен.
    _, _, current_engine = get_session(*key)
    if target in SUPPORTED_ENGINES and target == current_engine and len(args) >= 2:
        target_engine = get_engine_by_name(target)
        models = list(target_engine.models)
        substr = args[1].strip().lower()
        exact = [m for m in models if m.lower() == substr]
        if exact:
            chosen = exact[0]
        else:
            matches = [m for m in models if substr in m.lower()]
            if len(matches) != 1:
                await update.message.reply_text(
                    f"Подстрока {substr!r} матчит {len(matches)} модель(и) у `{target}`. "
                    f"Доступны: {', '.join(_model_label(m) for m in models)}."
                )
                return
            chosen = matches[0]
        update_model_only(key[0], key[1], chosen)
        await update.message.reply_text(
            f"Модель движка `{target}` изменена: → {_model_label(chosen)}.\n"
            f"Контекст сессии сохранён.",
        )
        logger.info(
            "model changed in-place via /engine for key=%s engine=%s: -> %s",
            key, target, chosen,
        )
        return

    ok, msg, _ = _engine_precheck(key, target)
    if not ok:
        await update.message.reply_text(msg)
        return

    target_engine = get_engine_by_name(target)
    models = list(target_engine.models)
    chosen_model: str | None = None
    if len(models) == 1:
        chosen_model = models[0]
    elif len(models) > 1:
        if len(args) < 2:
            await update.message.reply_text(
                f"У движка `{target}` несколько моделей: "
                + ", ".join(_model_label(m) for m in models)
                + ".\nИспользуй /engine без аргументов и выбери в UI, "
                "или передай подстроку модели: /engine "
                f"{target} {_model_label(models[0])}."
            )
            return
        substr = args[1].strip().lower()
        exact = [m for m in models if m.lower() == substr]
        if len(exact) == 1:
            chosen_model = exact[0]
        else:
            matches = [m for m in models if substr in m.lower()]
            if len(matches) != 1:
                await update.message.reply_text(
                    f"Подстрока {substr!r} матчит {len(matches)} модель(и) у `{target}`. "
                    f"Доступны: {', '.join(_model_label(m) for m in models)}."
                )
                return
            chosen_model = matches[0]

    if keep_context:
        async def _progress_edit(text: str) -> None:
            pass  # из текстовой команды не можем обновлять карточку
        text = await _do_engine_handoff(
            key, current_engine, target, _progress_edit, model=chosen_model,
        )
        text = re.sub(r"<[^>]+>", "", text)
        await update.message.reply_text(text)
    else:
        text = await _do_engine_switch(key, target, model=chosen_model)
        await update.message.reply_text(text + "\nКонтекст прежнего диалога не переносится.")


async def on_engine_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Callback от inline-кнопки выбора движка. Не переключает сразу — после
    pre-check'а спрашивает: переносить контекст?"""
    query = update.callback_query
    if query is None:
        return
    data = query.data or ""
    if not data.startswith("engine_select:"):
        return
    target = data.split(":", 1)[1].strip().lower()
    key = _key(update)

    # Same-engine click: предлагаем смену модели вместо отказа.
    _, _, current_engine = get_session(*key)
    if target == current_engine:
        try:
            await query.answer()
        except Exception:
            pass
        target_engine = get_engine_by_name(target)
        models = list(target_engine.models)
        current_model = get_model(*key)
        if len(models) > 1:
            prompt_text = (
                f"Движок `{target}` уже активен.\n"
                f"Текущая модель: {current_model or '(дефолт движка)'}.\n"
                f"Выбери другую модель — контекст сессии сохранится:"
            )
            try:
                await query.edit_message_text(
                    prompt_text,
                    reply_markup=_model_keyboard(target, models),
                )
            except BadRequest:
                await send_to_topic(
                    update.effective_chat, key[1],
                    prompt_text,
                    reply_markup=_model_keyboard(target, models),
                )
        else:
            msg = (
                f"Движок `{target}` уже активен. "
                + (
                    f"У него только одна модель ({models[0]}), сменить не на что."
                    if models
                    else "Выбор модели для этого движка недоступен."
                )
            )
            try:
                await query.edit_message_text(
                    msg, reply_markup=_engine_keyboard(current_engine),
                )
            except BadRequest:
                await send_to_topic(update.effective_chat, key[1], msg)
        return

    ok, msg, current = _engine_precheck(key, target)
    if not ok:
        try:
            await query.answer("Не могу переключить", show_alert=False)
        except Exception:
            pass
        try:
            await query.edit_message_text(
                msg + (f"\n\n(текущий движок: {current})" if current else ""),
                reply_markup=_engine_keyboard(current) if current else None,
            )
        except BadRequest:
            await send_to_topic(update.effective_chat, key[1], msg)
        return

    try:
        await query.answer()
    except Exception:
        pass

    # Шаг выбора модели: только если у целевого движка их >1.
    target_engine = get_engine_by_name(target)
    models = list(target_engine.models)
    if len(models) > 1:
        prompt_text = (
            f"Движок: {current} → {target}.\n"
            f"Выбери модель {target}:"
        )
        try:
            await query.edit_message_text(
                prompt_text,
                reply_markup=_model_keyboard(target, models),
            )
        except BadRequest:
            await send_to_topic(
                update.effective_chat, key[1],
                prompt_text,
                reply_markup=_model_keyboard(target, models),
            )
        return

    # 0 или 1 модель — сразу к шагу carry. Для одной модели сохраняем её индекс,
    # чтобы on_engine_carry знал, что записать в БД.
    model_idx = 0 if len(models) == 1 else None
    try:
        await query.edit_message_text(
            f"Переключаюсь {current} → {target}.\n"
            "Перенести контекст текущего диалога в новый движок?\n"
            "(резюме старого движка будет добавлено к первому твоему сообщению)",
            reply_markup=_carry_keyboard(current, target, model_idx),
        )
    except BadRequest:
        await send_to_topic(
            update.effective_chat, key[1],
            f"Переключаюсь {current} → {target}. Перенести контекст?",
            reply_markup=_carry_keyboard(current, target, model_idx),
        )


async def on_model_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Callback от кнопки выбора модели. После выбора — обычный шаг про
    перенос контекста."""
    query = update.callback_query
    if query is None:
        return
    data = query.data or ""
    if not data.startswith("model_select:"):
        return
    parts = data.split(":")
    if len(parts) != 3:
        return
    _, target, idx_str = parts
    target = target.strip().lower()
    try:
        model_idx = int(idx_str)
    except ValueError:
        return
    key = _key(update)

    target_engine = get_engine_by_name(target)
    models = list(target_engine.models)
    if model_idx < 0 or model_idx >= len(models):
        try:
            await query.answer("Модель не найдена", show_alert=True)
        except Exception:
            pass
        return
    chosen = models[model_idx]

    # Same-engine: меняем только модель в БД, session_id и контекст
    # сохраняются. Carry-этап не нужен.
    _, _, current_engine = get_session(*key)
    if target == current_engine:
        update_model_only(key[0], key[1], chosen)
        try:
            await query.answer()
        except Exception:
            pass
        new_text = (
            f"Модель движка `{target}` изменена: → {_model_label(chosen)}.\n"
            f"Контекст сессии сохранён."
        )
        try:
            await query.edit_message_text(new_text)
        except BadRequest:
            await send_to_topic(update.effective_chat, key[1], new_text)
        logger.info(
            "model changed in-place for key=%s engine=%s: -> %s",
            key, target, chosen,
        )
        return

    ok, msg, current = _engine_precheck(key, target)
    if not ok:
        try:
            await query.answer("Не могу переключить", show_alert=False)
        except Exception:
            pass
        try:
            await query.edit_message_text(
                msg + (f"\n\n(текущий движок: {current})" if current else ""),
                reply_markup=_engine_keyboard(current) if current else None,
            )
        except BadRequest:
            await send_to_topic(update.effective_chat, key[1], msg)
        return

    try:
        await query.answer()
    except Exception:
        pass
    try:
        await query.edit_message_text(
            f"Переключаюсь {current} → {target} ({_model_label(chosen)}).\n"
            "Перенести контекст текущего диалога в новый движок?\n"
            "(резюме старого движка будет добавлено к первому твоему сообщению)",
            reply_markup=_carry_keyboard(current, target, model_idx),
        )
    except BadRequest:
        await send_to_topic(
            update.effective_chat, key[1],
            f"Переключаюсь {current} → {target} ({_model_label(chosen)}). "
            "Перенести контекст?",
            reply_markup=_carry_keyboard(current, target, model_idx),
        )


async def on_engine_carry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Callback после ответа «Да/Нет» на вопрос о переносе контекста.
    Формат callback_data: engine_carry:<old>:<new>:<model_idx_or_dash>:<y|n>."""
    query = update.callback_query
    if query is None:
        return
    data = query.data or ""
    if not data.startswith("engine_carry:"):
        return
    parts = data.split(":")
    if len(parts) != 5:
        return
    _, old_engine, new_engine, model_token, choice = parts
    key = _key(update)

    # model_token: "-" → без модели, иначе индекс в target_engine.models.
    model_idx: int | None = None
    if model_token != "-":
        try:
            model_idx = int(model_token)
        except ValueError:
            return

    # Проверим, что состояние с момента предыдущего шага не изменилось.
    ok, msg, current = _engine_precheck(key, new_engine)
    if not ok or current != old_engine:
        try:
            await query.answer("Состояние изменилось", show_alert=False)
        except Exception:
            pass
        try:
            await query.edit_message_text(
                (msg or f"Состояние изменилось: текущий движок — {current}.")
                + ("\n\nВыбери движок заново." if current else ""),
                reply_markup=_engine_keyboard(current) if current else None,
            )
        except BadRequest:
            await send_to_topic(update.effective_chat, key[1],
                                msg or "Состояние изменилось.")
        return

    chosen_model = _resolve_target_model(new_engine, model_idx)

    try:
        await query.answer()
    except Exception:
        pass

    if choice == "n":
        text = await _do_engine_switch(key, new_engine, model=chosen_model)
        text += "\nКонтекст прежнего диалога не переносится."
        try:
            await query.edit_message_text(text)
        except BadRequest:
            await send_to_topic(update.effective_chat, key[1], text)
        return

    # choice == 'y' — handoff с резюме. Может занять десятки секунд.
    async def progress_edit(t: str) -> None:
        try:
            await query.edit_message_text(t)
        except BadRequest:
            pass

    text = await _do_engine_handoff(
        key, old_engine, new_engine, progress_edit, model=chosen_model,
    )
    try:
        await query.edit_message_text(text, parse_mode=ParseMode.HTML)
    except BadRequest:
        # Возможно HTML невалиден — fallback на plain.
        plain = re.sub(r"<[^>]+>", "", text)
        try:
            await query.edit_message_text(plain)
        except BadRequest:
            await send_to_topic(update.effective_chat, key[1], plain)


async def cmd_bind(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    key = _key(update)
    args = context.args or []
    if not args:
        await update.message.reply_text("Использование: /bind <абсолютный путь>")
        return
    raw = " ".join(args).strip()
    if raw.startswith("~"):
        raw = os.path.expanduser(raw)
    if not os.path.isabs(raw):
        await update.message.reply_text("Путь должен быть абсолютным.")
        return
    raw = os.path.normpath(raw)  # убираем trailing slash и т.п. — важно для slug'а сессий claude
    if not os.path.isdir(raw):
        await update.message.reply_text(f"Директория не существует: {raw}")
        return
    set_cwd(key[0], key[1], raw)
    logger.info("bind: key=%s cwd=%s", key, raw)
    await update.message.reply_text(
        f"Топик привязан к {raw}. Новые запросы будут исполняться оттуда."
    )


async def cmd_unbind(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    key = _key(update)
    clear_cwd(*key)
    logger.info("unbind: key=%s", key)
    await update.message.reply_text(
        f"Привязка снята. Эффективный cwd теперь — дефолт: {CLAUDE_CWD}"
    )


async def cmd_where(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    key = _key(update)
    _, cwd, _ = get_session(*key)
    effective = cwd or CLAUDE_CWD
    await update.message.reply_text(
        f"cwd: {effective}" + (" (дефолт)" if not cwd else " (bound)")
    )


# ---------- Handlers: обработка сообщений ----------

async def _finish_turn_reply(
    chat, thread_id: int, journal: "ProgressJournal", ok: bool, final_text: str,
    engine_name: str, key: tuple[int, int],
) -> None:
    """Общий хвост хода: закрыть журнал, отправить финальный ответ и файлы.
    Общий для разового вызова движка и живого процесса claude (/persistent)."""
    await journal.finish(final_text)

    if not ok and not final_text.strip():
        logger.info("llm call stopped without final reply: key=%s engine=%s", key, engine_name)
        final_text = (
            f"{engine_name} не прислал финальный ответ "
            "(процесс завершился без текста). "
            "Попробуй /new для новой сессии или переключи движок через /engine."
        )

    cleaned_text, file_markers = extract_file_markers(final_text)
    if not cleaned_text.strip():
        cleaned_text = "(пустой ответ)" if not file_markers else "(см. вложения)"
    meta = {"type": "claude_response", "engine": engine_name}
    try:
        await send_claude_reply(chat, thread_id, cleaned_text, meta)
    except Exception:
        logger.exception("failed to send llm reply: key=%s", key)
    if file_markers:
        await deliver_file_markers(chat, thread_id, file_markers)

    session_id, cwd, current_engine_name = get_session(*key)
    await _warn_large_context_if_needed(
        chat, thread_id, current_engine_name or engine_name, session_id, cwd, key,
    )
    await _ask_done_confirmation_if_needed(chat, thread_id, ok, final_text, session_id, key)


async def _handle_persistent_message(
    chat, thread_id: int, key: tuple[int, int], user_text: str, meta_block: str,
) -> None:
    """Путь для топиков с /persistent on: без topic-lock и без «в очереди».

    Сообщение уходит живому процессу движка — новым ходом, если он свободен
    между ходами, или довеском к уже идущему ходу, если он ещё работает (без
    ожидания и без нового subprocess). Для claude довесок пишется в stdin
    stream-json; для codex — отправляется JSON-RPC `turn/steer` в app-server."""
    worker = persistent_workers.get(key)
    if worker is not None and (worker.dead or worker.proc.returncode is not None):
        persistent_workers.pop(key, None)
        worker = None

    pending_summary_delivered = False
    if worker is None:
        session_id, cwd, engine_name, opened_new = ensure_active_session(*key)
        if not get_persistent_for_engine(*key, engine_name):
            # /engine сменили мимо /persistent — тихий fallback на обычный путь.
            await _process_prompt_locked(chat, thread_id, key, user_text, meta_block)
            return
        if _persistent_column_for_engine(engine_name) is None:
            await _process_prompt_locked(chat, thread_id, key, user_text, meta_block)
            return
        model = get_model(*key)

        if opened_new:
            try:
                await send_to_topic(
                    chat, thread_id,
                    f"🆕 Новый сеанс ({engine_name}). Контекст прошлых разговоров "
                    "не загружен — попроси поднять историю, если нужно.",
                )
            except Exception:
                logger.exception("failed to send new-session notice key=%s", key)

        pending_raw = get_pending_summary(*key)
        pending_summary = None
        if pending_raw:
            pending_summary = await _resolve_pending_summary(key, pending_raw)

        mcp_playwright = get_mcp_playwright(*key)
        mcp_topic_role = resolve_topic_role(key)
        effective_cwd = cwd or CLAUDE_CWD
        system_prefix = build_system_prefix(effective_cwd, mcp_playwright, key=key)

        try:
            if engine_name == "claude":
                worker = await start_persistent_claude(
                    key=key, session_id=session_id, cwd=effective_cwd, model=model,
                    system_prefix=system_prefix, mcp_playwright=mcp_playwright,
                    mcp_topic_role=mcp_topic_role,
                )
            elif engine_name == "codex":
                worker = await start_persistent_codex(
                    key=key, session_id=session_id, cwd=effective_cwd, model=model,
                    system_prefix=system_prefix, mcp_playwright=mcp_playwright,
                    mcp_topic_role=mcp_topic_role,
                )
                if worker.session_id and worker.session_id != session_id:
                    update_session_id(key[0], key[1], "codex", worker.session_id)
            else:
                raise RuntimeError(f"persistent is not supported for {engine_name}")
        except Exception as exc:
            logger.exception("persistent worker spawn failed key=%s", key)
            await send_to_topic(
                chat, thread_id,
                f"⚠️ Не удалось поднять живой процесс {engine_name}: {exc}",
            )
            return
        persistent_workers[key] = worker

        prompt_parts: list[str] = []
        if pending_summary:
            prompt_parts.append("[Контекст:]\n" + pending_summary)
            pending_summary_delivered = True
        if meta_block:
            prompt_parts.append(meta_block)
        prompt_parts.append("---\n\nСообщение пользователя:\n" + user_text)
        prompt = "\n\n".join(prompt_parts)
    else:
        prompt_parts = [meta_block] if meta_block else []
        prompt_parts.append("---\n\nСообщение пользователя:\n" + user_text)
        prompt = "\n\n".join(prompt_parts)

    is_new, fut = await worker.submit(prompt)

    if not is_new:
        try:
            await send_to_topic(
                chat, thread_id,
                "✅ Добавил к тому, над чем сейчас работаю — учту сразу после текущего шага.",
            )
        except Exception:
            logger.exception("failed to send persistent-append ack key=%s", key)
        return

    journal = ProgressJournal(chat, thread_id)
    await journal.start()
    worker.on_intermediate = journal.append
    timeout = CODEX_TIMEOUT if get_session(*key)[2] == "codex" else CLAUDE_TIMEOUT
    try:
        ok, final_text = await asyncio.wait_for(fut, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning("persistent worker timeout key=%s", key)
        await _kill_persistent_worker(key, "")
        engine_name = get_session(*key)[2]
        ok, final_text = False, f"Timeout: живой {engine_name} не ответил за {timeout}с."
    except Exception as exc:
        logger.exception("persistent worker await failed key=%s", key)
        ok, final_text = False, f"Внутренняя ошибка: {exc}"
    finally:
        if worker is not None:
            worker.on_intermediate = None

    if ok and pending_summary_delivered:
        clear_pending_summary(*key)

    engine_name = get_session(*key)[2]
    await _finish_turn_reply(chat, thread_id, journal, ok, final_text, engine_name, key)
    logger.info("persistent turn done: key=%s engine=%s ok=%s", key, engine_name, ok)


async def _process_prompt(
    update: Update,
    user_text: str,
    attachments: list[str] | None = None,
) -> None:
    key = _key(update)
    chat = update.effective_chat
    thread_id = key[1]

    # Логируем входящее в messages_log сразу — Менеджер через MCP должен видеть
    # запросы, прилетающие в проектные топики, даже если бот ещё в очереди.
    in_text = user_text
    if attachments:
        in_text = in_text + "\n" + "\n".join(f"[Прикреплён файл: {p}]" for p in attachments)
    tg_msg_id = update.message.message_id if update.message else None
    log_message(chat.id, thread_id, "in", "user_text", in_text, tg_msg_id)

    # Агент ждёт ответа на ask_user? Тогда это сообщение — ОТВЕТ ему, а не новый
    # запрос. Перехватываем ДО очереди и лока (или живого процесса) — агент
    # стоит в ask_user, и без перехвата ответ ушёл бы к нему вторым, отдельным ходом.
    pending_ask = get_pending_ask(chat.id, thread_id)
    if pending_ask is not None and user_text.strip():
        if answer_ask(pending_ask["id"], user_text.strip(), via="text"):
            logger.info("ask #%s answered by text: key=%s", pending_ask["id"], key)
            await _mark_ask_answered(chat, pending_ask, user_text.strip())
            return
        # Не смогли записать — вопрос уже закрыт (кнопка/таймаут). Обрабатываем
        # сообщение как обычный запрос.
        logger.info("ask #%s already closed, treating as normal message",
                    pending_ask["id"])

    # Reply-to контекст и вложения → meta_block
    extra_lines: list[str] = []
    reply = update.message.reply_to_message if update.message else None
    if reply is not None:
        ctx = load_message_context(chat.id, reply.message_id)
        if ctx:
            extra_lines.append(_build_reply_context_prefix(ctx))
    if attachments:
        for p in attachments:
            extra_lines.append(f"[Прикреплён файл: {p}]")
    meta_block = "\n".join(extra_lines)

    # Живой процесс (/persistent on) — своя ветка, без topic-lock:
    # сообщение, пришедшее пока агент работает, дописывается ему на лету.
    _peek_sid, _peek_cwd, peek_engine_name = get_session(*key)
    if get_persistent_for_engine(*key, peek_engine_name):
        await _handle_persistent_message(chat, thread_id, key, user_text, meta_block)
        return

    await _process_prompt_locked(chat, thread_id, key, user_text, meta_block)


async def _process_prompt_locked(
    chat, thread_id: int, key: tuple[int, int], user_text: str, meta_block: str,
) -> None:
    """Обычный путь: topic-lock, «в очереди», процесс движка на сообщение."""
    # Проверяем, занят ли lock. Если занят — сообщаем «в очереди» с кнопкой «Отменить».
    lock = _lock_for(key)
    queue_msg = None
    queue_id: str | None = None
    cancel_event: asyncio.Event | None = None
    if lock.locked():
        queue_id = uuid.uuid4().hex[:12]
        cancel_event = asyncio.Event()
        pending_queue[queue_id] = cancel_event
        try:
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_queue:{queue_id}")
            ]])
            queue_msg = await send_to_topic(
                chat, thread_id,
                "⏳ В очереди — текущий запрос ещё выполняется.",
                reply_markup=kb,
            )
        except Exception:
            queue_msg = None

    # Ожидание lock'а с возможностью отмены.
    if cancel_event is not None:
        acquire_task = asyncio.create_task(lock.acquire())
        cancel_task = asyncio.create_task(cancel_event.wait())
        try:
            await asyncio.wait(
                {acquire_task, cancel_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            cancel_task.cancel()
        if cancel_event.is_set():
            # Отменено. Если lock успел захватиться — освободим.
            if acquire_task.done() and not acquire_task.cancelled():
                try:
                    lock.release()
                except RuntimeError:
                    pass
            else:
                acquire_task.cancel()
            pending_queue.pop(queue_id, None)
            if queue_msg is not None:
                try:
                    await queue_msg.edit_text("❌ Отменено пользователем")
                except Exception:
                    pass
            logger.info("queued request cancelled: key=%s queue_id=%s", key, queue_id)
            return
        # Дождались lock'а.
        pending_queue.pop(queue_id, None)
        # Снимем кнопку «Отменить» — запрос стартует.
        if queue_msg is not None:
            try:
                await queue_msg.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
    else:
        await lock.acquire()

    try:
        logger.info("lock acquired: key=%s", key)
        # Сеанс под локом (вдруг /close, /reset или /engine сработали, пока мы
        # стояли в очереди). Протухший или закрытый сеанс здесь же открывается
        # заново — это аналог «открыть проект в терминале».
        session_id, cwd, engine_name, opened_new = ensure_active_session(*key)
        engine = get_engine_by_name(engine_name)
        model = get_model(*key)

        if opened_new:
            try:
                await send_to_topic(
                    chat, thread_id,
                    f"🆕 Новый сеанс ({engine_name}). Контекст прошлых разговоров "
                    "не загружен — попроси поднять историю, если нужно.",
                )
            except Exception:
                logger.exception("failed to send new-session notice key=%s", key)

        # Pending handoff: pop'аем атомарно — доставляется ровно один раз,
        # в первый prompt после переключения движка с переносом контекста.
        pending_raw = get_pending_summary(*key)
        pending_summary = None
        if pending_raw:
            pending_summary = await _resolve_pending_summary(key, pending_raw)
            if pending_summary:
                logger.info("delivering pending context to engine=%s key=%s (%d chars)",
                            engine_name, key, len(pending_summary))

        prompt_parts: list[str] = []  # [SYSTEM:]-блок уходит через системный канал движка
        if pending_summary:
            prompt_parts.append("[Контекст:]\n" + pending_summary)
        if meta_block:
            prompt_parts.append(meta_block)
        prompt_parts.append("---\n\nСообщение пользователя:\n" + user_text)
        prompt = "\n\n".join(prompt_parts)

        # Журнал хода: шаги агента копятся в одном сообщении и остаются в топике.
        journal = ProgressJournal(chat, thread_id)
        await journal.start()

        try:
            with engine_model_scope(engine.name, model):
                ok, final_text, _sid_after = await call_llm_stream(
                    engine, session_id, prompt, key, cwd, journal.append,
                )
            if ok and pending_summary:
                clear_pending_summary(*key)
        except Exception as exc:
            logger.exception("llm call crashed: key=%s engine=%s", key, engine.name)
            ok, final_text = False, f"Внутренняя ошибка: {exc}"

        await _finish_turn_reply(chat, thread_id, journal, ok, final_text, engine.name, key)

        logger.info("lock released: key=%s ok=%s engine=%s", key, ok, engine.name)
    finally:
        try:
            lock.release()
        except RuntimeError:
            pass


async def _run_spawn(update: Update, user_text: str) -> None:
    """Одноразовая параллельная сессия. Не использует lock топика,
    не сохраняет session_id в БД. Движок наследуется от топика. Все
    сообщения помечаются префиксом [#xxxx]."""
    key = _key(update)
    chat = update.effective_chat
    thread_id = key[1]

    spawn_id = secrets.token_hex(2)  # 4 hex-символа
    prefix = f"[#{spawn_id}] "

    # cwd, engine, model наследуются из топика; session_id новый и в БД не сохраняется.
    # Дефолт cwd подставит call_llm_stream — сюда передаём сырой cwd.
    _, cwd, engine_name = get_session(*key)
    engine = get_engine_by_name(engine_name)
    model = get_model(*key)
    session_id = engine.new_session_id()

    # [SYSTEM:]-блок уходит через системный канал движка (call_llm_stream).
    prompt = (
        "---\n\nСообщение пользователя (одноразовый spawn):\n"
        + user_text
    )

    journal = ProgressJournal(
        chat, thread_id, prefix=prefix, header="⏳ Spawn запущен...",
    )
    await journal.start()

    try:
        with engine_model_scope(engine.name, model):
            ok, final_text, _sid_after = await call_llm_stream(
                engine, session_id, prompt, key, cwd, journal.append, spawn_id=spawn_id,
            )
    except Exception as exc:
        logger.exception("spawn crashed: key=%s spawn=%s engine=%s",
                         key, spawn_id, engine.name)
        ok, final_text = False, f"Внутренняя ошибка: {exc}"

    await journal.finish(final_text)

    if not ok and not final_text.strip():
        logger.info("spawn stopped without final reply: key=%s spawn=%s engine=%s",
                    key, spawn_id, engine.name)
        return

    cleaned_text, file_markers = extract_file_markers(final_text)
    if not cleaned_text.strip():
        cleaned_text = "(пустой ответ)" if not file_markers else "(см. вложения)"
    meta = {"type": "claude_response", "spawn_id": spawn_id}
    try:
        await send_claude_reply(
            chat, thread_id, cleaned_text, meta,
            filename_prefix=f"spawn_{spawn_id}",
            html_prefix=_html_escape(prefix),
        )
    except Exception:
        logger.exception("failed to send spawn reply: key=%s spawn=%s", key, spawn_id)
    if file_markers:
        await deliver_file_markers(chat, thread_id, file_markers, notice_prefix=prefix)
    logger.info("spawn done: key=%s spawn=%s ok=%s files=%d",
                key, spawn_id, ok, len(file_markers))


async def _run_manager_job(app: Application, job: dict) -> tuple[bool, int | None, str | None]:
    """Drive a single queued job through the normal LLM pipeline.

    No Update object: the prompt is treated as user input but sourced from the
    Manager, so we ack it in the topic and post the bot's reply there. Per-key
    lock is respected — if a Telegram user is mid-conversation in the same
    topic, this waits its turn.
    """
    chat_id = job["chat_id"]
    thread_id = job["thread_id"]
    user_text = job["text"]
    job_id = job["id"]
    source = job.get("source") or "manager"
    is_self_kick = source == "self_notice"
    key = (chat_id, thread_id)
    bot = app.bot
    try:
        chat = await bot.get_chat(chat_id)
    except Exception:
        logger.exception("manager job %s: bot.get_chat(%s) failed", job_id, chat_id)
        return False, None, "failed to resolve target chat via Telegram API"

    lock = _lock_for(key)
    await lock.acquire()
    try:
        logger.info("manager job %s: lock acquired key=%s", job_id, key)
        # Задача исполняется в сеансе топика; протухший сеанс здесь тоже
        # переоткрывается. Job самодостаточен — полный текст задачи внутри.
        session_id, cwd, engine_name, opened_new = ensure_active_session(*key)
        engine = get_engine_by_name(engine_name)
        model = get_model(*key)
        effective_cwd = cwd or CLAUDE_CWD
        if opened_new:
            logger.info("manager job %s: opened new session %s for key=%s",
                        job_id, session_id, key)

        pending_raw = get_pending_summary(*key)
        pending_summary = await _resolve_pending_summary(key, pending_raw) if pending_raw else None

        prompt_parts: list[str] = []  # [SYSTEM:]-блок уходит через системный канал движка
        if pending_summary:
            prompt_parts.append("[Контекст:]\n" + pending_summary)
        mgr_target = resolve_manager_topic()
        if is_self_kick:
            prompt_parts.append(
                f"[SYSTEM NOTE: это AUTO-KICK для Менеджера (job_id={job_id}, "
                f"source=self_notice). Тебя разбудил бот, потому что в твой "
                f"топик пришли новые нотисы от исполнителей — обычно "
                f"safety-нотисы «📨 ✅ job #N: новый ответ» или health-нотисы "
                f"«⏳ работает долго».\n\n"
                f"Что делать:\n"
                f"1. Прочитай свежие нотисы через "
                f"mcp__jarvis__manager_inbox(chat_id={chat_id}, "
                f"thread_id={thread_id}, limit=10). Фильтруй по kind="
                f"'job_notification' / 'job_heartbeat_warn' / 'job_heartbeat_fail' / "
                f"'job_interrupted'.\n"
                f"2. Для каждого нотиса по необходимости — заходи в источник "
                f"через manager_inbox(thread_id=<src>) и читай развёрнутый "
                f"ответ агента.\n"
                f"3. Решай: либо ждать оператора (просто ничего не делай в "
                f"ответ), либо отвечать оператору в свой топик, либо двигать "
                f"задачу через manager_send(thread_id=<src>, as_user=True).\n"
                f"4. После обработки нотиса — manager_dismiss_notice "
                f"(message_id=...) чтобы топик не зарастал.\n"
                f"5. Если по содержанию нотисов делать нечего — кратко "
                f"подытожь («просмотрел нотисы #N..#M, оператор не нужен») "
                f"и заверши turn. Ничего substantive без одобрения.]"
            )
        elif mgr_target and mgr_target != (chat_id, thread_id):
            mgr_chat_id, mgr_thread_id = mgr_target
            prompt_parts.append(
                f"[SYSTEM NOTE: задача делегирована Менеджером через MCP "
                f"(job_id={job_id}). Финальный ответ этого turn'а идёт в "
                f"этот топик (thread_id={thread_id}, cwd={effective_cwd}). "
                f"Бот сам пришлёт Менеджеру короткий нотис «есть ответ» "
                f"после твоего bot_reply — отдельно слать manager_send не "
                f"нужно.\n\n"
                f"Правила:\n"
                f"1. Нетривиальная задача (любая правка кода / архитектурное "
                f"решение / >1 файла) — сначала предложи план, не начинай "
                f"реализацию. Финальный ответ = план. Менеджер либо одобрит, "
                f"либо корректирует следующим сообщением.\n"
                f"2. Если нужно уточнение по ToR — финальный ответ = вопрос "
                f"с тегом #ask_{job_id}, ничего не реализуй. Жди следующего "
                f"сообщения.\n"
                f"3. При правках на ПРОДЕ — обязательный smoke-check после: "
                f"ищи команду в документации проекта (например "
                f"production_smoke_check.md). Если smoke упал — откатить через "
                f"git revert и сообщить ❌ в финальном ответе. Если файла "
                f"smoke-check нет — задай уточняющий вопрос через #ask.\n"
                f"4. (опционально) По завершении CODE-задачи можешь "
                f"дополнительно прислать богатый отчёт в Менеджеров топик: "
                f"mcp__jarvis__manager_send(thread_id={mgr_thread_id}, "
                f"as_user=false, text='#job_{job_id} ✅ <одна строка> — "
                f"src: thread_id={thread_id}, cwd={effective_cwd}'). Это "
                f"улучшает поиск по хэштегу, но НЕ обязательно — бот пришлёт "
                f"свой нотис сам.]"
            )
        else:
            prompt_parts.append(
                "[SYSTEM NOTE: задача делегирована Менеджером через MCP "
                f"(job_id={job_id}). Отвечай как обычно.]"
            )
        prompt_parts.append("---\n\nСообщение пользователя:\n" + user_text)
        prompt = "\n\n".join(prompt_parts)

        # Self-kick — служебный ход Менеджера, топик им не засоряем: журнала нет.
        journal: ProgressJournal | None = None
        if not is_self_kick:
            journal = ProgressJournal(
                chat, thread_id,
                header=f"⏳ Manager делегировал задачу (job #{job_id})...",
            )
            await journal.start()

        async def on_intermediate(text: str) -> None:
            if journal is not None:
                await journal.append(text)

        # Watcher для interrupt: раз в 2с смотрит cancel_requested.
        # Если выставлен — терминирует subprocess; основной stream выйдет
        # с ошибкой, мы это поймаем по interrupted=True ниже.
        watcher_stop = asyncio.Event()
        interrupted = False

        async def _interrupt_watcher() -> None:
            nonlocal interrupted
            while not watcher_stop.is_set():
                try:
                    await asyncio.wait_for(watcher_stop.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass
                if watcher_stop.is_set():
                    return
                try:
                    with _db() as conn_:
                        row = conn_.execute(
                            "SELECT cancel_requested FROM jobs WHERE id = ?",
                            (job_id,),
                        ).fetchone()
                except Exception:
                    logger.exception("interrupt watcher poll failed job=%s", job_id)
                    continue
                if row and row[0]:
                    interrupted = True
                    proc = active_procs.get(key)
                    if proc is not None:
                        logger.info(
                            "manager job %s: cancel_requested, terminating proc",
                            job_id,
                        )
                        try:
                            await terminate_process_tree(proc)
                        except Exception:
                            logger.exception(
                                "terminate_process_tree failed job=%s", job_id,
                            )
                    return

        watcher_task = asyncio.create_task(_interrupt_watcher())

        try:
            with engine_model_scope(engine.name, model):
                ok, final_text, _sid_after = await call_llm_stream(
                    engine, session_id, prompt, key, cwd, on_intermediate,
                )
            if ok and pending_summary:
                clear_pending_summary(*key)
        except Exception as exc:
            logger.exception("manager job %s: llm crashed engine=%s", job_id, engine.name)
            ok, final_text = False, f"Внутренняя ошибка: {exc}"
        finally:
            watcher_stop.set()
            try:
                await asyncio.wait_for(watcher_task, timeout=3.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                watcher_task.cancel()

        if journal is not None:
            await journal.finish(final_text)

        if interrupted:
            # Менеджер прервал. Доделаем то немногое что успели увидеть в
            # final_text (если что-то пришло), но job помечаем как cancelled
            # и шлём отдельный нотис.
            try:
                await send_to_topic(
                    chat, thread_id,
                    f"⏹ job #{job_id} остановлен Менеджером. "
                    "Жду уточняющий вопрос или новые инструкции.",
                )
            except Exception:
                logger.exception("manager job %s: failed to post interrupt notice", job_id)
            await _send_manager_notice(
                app,
                f"⏹ job #{job_id}: subprocess остановлен по твоему запросу "
                f"(thread_id={thread_id}). Теперь можно прислать уточняющий "
                f"вопрос обычным manager_send(as_user=True) — агент resume "
                f"той же сессии и увидит контекст до прерывания.",
                kind="job_interrupted",
            )
            logger.info("manager job %s: interrupted by manager", job_id)
            return False, None, "interrupted by manager request"

        if not ok and not final_text.strip():
            logger.info("manager job %s stopped without final reply", job_id)
            final_text = (
                f"{engine.name} не прислал финальный ответ "
                "(процесс завершился без текста). "
                "Рекомендуется запустить /new в этом топике и повторить задачу."
            )

        cleaned_text, file_markers = extract_file_markers(final_text)
        if not cleaned_text.strip():
            cleaned_text = "(пустой ответ)" if not file_markers else "(см. вложения)"
        meta = {"type": "claude_response", "engine": engine.name, "job_id": job_id}
        sent_msg_id = None
        try:
            sent = await send_claude_reply(chat, thread_id, cleaned_text, meta)
            if sent is not None:
                sent_msg_id = sent.message_id
        except Exception:
            logger.exception("manager job %s: failed to send reply", job_id)
        if file_markers:
            await deliver_file_markers(chat, thread_id, file_markers)

        # Safety notice в топик Менеджера — гарантированно даём знать что
        # есть ответ. Не зависит от того, прислал ли агент сам что-то
        # через mcp__jarvis__manager_send. Не шлём для self_kick — Менеджер
        # сам себе нотис не нужен, он уже разбирает свой inbox.
        if not is_self_kick and resolve_manager_topic() != (chat_id, thread_id):
            with _db() as conn_:
                row = conn_.execute(
                    "SELECT topic_title, cwd FROM sessions "
                    "WHERE chat_id = ? AND thread_id = ?",
                    (chat_id, thread_id),
                ).fetchone()
            title = (row[0] if row and row[0] else None) or f"thread_id={thread_id}"
            cwd_disp = (row[1] if row and row[1] else None) or f"{CLAUDE_CWD} (дефолт)"
            status_emoji = "✅" if ok else "⚠️"
            notice_text = (
                f"📨 {status_emoji} job #{job_id}: новый ответ от агента\n"
                f"Топик: «{title}» (thread_id={thread_id})\n"
                f"cwd: {cwd_disp}\n"
                f"Действие: прочитай через "
                f"manager_inbox(thread_id={thread_id}) или зайди в сам топик."
            )
            await _send_manager_notice(app, notice_text, kind="job_notification")

        logger.info("manager job %s done: ok=%s files=%d engine=%s",
                    job_id, ok, len(file_markers), engine.name)
        err_text = None if ok else (cleaned_text[:1000] if cleaned_text else "engine returned no usable reply")
        return ok, sent_msg_id, err_text
    finally:
        try:
            lock.release()
        except RuntimeError:
            pass


async def health_worker(app: Application) -> None:
    """Watcher для долгоиграющих job'ов.

    Сканирует in_progress job'ы каждые HEARTBEAT_INTERVAL секунд:
    - claimed_at < now - WARN и heartbeat_notified_at IS NULL → шлём
      нотис «job работает долго, проверь» (один раз за job).
    - claimed_at < now - FAIL → принудительно помечаем failed и шлём
      «job отменён по таймауту». Subprocess сам не убиваем — ответ
      возможно уже почти готов; если хочешь принудительно прервать —
      пользуйся manager_interrupt.
    """
    interval = _env_int("JARVIS_HEARTBEAT_INTERVAL", 300, 30)
    warn_s = _env_int("JARVIS_HEARTBEAT_WARN", 900, 60)
    fail_s = _env_int("JARVIS_HEARTBEAT_FAIL", 3600, 120)
    logger.info(
        "health_worker started (interval=%ds warn=%ds fail=%ds)",
        interval, warn_s, fail_s,
    )
    while True:
        try:
            now_dt = datetime.utcnow()
            warn_thr = (now_dt - timedelta(seconds=warn_s)).isoformat()
            fail_thr = (now_dt - timedelta(seconds=fail_s)).isoformat()
            now_iso = now_dt.isoformat()

            # WARN: in_progress + claimed_at < warn_thr + ещё не уведомляли.
            with _db() as conn:
                warn_rows = conn.execute(
                    "SELECT id, chat_id, thread_id, claimed_at FROM jobs "
                    "WHERE status='in_progress' AND claimed_at IS NOT NULL "
                    "AND claimed_at < ? AND heartbeat_notified_at IS NULL",
                    (warn_thr,),
                ).fetchall()
            for r in warn_rows:
                jid, jchat, jthread, jclaimed = r[0], r[1], r[2], r[3]
                try:
                    claimed_dt = datetime.fromisoformat(jclaimed)
                    mins = int((now_dt - claimed_dt).total_seconds() / 60)
                except Exception:
                    mins = warn_s // 60
                with _db() as conn:
                    title_row = conn.execute(
                        "SELECT topic_title FROM sessions WHERE chat_id=? AND thread_id=?",
                        (jchat, jthread),
                    ).fetchone()
                title = (
                    (title_row[0] if title_row and title_row[0] else None)
                    or f"thread_id={jthread}"
                )
                text = (
                    f"⏳ job #{jid} работает {mins} мин в топике «{title}» "
                    f"(thread_id={jthread}).\n"
                    f"Возможно ушло не туда. Если что — останови через "
                    f"manager_interrupt(thread_id={jthread}) и спроси, "
                    f"что происходит."
                )
                await _send_manager_notice(app, text, kind="job_heartbeat_warn")
                with _db() as conn:
                    conn.execute(
                        "UPDATE jobs SET heartbeat_notified_at = ? WHERE id = ?",
                        (now_iso, jid),
                    )

            # FAIL: in_progress + claimed_at < fail_thr → помечаем failed.
            with _db() as conn:
                fail_rows = conn.execute(
                    "SELECT id, chat_id, thread_id FROM jobs "
                    "WHERE status='in_progress' AND claimed_at IS NOT NULL "
                    "AND claimed_at < ?",
                    (fail_thr,),
                ).fetchall()
            for r in fail_rows:
                jid, jchat, jthread = r[0], r[1], r[2]
                with _db() as conn:
                    conn.execute(
                        "UPDATE jobs SET status='failed', "
                        "error='timeout > heartbeat_fail', finished_at=? "
                        "WHERE id=? AND status='in_progress'",
                        (now_iso, jid),
                    )
                text = (
                    f"❌ job #{jid}: принудительно помечен failed "
                    f"(работал >{fail_s // 60} мин в thread_id={jthread}). "
                    f"Subprocess не убит — если ответ всё-таки придёт, "
                    f"он окажется в проектном топике, но job уже закрыт. "
                    f"Чтобы реально прервать — используй "
                    f"manager_interrupt(thread_id={jthread})."
                )
                await _send_manager_notice(app, text, kind="job_heartbeat_fail")

            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info("health_worker cancelled")
            raise
        except Exception:
            logger.exception("health_worker loop crashed; sleeping 60s")
            await asyncio.sleep(60.0)


# Топики, у которых сейчас выполняется делегированная задача. Диспетчер не
# claim'ит новую задачу для топика из этого множества — это сохраняет порядок
# задач внутри топика и не даёт слоту пула висеть на per-topic локе.
_inflight_job_keys: set[tuple[int, int]] = set()


async def _run_job_slot(
    app: Application,
    job: dict,
    key: tuple[int, int],
    sem: asyncio.Semaphore,
) -> None:
    """Выполнить одну делегированную задачу в слоте пула и освободить слот.
    Сериализация внутри топика — через per-topic _lock_for в _run_manager_job."""
    job_id = job["id"]
    try:
        ok, msg_id, err_text = await _run_manager_job(app, job)
        finish_job(
            job_id, "done" if ok else "failed",
            None if ok else (err_text or "engine returned no usable reply"),
            msg_id,
        )
    except asyncio.CancelledError:
        finish_job(job_id, "failed", "worker cancelled", None)
        raise
    except Exception as exc:
        logger.exception("manager job %s crashed", job_id)
        finish_job(job_id, "failed", str(exc)[:1000], None)
    finally:
        _inflight_job_keys.discard(key)
        sem.release()


async def jobs_worker(app: Application) -> None:
    """Диспетчер делегированных задач: claim'ит pending-задачи и запускает их
    параллельными тасками, ограниченными семафором (JARVIS_JOBS_CONCURRENCY,
    дефолт 5). Задачи РАЗНЫХ топиков идут одновременно; задачи ОДНОГО топика
    сериализуются (исключение busy-топиков при claim + per-topic лок).

    Раньше воркер был один и await'ил каждую задачу до конца — длинный ресёрч в
    одном топике блокировал делегирование во все остальные. Теперь не блокирует.
    """
    concurrency = _env_int("JARVIS_JOBS_CONCURRENCY", 5, 1)
    sem = asyncio.Semaphore(concurrency)
    tasks: set[asyncio.Task] = set()
    logger.info("jobs_worker started (concurrency=%d)", concurrency)
    while True:
        # acquired: держим ли мы слот, который ещё не передан таске. Гарантирует
        # освобождение слота на любом пути ошибки (иначе пул бы протекал).
        acquired = False
        try:
            # Ждём свободный слот ДО claim'а — задача не помечается in_progress,
            # пока её некому выполнять (нет осиротевших claimed-but-idle задач).
            await sem.acquire()
            acquired = True
            job = claim_next_job(exclude_keys=frozenset(_inflight_job_keys))
            if job is None:
                sem.release()
                acquired = False
                await asyncio.sleep(2.0)
                continue
            key = (job["chat_id"], job["thread_id"])
            _inflight_job_keys.add(key)
            t = asyncio.create_task(_run_job_slot(app, job, key, sem))
            acquired = False  # владение слотом перешло к таске (release в её finally)
            tasks.add(t)
            t.add_done_callback(tasks.discard)
        except asyncio.CancelledError:
            if acquired:
                sem.release()
            logger.info("jobs_worker cancelled; cancelling %d in-flight job(s)", len(tasks))
            for t in list(tasks):
                t.cancel()
            raise
        except Exception:
            if acquired:
                sem.release()
            logger.exception("jobs_worker dispatcher crashed; sleeping 5s")
            await asyncio.sleep(5.0)


_inflight_trigger_keys: set[tuple[int, int]] = set()


async def _process_agent_trigger(app: Application, trigger: dict) -> tuple[bool, str | None]:
    """Run one non-job trigger through the normal topic LLM pipeline."""
    chat_id = trigger["chat_id"]
    thread_id = trigger["thread_id"]
    key = (chat_id, thread_id)
    text = trigger["text"]
    source = trigger.get("source") or "external"
    try:
        chat = await app.bot.get_chat(chat_id)
    except Exception:
        logger.exception("agent trigger %s: bot.get_chat(%s) failed", trigger["id"], chat_id)
        return False, "failed to resolve target chat via Telegram API"

    log_message(chat_id, thread_id, "in", f"{source}_inject", text, None)

    try:
        _sid, _cwd, engine_name = get_session(*key)
        if get_persistent_for_engine(*key, engine_name):
            await _handle_persistent_message(chat, thread_id, key, text, "")
        else:
            await _process_prompt_locked(chat, thread_id, key, text, "")
    except Exception as exc:
        logger.exception("agent trigger %s crashed key=%s", trigger["id"], key)
        return False, str(exc)[:1000]
    return True, None


async def _run_agent_trigger_slot(
    app: Application,
    trigger: dict,
    key: tuple[int, int],
    sem: asyncio.Semaphore,
) -> None:
    trigger_id = trigger["id"]
    try:
        ok, err_text = await _process_agent_trigger(app, trigger)
        finish_agent_trigger(
            trigger_id,
            "done" if ok else "failed",
            None if ok else (err_text or "engine returned no usable reply"),
            None,
        )
    except asyncio.CancelledError:
        finish_agent_trigger(trigger_id, "failed", "worker cancelled", None)
        raise
    except Exception as exc:
        logger.exception("agent trigger %s crashed", trigger_id)
        finish_agent_trigger(trigger_id, "failed", str(exc)[:1000], None)
    finally:
        _inflight_trigger_keys.discard(key)
        sem.release()


async def agent_triggers_worker(app: Application) -> None:
    """Dispatcher for non-job external triggers.

    Unlike jobs_worker, these turns have no job_id and are invisible to
    manager_interrupt/health_worker. They are still serialized per topic by the
    normal topic lock inside _process_prompt_locked/_handle_persistent_message.
    """
    concurrency = _env_int("JARVIS_AGENT_TRIGGERS_CONCURRENCY", 5, 1)
    sem = asyncio.Semaphore(concurrency)
    tasks: set[asyncio.Task] = set()
    logger.info("agent_triggers_worker started (concurrency=%d)", concurrency)
    while True:
        acquired = False
        try:
            await sem.acquire()
            acquired = True
            exclude = frozenset(_inflight_trigger_keys | _inflight_job_keys)
            trigger = claim_next_agent_trigger(exclude_keys=exclude)
            if trigger is None:
                sem.release()
                acquired = False
                await asyncio.sleep(2.0)
                continue
            key = (trigger["chat_id"], trigger["thread_id"])
            _inflight_trigger_keys.add(key)
            t = asyncio.create_task(_run_agent_trigger_slot(app, trigger, key, sem))
            acquired = False
            tasks.add(t)
            t.add_done_callback(tasks.discard)
        except asyncio.CancelledError:
            if acquired:
                sem.release()
            logger.info(
                "agent_triggers_worker cancelled; cancelling %d in-flight trigger(s)",
                len(tasks),
            )
            for t in list(tasks):
                t.cancel()
            raise
        except Exception:
            if acquired:
                sem.release()
            logger.exception("agent_triggers_worker dispatcher crashed; sleeping 5s")
            await asyncio.sleep(5.0)


async def cmd_spawn(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/spawn <prompt> — запустить одноразовую параллельную claude-сессию.
    Не блокируется lock'ом топика, не трогает основную сессию."""
    if not update.message:
        return
    raw = (update.message.text or "")
    # Убираем саму команду "/spawn" (с возможным @botname).
    parts = raw.split(None, 1)
    prompt = parts[1].strip() if len(parts) > 1 else ""
    if not prompt:
        await update.message.reply_text(
            "Использование: /spawn <prompt>\n"
            "Запускает параллельную одноразовую claude-сессию в cwd этого топика.\n"
            "Остановить конкретный spawn: /stop <id> (4 hex, напр. /stop a1b2)."
        )
        return
    # Фоновая задача, чтобы handler вернулся сразу и не блокировал polling.
    asyncio.create_task(_run_spawn(update, prompt))


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text:
        return
    await _process_prompt(update, update.message.text.strip())


async def _download_tg_file(update: Update, file_id: str, suggested_name: str) -> str:
    tg_file = await update.get_bot().get_file(file_id)
    safe_name = "".join(c for c in suggested_name if c.isalnum() or c in "._-") or "file"
    dest_dir = os.path.join(MEDIA_DIR, str(update.effective_chat.id))
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(
        dest_dir,
        f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{safe_name}",
    )
    await tg_file.download_to_drive(dest)
    return dest


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.photo:
        return
    photo = update.message.photo[-1]
    path = await _download_tg_file(update, photo.file_id, "photo.jpg")
    caption = (update.message.caption or "").strip() or "(опиши изображение)"
    await _process_prompt(update, caption, attachments=[path])


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.document:
        return
    doc = update.message.document
    path = await _download_tg_file(update, doc.file_id, doc.file_name or "document")
    caption = (update.message.caption or "").strip() or f"(прикреплён файл {doc.file_name or ''})"
    await _process_prompt(update, caption, attachments=[path])


async def on_cancel_queue(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка нажатия кнопки «❌ Отменить» на сообщении «в очереди»."""
    query = update.callback_query
    if query is None:
        return
    data = query.data or ""
    if not data.startswith("cancel_queue:"):
        return
    queue_id = data.split(":", 1)[1]
    event = pending_queue.get(queue_id)
    if event is None:
        # Уже не в очереди: либо стартовал, либо уже отменён ранее.
        try:
            await query.answer("Уже выполняется — используй /stop", show_alert=True)
        except Exception:
            pass
        # На всякий случай снимем кнопку, чтобы не нажималась повторно.
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        return
    event.set()
    try:
        await query.answer("Отменено")
    except Exception:
        pass
    # Само сообщение редактируется в ожидающей корутине (в «❌ Отменено пользователем»).


async def unauthorized_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if user is not None:
        logger.warning(
            "Unauthorized: user_id=%s username=%r full_name=%r",
            user.id, user.username, user.full_name,
        )
    if update.message is not None:
        try:
            await update.message.reply_text("Доступ запрещён.")
        except Exception:
            pass


# ---------- main ----------

# Команды, выводимые в нативное меню Telegram (синяя кнопка слева от поля ввода).
# Описания короткие — Telegram обрезает длинные.
BOT_COMMANDS: list[BotCommand] = [
    BotCommand("engine", "движок: показать/переключить (claude|codex|opencode)"),
    BotCommand("close", "закрыть сеанс (контекст сбрасывается)"),
    BotCommand("new", "закрыть сеанс и сразу открыть новый"),
    BotCommand("session", "session-id, cwd, движок и состояние сеанса"),
    BotCommand("tokens", "оценка размера текущей сессии"),
    BotCommand("stop", "прервать текущий запрос"),
    BotCommand("spawn", "одноразовая параллельная сессия — /spawn <prompt>"),
    BotCommand("bind", "привязать топик к каталогу — /bind <abs path>"),
    BotCommand("unbind", "снять привязку cwd, вернуть дефолт"),
    BotCommand("where", "показать эффективный cwd"),
    BotCommand("persistent", "живой процесс claude/codex: сообщения на лету"),
    BotCommand("start", "приветствие и состояние топика"),
]


async def _post_init(application: Application) -> None:
    """Регистрируем команды для всех контекстов (default + private + group)
    и явно ставим MenuButtonCommands — иначе в форум-группах нативная кнопка
    меню часто не появляется без явной настройки."""
    bot = application.bot
    scopes = [
        ("default", None),
        ("all_private_chats", BotCommandScopeAllPrivateChats()),
        ("all_group_chats", BotCommandScopeAllGroupChats()),
    ]
    for label, scope in scopes:
        try:
            if scope is None:
                await bot.set_my_commands(BOT_COMMANDS)
            else:
                await bot.set_my_commands(BOT_COMMANDS, scope=scope)
            logger.info("bot commands registered for scope=%s (%d entries)",
                        label, len(BOT_COMMANDS))
        except Exception:
            logger.exception("set_my_commands failed for scope=%s", label)
    try:
        await bot.set_chat_menu_button(menu_button=MenuButtonCommands())
        logger.info("default menu button set to MenuButtonCommands")
    except Exception:
        logger.exception("set_chat_menu_button failed (меню не критично)")

    # Списки моделей движков спрашиваются у их CLI (engines/model_cache.py).
    # Прогреваем в потоке, чтобы первый /engine не ждал `opencode models`.
    application.bot_data["models_prewarm_task"] = asyncio.create_task(
        asyncio.to_thread(prewarm_models)
    )

    # Запускаем worker для очереди jobs (delegations from Manager via MCP).
    # Хранить ссылку в bot_data на случай нужды в shutdown'е/тестах.
    task = asyncio.create_task(jobs_worker(application))
    application.bot_data["jobs_worker_task"] = task

    # Non-job external triggers (внешние интеграции): обычный LLM turn в топике
    # без job_id, health_worker и safety-notice Менеджеру.
    trigger_task = asyncio.create_task(agent_triggers_worker(application))
    application.bot_data["agent_triggers_worker_task"] = trigger_task

    # Гигиена: hourly cleanup старых записей messages_log + завершённых jobs.
    # TTL — env JARVIS_LOG_TTL_DAYS (дефолт 30, 0/none/off отключает).
    cleanup_task = asyncio.create_task(cleanup_worker(application))
    application.bot_data["cleanup_worker_task"] = cleanup_task

    # /persistent: убивает простаивающие живые процессы claude.
    persistent_reaper_task = asyncio.create_task(persistent_reaper(application))
    application.bot_data["persistent_reaper_task"] = persistent_reaper_task

    # Закрытия сеансов, запрошенные Менеджером через manager_close_session:
    # MCP помечает строку в sessions, добивает процессы топика бот.
    close_requests_task = asyncio.create_task(close_requests_worker(application))
    application.bot_data["close_requests_worker_task"] = close_requests_task

    # Health: следит за долгими in_progress jobs, шлёт Менеджеру нотисы.
    # Параметры в env JARVIS_HEARTBEAT_INTERVAL/WARN/FAIL (300/900/3600с).
    health_task = asyncio.create_task(health_worker(application))
    application.bot_data["health_worker_task"] = health_task

    # Reminders: cron-light напоминания для Менеджера.
    reminders_task = asyncio.create_task(reminders_worker(application))
    application.bot_data["reminders_worker_task"] = reminders_task

    # Общий callback для отправки нотисов в топик Менеджера.
    async def _notice(text: str, kind: str = "job_notification") -> None:
        await _send_manager_notice(application, text, kind)

    # Webhook-сервер для входящих событий Битрикс24.
    webhook_task = asyncio.create_task(run_webhook_server(_notice))
    application.bot_data["webhook_task"] = webhook_task

    # IMAP-поллер для новых писем.
    imap_task = asyncio.create_task(run_imap_watcher(_notice))
    application.bot_data["imap_task"] = imap_task


def build_application(
    token: str | None = None,
    allowed_user_ids: set[int] | None = None,
) -> Application:
    """Собрать Application со всеми хендлерами.

    Вынесено из main() ради теста-снапшота регистраций: он строит приложение с
    фейковым токеном и сверяет, что каждая команда и каждый callback-паттерн
    по-прежнему ведут в ту же функцию. Сети не требует.
    """
    # concurrent_updates=True: без этого PTB обрабатывает апдейты последовательно,
    # и per-key asyncio.Lock не даёт параллельности между разными топиками —
    # второй топик ждёт, пока освободится воркер PTB, а не сам lock.
    app = (
        Application.builder()
        .token(token if token is not None else TELEGRAM_TOKEN)
        .concurrent_updates(True)
        .post_init(_post_init)
        .build()
    )

    allowed = filters.User(
        user_id=allowed_user_ids if allowed_user_ids is not None else ALLOWED_USER_IDS
    )

    app.add_handler(CommandHandler("start", cmd_start, filters=allowed))
    app.add_handler(CommandHandler("new", cmd_reset, filters=allowed))
    app.add_handler(CommandHandler("reset", cmd_reset, filters=allowed))
    app.add_handler(CommandHandler("stop", cmd_stop, filters=allowed))
    app.add_handler(CommandHandler("spawn", cmd_spawn, filters=allowed))
    app.add_handler(CommandHandler("session", cmd_session, filters=allowed))
    app.add_handler(CommandHandler("tokens", cmd_tokens, filters=allowed))
    app.add_handler(CommandHandler("close", cmd_close, filters=allowed))
    app.add_handler(CommandHandler("engine", cmd_engine, filters=allowed))
    app.add_handler(CommandHandler("browser", cmd_browser, filters=allowed))
    app.add_handler(CommandHandler("persistent", cmd_persistent, filters=allowed))
    app.add_handler(CommandHandler("bind", cmd_bind, filters=allowed))
    app.add_handler(CommandHandler("unbind", cmd_unbind, filters=allowed))
    app.add_handler(CommandHandler("where", cmd_where, filters=allowed))

    app.add_handler(MessageHandler(allowed & filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(allowed & filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(allowed & filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(on_cancel_queue, pattern=r"^cancel_queue:"))
    app.add_handler(CallbackQueryHandler(on_engine_select, pattern=r"^engine_select:"))
    app.add_handler(CallbackQueryHandler(on_model_select, pattern=r"^model_select:"))
    app.add_handler(CallbackQueryHandler(on_engine_carry, pattern=r"^engine_carry:"))
    app.add_handler(CallbackQueryHandler(on_ask_answer, pattern=r"^ask:"))
    app.add_handler(CallbackQueryHandler(on_browser_toggle, pattern=r"^browser_toggle:"))
    app.add_handler(CallbackQueryHandler(on_persistent_toggle, pattern=r"^persistent_toggle:"))
    app.add_handler(CallbackQueryHandler(on_done_confirm, pattern=r"^done_confirm:"))

    app.add_handler(MessageHandler(~allowed, unauthorized_handler))

    return app


def main() -> None:
    print(f"=== Jarvis Telegram Bot (per-topic engine, default={DEFAULT_ENGINE_NAME}) ===")

    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN не задан. См. .env.example.")
    if not ALLOWED_USER_IDS:
        logger.warning("ALLOWED_USER_IDS пуст — никто не сможет писать боту.")

    init_db()
    mcp_ok, mcp_status = ensure_engine_tools(DEFAULT_ENGINE)
    if mcp_ok:
        logger.info("Default engine tools ready: %s", mcp_status)
    else:
        logger.warning("Default engine tools are not fully ready: %s", mcp_status)

    app = build_application()

    logger.info("Whitelisted user_ids: %s", sorted(ALLOWED_USER_IDS))
    logger.info("Default engine: %s  default cwd=%s", DEFAULT_ENGINE_NAME, CLAUDE_CWD)
    print(f"Бот запущен (default engine={DEFAULT_ENGINE_NAME}). Жду сообщения в Telegram...")
    app.run_polling()


if __name__ == "__main__":
    main()
