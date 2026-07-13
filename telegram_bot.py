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
    PersistentClaudeWorker,
    start_persistent as start_persistent_claude,
)

# ---------- Константы ----------

# Движок per-topic: env JARVIS_ENGINE — дефолт для новых топиков; существующие
# топики хранят свой engine в БД и переключаются командой /engine.
DEFAULT_ENGINE_NAME = default_engine_name()
DEFAULT_ENGINE = get_engine_by_name(DEFAULT_ENGINE_NAME)

# Дефолтный cwd для топиков без явного /bind. Имя переменной историческое (CLAUDE_CWD),
# для обратной совместимости: задаёт дефолт для любого движка.
CLAUDE_CWD = os.environ.get("CLAUDE_CWD", "/home/shevartv")

def _int_env_raw(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


MSG_LIMIT = 3500           # порог отправки ответа как документ
TG_HARD_LIMIT = 4096       # жёсткий лимит Telegram
TG_FILE_LIMIT_MB = 50      # Telegram Bot API лимит на sendDocument

# Сеанс = окно терминала. Открывается первым сообщением, закрывается командой
# /close или сам — после SESSION_IDLE_MINUTES без активности в топике. Топик
# (cwd, движок, модель) переживает закрытие, контекст сессии — нет.
SESSION_IDLE_MINUTES = _int_env_raw("JARVIS_SESSION_IDLE_MINUTES", 180)

# Маркер для отправки файлов из LLM-сессии: [[FILE: /abs/path]] или [[FILE: /path | подпись]].
# Должен стоять на отдельной строке (но допускаются пробелы вокруг).
FILE_MARKER_RE = re.compile(
    r"^[ \t]*\[\[FILE:\s*(?P<path>[^|\]\n]+?)(?:\s*\|\s*(?P<caption>[^\]\n]+?))?\s*\]\][ \t]*$",
    re.MULTILINE,
)

DB_PATH = os.path.join(BASE_DIR, "bot_state.db")
MEDIA_DIR = os.path.join(BASE_DIR, "temp", "media")
os.makedirs(MEDIA_DIR, exist_ok=True)

logging.basicConfig(
    format="[bot] %(asctime)s %(levelname)s %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def build_system_prefix(
    effective_cwd: str,
    mcp_playwright: bool = False,
    key: tuple[int, int] | None = None,
) -> str:
    """Постоянный [SYSTEM:]-блок для движка.

    Раньше клеился в тело КАЖDОГО user-сообщения и копился в транскрипте.
    Теперь передаётся в системный канал движка (claude --append-system-prompt;
    codex/opencode — префиксом только на новой сессии) — один раз на сессию.
    Строка про браузер добавляется ТОЛЬКО когда Playwright реально подключён
    (mcp_playwright), иначе не зовём модель пользоваться недоступными тулами.

    `key` — (chat_id, thread_id) топика. Нужен движку, чтобы он мог сам поднять
    историю топика через manager_inbox: сессия живёт один сеанс, а переписка
    переживает его в messages_log.
    """
    lines = [
        "[SYSTEM: Сообщение пришло от пользователя через Telegram-бота Jarvis.",
        f"Ты работаешь в проекте {effective_cwd}. Используй memory-правила из "
        "~/.claude/projects/-home-shevartv/memory/.",
    ]
    if key is not None:
        lines.append(
            f"Твой топик: chat_id={key[0]}, thread_id={key[1]}. Сессия живёт один "
            "сеанс и не помнит прошлые — но переписка топика сохраняется. Если "
            "нужен контекст прошлых разговоров, подними его сам через MCP-инструмент "
            f"manager_inbox(chat_id={key[0]}, thread_id={key[1]})."
        )
    if mcp_playwright:
        lines.append(
            "Если нужно работать с браузером, используй Playwright MCP browser_* tools, "
            "когда они доступны; если MCP недоступен, скажи об этом и выбери рабочий fallback."
        )
    if key is not None:
        lines.append(
            "Пользователь НЕ видит этот ход в реальном времени и не может тебя "
            "перебить — единственный способ что-то у него спросить и дождаться "
            f"ответа: MCP-инструмент ask_user(question, thread_id={key[1]}, "
            "options=[...]). Он блокирует тебя до ответа. Обязательно спрашивай "
            "ПЕРЕД опасными действиями (удаления, DELETE/DROP, sudo, push --force, "
            "что-либо на проде) и когда задача допускает разные толкования, а "
            "угадывание обесценит работу. Давай варианты в options — по ним "
            "отвечать быстрее. Не спрашивай о том, что можешь выяснить сам "
            "(прочитать код, запустить команду, посмотреть git)."
        )
    else:
        lines.append(
            "Опасные действия (удаления, DELETE/DROP, действия на проде, sudo, "
            "push --force) — переспрашивай."
        )
    lines[-1] += "]"
    return "\n".join(lines)


# ---------- SQLite ----------

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _backup_db_once() -> None:
    """Перед первой миграцией схемы — однократный бэкап bot_state.db.
    Имя содержит дату, поэтому «одна копия в сутки» защищает и от повторных перезаписей.
    """
    if not os.path.exists(DB_PATH):
        return
    stamp = datetime.utcnow().strftime("%Y%m%d")
    bak_path = f"{DB_PATH}.bak-{stamp}"
    if os.path.exists(bak_path):
        return
    try:
        shutil.copy2(DB_PATH, bak_path)
        logger.info("bot_state.db backed up to %s", bak_path)
    except OSError as exc:
        logger.warning("failed to backup bot_state.db: %s", exc)


def init_db() -> None:
    with _db() as conn:
        # Миграция: если sessions существует со старым PK (chat_id) без колонок thread_id/cwd —
        # пересоздаём таблицу и переносим данные (thread_id=0).
        cols = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
        if cols and "thread_id" not in cols:
            _backup_db_once()
            logger.info("migrating sessions table: adding thread_id/cwd, new PK (chat_id, thread_id)")
            conn.execute("ALTER TABLE sessions RENAME TO sessions_old")
            conn.execute(
                """
                CREATE TABLE sessions (
                    chat_id INTEGER NOT NULL,
                    thread_id INTEGER NOT NULL DEFAULT 0,
                    session_id TEXT NOT NULL,
                    cwd TEXT,
                    engine TEXT NOT NULL DEFAULT 'claude',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (chat_id, thread_id)
                )
                """
            )
            conn.execute(
                "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, updated_at) "
                "SELECT chat_id, 0, session_id, NULL, 'claude', updated_at FROM sessions_old"
            )
            conn.execute("DROP TABLE sessions_old")
            logger.info("sessions migration done")
        else:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sessions (
                    chat_id INTEGER NOT NULL,
                    thread_id INTEGER NOT NULL DEFAULT 0,
                    session_id TEXT NOT NULL,
                    cwd TEXT,
                    engine TEXT NOT NULL DEFAULT 'claude',
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (chat_id, thread_id)
                )
                """
            )
            # Idempotent миграция: добавляем engine в существующую таблицу, если её нет.
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "engine" not in cols_now:
                _backup_db_once()
                logger.info("adding 'engine' column to sessions (default='claude')")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN engine TEXT NOT NULL DEFAULT 'claude'"
                )
            # Idempotent миграция: pending_summary — резюме предыдущей сессии другого
            # движка, ждущее доставки в первый prompt после /engine с переносом.
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "pending_summary" not in cols_now:
                _backup_db_once()
                logger.info("adding 'pending_summary' column to sessions")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN pending_summary TEXT"
                )
            # Idempotent миграция: model — выбранная для топика модель движка.
            # NULL = «дефолт движка» (фолбэк на env / встроенный дефолт CLI).
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "model" not in cols_now:
                _backup_db_once()
                logger.info("adding 'model' column to sessions")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN model TEXT"
                )
            # Idempotent миграция: topic_title — название топика в Telegram.
            # Заполняется при `manager_create_topic`; Telegram API не отдаёт
            # имя топика обратно через getChat, поэтому нужен свой реестр.
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "topic_title" not in cols_now:
                _backup_db_once()
                logger.info("adding 'topic_title' column to sessions")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN topic_title TEXT"
                )
            # Idempotent миграция: topic_icon_color — цвет иконки топика
            # (один из 6 валидных RGB-кодов Telegram). Хранится, чтобы
            # manager_create_topic не дублировал цвета между топиками.
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "topic_icon_color" not in cols_now:
                _backup_db_once()
                logger.info("adding 'topic_icon_color' column to sessions")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN topic_icon_color INTEGER"
                )
            # Idempotent миграция: actual_model — реальная модель, которой
            # CLI ответил последний раз (парсится из stream-events каждого
            # адаптера). Отличается от sessions.model — там «что выбрали
            # руками», а actual_model — «что фактически работает».
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "actual_model" not in cols_now:
                _backup_db_once()
                logger.info("adding 'actual_model' column to sessions")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN actual_model TEXT"
                )
            # Idempotent миграция: mcp_playwright — per-topic флаг «браузер
            # подключён». 0 = off (дефолт): Playwright не грузится в контекст.
            # 1 = on: адаптер инъектит Playwright MCP per-invocation. Команда
            # /browser тоглит флаг (с пересозданием session_id — набор тулов).
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "mcp_playwright" not in cols_now:
                _backup_db_once()
                logger.info("adding 'mcp_playwright' column to sessions (default=0)")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN mcp_playwright INTEGER NOT NULL DEFAULT 0"
                )
            # Idempotent миграция: persistent_claude — per-topic флаг «живой
            # процесс claude». 0 = off (дефолт): сообщение на сообщение —
            # отдельный subprocess. 1 = on: один subprocess на сеанс
            # (--input-format stream-json), сообщение во время активного хода
            # дописывается в его stdin вместо ожидания очереди. Команда
            # /persistent тоглит флаг.
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "persistent_claude" not in cols_now:
                _backup_db_once()
                logger.info("adding 'persistent_claude' column to sessions (default=0)")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN persistent_claude INTEGER NOT NULL DEFAULT 0"
                )
            # Idempotent миграция: autocompact_enabled — легаси, автокомпакт
            # убран вместе с переходом на сеансы. Колонку не используем и не
            # удаляем (чтобы не терять данные на откате).
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "autocompact_enabled" not in cols_now:
                _backup_db_once()
                logger.info("adding 'autocompact_enabled' column to sessions")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN autocompact_enabled INTEGER"
                )
            # Idempotent миграция: last_activity_at — время последнего сообщения
            # в топике. По нему закрывается протухший сеанс (idle > порога).
            # NULL у старых строк = сеанс считается протухшим при первом
            # обращении, т.е. откроется новый — это и нужно.
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "last_activity_at" not in cols_now:
                _backup_db_once()
                logger.info("adding 'last_activity_at' column to sessions")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN last_activity_at TEXT"
                )
            # Idempotent миграция: session_started_at — когда открыт текущий сеанс.
            # Нужен, чтобы понять, не изменились ли с тех пор инструкции проекта
            # (AGENTS.md / CLAUDE.md): движки читают их при СТАРТЕ сессии, и без
            # этой проверки правка инструкций молча не действует — агент до конца
            # сеанса работает по версии, прочитанной когда-то давно.
            cols_now = [r[1] for r in conn.execute("PRAGMA table_info(sessions)").fetchall()]
            if cols_now and "session_started_at" not in cols_now:
                _backup_db_once()
                logger.info("adding 'session_started_at' column to sessions")
                conn.execute(
                    "ALTER TABLE sessions ADD COLUMN session_started_at TEXT"
                )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                context_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, message_id)
            )
            """
        )
        # Полный лог сообщений по топикам — нужен для Менеджера (MCP tool
        # manager_inbox). Пишем входящие пользовательские реплики и финальные
        # ответы бота. Промежуточные tool_use не логируем — слишком шумно.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS messages_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                thread_id INTEGER NOT NULL DEFAULT 0,
                direction TEXT NOT NULL,
                kind TEXT NOT NULL,
                text TEXT NOT NULL,
                telegram_message_id INTEGER,
                ts TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_messages_log_topic_ts "
            "ON messages_log(chat_id, thread_id, ts)"
        )
        # Вопросы агента пользователю (MCP tool ask_user). Канал между двумя
        # процессами: MCP-сервер пишет вопрос и поллит ответ, бот принимает
        # ответ (нажатие кнопки или обычное сообщение в топик) и кладёт сюда.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ask_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                thread_id INTEGER NOT NULL DEFAULT 0,
                question TEXT NOT NULL,
                options_json TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                answer TEXT,
                option_index INTEGER,
                via TEXT,
                telegram_message_id INTEGER,
                created_at TEXT NOT NULL,
                answered_at TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ask_requests_pending "
            "ON ask_requests(chat_id, thread_id, status, id)"
        )
        # Очередь задач от Менеджера (MCP tool manager_send as_user=True).
        # Worker внутри бота забирает pending и прокручивает их через
        # обычный LLM-pipeline в указанном топике.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                thread_id INTEGER NOT NULL,
                text TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'manager',
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                claimed_at TEXT,
                finished_at TEXT,
                error TEXT,
                result_message_id INTEGER
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, created_at)"
        )
        # Idempotent миграция: not_before — момент времени, когда job
        # становится доступным для worker'а. NULL = доступен немедленно.
        # Используется для «авто-go через 10 мин» сценария Менеджера:
        # план готов → ставится scheduled job с delay=600s; если оператор
        # одобрил/корректирует раньше — новый manager_send в этот же
        # thread_id автоматически переводит pending scheduled в cancelled.
        cols_now = [r[1] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()]
        if cols_now and "not_before" not in cols_now:
            _backup_db_once()
            logger.info("adding 'not_before' column to jobs")
            conn.execute("ALTER TABLE jobs ADD COLUMN not_before TEXT")
            # Поправляем индекс под новый ORDER BY.
            conn.execute("DROP INDEX IF EXISTS idx_jobs_status")
        # Idempotent миграция: heartbeat_notified_at — когда health_worker
        # уже шлёт warn-нотис по этому job'у. NULL = ещё не уведомлял.
        # Это защита от спама — нотис идёт один раз за job.
        cols_now = [r[1] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()]
        if cols_now and "heartbeat_notified_at" not in cols_now:
            logger.info("adding 'heartbeat_notified_at' column to jobs")
            conn.execute(
                "ALTER TABLE jobs ADD COLUMN heartbeat_notified_at TEXT"
            )
        # Idempotent миграция: cancel_requested — флаг для manager_interrupt.
        # MCP-сервер ставит timestamp, watcher в _run_manager_job его читает
        # раз в N секунд и убивает subprocess. NULL = не запрошено.
        cols_now = [r[1] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()]
        if cols_now and "cancel_requested" not in cols_now:
            logger.info("adding 'cancel_requested' column to jobs")
            conn.execute(
                "ALTER TABLE jobs ADD COLUMN cancel_requested TEXT"
            )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_pending "
            "ON jobs(status, not_before, created_at)"
        )
        # Напоминания Менеджеру (cron-light). schedule — простой текст,
        # парсится в _parse_schedule(): daily HH:MM, weekday HH:MM,
        # weekend HH:MM, weekly DAY[,DAY] HH:MM, monthly D HH:MM,
        # once YYYY-MM-DD HH:MM (все времена в JARVIS_REMINDERS_TZ).
        # next_fire_at хранится в UTC ISO.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                thread_id INTEGER NOT NULL,
                text TEXT NOT NULL,
                schedule TEXT NOT NULL,
                next_fire_at TEXT NOT NULL,
                last_fired_at TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_reminders_due "
            "ON reminders(enabled, next_fire_at)"
        )
        # imap_state: UIDs уже отправленных нотисов, чтобы не дублировать.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS imap_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account TEXT NOT NULL,
                uid INTEGER NOT NULL,
                seen_at TEXT NOT NULL,
                UNIQUE(account, uid)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_imap_state_account "
            "ON imap_state(account, uid)"
        )
        # webhook_log: входящие события от Битрикс24 и других вебхуков.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS webhook_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                event TEXT NOT NULL,
                payload TEXT NOT NULL,
                received_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_webhook_log_received "
            "ON webhook_log(source, received_at)"
        )


def log_message(
    chat_id: int,
    thread_id: int,
    direction: str,
    kind: str,
    text: str,
    telegram_message_id: int | None = None,
) -> None:
    """Пишет одну запись в messages_log. direction: 'in' | 'out'.

    Поглощает ошибки записи: логирование — вторичная функция, не должна
    блокировать бот при проблемах с БД.
    """
    if not text:
        return
    try:
        with _db() as conn:
            conn.execute(
                "INSERT INTO messages_log(chat_id, thread_id, direction, kind, "
                "text, telegram_message_id, ts) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    chat_id, thread_id, direction, kind, text,
                    telegram_message_id, datetime.utcnow().isoformat(),
                ),
            )
    except Exception:
        logger.exception("log_message failed (chat=%s thread=%s kind=%s)",
                         chat_id, thread_id, kind)


def claim_next_job(
    exclude_keys: frozenset[tuple[int, int]] | None = None,
) -> dict | None:
    """Atomically claim one pending job that's due now. Returns its row as
    a dict or None.

    Scheduled jobs (`not_before > NOW()`) are skipped; immediate jobs
    (not_before IS NULL) and overdue scheduled ones are eligible. Ordering
    is by "effective firing time" (`COALESCE(not_before, created_at)`) so
    that a live live job created during a 10-min wait gets handled before
    the wait expires.

    `exclude_keys` — топики (chat_id, thread_id), у которых уже есть задача в
    работе. Пропускаются, чтобы (а) сохранить порядок внутри топика и (б) не
    занимать слот пула ожиданием per-topic лока. Параллельный диспетчер
    обрабатывает задачи РАЗНЫХ топиков одновременно; claim атомарен и
    multi-worker-safe (см. rowcount-проверку ниже).
    """
    now = datetime.utcnow().isoformat()
    sql = (
        "SELECT id, chat_id, thread_id, text, source FROM jobs "
        "WHERE status = 'pending' AND (not_before IS NULL OR not_before <= ?)"
    )
    params = [now]
    for chat_id, thread_id in (exclude_keys or ()):
        sql += " AND NOT (chat_id = ? AND thread_id = ?)"
        params.extend([chat_id, thread_id])
    sql += " ORDER BY COALESCE(not_before, created_at) ASC, id ASC LIMIT 1"
    with _db() as conn:
        row = conn.execute(sql, params).fetchone()
        if not row:
            return None
        cur = conn.execute(
            "UPDATE jobs SET status = 'in_progress', claimed_at = ? "
            "WHERE id = ? AND status = 'pending'",
            (now, row[0]),
        )
        if cur.rowcount != 1:
            # Раса с другим worker'ом — пусть следующий цикл подберёт.
            return None
    return {
        "id": row[0],
        "chat_id": row[1],
        "thread_id": row[2],
        "text": row[3],
        "source": row[4],
    }


def finish_job(
    job_id: int,
    status: str,
    error: str | None = None,
    result_message_id: int | None = None,
) -> None:
    """status: 'done' | 'failed'."""
    with _db() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, error = ?, result_message_id = ?, "
            "finished_at = ? WHERE id = ?",
            (status, error, result_message_id, datetime.utcnow().isoformat(), job_id),
        )


def _log_ttl_days() -> int:
    """How long to retain messages_log + completed jobs. 0/'none'/'off' disables."""
    raw = (os.environ.get("JARVIS_LOG_TTL_DAYS") or "30").strip().lower()
    if raw in {"0", "none", "off", "false", "no"}:
        return 0
    try:
        n = int(raw)
        return max(0, n)
    except ValueError:
        logger.warning("JARVIS_LOG_TTL_DAYS=%r is not an int, defaulting to 30", raw)
        return 30


def cleanup_old_log_entries(ttl_days: int) -> dict[str, int]:
    """Delete messages_log entries and terminal-status jobs older than ttl_days.

    Pending jobs (incl. scheduled with future not_before) are NEVER deleted —
    they may be valid auto-go timers. Returns counts of deleted rows.
    """
    if ttl_days <= 0:
        return {"messages_log": 0, "jobs": 0}
    threshold = (datetime.utcnow() - timedelta(days=ttl_days)).isoformat()
    with _db() as conn:
        log_n = conn.execute(
            "DELETE FROM messages_log WHERE ts < ?", (threshold,),
        ).rowcount
        jobs_n = conn.execute(
            "DELETE FROM jobs WHERE status IN ('done', 'failed', 'cancelled') "
            "AND COALESCE(finished_at, created_at) < ?",
            (threshold,),
        ).rowcount
    return {"messages_log": log_n, "jobs": jobs_n}


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
            if stats["messages_log"] or stats["jobs"]:
                logger.info(
                    "cleanup_worker: pruned messages_log=%d jobs=%d (TTL=%dd)",
                    stats["messages_log"], stats["jobs"], ttl,
                )
            await asyncio.sleep(3600.0)
        except asyncio.CancelledError:
            logger.info("cleanup_worker cancelled")
            raise
        except Exception:
            logger.exception("cleanup_worker loop crashed; sleeping 5min")
            await asyncio.sleep(300.0)


def _reminders_tz():
    """Local timezone для парсера schedule. Default Europe/Moscow."""
    from zoneinfo import ZoneInfo
    name = os.environ.get("JARVIS_REMINDERS_TZ", "Europe/Moscow")
    try:
        return ZoneInfo(name)
    except Exception:
        logger.warning("JARVIS_REMINDERS_TZ=%r invalid, using Europe/Moscow", name)
        return ZoneInfo("Europe/Moscow")


_DAY_NAMES = {
    "mon": 0, "tue": 1, "wed": 2, "thu": 3,
    "fri": 4, "sat": 5, "sun": 6,
}


def parse_reminder_schedule(schedule: str) -> dict:
    """Парсит человекочитаемое расписание в структуру.

    Поддерживаемые форматы:
      daily HH:MM
      weekday HH:MM        (Пн-Пт)
      weekend HH:MM        (Сб-Вс)
      weekly DAY[,DAY,...] HH:MM   (DAY: mon|tue|wed|thu|fri|sat|sun)
      monthly D HH:MM      (D: 1..28)
      once YYYY-MM-DD HH:MM

    Возвращает dict с ключами:
      type: 'daily'|'weekday'|'weekend'|'weekly'|'monthly'|'once'
      hour, minute: int
      days: list[int] — для 'weekly', индексы 0=mon..6=sun
      day: int — для 'monthly' (1..28)
      date: 'YYYY-MM-DD' — для 'once'
    """
    raw = " ".join((schedule or "").split()).strip().lower()
    if not raw:
        raise ValueError("schedule is empty")

    def parse_hm(token: str) -> tuple[int, int]:
        try:
            h, m = token.split(":")
            h, m = int(h), int(m)
            if not (0 <= h <= 23 and 0 <= m <= 59):
                raise ValueError
            return h, m
        except (ValueError, AttributeError):
            raise ValueError(f"invalid HH:MM: {token!r}")

    parts = raw.split()
    kind = parts[0]

    if kind in ("daily", "weekday", "weekend") and len(parts) == 2:
        h, m = parse_hm(parts[1])
        return {"type": kind, "hour": h, "minute": m}

    if kind == "weekly" and len(parts) == 3:
        days_token = parts[1]
        days_idx: list[int] = []
        for d in days_token.split(","):
            d = d.strip()
            if d not in _DAY_NAMES:
                raise ValueError(f"unknown day: {d!r}; expected one of {list(_DAY_NAMES)}")
            if _DAY_NAMES[d] not in days_idx:
                days_idx.append(_DAY_NAMES[d])
        if not days_idx:
            raise ValueError("weekly: at least one day required")
        h, m = parse_hm(parts[2])
        return {"type": "weekly", "days": sorted(days_idx), "hour": h, "minute": m}

    if kind == "monthly" and len(parts) == 3:
        try:
            day = int(parts[1])
        except ValueError:
            raise ValueError(f"monthly: day must be int, got {parts[1]!r}")
        if not (1 <= day <= 28):
            raise ValueError("monthly: day must be 1..28 (защита от февраля)")
        h, m = parse_hm(parts[2])
        return {"type": "monthly", "day": day, "hour": h, "minute": m}

    if kind == "once" and len(parts) == 3:
        date_str = parts[1]
        h, m = parse_hm(parts[2])
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"once: date must be YYYY-MM-DD, got {date_str!r}")
        return {"type": "once", "date": date_str, "hour": h, "minute": m}

    raise ValueError(
        f"can't parse schedule: {schedule!r}. Examples: "
        "'daily 09:30', 'weekday 09:30', 'weekly mon,wed 14:00', "
        "'monthly 1 10:00', 'once 2026-06-01 09:00'."
    )


def compute_next_fire(parsed: dict, after_utc: datetime | None = None) -> datetime | None:
    """Возвращает следующий момент срабатывания (datetime, UTC, naive ISO-able).

    Возвращает None для 'once' если дата уже в прошлом — такой reminder
    в БД пометится disabled при INSERT/обновлении.
    """
    from zoneinfo import ZoneInfo
    tz = _reminders_tz()
    if after_utc is None:
        after_utc = datetime.utcnow()
    # UTC naive → aware
    now_aware = after_utc.replace(tzinfo=ZoneInfo("UTC")).astimezone(tz)
    hh = parsed["hour"]
    mm = parsed["minute"]

    def at_local(year: int, month: int, day: int) -> datetime:
        local = datetime(year, month, day, hh, mm, tzinfo=tz)
        return local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)

    ptype = parsed["type"]
    if ptype == "once":
        y, mo, d = map(int, parsed["date"].split("-"))
        fire = at_local(y, mo, d)
        return fire if fire > after_utc else None

    today_local = now_aware.replace(hour=hh, minute=mm, second=0, microsecond=0)

    def in_set(weekday: int) -> bool:
        if ptype == "daily":
            return True
        if ptype == "weekday":
            return weekday < 5
        if ptype == "weekend":
            return weekday >= 5
        if ptype == "weekly":
            return weekday in parsed["days"]
        if ptype == "monthly":
            return False  # для monthly другой механизм ниже
        return False

    if ptype == "monthly":
        target_day = parsed["day"]
        candidate_local = now_aware.replace(day=target_day, hour=hh, minute=mm, second=0, microsecond=0)
        if candidate_local <= now_aware:
            # следующий месяц
            year = candidate_local.year
            month = candidate_local.month + 1
            if month > 12:
                month = 1
                year += 1
            candidate_local = candidate_local.replace(year=year, month=month)
        return candidate_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)

    # daily/weekday/weekend/weekly — итеративный поиск ближайшего дня.
    for delta in range(0, 8):
        cand_local = today_local + timedelta(days=delta)
        if delta == 0 and cand_local <= now_aware:
            continue
        if in_set(cand_local.weekday()):
            return cand_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    return None  # не должно случаться для рекуррентных


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


async def _send_manager_notice(
    app: Application, text: str, kind: str = "job_notification",
) -> int | None:
    """Шлёт plain-сообщение в топик Менеджера, логирует и будит Менеджера.

    Помимо доставки plain-сообщения через Telegram, ставит auto-job
    source='self_notice' в очередь Менеджера, чтобы worker запустил его
    LLM-сессию и тот прочитал свой inbox. Дедуп: если у Менеджера уже
    есть pending/in_progress job — не создаём, он обработает все
    свежие нотисы вместе при текущем запуске.

    Используется safety-нотисом из _run_manager_job, health_worker'ом,
    и manager_interrupt event'ом. Возвращает telegram_message_id или None
    при сбое (например, бот не админ в группе).
    """
    mgr_target = resolve_manager_topic()
    if not mgr_target:
        return None
    chat_id, thread_id = mgr_target
    try:
        chat = await app.bot.get_chat(chat_id)
        sent = await send_to_topic(chat, thread_id, text)
        msg_id = sent.message_id if sent is not None else None
        log_message(chat_id, thread_id, "out", kind, text, msg_id)
    except Exception:
        logger.exception("_send_manager_notice failed (kind=%s)", kind)
        return None

    # Auto-kick: создать job для Менеджера, чтобы он сам активировался и
    # обработал свежие нотисы. С дедупом — один job на всю серию нотисов
    # пока Менеджер их не разгребёт.
    try:
        now = datetime.utcnow().isoformat()
        with _db() as conn:
            existing = conn.execute(
                "SELECT COUNT(*) FROM jobs "
                "WHERE chat_id = ? AND thread_id = ? "
                "AND status IN ('pending', 'in_progress')",
                (chat_id, thread_id),
            ).fetchone()
            if existing and existing[0] > 0:
                return msg_id
            conn.execute(
                "INSERT INTO jobs(chat_id, thread_id, text, source, status, "
                "created_at) VALUES (?, ?, ?, 'self_notice', 'pending', ?)",
                (
                    chat_id, thread_id,
                    "[AUTO-KICK] В твой топик пришли новые нотисы от бота. "
                    "Прочитай свой manager_inbox(thread_id=<свой>) и решай "
                    "что с ними делать.",
                    now,
                ),
            )
        logger.info(
            "manager auto-kick job created chat=%s thread=%s", chat_id, thread_id,
        )
    except Exception:
        logger.exception("auto-kick INSERT failed (kind=%s)", kind)
    return msg_id


def resolve_manager_topic() -> tuple[int, int] | None:
    """Return (chat_id, thread_id) of the Manager's topic, or None.

    Used to inject the «report back to Manager» instruction into the SYSTEM
    NOTE of delegated jobs. Falls back to a SQL lookup so the bot works out
    of the box for the default Shevartv setup; explicit env vars override
    for unusual deployments.
    """
    raw_chat = os.environ.get("JARVIS_MANAGER_CHAT_ID")
    raw_thread = os.environ.get("JARVIS_MANAGER_THREAD_ID")
    if raw_chat and raw_thread:
        try:
            return int(raw_chat), int(raw_thread)
        except ValueError:
            logger.warning(
                "JARVIS_MANAGER_{CHAT,THREAD}_ID not int: %r/%r",
                raw_chat, raw_thread,
            )
    try:
        with _db() as conn:
            row = conn.execute(
                "SELECT chat_id, thread_id FROM sessions "
                "WHERE thread_id > 0 AND cwd LIKE '%/knowledge-base/manager' "
                "ORDER BY updated_at DESC LIMIT 1"
            ).fetchone()
        if row:
            return row[0], row[1]
    except Exception:
        logger.exception("resolve_manager_topic failed")
    return None


def save_message_context(chat_id: int, message_id: int, ctx: dict) -> None:
    with _db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO messages(chat_id, message_id, context_json, created_at) "
            "VALUES (?, ?, ?, ?)",
            (chat_id, message_id, json.dumps(ctx, ensure_ascii=False),
             datetime.utcnow().isoformat()),
        )


def load_message_context(chat_id: int, message_id: int) -> dict | None:
    with _db() as conn:
        row = conn.execute(
            "SELECT context_json, created_at FROM messages WHERE chat_id = ? AND message_id = ?",
            (chat_id, message_id),
        ).fetchone()
        if not row:
            return None
        try:
            ctx = json.loads(row[0])
        except json.JSONDecodeError:
            return None
        ctx["_created_at"] = row[1]
        return ctx


# ---------- Сессии ----------

def get_session(chat_id: int, thread_id: int) -> tuple[str, str | None, str]:
    """Возвращает (session_id, cwd, engine_name) для топика.

    Если записи нет — создаёт новую под дефолтный движок (DEFAULT_ENGINE).
    Engine хранится per-topic; при смене JARVIS_ENGINE существующие топики
    продолжают работать со своим движком, переключение — через /engine.
    """
    now = datetime.utcnow().isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT session_id, cwd, engine FROM sessions "
            "WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        if row:
            session_id, cwd, engine_in_db = row
            return session_id, cwd, engine_in_db
        new_id = DEFAULT_ENGINE.new_session_id()
        conn.execute(
            "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, updated_at) "
            "VALUES (?, ?, ?, NULL, ?, ?)",
            (chat_id, thread_id, new_id, DEFAULT_ENGINE_NAME, now),
        )
        return new_id, None, DEFAULT_ENGINE_NAME


def update_session_id(
    chat_id: int, thread_id: int, expected_engine: str, new_session_id: str,
) -> None:
    """Обновляет session_id, только если в БД для топика всё ещё лежит
    `expected_engine`. Используется codex/opencode-движком: при первом запуске
    они отдают реальный id в stream'е, мы подменяем placeholder. Условие
    `engine = expected_engine` защищает от race с командой /engine: если
    пользователь успел переключиться на другой движок, старый стрим не должен
    перезаписывать новый id."""
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET session_id = ?, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ? AND engine = ?",
            (new_session_id, datetime.utcnow().isoformat(),
             chat_id, thread_id, expected_engine),
        )


def reset_session(chat_id: int, thread_id: int) -> tuple[str, str | None, str]:
    """Новый id для движка топика; cwd и engine сохраняются. Если записи
    нет — создаётся под DEFAULT_ENGINE."""
    now = datetime.utcnow().isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT cwd, engine FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        if row:
            cwd, engine_name = row
        else:
            cwd, engine_name = None, DEFAULT_ENGINE_NAME
        engine = get_engine_by_name(engine_name)
        new_id = engine.new_session_id()
        conn.execute(
            "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(chat_id, thread_id) DO UPDATE SET "
            "session_id=excluded.session_id, updated_at=excluded.updated_at",
            (chat_id, thread_id, new_id, cwd, engine_name, now),
        )
    return new_id, cwd, engine_name


# ---------- Жизненный цикл сеанса ----------
#
# Сеанс — это окно терминала: открывается первым сообщением, закрывается /close
# или по простою. Топик (cwd, движок, модель) переживает закрытие, контекст
# сессии — нет; переписка остаётся в messages_log и доступна движку через
# manager_inbox.
#
# Признак закрытого сеанса — last_activity_at IS NULL (session_id объявлен
# NOT NULL, обнулить его нельзя). Новый session_id создаётся лениво, при
# следующем сообщении, чтобы не плодить пустые сессии.


def mark_session_start(chat_id: int, thread_id: int) -> None:
    """Запомнить, когда открыт сеанс. По этой метке видно, не изменились ли с тех
    пор инструкции проекта (движок читает их только при старте сессии)."""
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET session_started_at = ? "
            "WHERE chat_id = ? AND thread_id = ?",
            (datetime.utcnow().isoformat(), chat_id, thread_id),
        )


def touch_session(chat_id: int, thread_id: int) -> None:
    """Отметить активность в топике — продлевает текущий сеанс."""
    now = datetime.utcnow().isoformat()
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET last_activity_at = ?, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ?",
            (now, now, chat_id, thread_id),
        )


def close_session(chat_id: int, thread_id: int) -> bool:
    """Закрыть сеанс топика. Возвращает False, если он и так был закрыт.

    session_id не трогаем — он пересоздастся при следующем сообщении.
    """
    with _db() as conn:
        row = conn.execute(
            "SELECT last_activity_at FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        if row is None or row[0] is None:
            return False
        conn.execute(
            "UPDATE sessions SET last_activity_at = NULL, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ?",
            (datetime.utcnow().isoformat(), chat_id, thread_id),
        )
    return True


def _session_is_stale(last_activity_at: str | None) -> bool:
    """Протух ли сеанс: закрыт (NULL) или простаивал дольше порога."""
    if not last_activity_at:
        return True
    if SESSION_IDLE_MINUTES <= 0:
        return False  # 0/отрицательное — авто-закрытие выключено
    try:
        last = datetime.fromisoformat(last_activity_at)
    except ValueError:
        logger.warning("bad last_activity_at=%r, treating session as stale",
                       last_activity_at)
        return True
    return datetime.utcnow() - last > timedelta(minutes=SESSION_IDLE_MINUTES)


def _session_state_line(key: tuple[int, int]) -> str:
    """Человекочитаемое состояние сеанса для /session и /tokens."""
    with _db() as conn:
        row = conn.execute(
            "SELECT last_activity_at FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (key[0], key[1]),
        ).fetchone()
    if row is None or row[0] is None:
        return "закрыт (следующее сообщение откроет новый)"
    if SESSION_IDLE_MINUTES <= 0:
        return "открыт (авто-закрытие выключено)"
    try:
        last = datetime.fromisoformat(row[0])
    except ValueError:
        return "открыт (не удалось прочитать last_activity_at)"
    idle_min = int((datetime.utcnow() - last).total_seconds() // 60)
    left = SESSION_IDLE_MINUTES - idle_min
    if left <= 0:
        return "протух (следующее сообщение откроет новый)"
    return f"открыт, простой {idle_min} мин, закроется через {left} мин"


INSTRUCTION_FILES = ("AGENTS.md", "CLAUDE.md")


def _instructions_changed(cwd: str | None, started_at: str | None) -> bool:
    """Изменились ли инструкции проекта с момента открытия сеанса.

    Движки читают AGENTS.md / CLAUDE.md при СТАРТЕ сессии. Пока сеанс жив, правка
    инструкций молча не действует: агент до конца сеанса работает по версии,
    прочитанной когда-то давно. Это тихая ловушка — правишь файл, перезапускаешь
    задачу, а поведение прежнее, и непонятно почему (напоролись 2026-07-11 на
    новостном топике: правки формата дайджеста не доходили до агента).

    Поэтому: инструкции новее сеанса → сеанс переоткрываем.
    """
    if not cwd or not started_at:
        return False
    try:
        # session_started_at пишется в UTC, а getmtime отдаёт epoch. Без явного
        # tzinfo naive-строка трактуется как ЛОКАЛЬНОЕ время — и в UTC+3 сеанс
        # переоткрывался бы на каждом сообщении.
        started = datetime.fromisoformat(started_at).replace(
            tzinfo=timezone.utc).timestamp()
    except ValueError:
        return False
    for name in INSTRUCTION_FILES:
        path = os.path.join(cwd, name)
        try:
            if os.path.getmtime(path) > started:
                logger.info("instructions changed: %s newer than session start", path)
                return True
        except OSError:
            continue
    return False


def ensure_active_session(
    chat_id: int, thread_id: int,
) -> tuple[str, str | None, str, bool]:
    """Сеанс топика для текущего сообщения: (session_id, cwd, engine, opened_new).

    Новый сеанс открывается, если прошлый закрыт, протух ИЛИ если изменились
    инструкции проекта (их движок читает только при старте сессии).
    opened_new=True, чтобы вызывающий сообщил об этом пользователю.
    """
    with _db() as conn:
        row = conn.execute(
            "SELECT last_activity_at, cwd, session_started_at FROM sessions "
            "WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()

    if row is None:
        # Первое сообщение в топике: get_session заведёт запись.
        session_id, cwd, engine_name = get_session(chat_id, thread_id)
        touch_session(chat_id, thread_id)
        mark_session_start(chat_id, thread_id)
        logger.info("session opened (new topic): key=%s session=%s",
                    (chat_id, thread_id), session_id)
        return session_id, cwd, engine_name, True

    stale = _session_is_stale(row[0])
    fresh_instructions = not stale and _instructions_changed(row[1], row[2])

    if stale or fresh_instructions:
        # Делегированные задачи (jobs) переоткрытию сеанса не мешают: job несёт
        # полный текст задачи и не зависит от контекста прошлого сеанса, а
        # Менеджер поднимает своё состояние через manager_inbox.
        session_id, cwd, engine_name = reset_session(chat_id, thread_id)
        touch_session(chat_id, thread_id)
        mark_session_start(chat_id, thread_id)
        if fresh_instructions:
            reason = "instructions changed"
        else:
            reason = "closed" if row[0] is None else "stale"
        logger.info("session opened (%s): key=%s session=%s",
                    reason, (chat_id, thread_id), session_id)
        return session_id, cwd, engine_name, True

    session_id, cwd, engine_name = get_session(chat_id, thread_id)
    touch_session(chat_id, thread_id)
    return session_id, cwd, engine_name, False


def set_engine(
    chat_id: int, thread_id: int, new_engine_name: str, model: str | None = None,
) -> tuple[str, str | None]:
    """Меняет движок топика: создаёт новый session_id под новый движок, cwd
    сохраняется, model записывается явно (NULL допустим). Если записи не было —
    создаётся. Возвращает (session_id, cwd).

    Engine-проверка (поддерживается ли имя) — на стороне get_engine_by_name."""
    new_engine = get_engine_by_name(new_engine_name)
    new_id = new_engine.new_session_id()
    now = datetime.utcnow().isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT cwd FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        cwd = row[0] if row else None
        conn.execute(
            "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, model, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(chat_id, thread_id) DO UPDATE SET "
            "session_id=excluded.session_id, engine=excluded.engine, "
            "model=excluded.model, updated_at=excluded.updated_at",
            (chat_id, thread_id, new_id, cwd, new_engine.name, model, now),
        )
    return new_id, cwd


def get_model(chat_id: int, thread_id: int) -> str | None:
    """Возвращает выбранную для топика модель (или None — дефолт движка)."""
    with _db() as conn:
        row = conn.execute(
            "SELECT model FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
    if not row:
        return None
    return row[0] or None


def get_mcp_playwright(chat_id: int, thread_id: int) -> bool:
    """True, если для топика включён браузер (Playwright грузится per-call)."""
    with _db() as conn:
        row = conn.execute(
            "SELECT mcp_playwright FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
    return bool(row[0]) if row and row[0] is not None else False


def set_mcp_playwright(chat_id: int, thread_id: int, enabled: bool) -> None:
    """Выставляет per-topic флаг браузера. Применяется со СЛЕДУЮЩЕГО сообщения:
    набор тулов движка меняется на лету, сессия и контекст сохраняются.
    Если записи топика ещё нет — создаёт под дефолтный движок."""
    now = datetime.utcnow().isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT cwd, engine FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        if row is None:
            engine_name = DEFAULT_ENGINE_NAME
            new_id = get_engine_by_name(engine_name).new_session_id()
            conn.execute(
                "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, "
                "mcp_playwright, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (chat_id, thread_id, new_id, None, engine_name,
                 1 if enabled else 0, now),
            )
        else:
            conn.execute(
                "UPDATE sessions SET mcp_playwright = ?, updated_at = ? "
                "WHERE chat_id = ? AND thread_id = ?",
                (1 if enabled else 0, now, chat_id, thread_id),
            )


def get_persistent_claude(chat_id: int, thread_id: int) -> bool:
    """True, если для топика включён живой процесс claude (/persistent on)."""
    with _db() as conn:
        row = conn.execute(
            "SELECT persistent_claude FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
    return bool(row[0]) if row and row[0] is not None else False


def set_persistent_claude(chat_id: int, thread_id: int, enabled: bool) -> None:
    """Выставляет per-topic флаг живого процесса claude."""
    now = datetime.utcnow().isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT cwd, engine FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        if row is None:
            engine_name = DEFAULT_ENGINE_NAME
            new_id = get_engine_by_name(engine_name).new_session_id()
            conn.execute(
                "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, "
                "persistent_claude, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (chat_id, thread_id, new_id, None, engine_name,
                 1 if enabled else 0, now),
            )
        else:
            conn.execute(
                "UPDATE sessions SET persistent_claude = ?, updated_at = ? "
                "WHERE chat_id = ? AND thread_id = ?",
                (1 if enabled else 0, now, chat_id, thread_id),
            )


def update_actual_model(
    chat_id: int, thread_id: int, engine_name: str, model: str | None,
) -> None:
    """Сохранить реальную модель, которой ответил CLI в последнем запуске.

    Engine-проверка как у update_session_id — защита от race с /engine: если
    оператор успел переключиться, прошлый стрим не должен перезаписать
    actual_model нового движка.
    """
    if not model:
        return
    try:
        with _db() as conn:
            conn.execute(
                "UPDATE sessions SET actual_model = ? "
                "WHERE chat_id = ? AND thread_id = ? AND engine = ?",
                (model, chat_id, thread_id, engine_name),
            )
    except Exception:
        logger.exception(
            "update_actual_model failed chat=%s thread=%s engine=%s",
            chat_id, thread_id, engine_name,
        )


def get_actual_model(chat_id: int, thread_id: int) -> str | None:
    """Возвращает последнюю реально использованную моделью или None."""
    with _db() as conn:
        row = conn.execute(
            "SELECT actual_model FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
    if not row:
        return None
    return row[0] or None


def update_model_only(chat_id: int, thread_id: int, model: str | None) -> bool:
    """Меняет только модель текущего движка топика, не трогая session_id.

    Используется, когда оператор тыкает в свой же активный движок и
    выбирает другую модель. Контекст сессии сохраняется (тот же jsonl).
    Возвращает True если запись существовала.
    """
    with _db() as conn:
        cur = conn.execute(
            "UPDATE sessions SET model = ?, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ?",
            (model, datetime.utcnow().isoformat(), chat_id, thread_id),
        )
    return cur.rowcount == 1


# ---------- Вопросы агента пользователю (ask_user) ----------
#
# Агент зовёт MCP-инструмент ask_user; тот пишет вопрос в ask_requests, шлёт его
# в топик (с кнопками, если заданы варианты) и БЛОКИРУЕТСЯ, полля таблицу.
# Ответ кладёт сюда бот — из нажатия кнопки или из обычного сообщения в топик.


def get_pending_ask(chat_id: int, thread_id: int) -> dict | None:
    """Незакрытый вопрос топика (последний, если их вдруг несколько)."""
    with _db() as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM ask_requests WHERE chat_id = ? AND thread_id = ? "
            "AND status = 'pending' ORDER BY id DESC LIMIT 1",
            (chat_id, thread_id),
        ).fetchone()
    return dict(row) if row is not None else None


def answer_ask(
    ask_id: int, answer: str, via: str, option_index: int | None = None,
) -> bool:
    """Записать ответ. False, если вопрос уже закрыт (ответили дважды/таймаут)."""
    with _db() as conn:
        cur = conn.execute(
            "UPDATE ask_requests SET status = 'answered', answer = ?, "
            "option_index = ?, via = ?, answered_at = ? "
            "WHERE id = ? AND status = 'pending'",
            (answer, option_index, via, datetime.utcnow().isoformat(), ask_id),
        )
    return cur.rowcount == 1


def ask_question_text(question: str, options: list[str] | None) -> str:
    """Как вопрос выглядит в топике. Используется и ботом, и MCP-сервером."""
    body = f"❓ {question}"
    if not options:
        body += "\n\n<i>Ответь сообщением в этот топик.</i>"
    else:
        body += "\n\n<i>Выбери вариант или ответь сообщением.</i>"
    return body


async def _mark_ask_answered(chat, ask: dict, answer: str) -> None:
    """Погасить кнопки у заданного вопроса и показать выбранный ответ."""
    tg_msg_id = ask.get("telegram_message_id")
    if not tg_msg_id:
        return
    body = (
        f"❓ {_html_escape(ask['question'])}\n\n"
        f"✅ <b>Ответ:</b> {_html_escape(answer)}"
    )
    try:
        await chat.get_bot().edit_message_text(
            chat_id=ask["chat_id"], message_id=tg_msg_id, text=body,
            parse_mode=ParseMode.HTML, reply_markup=None,
        )
    except Exception:
        # Сообщение могли удалить/изменить — ответ уже в БД, агент его получит.
        logger.warning("ask #%s: failed to update question message", ask["id"])


async def on_ask_answer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Нажата кнопка варианта. callback_data: ask:<ask_id>:<option_index>."""
    query = update.callback_query
    if query is None or not (query.data or "").startswith("ask:"):
        return
    parts = (query.data or "").split(":")
    if len(parts) != 3:
        return
    try:
        ask_id = int(parts[1])
        option_index = int(parts[2])
    except ValueError:
        return

    with _db() as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM ask_requests WHERE id = ?", (ask_id,)).fetchone()
    if row is None:
        await query.answer("Вопрос не найден", show_alert=False)
        return
    ask = dict(row)

    options = json.loads(ask["options_json"] or "[]")
    if not 0 <= option_index < len(options):
        await query.answer("Неизвестный вариант", show_alert=False)
        return
    answer = options[option_index]

    if not answer_ask(ask_id, answer, via="button", option_index=option_index):
        # Уже отвечено текстом или истёк таймаут.
        await query.answer("Вопрос уже закрыт", show_alert=False)
        return

    logger.info("ask #%s answered by button: %r", ask_id, answer)
    try:
        await query.answer(f"Принято: {answer}"[:200])
    except Exception:
        pass
    try:
        await query.edit_message_text(
            f"❓ {_html_escape(ask['question'])}\n\n"
            f"✅ <b>Ответ:</b> {_html_escape(answer)}",
            parse_mode=ParseMode.HTML, reply_markup=None,
        )
    except Exception:
        logger.warning("ask #%s: failed to edit question message", ask_id)


def set_pending_summary(chat_id: int, thread_id: int, summary: str) -> None:
    """Сохраняет резюме предыдущей сессии — будет доставлено в первый prompt
    после переключения движка."""
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET pending_summary = ?, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ?",
            (summary, datetime.utcnow().isoformat(), chat_id, thread_id),
        )


def get_pending_summary(chat_id: int, thread_id: int) -> str | None:
    """Вернуть pending_summary без очистки.

    Резюме удаляем только после успешного первого хода новой сессии, иначе при
    падении этого хода контекст потеряется без возможности ретрая.
    """
    with _db() as conn:
        row = conn.execute(
            "SELECT pending_summary FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
    if not row or not row[0]:
        return None
    return row[0]


def clear_pending_summary(chat_id: int, thread_id: int) -> None:
    """Очистить pending_summary после того, как summary уже успешно попало
    в новый ход LLM-сессии."""
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET pending_summary = NULL, updated_at = ? "
            "WHERE chat_id = ? AND thread_id = ?",
            (datetime.utcnow().isoformat(), chat_id, thread_id),
        )


def _transfer_marker(old_engine_name: str) -> str:
    """JSON-маркер «контекст просили перенести». Кладётся в pending_summary при
    смене движка и разворачивается в указание прочитать историю (см.
    _resolve_pending_summary) при первом сообщении новому движку."""
    return json.dumps(
        {"transfer_requested": True, "old_engine": old_engine_name},
        ensure_ascii=False,
    )


def _parse_transfer_marker(pending: str) -> dict | None:
    """Распарсить JSON-маркер {transfer_requested, old_engine, old_session_id}
    или вернуть None, если не маркер / невалидный JSON."""
    try:
        data = json.loads(pending)
    except (json.JSONDecodeError, TypeError):
        return None
    if data.get("transfer_requested") and data.get("old_engine"):
        return data
    return None


def build_context_handoff(key: tuple[int, int], old_engine_name: str) -> str:
    """Указание новому движку поднять контекст самому.

    Раньше здесь старый движок гонялся за резюме — полный проход по всей
    истории, самый дорогой вызов из возможных. Теперь платит только новый
    движок и только за то, что реально прочитал: историю топика он берёт через
    manager_inbox, рабочее состояние — из кода и git.
    """
    chat_id, thread_id = key
    return (
        f"Ты подхватываешь диалог, который до тебя вёл другой агент "
        f"({old_engine_name}). Его контекст тебе НЕ передан.\n"
        f"Прежде чем отвечать, подними историю сам: вызови MCP-инструмент "
        f"manager_inbox(chat_id={chat_id}, thread_id={thread_id}) и прочитай "
        f"последние сообщения — столько, сколько нужно, чтобы понять задачу и "
        f"на чём остановились. Рабочее состояние сверяй с кодом и git, а не с "
        f"пересказом."
    )


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


def set_cwd(chat_id: int, thread_id: int, cwd: str) -> None:
    """Создаёт запись, если её нет (session_id — новый id для дефолтного движка),
    либо обновляет cwd."""
    now = datetime.utcnow().isoformat()
    with _db() as conn:
        row = conn.execute(
            "SELECT session_id FROM sessions WHERE chat_id = ? AND thread_id = ?",
            (chat_id, thread_id),
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE sessions SET cwd = ?, updated_at = ? WHERE chat_id = ? AND thread_id = ?",
                (cwd, now, chat_id, thread_id),
            )
        else:
            conn.execute(
                "INSERT INTO sessions(chat_id, thread_id, session_id, cwd, engine, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (chat_id, thread_id, DEFAULT_ENGINE.new_session_id(), cwd,
                 DEFAULT_ENGINE_NAME, now),
            )


def clear_cwd(chat_id: int, thread_id: int) -> None:
    with _db() as conn:
        conn.execute(
            "UPDATE sessions SET cwd = NULL, updated_at = ? WHERE chat_id = ? AND thread_id = ?",
            (datetime.utcnow().isoformat(), chat_id, thread_id),
        )


# ---------- Ключ per-topic ----------

def _key(update: Update) -> tuple[int, int]:
    chat_id = update.effective_chat.id
    msg = update.message or update.effective_message
    thread_id = 0
    if msg is not None and getattr(msg, "is_topic_message", False):
        thread_id = msg.message_thread_id or 0
    return chat_id, thread_id


chat_locks: dict[tuple[int, int], asyncio.Lock] = {}
active_procs: dict[tuple[int, int], asyncio.subprocess.Process] = {}
# Отдельный реестр для /spawn: key=(chat_id, thread_id, spawn_id_hex).
# Основной /stop не трогает эти процессы; снять spawn можно через /stop <spawn_id>.
spawn_procs: dict[tuple[int, int, str], asyncio.subprocess.Process] = {}

# Живые процессы claude для топиков с /persistent on. Отдельно от active_procs:
# эти сообщения НЕ идут через chat_locks — сообщение, пришедшее пока живой
# процесс занят ходом, дописывается в его stdin, а не ждёт очереди.
persistent_workers: dict[tuple[int, int], PersistentClaudeWorker] = {}

# Простаивающий живой процесс не экономит токены (сессия и так резюмируется
# с диска) — только задержку на старте. Держать его вечно смысла нет.
PERSISTENT_IDLE_MINUTES = _int_env_raw("JARVIS_PERSISTENT_IDLE_MINUTES", 20)


async def _kill_persistent_worker(key: tuple[int, int], reason: str) -> bool:
    """Убить живой процесс claude топика, если есть. Будит того, кто ждёт
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
    await terminate_process_tree(worker.proc)
    return True


async def persistent_reaper(app: Application) -> None:
    """Убивает простаивающие живые процессы claude (свободные между ходами)
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


def _lock_for(key: tuple[int, int]) -> asyncio.Lock:
    lock = chat_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        chat_locks[key] = lock
    return lock


# Реестр отменяемых ожидающих запросов: queue_id -> asyncio.Event.
# Когда запрос ждёт освобождения lock'а топика, в реестре лежит его event.
# Callback "cancel_queue:<queue_id>" выставляет event, ожидающая корутина видит
# это и выходит, не захватывая lock и не вызывая claude.
# Когда запрос уже начал выполняться (lock захвачен) — его id удаляется из реестра;
# попытка отменить в этот момент отвечает пользователю «используй /stop».
pending_queue: dict[str, asyncio.Event] = {}


# ---------- Markdown → HTML для Telegram ----------

def _html_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ```lang\n...\n```  (multiline) или ```...```
_FENCE_RE = re.compile(r"```([A-Za-z0-9_+\-]*)\n?(.*?)```", re.DOTALL)
_INLINE_CODE_RE = re.compile(r"`([^`\n]+)`")
_BOLD_RE = re.compile(r"\*\*(.+?)\*\*", re.DOTALL)
_ITALIC_RE = re.compile(r"(?<![\*A-Za-z0-9])\*(?!\s)(.+?)(?<!\s)\*(?![\*A-Za-z0-9])", re.DOTALL)


def md_to_html(text: str) -> str:
    """Конвертирует упрощённый markdown от claude в HTML, понятный Telegram.
    Поддерживает: ```code blocks``` (с языком), `inline`, **bold**, *italic*.
    Всё, что вне кода, экранируется (<, >, &); внутри кода — тоже."""
    placeholders: list[str] = []

    def _stash(html: str) -> str:
        placeholders.append(html)
        return f"\x00PH{len(placeholders) - 1}\x00"

    def _fence(m: re.Match) -> str:
        lang = m.group(1) or ""
        body = m.group(2)
        body_esc = _html_escape(body)
        if lang:
            return _stash(f'<pre><code class="language-{_html_escape(lang)}">{body_esc}</code></pre>')
        return _stash(f"<pre><code>{body_esc}</code></pre>")

    def _inline(m: re.Match) -> str:
        return _stash(f"<code>{_html_escape(m.group(1))}</code>")

    text = _FENCE_RE.sub(_fence, text)
    text = _INLINE_CODE_RE.sub(_inline, text)
    text = _html_escape(text)
    text = _BOLD_RE.sub(lambda m: f"<b>{m.group(1)}</b>", text)
    text = _ITALIC_RE.sub(lambda m: f"<i>{m.group(1)}</i>", text)

    def _restore(m: re.Match) -> str:
        return placeholders[int(m.group(1))]

    return re.sub(r"\x00PH(\d+)\x00", _restore, text)


def split_html_for_telegram(html: str, limit: int = TG_HARD_LIMIT) -> list[str]:
    """Бьёт HTML на куски ≤ limit, не разрывая открытые <pre>/<code>.
    Стратегия: режем по \\n, если внутри куска остался незакрытый <pre><code> —
    закрываем в конце куска и переоткрываем в начале следующего."""
    if len(html) <= limit:
        return [html]
    # Делим по строкам.
    lines = html.split("\n")
    chunks: list[str] = []
    cur = ""
    for line in lines:
        candidate = (cur + "\n" + line) if cur else line
        if len(candidate) <= limit:
            cur = candidate
            continue
        if cur:
            chunks.append(cur)
        # Если сама строка длиннее лимита — режем грубо по символам.
        while len(line) > limit:
            chunks.append(line[:limit])
            line = line[limit:]
        cur = line
    if cur:
        chunks.append(cur)
    # Балансируем <pre><code> между чанками.
    balanced: list[str] = []
    open_pre = False
    for ch in chunks:
        prefix = "<pre><code>" if open_pre else ""
        body = prefix + ch
        # Простой подсчёт: count open vs close <pre>.
        opens = body.count("<pre>")
        closes = body.count("</pre>")
        if opens > closes:
            body += "</code></pre>"
            open_pre = True
        else:
            open_pre = False
        balanced.append(body)
    return balanced


# ---------- Отправка в топик ----------

async def _send_with_html_fallback(send_func, text: str, **kwargs):
    """Шлёт text как HTML; при ошибке парсинга — повторяет без parse_mode (plain)."""
    try:
        return await send_func(text=text, parse_mode=ParseMode.HTML, **kwargs)
    except BadRequest as exc:
        if "parse" in str(exc).lower() or "entit" in str(exc).lower():
            logger.warning("HTML parse failed, falling back to plain: %s", exc)
            kwargs.pop("parse_mode", None)
            return await send_func(text=text, **kwargs)
        raise


async def send_to_topic(chat, thread_id: int, text: str, **kwargs):
    if thread_id:
        kwargs.setdefault("message_thread_id", thread_id)
    return await chat.send_message(text=text, **kwargs)


async def send_document_to_topic(chat, thread_id: int, document, **kwargs):
    if thread_id:
        kwargs.setdefault("message_thread_id", thread_id)
    return await chat.send_document(document=document, **kwargs)


JOURNAL_MAX_CHARS = 3400   # запас до TG_HARD_LIMIT на HTML-разметку
JOURNAL_LINE_CHARS = 400   # длинную строку шага режем — журнал, не транскрипт


class ProgressJournal:
    """Журнал хода работы агента — одно накопительное сообщение в топике.

    Раньше промежуточные шаги писались в сообщение-индикатор: каждый апдейт
    ЗАТИРАЛ предыдущий, а в конце индикатор удалялся — так что ход работы
    (какие команды агент выполнял, что читал, о чём рассуждал) исчезал целиком.
    Теперь шаги дописываются и остаются в топике после ответа.

    Когда сообщение упирается в лимит Telegram, журнал продолжается в новом;
    заполненное остаётся в истории как есть. Если агент не сделал ни одного
    шага, стартовая плашка удаляется, чтобы не мусорить в топике.
    """

    def __init__(self, chat, thread_id: int, prefix: str = "",
                 header: str = "⏳ Думаю..."):
        self.chat = chat
        self.thread_id = thread_id
        self.prefix = prefix
        self.header = header
        self.msg = None
        self.lines: list[str] = []   # строки ТЕКУЩЕГО сообщения
        self.total_steps = 0
        self._broken = False         # Telegram не даёт писать — тихо выключаемся

    async def start(self) -> None:
        try:
            self.msg = await send_to_topic(
                self.chat, self.thread_id, f"{self.prefix}{self.header}",
            )
        except Exception:
            logger.exception("journal: failed to post header")
            self.msg = None

    def _render(self) -> str:
        return self.prefix + "\n".join(self.lines)

    async def append(self, chunk: str) -> None:
        """Дописать шаги. `chunk` — только новое с прошлого флеша (движки шлют
        дельту, а не весь буфер)."""
        if self._broken:
            return
        new_lines = [
            line.strip()[:JOURNAL_LINE_CHARS]
            for line in chunk.splitlines() if line.strip()
        ]
        if not new_lines:
            return

        for line in new_lines:
            # Не влезает в текущее сообщение — начинаем новое, старое остаётся.
            if self.lines and len(self._render()) + len(line) + 1 > JOURNAL_MAX_CHARS:
                self.msg = None
                self.lines = []
            self.lines.append(line)
            self.total_steps += 1

        await self._flush()

    async def _flush(self) -> None:
        body = md_to_html(self._render())
        try:
            if self.msg is None:
                self.msg = await _send_with_html_fallback(
                    self.chat.send_message, body,
                    **({"message_thread_id": self.thread_id} if self.thread_id else {}),
                )
                return
            try:
                await self.msg.edit_text(body, parse_mode=ParseMode.HTML)
            except BadRequest as exc:
                text = str(exc).lower()
                if "not modified" in text:
                    return
                if "parse" in text or "entit" in text:
                    await self.msg.edit_text(self._render())
                    return
                raise
        except RetryAfter:
            # Telegram троттлит правки. Пропускаем ЭТОТ апдейт — строки уже в
            # self.lines и уедут со следующим флешем. Глушить журнал нельзя:
            # на длинном ходе троттлинг — норма, а не отказ.
            logger.info("journal: throttled by Telegram, skipping this update")
        except Exception:
            # Удалённый топик, потеря прав, слишком длинное сообщение — журнал
            # не критичен, ответ пользователю важнее. Замолкаем.
            logger.warning("journal: giving up on updates", exc_info=True)
            self._broken = True

    def _strip_final_echo(self, final_text: str) -> None:
        """Убрать из хвоста журнала текст, который сейчас уйдёт финальным ответом.

        Движки шлют в промежуточный поток и текстовые блоки ассистента — для
        claude это вообще единственная видимая часть его рассуждений (блок
        thinking приходит пустым), так что выбрасывать их нельзя. Но ПОСЛЕДНИЙ
        такой блок и есть финальный ответ: без этой чистки он виден дважды —
        в журнале и отдельным сообщением.

        Режем только хвост и только строки без префикса шага (🔧 / 💭):
        промежуточные реплики агента («сейчас проверю X») в финал не входят и
        должны остаться.
        """
        final = (final_text or "").strip()
        if not final:
            return
        while self.lines:
            last = self.lines[-1]
            if last.startswith(("🔧", "💭")):
                break
            # Строки в журнале обрезаны (JOURNAL_LINE_CHARS), поэтому ищем
            # вхождение, а не равенство.
            if last and last in final:
                self.lines.pop()
                self.total_steps -= 1
                continue
            break

    async def finish(self, final_text: str | None = None) -> None:
        """Оставить журнал в топике, вычистив эхо финального ответа.

        Журнал без единого шага (или из одного лишь финального текста) —
        удаляем: он ничего не добавляет к ответу.
        """
        if self.msg is None:
            return
        if final_text:
            self._strip_final_echo(final_text)
        if self.total_steps > 0 and self.lines:
            await self._flush()   # перерисовать без вырезанного хвоста
            return
        try:
            await self.msg.delete()
        except Exception:
            pass


def extract_file_markers(text: str) -> tuple[str, list[tuple[str, str | None]]]:
    """Парсит маркеры [[FILE: /path]] / [[FILE: /path | caption]] на отдельных строках.
    Возвращает (текст без маркеров, список (path, caption|None))."""
    markers: list[tuple[str, str | None]] = []

    def _collect(m: re.Match) -> str:
        path = m.group("path").strip()
        cap = m.group("caption")
        cap = cap.strip() if cap else None
        markers.append((path, cap))
        return ""

    cleaned = FILE_MARKER_RE.sub(_collect, text)
    # Подчищаем пустые строки, оставшиеся после вырезания маркеров.
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned, markers


async def deliver_file_markers(
    chat,
    thread_id: int,
    markers: list[tuple[str, str | None]],
    notice_prefix: str = "",
) -> None:
    """Отправляет файлы по списку маркеров. Ошибки сообщает текстом в тот же топик
    (с notice_prefix, например '[#xxxx] '). Сами файлы шлёт без префикса."""
    for path, caption in markers:
        try:
            if not os.path.isabs(path):
                await send_to_topic(
                    chat, thread_id,
                    f"{notice_prefix}⚠️ путь не абсолютный: {path}",
                )
                continue
            if not os.path.exists(path):
                await send_to_topic(
                    chat, thread_id,
                    f"{notice_prefix}⚠️ файл {path} не найден",
                )
                continue
            if not os.path.isfile(path):
                await send_to_topic(
                    chat, thread_id,
                    f"{notice_prefix}⚠️ {path} не является обычным файлом",
                )
                continue
            size = os.path.getsize(path)
            size_mb = size / (1024 * 1024)
            if size_mb > TG_FILE_LIMIT_MB:
                await send_to_topic(
                    chat, thread_id,
                    f"{notice_prefix}⚠️ файл {path} слишком большой "
                    f"({size_mb:.1f} MB), лимит Telegram {TG_FILE_LIMIT_MB} MB",
                )
                continue
            kwargs = {"filename": os.path.basename(path)}
            if caption:
                kwargs["caption"] = caption[:1024]
            with open(path, "rb") as fh:
                await send_document_to_topic(chat, thread_id, document=fh, **kwargs)
            logger.info("delivered file marker: %s (%.1f MB)", path, size_mb)
        except Exception as exc:
            logger.exception("deliver_file_markers failed for %s", path)
            try:
                await send_to_topic(
                    chat, thread_id,
                    f"{notice_prefix}⚠️ не удалось отправить {path}: {exc}",
                )
            except Exception:
                pass


async def send_claude_reply(
    chat, thread_id: int, text: str, meta: dict, filename_prefix: str = "reply",
    html_prefix: str = "",
):
    """Короткий текст — send_message с HTML-форматированием; длинный — .md вложение.
    `html_prefix` (например '[#xxxx] ') добавляется как уже готовый HTML-фрагмент
    перед сконвертированным телом."""
    log_kind = "spawn_reply" if meta.get("spawn_id") else "bot_reply"

    if len(text) <= MSG_LIMIT:
        html_body = html_prefix + md_to_html(text)
        chunks = split_html_for_telegram(html_body, TG_HARD_LIMIT)
        send_kwargs = {}
        if thread_id:
            send_kwargs["message_thread_id"] = thread_id
        sent = None
        for chunk in chunks:
            sent = await _send_with_html_fallback(chat.send_message, chunk, **send_kwargs)
        try:
            if sent is not None:
                save_message_context(chat.id, sent.message_id, meta)
                log_message(
                    chat.id, thread_id, "out", log_kind, text, sent.message_id,
                )
        except Exception:
            logger.exception("save_message_context failed")
        return sent

    plain_prefix = re.sub(r"<[^>]+>", "", html_prefix) if html_prefix else ""
    preview = plain_prefix + text[:200].rstrip() + "...\n\nполный ответ во вложении"
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".md", delete=False, encoding="utf-8", dir=MEDIA_DIR,
    ) as f:
        f.write(text)
        path = f.name
    try:
        with open(path, "rb") as fh:
            sent = await send_document_to_topic(
                chat, thread_id,
                document=fh,
                filename=f"{filename_prefix}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.md",
                caption=preview[:1024],
            )
        try:
            save_message_context(chat.id, sent.message_id, meta)
            log_message(
                chat.id, thread_id, "out", log_kind, text,
                sent.message_id if sent is not None else None,
            )
        except Exception:
            logger.exception("save_message_context failed")
        return sent
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


# ---------- Вызов LLM CLI (stream) ----------

async def call_llm_stream(
    engine: Engine,
    session_id: str,
    prompt: str,
    key: tuple[int, int],
    cwd: str | None,
    on_intermediate,
    spawn_id: str | None = None,
) -> tuple[bool, str, str | None]:
    """Обёртка над engine.call_stream. Обновляет session_id в БД, если движок
    вернул изменённый id (актуально для codex/opencode — они сами назначают
    реальный id при первом запуске). Для spawn'а id в БД не сохраняется.

    Также сохраняет actual_model в sessions — реальное имя модели,
    которым CLI ответил (из stream-events). Это позволяет /session
    показать точную модель, не догадки.

    Возвращает (ok, final_text, session_id_after).
    """
    mcp_ok, mcp_status = ensure_engine_tools(engine)
    if not mcp_ok:
        logger.warning("engine=%s MCP setup issue: %s", engine.name, mcp_status)

    # Браузер — on-demand, флаг per-topic. Системный блок строим под флаг
    # (строка про browser_* только когда Playwright подключён) и передаём в
    # системный канал движка вместо вшивания в каждый prompt.
    mcp_playwright = get_mcp_playwright(*key)
    effective_cwd = cwd or CLAUDE_CWD
    system_prefix = build_system_prefix(effective_cwd, mcp_playwright, key=key)

    ok, final_text, sid_after, actual_model = await engine.call_stream(
        session_id=session_id,
        prompt=prompt,
        key=key,
        cwd=cwd,
        on_intermediate=on_intermediate,
        active_procs=active_procs,
        spawn_procs=spawn_procs,
        spawn_id=spawn_id,
        system_prefix=system_prefix,
        mcp_playwright=mcp_playwright,
    )
    # Recovery: иногда opencode/codex на resume могут вернуть rc=0, но пустой
    # текст. Для постоянной сессии делаем один автоповтор в новой сессии.
    if (
        spawn_id is None
        and (not ok)
        and engine.name in {"opencode", "codex"}
        and "вернул пустой ответ" in (final_text or "")
    ):
        try:
            new_sid, _, _ = reset_session(key[0], key[1])
            logger.warning(
                "engine=%s empty reply on session=%s key=%s; retrying with new session=%s",
                engine.name, session_id, key, new_sid,
            )
            ok2, final_text2, sid_after2, actual_model2 = await engine.call_stream(
                session_id=new_sid,
                prompt=prompt,
                key=key,
                cwd=cwd,
                on_intermediate=on_intermediate,
                active_procs=active_procs,
                spawn_procs=spawn_procs,
                spawn_id=spawn_id,
                system_prefix=system_prefix,
                mcp_playwright=mcp_playwright,
            )
            ok, final_text, sid_after, actual_model = ok2, final_text2, sid_after2, actual_model2
            session_id = new_sid
        except Exception:
            logger.exception(
                "recovery retry failed after empty reply: engine=%s key=%s",
                engine.name, key,
            )
    # Для постоянной сессии (не spawn) — если движок отдал новый id, сохраняем.
    if spawn_id is None and sid_after and sid_after != session_id:
        try:
            update_session_id(key[0], key[1], engine.name, sid_after)
            logger.info(
                "session_id updated by engine=%s for key=%s: %s -> %s",
                engine.name, key, session_id, sid_after,
            )
        except Exception:
            logger.exception("failed to persist new session_id from engine")
    # Реальная модель — сохраняем в sessions для /session и manager_topics.
    if spawn_id is None and actual_model:
        update_actual_model(key[0], key[1], engine.name, actual_model)
    return ok, final_text, sid_after


# ---------- Reply-to контекст ----------

def _build_reply_context_prefix(ctx: dict) -> str:
    parts = []
    t = ctx.get("type")
    created = ctx.get("_created_at", "")
    if t == "claude_response":
        parts.append(f"пользователь отвечает на твой предыдущий ответ (время {created})")
    else:
        parts.append(f"пользователь отвечает на твоё сообщение типа {t!r} (время {created})")
        extras = {k: v for k, v in ctx.items() if k not in ("type", "_created_at")}
        if extras:
            parts.append(f"метаданные: {json.dumps(extras, ensure_ascii=False)}")
    return "[Пользователь отвечает на:] " + "; ".join(parts)


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
        "/persistent [on|off] — живой процесс claude: сообщения на лету, без очереди\n"
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
        await _kill_persistent_worker(key, "прервано через /stop")
        await update.message.reply_text(
            "⛔ Живой процесс claude прерван. Сессия сохранена — следующее "
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
    persistent_state = "on" if get_persistent_claude(*key) else "off"
    body = (
        f"engine     : {engine_name}\n"
        f"model      : {model_line}\n"
        f"session-id : {session_id}\n"
        f"cwd        : {effective_cwd}{cwd_suffix}\n"
        f"browser    : {browser_state}  (Playwright MCP, on-demand)\n"
        f"persistent : {persistent_state}  (живой процесс claude)\n"
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
    session_id, _cwd, engine_name = get_session(*key)
    if enable and engine_name != "claude":
        return (
            f"⚠️ Живой процесс сейчас есть только для claude, а у топика "
            f"движок `{engine_name}`. Переключи `/engine claude` или включай "
            "после."
        )
    set_persistent_claude(key[0], key[1], enable)
    logger.info("persistent toggled for key=%s: %s", key, "on" if enable else "off")
    if enable:
        return (
            "⚡ Живой процесс claude включён для топика. Со следующего "
            "сообщения claude поднимается один раз на весь сеанс: то, что "
            "прилетит, пока он ещё работает над предыдущим, допишется ему "
            "прямо во время работы, а не будет ждать своей очереди. "
            "Уже начатую команду это не остановит — только подхватится, как "
            "только он освободится от неё. Выключай через /persistent off, "
            "когда не нужно — простаивающий процесс просто занимает память."
        )
    await _kill_persistent_worker(key, "выключено через /persistent off")
    return "🚫 Живой процесс выключен. Дальше — как обычно, процесс на сообщение."


async def cmd_persistent(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/persistent — статус + кнопка; /persistent on|off — включить/выключить
    живой процесс claude для топика (сообщения во время работы агента
    подхватываются на лету, а не ждут своей очереди)."""
    key = _key(update)
    args = [a.strip().lower() for a in (context.args or [])]
    current = get_persistent_claude(*key)

    if not args:
        state = "включён" if current else "выключен"
        await update.message.reply_text(
            f"Живой процесс claude сейчас {state} для этого топика.\n\n"
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
            text, reply_markup=_persistent_keyboard(get_persistent_claude(*key)),
        )
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


async def _handle_persistent_message(
    chat, thread_id: int, key: tuple[int, int], user_text: str, meta_block: str,
) -> None:
    """Путь для топиков с /persistent on: без topic-lock и без «в очереди».

    Сообщение уходит живому процессу claude — новым ходом, если он свободен
    между ходами, или довеском к уже идущему ходу, если он ещё работает (без
    ожидания и без нового subprocess). Уже начатый tool-вызов это не обрывает
    — довесок подхватывается сразу после него."""
    worker = persistent_workers.get(key)
    if worker is not None and (worker.dead or worker.proc.returncode is not None):
        persistent_workers.pop(key, None)
        worker = None

    pending_summary_delivered = False
    if worker is None:
        session_id, cwd, engine_name, opened_new = ensure_active_session(*key)
        if engine_name != "claude":
            # /engine сменили мимо /persistent — тихий fallback на обычный путь.
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
        effective_cwd = cwd or CLAUDE_CWD
        system_prefix = build_system_prefix(effective_cwd, mcp_playwright, key=key)

        try:
            worker = await start_persistent_claude(
                key=key, session_id=session_id, cwd=effective_cwd, model=model,
                system_prefix=system_prefix, mcp_playwright=mcp_playwright,
            )
        except Exception as exc:
            logger.exception("persistent worker spawn failed key=%s", key)
            await send_to_topic(
                chat, thread_id, f"⚠️ Не удалось поднять живой процесс claude: {exc}",
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
    try:
        ok, final_text = await asyncio.wait_for(fut, timeout=CLAUDE_TIMEOUT)
    except asyncio.TimeoutError:
        logger.warning("persistent worker timeout key=%s", key)
        await _kill_persistent_worker(key, "")
        ok, final_text = False, f"Timeout: живой claude не ответил за {CLAUDE_TIMEOUT}с."
    except Exception as exc:
        logger.exception("persistent worker await failed key=%s", key)
        ok, final_text = False, f"Внутренняя ошибка: {exc}"
    finally:
        if worker is not None:
            worker.on_intermediate = None

    if ok and pending_summary_delivered:
        clear_pending_summary(*key)

    await _finish_turn_reply(chat, thread_id, journal, ok, final_text, "claude", key)
    logger.info("persistent turn done: key=%s ok=%s", key, ok)


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

    # Живой процесс claude (/persistent on) — своя ветка, без topic-lock:
    # сообщение, пришедшее пока агент работает, дописывается ему на лету.
    _peek_sid, _peek_cwd, peek_engine_name = get_session(*key)
    if peek_engine_name == "claude" and get_persistent_claude(*key):
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
                f"ищи команду в knowledge-base/projects/<имя>/production_smoke_check.md "
                f"(или подобном файле). Если smoke упал — откатить через "
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
    BotCommand("persistent", "живой процесс claude: сообщения на лету, без очереди"),
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

    # Гигиена: hourly cleanup старых записей messages_log + завершённых jobs.
    # TTL — env JARVIS_LOG_TTL_DAYS (дефолт 30, 0/none/off отключает).
    cleanup_task = asyncio.create_task(cleanup_worker(application))
    application.bot_data["cleanup_worker_task"] = cleanup_task

    # /persistent: убивает простаивающие живые процессы claude.
    persistent_reaper_task = asyncio.create_task(persistent_reaper(application))
    application.bot_data["persistent_reaper_task"] = persistent_reaper_task

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

    # concurrent_updates=True: без этого PTB обрабатывает апдейты последовательно,
    # и per-key asyncio.Lock не даёт параллельности между разными топиками —
    # второй топик ждёт, пока освободится воркер PTB, а не сам lock.
    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .concurrent_updates(True)
        .post_init(_post_init)
        .build()
    )

    allowed = filters.User(user_id=ALLOWED_USER_IDS)

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

    app.add_handler(MessageHandler(~allowed, unauthorized_handler))

    logger.info("Whitelisted user_ids: %s", sorted(ALLOWED_USER_IDS))
    logger.info("Default engine: %s  default cwd=%s", DEFAULT_ENGINE_NAME, CLAUDE_CWD)
    print(f"Бот запущен (default engine={DEFAULT_ENGINE_NAME}). Жду сообщения в Telegram...")
    app.run_polling()


if __name__ == "__main__":
    main()
