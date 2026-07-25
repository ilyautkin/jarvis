"""Топик как рабочее место: ключ, роль, разделяемое состояние.

Топик Telegram — единица работы Jarvis: у него свой cwd, движок и своя очередь.
Здесь то, что относится к топику как таковому, до всякой бизнес-логики.

**Про разделяемое состояние.** Словари ниже — не кэш, а рабочие реестры живых
объектов: локов, процессов, живых воркеров. Они делятся ПО ССЫЛКЕ, поэтому
модули, которым они нужны, импортируют их отсюда и работают с тем же объектом.
Переприсваивать их нельзя (``chat_locks = {}`` в другом модуле создаст второй
реестр, и per-topic лок перестанет что-либо защищать) — только мутировать.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime

from telegram import Update

from bot.db import _db
from bot.settings import int_env

logger = logging.getLogger(__name__)

def resolve_manager_topic() -> tuple[int, int] | None:
    """Return (chat_id, thread_id) of the Manager's topic, or None.

    Two uses: the «report back to Manager» instruction in the SYSTEM NOTE of
    delegated jobs, and the topic role that decides which credentials an
    external MCP server gets (see resolve_topic_role).

    Set both JARVIS_MANAGER_CHAT_ID and JARVIS_MANAGER_THREAD_ID to enable it.
    Without them Jarvis has no Manager topic — a single-topic install does not
    need one. (Until 2026-07-25 this fell back to a SQL lookup for a directory
    layout private to the author, which silently did nothing for anyone else.)
    """
    raw_chat = os.environ.get("JARVIS_MANAGER_CHAT_ID")
    raw_thread = os.environ.get("JARVIS_MANAGER_THREAD_ID")
    if not (raw_chat and raw_thread):
        return None
    try:
        return int(raw_chat), int(raw_thread)
    except ValueError:
        logger.warning(
            "JARVIS_MANAGER_{CHAT,THREAD}_ID not int: %r/%r", raw_chat, raw_thread,
        )
        return None


def resolve_topic_role(key: tuple[int, int]) -> str:
    """Role of a Jarvis topic: 'manager' for the orchestrating topic, else 'agent'.

    The selected LLM engine is irrelevant here — the role belongs to the topic.
    External MCP servers use it to pick credentials, so that one forum can act
    under two identities without either leaking into the other's topics.
    """
    return "manager" if resolve_manager_topic() == key else "agent"


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


# Живые процессы для топиков с /persistent on. Отдельно от active_procs:
# эти сообщения НЕ идут через chat_locks — сообщение, пришедшее пока живой
# процесс занят ходом, дописывается в него, а не ждёт очереди.
persistent_workers: dict[tuple[int, int], object] = {}


# Простаивающий живой процесс не экономит токены (сессия и так резюмируется
# с диска) — только задержку на старте. Держать его вечно смысла нет.
PERSISTENT_IDLE_MINUTES = int_env("JARVIS_PERSISTENT_IDLE_MINUTES", 20)


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
