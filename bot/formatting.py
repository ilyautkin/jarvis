"""Markdown → HTML для Telegram и нарезка длинных сообщений.

Telegram принимает узкое подмножество HTML, а не Markdown, и жёстко ограничивает
сообщение 4096 символами. Разбивать текст надо по границам тегов: строка,
разрезанная посреди ``<code>``, приедет как ошибка парсинга, а не как текст.
"""

from __future__ import annotations

import logging
import re

from bot.settings import TG_HARD_LIMIT

logger = logging.getLogger(__name__)

_FENCE_RE = re.compile(r"```([A-Za-z0-9_+\-]*)\n?(.*?)```", re.DOTALL)
_INLINE_CODE_RE = re.compile(r"`([^`\n]+)`")
_BOLD_RE = re.compile(r"\*\*(.+?)\*\*", re.DOTALL)
_ITALIC_RE = re.compile(r"(?<![\*A-Za-z0-9])\*(?!\s)(.+?)(?<!\s)\*(?![\*A-Za-z0-9])", re.DOTALL)

def _html_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


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
