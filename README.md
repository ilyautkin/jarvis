# Jarvis

Тонкая обёртка Telegram-бота над LLM CLI (`claude`, `codex` или `opencode`). Один топик = одна непрерывная сессия.
Пишешь в Telegram — получаешь ответ, как если бы запускал CLI в терминале.

## Что умеет

- Передаёт любые текстовые запросы в выбранный движок (`claude`, OpenAI `codex` или `opencode`).
- Запоминает session-id на каждый топик — контекст диалога сохраняется.
- Движок и модель per-topic: `JARVIS_ENGINE=claude|codex|opencode` задаёт
  дефолтный движок для новых топиков; в любом топике можно переключиться
  командой `/engine <name> [model-substring]`.
- `/engine` — показать текущий движок и список доступных; `/engine <name>` —
  переключить движок, `/engine codex gpt-5.4-mini` — переключить движок и модель
  (новый session_id под новый движок, cwd сохраняется, контекст прежнего диалога
  не переносится).
- `/new` или `/reset` — начать новую сессию (движок сохраняется).
- `/session` — показать текущий session-id, cwd, движок и состояние браузера.
- `/browser [on|off]` — включить/выключить браузер (Playwright MCP) для топика.
  По умолчанию **выключен** (on-demand): браузерные tools грузятся в контекст
  только там, где реально нужны — иначе ~30 `browser_*` тулов висят в каждом
  запросе и зря жгут токены.
- Длинные ответы (> 3500 символов) присылаются как `.md`-файл с коротким превью.
- Reply-to на сообщение бота → в запрос подмешивается скрытый контекст о том, на что ты отвечаешь.
- Фото/документы скачиваются локально, путь прокидывается в prompt (`[Прикреплён файл: ...]`).
- Playwright MCP — **on-demand** для любого движка (`claude`/`codex`/`opencode`):
  включается per-topic командой `/browser on` и инъектируется в CLI на каждый
  запрос, без постоянной глобальной регистрации.
- Голосовые не поддерживаются.
- Whitelist по `user_id` (см. `ALLOWED_USER_IDS`).

## Установка

```bash
cd ~/projects/jarvis
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
cp .env.example .env
# отредактировать .env: TELEGRAM_TOKEN + ALLOWED_USER_IDS
```

Убедись, что `claude` доступен в PATH и авторизован:

```bash
claude --version
claude -p "hello"   # проверка, что авторизация работает
```

### Whitelist

`ALLOWED_USER_IDS` в `.env` — запятая-разделённый список Telegram user-id, которым разрешено
писать боту. Узнать свой id можно через `@userinfobot`.

### Переменные окружения (опционально)

- `JARVIS_ENGINE` — `claude` (дефолт), `codex` или `opencode`. Задаёт **дефолтный
  движок для новых топиков**. Существующие топики хранят свой движок в БД и
  не пересоздаются при смене env — для переключения активного топика используй
  команду `/engine <name>` прямо в Telegram.
- `CLAUDE_BIN` — путь к бинарю claude (по умолчанию `claude`).
- `CODEX_BIN` — путь к бинарю codex (по умолчанию `codex`).
- `OPENCODE_BIN` — путь к бинарю opencode (по умолчанию `opencode`).
- `CLAUDE_CWD` — дефолтный рабочий каталог (общий для всех движков).
- `CLAUDE_TIMEOUT` — таймаут claude, секунд (по умолчанию `3600`).
- `CODEX_TIMEOUT` — таймаут codex, секунд (по умолчанию `3600`).
- `OPENCODE_TIMEOUT` — таймаут opencode, секунд (по умолчанию `3600`).
- `CODEX_MODEL` — дефолтная модель для Codex CLI, если в топике модель не
  выбрана явно через `/engine`.
- `CODEX_MODELS` — запятая-разделённый список моделей для UI `/engine`.
  Если не задан, Jarvis читает `~/.codex/models_cache.json`; если cache
  недоступен, использует fallback:
  `gpt-5.5,gpt-5.4,gpt-5.4-mini,gpt-5.3-codex,gpt-5.2`.
- `OPENCODE_MODEL`, `OPENCODE_AGENT`, `OPENCODE_VARIANT` — опциональные параметры
  для `opencode run`; если не заданы, используются настройки самого opencode.
- `PLAYWRIGHT_MCP_NPX` — абсолютный путь к `npx` для Playwright MCP. Если не
  задан, runtime-хелпер ищет `npx` в `PATH` и `~/.nvm/versions/node/*/bin/npx`.
- `PLAYWRIGHT_MCP_PACKAGE` — npm-пакет MCP-сервера (по умолчанию
  `@playwright/mcp@latest`).
- `PLAYWRIGHT_MCP_MODE` — `cdp` (по умолчанию) или `launch`.
- `PLAYWRIGHT_MCP_CDP_ENDPOINT` — endpoint для CDP. По умолчанию `chrome`,
  но можно указать `http://127.0.0.1:9222` или другой доступный endpoint.
- `PLAYWRIGHT_MCP_ARGS` — дополнительные аргументы Playwright MCP. Для CDP
  режима обычно используют capabilities, например `--caps=vision,pdf,devtools`.
- `JARVIS_PLAYWRIGHT_MCP` — `1`/`0`, глобальный рубильник браузера. `1`
  (по умолчанию) — `/browser on` разрешён, Playwright инъектируется per-topic.
  `0` — браузер недоступен вообще (даже если флаг топика включён).
- `JARVIS_MANAGER_MCP` — `1`/`0`, аналогично для Jarvis Manager MCP (см. ниже).
- `JARVIS_MCP_NAME` — имя MCP-сервера в конфигах движков (по умолчанию `jarvis`).
- `JARVIS_MCP_PYTHON` — путь к Python для запуска MCP-сервера (по умолчанию
  `venv/bin/python` репозитория jarvis).
- `JARVIS_MCP_SCRIPT` / `JARVIS_MCP_DB` — переопределить пути к серверу/БД.
- `JARVIS_LOG_TTL_DAYS` — сколько дней хранить записи `messages_log` и
  завершённые (`done`/`failed`/`cancelled`) `jobs`. Дефолт `30`. `0`,
  `none`, `off`, `false`, `no` — отключают авто-cleanup. `pending` jobs
  (включая scheduled с `not_before` в будущем) **никогда** не удаляются.
- `JARVIS_JOBS_CONCURRENCY` — сколько делегированных Менеджером задач
  выполняется параллельно. Дефолт `5`, минимум `1`. Задачи разных топиков идут
  одновременно; внутри одного топика — строго по очереди (per-topic лок). При
  `1` поведение как раньше (одна задача за раз).
- `JARVIS_HEARTBEAT_INTERVAL` — частота сканирования in_progress job'ов
  (секунды). Дефолт `300` (5 мин), минимум `30`.
- `JARVIS_HEARTBEAT_WARN` — после скольких секунд работы job'а слать в
  топик Менеджера нотис «работает долго». Дефолт `900` (15 мин).
- `JARVIS_HEARTBEAT_FAIL` — после скольких секунд принудительно помечать
  job как failed. Subprocess сам по себе не убивается — для реального
  прерывания агент Менеджер использует `manager_interrupt`. Дефолт
  `3600` (60 мин).
- `JARVIS_REMINDERS_INTERVAL` — частота сканирования `reminders` (секунды).
  Дефолт `60`.
- `JARVIS_REMINDERS_TZ` — таймзона для парсинга времён в schedule
  (`daily HH:MM` и т.п.). Дефолт `Europe/Moscow`.

### Playwright MCP — on-demand для всех движков

Jarvis сам не является MCP-клиентом: браузерные tools поднимают внешние CLI.
~30 `browser_*` тулов — это заметный объём контекста в каждом запросе, поэтому
Playwright **не** регистрируется глобально, а инъектируется **per-invocation**
только для топиков, где включён флаг `mcp_playwright` (команда `/browser on`
или MCP-tool `manager_set_browser`). Manager MCP, напротив, остаётся глобальным
(он лёгкий и нужен оркестрации) — см. ниже.

Единый источник спеки сервера — `playwright_command_args()` в
`engines/playwright_mcp.py` (резолвит `npx` + аргументы). Каждый адаптер
переводит её в свой диалект CLI на каждый вызов:

- **claude**: флаг `--mcp-config '<inline-json>'` (аддитивно к глобальному
  Manager MCP, без `--strict-mcp-config`).
- **codex**: оверрайды `-c mcp_servers.playwright.command=… -c …args=[…] -c
  …enabled=true` поверх `~/.codex/config.toml`.
- **opencode**: у `opencode run` нет per-invocation MCP-флага, поэтому Jarvis
  клонирует глобальный `opencode.json` (Manager MCP и provider-настройки
  сохраняются), добавляет `mcp.playwright` во временный файл и подсовывает его
  через `OPENCODE_CONFIG=<tempfile>` на конкретный запуск. Temp-файл удаляется
  после ответа.

Команда MCP по умолчанию: абсолютный `npx -y @playwright/mcp@latest --cdp-endpoint=chrome`.
Абсолютный путь важен для systemd: в сервисе Jarvis nvm обычно не попадает в
`PATH`, а `npx` лежит именно там.

Если нужен порт, а не channel name, задай `PLAYWRIGHT_MCP_CDP_ENDPOINT=http://127.0.0.1:9222`.

При старте/активации движка Jarvis ещё и **снимает** старую глобальную
регистрацию Playwright (`disable_global_playwright_mcp`), если она осталась от
прежних версий — чтобы on-demand-модель не нарушалась always-on сервером.

> **codex/opencode — проверка на боевой версии CLI.** Парсинг `-c
> mcp_servers.playwright.args=[…]` у codex и поведение `OPENCODE_CONFIG` у
> opencode зависят от версии CLI. Если браузер в этих движках не поднимается:
>
> - **codex**: пропиши Playwright руками в `~/.codex/config.toml`
>   (`[mcp_servers.playwright]` с `command`/`args`/`enabled = true`) — тогда
>   браузер будет always-on для codex; либо проверь формат `-c` своей версии
>   (`codex exec -c 'mcp_servers.playwright.enabled=true' …`).
> - **opencode**: пропиши `mcp.playwright` прямо в
>   `~/.config/opencode/opencode.json` (always-on), либо убедись, что твоя
>   версия читает `OPENCODE_CONFIG`.
>
> claude (основной канал) работает через `--mcp-config` без оговорок.

### Как подключить новый движок

Браузер on-demand — часть контракта `Engine.call_stream` (см.
`engines/__init__.py`). Новый адаптер обязан принять и обработать два
параметра:

- `system_prefix: str | None` — постоянный `[SYSTEM:]`-блок. Положи его в
  системный канал своего CLI (как `--append-system-prompt` у claude). Если
  канала нет — префиксуй prompt **только на новой сессии** (на resume он уже
  в транскрипте), как сделано в codex/opencode.
- `mcp_playwright: bool` — если `True`, инъектируй Playwright per-invocation:
  возьми спеку из `playwright_command_args()` и переведи в механизм своего CLI
  (флаг конфига / оверрайд / временный конфиг через env). Если CLI вообще не
  умеет per-invocation MCP — задокументируй ручную глобальную настройку как
  фолбэк (раздел выше).

### Jarvis Manager MCP (для агента-Менеджера)

По той же схеме Jarvis регистрирует свой собственный stdio MCP-сервер,
дающий read-only-доступ к состоянию бота (топики, лог сообщений). Сервер
живёт в `scripts/jarvis_mcp_server.py` и запускается тем же venv-Python'ом.

Доступные tools (Этап 1):

- `manager_topics` — список всех топиков (chat_id, thread_id, title, cwd,
  engine, model, session_id, updated_at, last_message_at). Фильтры:
  `cwd_contains`, `engine`, `limit`.
- `manager_inbox` — лог сообщений одного топика. Параметры: `chat_id`,
  `thread_id`, `since` (ISO UTC), `limit`, `text_limit`, `direction`.
- `manager_set_browser` — включить/выключить браузер (Playwright MCP) для
  топика (`thread_id`, `enabled`). Аналог команды `/browser`. Применяется со
  следующего сообщения/джоба, контекст сессии сохраняется.

(Это не полный список — есть и write-tools: `manager_send`, `manager_set_engine`,
`manager_create_topic` и др. См. декораторы `@mcp.tool` в `scripts/jarvis_mcp_server.py`.)

Точки записи в `messages_log`: входящие пользовательские реплики
(`direction='in'`, `kind='user_text'`) и финальные ответы бота
(`direction='out'`, `kind='bot_reply'` / `'spawn_reply'`). Промежуточные
tool-use'ы не логируются — они шумные.

Проверка:

```bash
claude mcp list
codex mcp list
opencode mcp list
```

### Переход на Codex CLI

1. `npm i -g @openai/codex`, затем `codex login` (ChatGPT) или `export OPENAI_API_KEY=...`.
2. Синхронизировать пользовательские правила и память в `~/.codex/AGENTS.md`:
   ```bash
   ./venv/bin/python scripts/sync_codex_knowledge.py
   ```
   Скрипт идемпотентен, запускается перед стартом бота или вручную.
3. Добавить в `.env`: `JARVIS_ENGINE=codex`. При необходимости зафиксировать
   модель: `CODEX_MODEL=gpt-5.5`.
4. `systemctl --user restart jarvis-bot.service`.

### Переход на opencode

1. Убедиться, что `opencode` установлен и авторизован:
   ```bash
   opencode --version
   opencode auth list
   ```
2. При необходимости задать модель/агента через opencode config или env:
   `OPENCODE_MODEL=provider/model`, `OPENCODE_AGENT=build`.
3. Добавить в `.env`: `JARVIS_ENGINE=opencode`. Если `opencode` установлен через nvm
   и не виден systemd-сервису, также задать `OPENCODE_BIN=/полный/путь/к/opencode`.
4. `systemctl --user restart jarvis-bot.service`.

## Запуск вручную

```bash
./venv/bin/python telegram_bot.py
```

## Автозапуск через systemd (user unit)

```bash
mkdir -p ~/.config/systemd/user
cp systemd/jarvis-bot.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now jarvis-bot.service

# чтобы бот жил без активной сессии:
sudo loginctl enable-linger "$USER"
```

Полезные команды:

```bash
systemctl --user status jarvis-bot
systemctl --user restart jarvis-bot
systemctl --user stop jarvis-bot
journalctl --user -u jarvis-bot -f
```

## Файлы

- `telegram_bot.py` — весь бот.
- `config.py` — чтение `.env`, токен и whitelist.
- `requirements.txt` — зависимости (`python-telegram-bot`, `python-dotenv`).
- `engines/playwright_mcp.py` — runtime-настройка Playwright MCP для
  активного движка.
- `bot_state.db` — sqlite: session-id на чат + метаданные исходящих сообщений (для reply-to).
- `temp/media/` — скачанные пользовательские вложения.
- `systemd/jarvis-bot.service` — user-unit.

## Известные ограничения

- Session-id у `claude` генерируется ботом и передаётся через `--session-id`; если удалить каталог
  `~/.claude/projects/...` или история будет повреждена, сессия «забудет» контекст.
- У `codex` и `opencode` настоящий id создаёт сам CLI; до первого ответа в БД лежит
  временный placeholder, затем бот заменяет его на реальный id.
- `claude` запускается с `--permission-mode bypassPermissions`, чтобы не зависать
  на подтверждениях tool-use. Это значит, что агент может делать в `CLAUDE_CWD`
  всё, что умеет. Ограничивай каталог по необходимости.
- Голосовые не распознаются — нужно печатать или диктовать с клавиатуры телефона.
- Telegram-лимит на документ — 50 МБ; на текст — 4096 символов.
