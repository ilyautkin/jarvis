"""Per-topic mxBoard MCP wiring for Jarvis engines.

mxBoard identity is role-based, not engine-based: the Manager topic connects as
the board manager user, every execution topic connects as the board agent user.
Tokens live in the gitignored ``apps/mxboard/config/mxboard-agents.json`` file.
"""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)

HOME = Path.home()
SERVER_NAME = os.environ.get("JARVIS_MXBOARD_MCP_NAME", "mxboard")
DEFAULT_CONFIG = Path(
    os.environ.get(
        "JARVIS_MXBOARD_AGENTS_CONFIG",
        "/home/shevartv/projects/apps/mxboard/config/mxboard-agents.json",
    )
).expanduser()

ROLE_MANAGER = "manager"
ROLE_AGENT = "agent"
ROLES = {ROLE_MANAGER, ROLE_AGENT}


def mxboard_enabled() -> bool:
    raw = os.environ.get("JARVIS_MXBOARD_MCP", "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def mxboard_server_name() -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", SERVER_NAME):
        raise RuntimeError(
            "JARVIS_MXBOARD_MCP_NAME may contain only letters, digits, '_' and '-'"
        )
    return SERVER_NAME


def _load_config(path: Path | None = None) -> dict[str, Any]:
    cfg_path = path or DEFAULT_CONFIG
    if not cfg_path.is_file():
        raise RuntimeError(f"mxBoard agents config not found: {cfg_path}")
    try:
        data = json.loads(cfg_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Cannot parse mxBoard agents config {cfg_path}: {exc}") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"{cfg_path} must contain a JSON object")
    return data


def _token_from_entry(entry: Any, label: str) -> str:
    if not isinstance(entry, dict):
        raise RuntimeError(f"mxBoard {label} entry is missing")
    token = str(entry.get("token") or "").strip()
    if not token:
        raise RuntimeError(f"mxBoard {label} token is empty")
    return token


def _agent_entry(data: dict[str, Any]) -> dict[str, Any]:
    direct = data.get("agent")
    if isinstance(direct, dict) and direct.get("token"):
        return direct

    executors = data.get("executors")
    if not isinstance(executors, list) or not executors:
        raise RuntimeError("mxBoard agent token is missing: no agent/executors entry")

    wanted = os.environ.get("JARVIS_MXBOARD_AGENT_USERNAME", "ai-agent").strip()
    if wanted:
        for item in executors:
            if isinstance(item, dict) and str(item.get("username") or "") == wanted:
                return item

    # Transitional fallback: after renaming a MODX user, old username metadata in
    # the local gitignored file can be stale, while the token still points to the
    # same user_id. The first executor has historically been the single agent.
    for item in executors:
        if isinstance(item, dict) and item.get("token"):
            return item
    raise RuntimeError("mxBoard agent token is missing: executors have no token")


def mxboard_role_spec(role: str, path: Path | None = None) -> dict[str, Any] | None:
    """Return ``{name, url, headers, username}`` for the requested board role.

    ``None`` means mxBoard MCP is globally disabled for Jarvis.
    """
    if not mxboard_enabled():
        return None
    role = role.strip().lower()
    if role not in ROLES:
        raise RuntimeError(f"mxBoard role must be one of {sorted(ROLES)}, got {role!r}")

    data = _load_config(path)
    url = str(data.get("mcp_url") or "").strip()
    if not url:
        raise RuntimeError("mxBoard mcp_url is empty")

    if role == ROLE_MANAGER:
        entry = data.get("manager")
        token = _token_from_entry(entry, "manager")
    else:
        entry = _agent_entry(data)
        token = _token_from_entry(entry, "agent")

    username = (
        os.environ.get("JARVIS_MXBOARD_MANAGER_USERNAME", "ai-manager").strip()
        if role == ROLE_MANAGER
        else os.environ.get("JARVIS_MXBOARD_AGENT_USERNAME", "ai-agent").strip()
    )
    return {
        "name": mxboard_server_name(),
        "role": role,
        "url": url,
        "headers": {"Authorization": "Bearer " + token},
        "username": username,
    }


def log_role(role: str) -> None:
    try:
        spec = mxboard_role_spec(role)
    except Exception as exc:
        logger.warning("mxBoard MCP setup failed for role=%s: %s", role, exc)
        return
    if spec is None:
        logger.info("mxBoard MCP disabled")
        return
    logger.info(
        "mxBoard MCP selected role=%s user=%s server=%s",
        spec["role"],
        spec["username"],
        spec["name"],
    )


def create_codex_profile(role: str) -> tuple[str, Path] | None:
    """Create a temporary Codex profile-v2 config for mxBoard MCP.

    Codex ``-c`` overrides would put the Bearer token into process argv. A named
    profile keeps argv clean: Jarvis passes only ``--profile-v2 <name>`` and
    removes the generated config after the Codex process has loaded it.
    """
    spec = mxboard_role_spec(role)
    if spec is None:
        return None

    codex_home = Path(os.environ.get("CODEX_HOME", HOME / ".codex")).expanduser()
    codex_home.mkdir(mode=0o700, parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=codex_home,
        prefix="jarvis-mxboard-",
        suffix=".config.toml",
        delete=False,
    ) as fh:
        path = Path(fh.name)
        try:
            path.chmod(0o600)
        except OSError:
            logger.debug("cannot chmod Codex mxBoard profile %s", path, exc_info=True)

        headers = spec["headers"]
        header_items = ", ".join(
            f"{key} = {json.dumps(str(value), ensure_ascii=False)}"
            for key, value in headers.items()
        )
        fh.write(f"[mcp_servers.{spec['name']}]\n")
        fh.write(f"url = {json.dumps(spec['url'], ensure_ascii=False)}\n")
        fh.write("enabled = true\n")
        fh.write(f"http_headers = {{ {header_items} }}\n")

    profile_name = path.name.removesuffix(".config.toml")
    logger.info(
        "mxBoard Codex profile created role=%s user=%s server=%s profile=%s",
        spec["role"],
        spec["username"],
        spec["name"],
        profile_name,
    )
    return profile_name, path


def codex_inline_config_flags(role: str) -> list[str]:
    """Return Codex ``-c`` flags for commands that cannot use profile-v2."""
    spec = mxboard_role_spec(role)
    if spec is None:
        return []

    table = f"mcp_servers.{spec['name']}"
    header_items = ", ".join(
        f"{key} = {json.dumps(str(value), ensure_ascii=False)}"
        for key, value in spec["headers"].items()
    )
    return [
        "-c", f"{table}.url={json.dumps(spec['url'], ensure_ascii=False)}",
        "-c", f"{table}.enabled=true",
        "-c", f"{table}.http_headers={{ {header_items} }}",
    ]


def cleanup_codex_profile(path: Path | None) -> None:
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
    except OSError:
        logger.warning("cannot remove Codex mxBoard profile %s", path, exc_info=True)
