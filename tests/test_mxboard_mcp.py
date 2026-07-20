from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from engines import mxboard_mcp
from engines.codex_engine import _mcp_config_overrides as codex_mcp_overrides
from engines.opencode_engine import _opencode_mcp_config
from engines.persistent_codex import _split_codex_global_flags


CONFIG = {
    "mcp_url": "https://example.test/mcp.php",
    "manager": {
        "username": "ai-manager",
        "token": "manager-token",
    },
    "executors": [
        {
            "username": "ai-agent",
            "token": "agent-token",
        }
    ],
}


class MxBoardMcpTest(unittest.TestCase):
    def _config_file(self, tmp: str, data: dict | None = None) -> Path:
        path = Path(tmp) / "mxboard-agents.json"
        path.write_text(json.dumps(data or CONFIG), encoding="utf-8")
        return path

    def test_manager_and_agent_roles_use_different_tokens(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._config_file(tmp)

            manager = mxboard_mcp.mxboard_role_spec("manager", path)
            agent = mxboard_mcp.mxboard_role_spec("agent", path)

        self.assertEqual(manager["username"], "ai-manager")
        self.assertEqual(manager["headers"]["Authorization"], "Bearer manager-token")
        self.assertEqual(agent["username"], "ai-agent")
        self.assertEqual(agent["headers"]["Authorization"], "Bearer agent-token")

    def test_agent_falls_back_to_first_executor_for_renamed_users(self) -> None:
        data = {
            "mcp_url": "https://example.test/mcp.php",
            "manager": {"username": "codex", "token": "manager-token"},
            "executors": [
                {"username": "claude-agent", "token": "old-agent-token"},
                {"username": "codex-agent", "token": "wrong-token"},
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            spec = mxboard_mcp.mxboard_role_spec("agent", self._config_file(tmp, data))

        self.assertEqual(spec["username"], "ai-agent")
        self.assertEqual(spec["headers"]["Authorization"], "Bearer old-agent-token")

    def test_codex_overrides_use_profile_without_token_in_argv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._config_file(tmp)
            codex_home = Path(tmp) / "codex-home"
            with (
                patch.object(mxboard_mcp, "DEFAULT_CONFIG", path),
                patch.dict("os.environ", {"CODEX_HOME": str(codex_home)}),
            ):
                flags, cleanup_paths = codex_mcp_overrides(False, "manager")
                profile_text = cleanup_paths[0].read_text(encoding="utf-8")

            for cleanup_path in cleanup_paths:
                cleanup_path.unlink(missing_ok=True)

        self.assertEqual(flags[0], "--profile-v2")
        self.assertNotIn("manager-token", "\n".join(flags))
        self.assertIn("[mcp_servers.mxboard]", profile_text)
        self.assertIn("manager-token", profile_text)
        self.assertNotIn("agent-token", profile_text)

    def test_opencode_temp_config_uses_remote_mxboard(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._config_file(tmp)
            base = Path(tmp) / "opencode.json"
            base.write_text(json.dumps({"mcp": {"jarvis": {"type": "local"}}}), encoding="utf-8")
            with (
                patch.object(mxboard_mcp, "DEFAULT_CONFIG", path),
                patch("engines.playwright_mcp.OPENCODE_CONFIG", base),
            ):
                temp_path = _opencode_mcp_config(False, "agent")

            try:
                data = json.loads(Path(temp_path).read_text(encoding="utf-8"))
            finally:
                Path(temp_path).unlink(missing_ok=True)

        self.assertEqual(data["mcp"]["jarvis"]["type"], "local")
        self.assertEqual(data["mcp"]["mxboard"]["type"], "remote")
        self.assertEqual(data["mcp"]["mxboard"]["url"], "https://example.test/mcp.php")
        self.assertEqual(
            data["mcp"]["mxboard"]["headers"]["Authorization"],
            "Bearer agent-token",
        )

    def test_persistent_codex_moves_profile_before_subcommand(self) -> None:
        global_flags, command_flags = _split_codex_global_flags([
            "-c",
            "mcp_servers.playwright.enabled=true",
            "--profile-v2",
            "jarvis-mxboard-test",
        ])

        self.assertEqual(global_flags, ["--profile-v2", "jarvis-mxboard-test"])
        self.assertEqual(command_flags, ["-c", "mcp_servers.playwright.enabled=true"])


if __name__ == "__main__":
    unittest.main()
