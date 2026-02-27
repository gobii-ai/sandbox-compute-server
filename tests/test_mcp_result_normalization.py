import json
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from sandbox_compute_server.mcp import _handle_mcp_request, _normalize_mcp_result, _parse_mcp_server_payload


class _TextContent:
    def __init__(self, text: str):
        self.text = text


class _MCPCallResult:
    def model_dump(self, mode: str | None = None):
        return {
            "content": [_TextContent("hello")],
            "meta": {"count": 1},
            "mode": mode,
        }


class MCPResultNormalizationTests(unittest.TestCase):
    def test_parse_payload_injects_writable_stdio_cache_env(self):
        runtime, error = _parse_mcp_server_payload(
            {
                "server": {
                    "config_id": "cfg-1",
                    "name": "postgres",
                    "command": "npx",
                    "args": ["-y", "@modelcontextprotocol/server-postgres"],
                    "env": {},
                }
            }
        )
        self.assertIsNone(error)
        assert runtime is not None
        self.assertEqual(runtime["env"].get("NPM_CONFIG_CACHE"), "/workspace/.npm-cache")
        self.assertEqual(runtime["env"].get("npm_config_cache"), "/workspace/.npm-cache")
        self.assertEqual(runtime["env"].get("XDG_CACHE_HOME"), "/workspace/.cache")
        self.assertEqual(runtime["env"].get("HOME"), "/workspace")

    def test_normalize_result_handles_nested_non_json_objects(self):
        normalized = _normalize_mcp_result(_MCPCallResult())
        self.assertEqual(
            normalized,
            {
                "content": [{"text": "hello"}],
                "meta": {"count": 1},
                "mode": "json",
            },
        )
        json.dumps(normalized)

    def test_handle_mcp_request_returns_json_safe_payload(self):
        payload = {
            "agent_id": "agent-1",
            "tool_name": "pg_manage_query",
            "params": {"sql": "select 1"},
            "server": {
                "config_id": "cfg-1",
                "name": "postgres",
                "command": "npx",
                "args": ["-y", "@modelcontextprotocol/server-postgres"],
            },
        }
        runtime = {
            "config_id": "cfg-1",
            "name": "postgres",
            "command": "npx",
            "args": ["-y", "@modelcontextprotocol/server-postgres"],
            "url": "",
            "env": {},
            "headers": {},
        }
        with patch("sandbox_compute_server.mcp._require_agent_id", return_value=("agent-1", None)), patch(
            "sandbox_compute_server.mcp._agent_workspace",
            return_value=Path("/tmp/workspace"),
        ), patch("sandbox_compute_server.mcp._store_proxy_env"), patch(
            "sandbox_compute_server.mcp._proxy_env_from_manifest",
            return_value=None,
        ), patch(
            "sandbox_compute_server.mcp._parse_mcp_server_payload",
            return_value=(runtime, None),
        ), patch(
            "sandbox_compute_server.mcp._call_mcp_tool",
            new=AsyncMock(return_value=_MCPCallResult()),
        ):
            response = _handle_mcp_request(payload)

        self.assertEqual(response.get("status"), "ok")
        json.dumps(response)


if __name__ == "__main__":
    unittest.main()
