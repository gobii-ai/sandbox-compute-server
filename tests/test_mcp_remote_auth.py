import asyncio
import time
import unittest
from urllib.parse import parse_qs, urlparse
from unittest.mock import patch

from sandbox_compute_server import mcp_remote_auth


def _server_payload() -> dict:
    return {
        "config_id": "config-1",
        "name": "remote",
        "command": "npx",
        "command_args": ["mcp-remote", "https://remote.example.com/sse"],
        "env": {},
        "is_remote_mcp_remote": True,
        "remote_auth_callback_base_url": "https://app.example.com/console/mcp/oauth/callback/",
    }


class MCPRemoteAuthTests(unittest.TestCase):
    def setUp(self) -> None:
        with mcp_remote_auth._MANAGER._lock:
            mcp_remote_auth._MANAGER._sessions.clear()

    def tearDown(self) -> None:
        with mcp_remote_auth._MANAGER._lock:
            mcp_remote_auth._MANAGER._sessions.clear()

    def test_poll_contract(self):
        status, payload = mcp_remote_auth.handle_internal_poll({"session_id": "missing"})
        self.assertEqual(status, 410)
        self.assertEqual(payload["status"], "expired")

        session_id = "session-poll"

        async def _wait_for_code(_runtime):
            for _ in range(500):
                session = mcp_remote_auth._MANAGER.get(session_id)
                if session and session.auth_code:
                    return []
                await asyncio.sleep(0.01)
            raise RuntimeError("no auth code")

        with patch("sandbox_compute_server.mcp_remote_auth._discover_tools", new=_wait_for_code):
            started = mcp_remote_auth.handle_remote_auth_start(
                {
                    "session_id": session_id,
                    "server": _server_payload(),
                    "redirect_url": "https://app.example.com/console/mcp/oauth/callback/?remote_auth_session_id=session-poll",
                    "source": "setup",
                }
            )
            self.assertEqual(started["session_id"], session_id)

            notify = mcp_remote_auth.handle_internal_notify(
                {
                    "session_id": session_id,
                    "authorization_url": "https://idp.example.com/auth",
                    "state": "state-1",
                }
            )
            self.assertEqual(notify["status"], "pending_auth")

            status, payload = mcp_remote_auth.handle_internal_poll({"session_id": session_id})
            self.assertEqual(status, 202)
            self.assertIn(payload["status"], {"pending_auth", "code_submitted"})

            submitted = mcp_remote_auth.handle_remote_auth_authorize(
                {
                    "session_id": session_id,
                    "authorization_code": "code-1",
                    "state": "state-1",
                }
            )
            self.assertEqual(submitted["status"], "code_submitted")

            status, payload = mcp_remote_auth.handle_internal_poll({"session_id": session_id})
            self.assertEqual(status, 200)
            self.assertEqual(payload["code"], "code-1")

    def test_start_authorize_completion_lifecycle(self):
        session_id = "session-complete"

        async def _wait_for_code(_runtime):
            for _ in range(500):
                session = mcp_remote_auth._MANAGER.get(session_id)
                if session and session.auth_code:
                    return [{"name": "tool-a"}]
                await asyncio.sleep(0.01)
            raise RuntimeError("no auth code")

        with patch("sandbox_compute_server.mcp_remote_auth._discover_tools", new=_wait_for_code):
            started = mcp_remote_auth.handle_remote_auth_start(
                {
                    "session_id": session_id,
                    "server": _server_payload(),
                    "redirect_url": "https://app.example.com/console/mcp/oauth/callback/?remote_auth_session_id=session-complete",
                    "source": "setup",
                }
            )
            self.assertIn(started["status"], {"started", "pending_auth"})

            mcp_remote_auth.handle_internal_notify(
                {
                    "session_id": session_id,
                    "authorization_url": "https://idp.example.com/auth2",
                    "state": "state-2",
                }
            )
            mcp_remote_auth.handle_remote_auth_authorize(
                {
                    "session_id": session_id,
                    "authorization_code": "code-2",
                    "state": "state-2",
                }
            )

            for _ in range(100):
                status_payload = mcp_remote_auth.handle_remote_auth_status({"session_id": session_id})
                if status_payload.get("status") == "authorized":
                    break
                time.sleep(0.02)
            else:
                self.fail("remote auth session did not reach authorized state")

            self.assertEqual(status_payload["tool_count"], 1)

    def test_session_expiration(self):
        async def _pending_discovery(_runtime):
            await asyncio.sleep(0.2)
            return []

        with patch("sandbox_compute_server.mcp_remote_auth._discover_tools", new=_pending_discovery):
            mcp_remote_auth.handle_remote_auth_start(
                {
                    "session_id": "session-expired",
                    "server": _server_payload(),
                    "redirect_url": "https://app.example.com/console/mcp/oauth/callback/?remote_auth_session_id=session-expired",
                    "source": "setup",
                }
            )
            session = mcp_remote_auth._MANAGER.get("session-expired")
            self.assertIsNotNone(session)
            session.expires_at = mcp_remote_auth._now() - 1

            status, payload = mcp_remote_auth.handle_internal_poll({"session_id": "session-expired"})
            self.assertEqual(status, 410)
            self.assertEqual(payload["status"], "expired")

    def test_runtime_redirect_url_is_stable_and_uses_state_for_session(self):
        runtime = _server_payload()
        runtime["remote_auth_callback_base_url"] = "https://app.example.com/console/mcp/oauth/callback/"

        with patch("sandbox_compute_server.mcp_remote_auth.handle_remote_auth_start") as mock_start, patch.object(
            mcp_remote_auth._MANAGER,
            "wait_runtime_probe",
        ) as mock_wait:
            mock_start.return_value = {"status": "pending_auth"}
            mock_wait.return_value = {
                "status": "pending_auth",
                "authorization_url": "https://idp.example.com/auth",
                "config_id": runtime["config_id"],
            }

            result = mcp_remote_auth.ensure_runtime_remote_auth(runtime)

        self.assertEqual(result["status"], "action_required")
        self.assertEqual(result["connect_url"], "https://idp.example.com/auth")
        start_payload = mock_start.call_args[0][0]
        redirect_url = start_payload["redirect_url"]
        parsed = urlparse(redirect_url)
        query = parse_qs(parsed.query)
        self.assertEqual(query, {})


if __name__ == "__main__":
    unittest.main()
