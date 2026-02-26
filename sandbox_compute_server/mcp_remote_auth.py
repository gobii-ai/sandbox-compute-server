import asyncio
import base64
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


logger = logging.getLogger(__name__)

_DEFAULT_AUTH_TIMEOUT_SECONDS = 600
_DEFAULT_SESSION_TTL_SECONDS = 900
_DEFAULT_INTERNAL_PORT = 8080
_DEFAULT_CONFIG_DIR = "/workspace/.mcp-auth"
_START_WAIT_SECONDS = 1.5
_RUNTIME_WAIT_SECONDS = 2.0
_FINISHED_RETENTION_SECONDS = 600
_REMOTE_AUTH_STATE_VERSION = 1
_REMOTE_AUTH_STATE_MAX_FILES = 64
_REMOTE_AUTH_STATE_MAX_BYTES = 512 * 1024
_REMOTE_AUTH_STATE_ALLOWED_SUFFIXES = (
    "_client_info.json",
    "_tokens.json",
    "_code_verifier.txt",
)


def _now() -> float:
    return time.time()


def _log_lifecycle(event: str, session: "RemoteAuthSession", **extra: Any) -> None:
    logger.info(
        "mcp_remote_auth lifecycle event=%s session_id=%s config_id=%s source=%s status=%s expires_at=%s details=%s",
        event,
        session.session_id,
        session.config_id,
        session.source,
        session.status,
        int(session.expires_at),
        extra or {},
    )


def _strip_flags(args: list[str]) -> list[str]:
    with_value = {
        "--auth-mode",
        "--redirect-url",
        "--auth-bridge-poll-url",
        "--auth-bridge-notify-url",
        "--auth-session-id",
        "--auth-bridge-poll-interval",
        "--auth-timeout",
    }
    booleans = {"--auth-bridge-exit-after-authorize-url"}
    cleaned: list[str] = []
    idx = 0
    while idx < len(args):
        token = args[idx]
        if token in with_value:
            idx += 2
            continue
        if token in booleans:
            idx += 1
            continue
        cleaned.append(token)
        idx += 1
    return cleaned


def _resolve_config_dir(env: Dict[str, str]) -> Path:
    raw = str(env.get("MCP_REMOTE_CONFIG_DIR") or _DEFAULT_CONFIG_DIR).strip() or _DEFAULT_CONFIG_DIR
    return Path(raw)


def _allowed_remote_auth_path(path: str) -> bool:
    filename = Path(path).name
    return any(filename.endswith(suffix) for suffix in _REMOTE_AUTH_STATE_ALLOWED_SUFFIXES)


def _restore_remote_auth_state(runtime: Dict[str, Any]) -> None:
    state = runtime.get("remote_auth_state")
    if not isinstance(state, dict):
        return

    files = state.get("files")
    if not isinstance(files, list):
        return

    env = runtime.get("env")
    if not isinstance(env, dict):
        return

    config_dir = _resolve_config_dir({str(k): str(v) for k, v in env.items() if k is not None and v is not None})
    base_dir = config_dir.resolve()
    base_dir.mkdir(parents=True, exist_ok=True)

    restored = 0
    total_bytes = 0
    for entry in files:
        if restored >= _REMOTE_AUTH_STATE_MAX_FILES:
            break
        if not isinstance(entry, dict):
            continue

        rel_path = str(entry.get("path") or "").strip()
        content_b64 = entry.get("content_b64")
        if not rel_path or not isinstance(content_b64, str):
            continue
        if not _allowed_remote_auth_path(rel_path):
            continue

        rel = Path(rel_path)
        if rel.is_absolute() or ".." in rel.parts:
            continue

        try:
            content = base64.b64decode(content_b64.encode("ascii"), validate=True)
        except (ValueError, TypeError):
            continue

        total_bytes += len(content)
        if total_bytes > _REMOTE_AUTH_STATE_MAX_BYTES:
            logger.warning("Remote auth state too large while restoring; skipping remaining files")
            break

        target = (base_dir / rel).resolve()
        if base_dir != target and base_dir not in target.parents:
            continue

        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            target.write_bytes(content)
        except OSError:
            logger.warning("Failed writing restored remote auth file path=%s", target, exc_info=True)
            continue
        restored += 1

    if restored:
        logger.info("Restored remote auth state files=%s dir=%s", restored, str(base_dir))


def _capture_remote_auth_state(runtime: Dict[str, Any]) -> Dict[str, Any]:
    env = runtime.get("env")
    if not isinstance(env, dict):
        return {}
    config_dir = _resolve_config_dir({str(k): str(v) for k, v in env.items() if k is not None and v is not None})
    if not config_dir.exists():
        return {}

    base_dir = config_dir.resolve()
    files: list[Dict[str, str]] = []
    total_bytes = 0

    try:
        candidates = sorted(path for path in base_dir.rglob("*") if path.is_file())
    except OSError:
        logger.warning("Failed scanning remote auth config dir=%s", str(base_dir), exc_info=True)
        return {}

    for path in candidates:
        if len(files) >= _REMOTE_AUTH_STATE_MAX_FILES:
            break
        rel_path = path.relative_to(base_dir).as_posix()
        if not _allowed_remote_auth_path(rel_path):
            continue
        try:
            content = path.read_bytes()
        except OSError:
            continue
        total_bytes += len(content)
        if total_bytes > _REMOTE_AUTH_STATE_MAX_BYTES:
            logger.warning("Remote auth state too large while capturing dir=%s", str(base_dir))
            break
        files.append(
            {
                "path": rel_path,
                "content_b64": base64.b64encode(content).decode("ascii"),
            }
        )

    if not files:
        return {}

    return {
        "version": _REMOTE_AUTH_STATE_VERSION,
        "files": files,
    }


@dataclass
class RemoteAuthSession:
    session_id: str
    config_id: str
    source: str
    redirect_url: str
    created_at: float
    expires_at: float
    status: str = "started"
    authorization_url: str = ""
    auth_state: str = ""
    error: str = ""
    auth_code: str = ""
    code_consumed: bool = False
    completed_at: float = 0.0
    tool_count: int = 0
    remote_auth_state: Dict[str, Any] = field(default_factory=dict)
    auth_url_event: threading.Event = field(default_factory=threading.Event)
    done_event: threading.Event = field(default_factory=threading.Event)
    lock: threading.Lock = field(default_factory=threading.Lock)
    worker: Optional[threading.Thread] = None

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            payload = {
                "status": self.status,
                "session_id": self.session_id,
                "config_id": self.config_id,
                "source": self.source,
                "authorization_url": self.authorization_url,
                "state": self.auth_state,
                "error": self.error,
                "expires_at": int(self.expires_at),
                "tool_count": self.tool_count,
            }
            if self.remote_auth_state and self.status == "authorized":
                payload["remote_auth_state"] = self.remote_auth_state
            return payload


class MCPRemoteAuthManager:
    def __init__(self) -> None:
        self._sessions: Dict[str, RemoteAuthSession] = {}
        self._lock = threading.Lock()

    def _cleanup_locked(self) -> None:
        now = _now()
        stale_ids: list[str] = []
        for session_id, session in self._sessions.items():
            finished = bool(session.completed_at)
            if finished and (now - session.completed_at > _FINISHED_RETENTION_SECONDS):
                stale_ids.append(session_id)
                continue
            if now > session.expires_at and not session.done_event.is_set():
                with session.lock:
                    if session.status not in {"authorized", "failed", "expired"}:
                        session.status = "expired"
                        session.error = session.error or "Auth session expired."
                        _log_lifecycle("expired", session, reason="ttl_cleanup")
                session.done_event.set()
            if now > session.expires_at + _FINISHED_RETENTION_SECONDS:
                stale_ids.append(session_id)
        for session_id in stale_ids:
            self._sessions.pop(session_id, None)

    def get(self, session_id: str) -> Optional[RemoteAuthSession]:
        with self._lock:
            self._cleanup_locked()
            return self._sessions.get(session_id)

    def start_session(
        self,
        *,
        session_id: str,
        config_id: str,
        source: str,
        redirect_url: str,
        server: Dict[str, Any],
        wait_seconds: float,
    ) -> Dict[str, Any]:
        now = _now()
        with self._lock:
            self._cleanup_locked()
            existing = self._sessions.get(session_id)
            if existing is not None:
                session = existing
            else:
                session = RemoteAuthSession(
                    session_id=session_id,
                    config_id=config_id,
                    source=source,
                    redirect_url=redirect_url,
                    created_at=now,
                    expires_at=now + _DEFAULT_SESSION_TTL_SECONDS,
                )
                worker = threading.Thread(
                    target=self._run_session,
                    args=(session, server),
                    name=f"mcp-remote-auth-{session_id}",
                    daemon=True,
                )
                session.worker = worker
                self._sessions[session_id] = session
                _log_lifecycle("started", session)
                worker.start()

        if wait_seconds > 0:
            session.auth_url_event.wait(wait_seconds)
            if not session.auth_url_event.is_set():
                session.done_event.wait(0.1)
        return session.snapshot()

    def submit_auth_code(self, *, session_id: str, code: str, state: str, error: str = "") -> Dict[str, Any]:
        session = self.get(session_id)
        if session is None:
            return {"status": "error", "message": "Remote auth session not found."}
        with session.lock:
            if _now() > session.expires_at:
                session.status = "expired"
                session.error = "Auth session expired."
                session.done_event.set()
                _log_lifecycle("expired", session, reason="authorize_after_expiry")
                return {"status": "error", "message": "Remote auth session expired."}
            if error:
                session.status = "failed"
                session.error = error
                session.done_event.set()
                _log_lifecycle("failed", session, reason="authorize_error", error=error)
                return {"status": "failed", "session_id": session_id, "error": error}
            session.auth_code = code
            session.code_consumed = False
            if state:
                session.auth_state = state
            if session.status not in {"authorized"}:
                session.status = "code_submitted"
        session.done_event.wait(2.0)
        return session.snapshot()

    def notify_authorization_url(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        session_id = str(payload.get("session_id") or "").strip()
        if not session_id:
            return {"status": "error", "message": "session_id is required"}
        session = self.get(session_id)
        if session is None:
            return {"status": "error", "message": "Remote auth session not found."}

        authorization_url = str(payload.get("authorization_url") or "").strip()
        state = str(payload.get("state") or "").strip()
        with session.lock:
            if authorization_url:
                session.authorization_url = authorization_url
            if state:
                session.auth_state = state
            if session.status in {"started", "code_submitted"}:
                session.status = "pending_auth"
            session.auth_url_event.set()
            _log_lifecycle(
                "auth_url_emitted",
                session,
                has_url=bool(session.authorization_url),
                has_state=bool(session.auth_state),
            )
        return session.snapshot()

    def poll_for_auth_code(self, session_id: str) -> Tuple[int, Dict[str, Any]]:
        session = self.get(session_id)
        if session is None:
            return 410, {"status": "expired", "message": "Remote auth session not found."}

        with session.lock:
            if _now() > session.expires_at:
                session.status = "expired"
                session.error = session.error or "Auth session expired."
                session.done_event.set()
                _log_lifecycle("expired", session, reason="poll_after_expiry")
                return 410, {"status": "expired", "message": "Remote auth session expired."}
            if session.auth_code and not session.code_consumed:
                session.code_consumed = True
                session.status = "code_delivered"
                return 200, {"code": session.auth_code}
            return 202, {"status": session.status}

    def wait_runtime_probe(self, session_id: str) -> Dict[str, Any]:
        session = self.get(session_id)
        if session is None:
            return {"status": "error", "message": "Remote auth session not found."}
        session.auth_url_event.wait(_RUNTIME_WAIT_SECONDS)
        if not session.auth_url_event.is_set():
            session.done_event.wait(0.2)
        return session.snapshot()

    def _run_session(self, session: RemoteAuthSession, server: Dict[str, Any]) -> None:
        try:
            runtime = _build_bridge_runtime(server, session)
            tools = asyncio.run(_discover_tools(runtime))
            remote_auth_state = _capture_remote_auth_state(runtime)
            with session.lock:
                session.status = "authorized"
                session.error = ""
                session.tool_count = len(tools)
                session.remote_auth_state = remote_auth_state
                session.completed_at = _now()
                session.done_event.set()
                _log_lifecycle("authorized", session, tool_count=session.tool_count)
        except Exception as exc:
            message = str(exc)
            logger.exception("MCP remote auth session failed session=%s", session.session_id)
            with session.lock:
                lower = message.lower()
                if "timed out waiting for auth code from bridge" in lower:
                    session.status = "expired"
                elif session.status == "pending_auth" and _now() > session.expires_at:
                    session.status = "expired"
                elif session.authorization_url and session.status in {"started", "pending_auth", "code_submitted"}:
                    session.status = "pending_auth"
                else:
                    session.status = "failed"
                session.error = message
                session.completed_at = _now()
                session.done_event.set()
                if session.status == "expired":
                    _log_lifecycle("expired", session, reason="worker_timeout", error=message)
                else:
                    _log_lifecycle("failed", session, reason="worker_error", error=message)


def _discover_tools(runtime: Dict[str, Any]) -> Any:
    from sandbox_compute_server.mcp import _discover_mcp_tools

    return _discover_mcp_tools(runtime)


def _build_bridge_runtime(server: Dict[str, Any], session: RemoteAuthSession) -> Dict[str, Any]:
    command = str(server.get("command") or "").strip()
    args = [str(item) for item in (server.get("command_args") or server.get("args") or []) if item is not None]
    env = dict(server.get("env") or {})
    callback_url = session.redirect_url
    internal_port = int(server.get("internal_port") or _DEFAULT_INTERNAL_PORT)
    poll_url = f"http://127.0.0.1:{internal_port}/sandbox/internal/mcp_remote_auth/poll?session_id={session.session_id}"
    notify_url = f"http://127.0.0.1:{internal_port}/sandbox/internal/mcp_remote_auth/notify"
    timeout_seconds = int(server.get("auth_timeout_seconds") or _DEFAULT_AUTH_TIMEOUT_SECONDS)

    bridge_args = _strip_flags(args)
    bridge_args.extend(
        [
            "--auth-mode",
            "bridge",
            "--auth-session-id",
            session.session_id,
            "--auth-bridge-poll-url",
            poll_url,
            "--auth-bridge-notify-url",
            notify_url,
            "--redirect-url",
            callback_url,
            "--auth-timeout",
            str(timeout_seconds),
        ]
    )
    env.setdefault("MCP_REMOTE_CONFIG_DIR", _DEFAULT_CONFIG_DIR)
    runtime = {
        "config_id": server.get("config_id") or session.config_id,
        "name": server.get("name") or "mcp-remote",
        "command": command,
        "args": bridge_args,
        "url": "",
        "env": env,
        "headers": {},
    }
    _restore_remote_auth_state(runtime)
    return runtime


_MANAGER = MCPRemoteAuthManager()


def handle_remote_auth_start(payload: Dict[str, Any]) -> Dict[str, Any]:
    server = payload.get("server")
    if not isinstance(server, dict):
        return {"status": "error", "message": "Missing MCP server payload."}
    if not bool(server.get("is_remote_mcp_remote")):
        return {"status": "error", "message": "Server is not configured with mcp-remote."}

    session_id = str(payload.get("session_id") or "").strip() or str(uuid.uuid4())
    config_id = str(server.get("config_id") or payload.get("server_id") or "").strip()
    if not config_id:
        return {"status": "error", "message": "Missing MCP server config id."}
    redirect_url = str(payload.get("redirect_url") or "").strip()
    if not redirect_url:
        return {"status": "error", "message": "Missing redirect URL."}
    source = str(payload.get("source") or "setup").strip() or "setup"

    snapshot = _MANAGER.start_session(
        session_id=session_id,
        config_id=config_id,
        source=source,
        redirect_url=redirect_url,
        server=server,
        wait_seconds=_START_WAIT_SECONDS,
    )
    return snapshot


def handle_remote_auth_status(payload: Dict[str, Any]) -> Dict[str, Any]:
    session_id = str(payload.get("session_id") or "").strip()
    if not session_id:
        return {"status": "error", "message": "session_id is required"}
    session = _MANAGER.get(session_id)
    if session is None:
        return {"status": "error", "message": "Remote auth session not found."}
    return session.snapshot()


def handle_remote_auth_authorize(payload: Dict[str, Any]) -> Dict[str, Any]:
    session_id = str(payload.get("session_id") or "").strip()
    if not session_id:
        return {"status": "error", "message": "session_id is required"}
    code = str(payload.get("authorization_code") or "").strip()
    state = str(payload.get("state") or "").strip()
    error = str(payload.get("error") or "").strip()
    return _MANAGER.submit_auth_code(session_id=session_id, code=code, state=state, error=error)


def handle_internal_poll(query_params: Dict[str, str]) -> Tuple[int, Dict[str, Any]]:
    session_id = str(query_params.get("session_id") or "").strip()
    if not session_id:
        return 400, {"status": "error", "message": "session_id is required"}
    return _MANAGER.poll_for_auth_code(session_id)


def handle_internal_notify(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _MANAGER.notify_authorization_url(payload)


def ensure_runtime_remote_auth(runtime: Dict[str, Any]) -> Dict[str, Any]:
    if not bool(runtime.get("is_remote_mcp_remote")):
        return {"status": "ok", "authorized": True}

    callback_base = str(runtime.get("remote_auth_callback_base_url") or "").strip()
    if not callback_base:
        return {"status": "error", "message": "Remote auth callback base URL is not configured."}

    session_id = str(uuid.uuid4())
    redirect_url = callback_base
    payload = {
        "session_id": session_id,
        "server": runtime,
        "redirect_url": redirect_url,
        "source": "runtime",
    }
    started = handle_remote_auth_start(payload)
    if started.get("status") == "error":
        return started

    snapshot = _MANAGER.wait_runtime_probe(session_id)
    if snapshot.get("status") == "authorized":
        return {"status": "ok", "authorized": True}

    auth_url = str(snapshot.get("authorization_url") or "").strip()
    if auth_url:
        return {
            "status": "action_required",
            "authorized": False,
            "session_id": session_id,
            "connect_url": auth_url,
            "message": "Authorization required before this tool can run.",
            "config_id": snapshot.get("config_id"),
        }

    if snapshot.get("status") == "started":
        return {
            "status": "action_required",
            "authorized": False,
            "session_id": session_id,
            "message": "Authorization is initializing. Retry in a moment.",
            "config_id": snapshot.get("config_id"),
        }

    return {
        "status": "error",
        "message": str(snapshot.get("error") or "Remote auth session failed."),
    }


def hydrate_runtime_remote_auth_state(runtime: Dict[str, Any]) -> None:
    _restore_remote_auth_state(runtime)


def snapshot_runtime_remote_auth_state(runtime: Dict[str, Any]) -> Dict[str, Any]:
    return _capture_remote_auth_state(runtime)
