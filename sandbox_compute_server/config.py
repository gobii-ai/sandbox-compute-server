import os
from pathlib import Path
from typing import Dict, Optional

_DEFAULT_ALLOWED_ENV_KEYS = {
    "PATH",
    "HOME",
    "USER",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "TMPDIR",
    "TERM",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "PYTHONUNBUFFERED",
    "PYTHONIOENCODING",
}

_PROXY_ENV_KEYS = {"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY"}
_TRACEPARENT_HEADER = "HTTP_TRACEPARENT"
_TRACE_ID_HEX_LEN = 32
_TRACEPARENT_PARTS = 4


def _allowed_env_keys() -> set[str]:
    raw = os.environ.get("SANDBOX_COMPUTE_ALLOWED_ENV_KEYS", "")
    if not raw.strip():
        return set(_DEFAULT_ALLOWED_ENV_KEYS)
    parts = [part.strip() for part in raw.replace("\n", ",").split(",") if part.strip()]
    return set(parts) or set(_DEFAULT_ALLOWED_ENV_KEYS)


def _sandbox_env(agent_root: Optional[Path] = None, extra_env: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    from sandbox_compute_server.manifest import _proxy_env_from_manifest

    allowed = _allowed_env_keys()
    env = {key: value for key, value in os.environ.items() if key in allowed}
    env.setdefault("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
    if extra_env:
        for key, value in extra_env.items():
            if key in allowed or key.startswith("SANDBOX_"):
                env[key] = str(value)
    if agent_root:
        env.update(_proxy_env_from_manifest(agent_root))
    return env


def _workspace_root() -> Path:
    root = os.environ.get("SANDBOX_WORKSPACE_ROOT", "/workspace").strip() or "/workspace"
    return Path(root)


def _workspace_max_bytes() -> int:
    raw = os.environ.get("SANDBOX_WORKSPACE_MAX_BYTES") or os.environ.get("SANDBOX_COMPUTE_WORKSPACE_LIMIT_BYTES")
    if raw:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            pass
    return 1024 * 1024 * 1024


def _stdio_max_bytes() -> int:
    raw = os.environ.get("SANDBOX_COMPUTE_STDIO_MAX_BYTES")
    if raw:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            pass
    return 1024 * 1024


def _run_command_timeout_seconds() -> int:
    raw = os.environ.get("SANDBOX_COMPUTE_RUN_COMMAND_TIMEOUT_SECONDS")
    if raw:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            pass
    return 120


def _python_default_timeout_seconds() -> int:
    raw = os.environ.get("SANDBOX_COMPUTE_PYTHON_DEFAULT_TIMEOUT_SECONDS")
    if raw:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            pass
    return 30


def _python_max_timeout_seconds() -> int:
    raw = os.environ.get("SANDBOX_COMPUTE_PYTHON_MAX_TIMEOUT_SECONDS")
    if raw:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            pass
    return 120


def _agent_workspace(agent_id: str) -> Path:
    root = _workspace_root()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _mcp_timeout_seconds() -> int:
    raw = os.environ.get("SANDBOX_COMPUTE_MCP_TIMEOUT_SECONDS")
    if raw:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            pass
    return 120
