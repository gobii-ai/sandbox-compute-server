import asyncio
import base64
import contextlib
import hashlib
import json
import logging
import mimetypes
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional, Tuple
from urllib.parse import urlsplit

import requests
from fastmcp import Client
from fastmcp.client.transports import StdioTransport as FastMCPStdioTransport
from fastmcp.client.transports import StreamableHttpTransport
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from logging_config import configure_logging
from sandbox_utils import normalize_timeout as _normalize_timeout

logger = logging.getLogger(__name__)
configure_logging()

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


def _manifest_path(agent_root: Path) -> Path:
    meta_dir = agent_root / ".gobii"
    meta_dir.mkdir(parents=True, exist_ok=True)
    return meta_dir / "manifest.json"


def _load_manifest(agent_root: Path) -> Dict[str, Any]:
    path = _manifest_path(agent_root)
    if not path.exists():
        return {"files": {}, "deleted": {}}
    try:
        data = json.loads(path.read_text())
    except (ValueError, OSError):
        return {"files": {}, "deleted": {}}
    if not isinstance(data, dict):
        return {"files": {}, "deleted": {}}
    data.setdefault("files", {})
    data.setdefault("deleted", {})
    if not isinstance(data["files"], dict):
        data["files"] = {}
    if not isinstance(data["deleted"], dict):
        data["deleted"] = {}
    return data


def _save_manifest(agent_root: Path, manifest: Dict[str, Any]) -> None:
    path = _manifest_path(agent_root)
    path.write_text(json.dumps(manifest, sort_keys=True))


def _proxy_env_from_manifest(agent_root: Path) -> Dict[str, str]:
    manifest = _load_manifest(agent_root)
    env = manifest.get("env", {})
    if not isinstance(env, dict):
        return {}
    filtered: Dict[str, str] = {}
    for key in _PROXY_ENV_KEYS:
        value = env.get(key)
        if isinstance(value, str) and value.strip():
            filtered[key] = value.strip()
    return filtered


def _store_proxy_env(agent_root: Path, payload: Dict[str, Any]) -> bool:
    proxy_env = payload.get("proxy_env")
    if not isinstance(proxy_env, dict):
        return False
    filtered: Dict[str, str] = {}
    for key in _PROXY_ENV_KEYS:
        value = proxy_env.get(key)
        if isinstance(value, str) and value.strip():
            filtered[key] = value.strip()
    if not filtered:
        return False
    manifest = _load_manifest(agent_root)
    current_env = manifest.get("env", {})
    if not isinstance(current_env, dict):
        current_env = {}
    next_env = {**current_env, **filtered}
    if next_env == current_env:
        return False
    changed_keys = sorted(key for key in filtered if current_env.get(key) != filtered[key])
    manifest["env"] = next_env
    _save_manifest(agent_root, manifest)
    logger.info(
        "Sandbox proxy env updated workspace=%s changed_keys=%s",
        str(agent_root),
        ",".join(changed_keys) if changed_keys else "none",
    )
    return True


def _parse_since(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except ValueError:
        return None


def _parse_entry_updated_at(entry: Dict[str, Any]) -> Optional[float]:
    raw = entry.get("updated_at")
    if not isinstance(raw, str) or not raw.strip():
        return None
    value = raw.strip()
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    except ValueError:
        return None


def _normalize_checksum(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    checksum = value.strip().lower()
    if len(checksum) != 64:
        return None
    if any(ch not in "0123456789abcdef" for ch in checksum):
        return None
    return checksum


def _checksum_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _read_file_checksum(path: Path) -> Optional[str]:
    try:
        with open(path, "rb") as handle:
            digest = hashlib.sha256()
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
            return digest.hexdigest()
    except OSError:
        return None


def _resolve_local_checksum(path: Path, cache_entry: Any) -> Optional[str]:
    try:
        stat = path.stat()
    except OSError:
        return None

    if isinstance(cache_entry, dict):
        cached_checksum = _normalize_checksum(cache_entry.get("checksum_sha256"))
        cached_mtime = cache_entry.get("mtime")
        cached_size = cache_entry.get("size")
        if (
            cached_checksum
            and isinstance(cached_mtime, (int, float))
            and isinstance(cached_size, int)
            and stat.st_size == cached_size
            and abs(stat.st_mtime - float(cached_mtime)) <= 1e-6
        ):
            return cached_checksum

    return _read_file_checksum(path)


def _normalize_workspace_path(agent_root: Path, path: str) -> Tuple[Optional[Path], Optional[str]]:
    if not isinstance(path, str):
        return None, None
    cleaned = path.strip()
    if not cleaned:
        return None, None
    rel = cleaned.lstrip("/")
    rel_path = Path(rel)
    if rel_path.is_absolute() or ".." in rel_path.parts:
        return None, None
    full_path = (agent_root / rel_path).resolve()
    if agent_root not in full_path.parents and full_path != agent_root:
        return None, None
    return full_path, "/" + rel_path.as_posix()


def _workspace_size_bytes(agent_root: Path) -> int:
    total = 0
    for path in _iter_workspace_files(agent_root):
        try:
            total += path.stat().st_size
        except OSError:
            continue
    return total


def _ensure_capacity(agent_root: Path, new_bytes: int, *, existing_bytes: int = 0) -> Optional[Dict[str, Any]]:
    max_bytes = _workspace_max_bytes()
    if max_bytes <= 0:
        return None
    current = _workspace_size_bytes(agent_root)
    adjusted = max(current - max(existing_bytes, 0), 0)
    if adjusted + new_bytes > max_bytes:
        return {
            "status": "error",
            "message": "Workspace size limit exceeded.",
            "limit_bytes": max_bytes,
            "current_bytes": current,
            "attempted_bytes": new_bytes,
            "existing_bytes": existing_bytes,
        }
    return None


def _iter_workspace_files(agent_root: Path) -> Iterable[Path]:
    for root, dirs, files in os.walk(agent_root):
        root_path = Path(root)
        if ".gobii" in root_path.parts:
            continue
        for name in files:
            path = root_path / name
            if ".gobii" in path.parts:
                continue
            yield path


def _decode_content(change: Dict[str, Any]) -> Optional[bytes]:
    if "content_b64" in change:
        raw = change.get("content_b64")
        if isinstance(raw, str):
            try:
                return base64.b64decode(raw.encode("utf-8"))
            except (ValueError, OSError):
                return None
    content = change.get("content")
    if isinstance(content, bytes):
        return content
    if isinstance(content, str):
        return content.encode("utf-8")
    return None


def _json_response(start_response: Callable, status: str, payload: Dict[str, Any]) -> list[bytes]:
    body = json.dumps(payload).encode("utf-8")
    headers = [
        ("Content-Type", "application/json"),
        ("Content-Length", str(len(body))),
    ]
    start_response(status, headers)
    return [body]


def _truncate_streams(stdout: str, stderr: str) -> Tuple[str, str]:
    max_bytes = _stdio_max_bytes()
    stdout_bytes = stdout.encode("utf-8")
    stderr_bytes = stderr.encode("utf-8")
    total = len(stdout_bytes) + len(stderr_bytes)
    if total <= max_bytes:
        return stdout, stderr
    remaining = max_bytes
    truncated_stdout = stdout_bytes[:remaining]
    remaining -= len(truncated_stdout)
    truncated_stderr = stderr_bytes[:remaining]
    return (
        truncated_stdout.decode("utf-8", errors="ignore"),
        truncated_stderr.decode("utf-8", errors="ignore"),
    )


def _extract_traceparent(environ: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    raw = environ.get(_TRACEPARENT_HEADER)
    if not isinstance(raw, str) or not raw.strip():
        return None, None
    traceparent = raw.strip()
    parts = traceparent.split("-")
    if len(parts) < _TRACEPARENT_PARTS:
        return traceparent, None
    trace_id = parts[1].lower()
    if len(trace_id) != _TRACE_ID_HEX_LEN or any(ch not in "0123456789abcdef" for ch in trace_id):
        return traceparent, None
    if trace_id == "0" * _TRACE_ID_HEX_LEN:
        return traceparent, None
    return traceparent, trace_id


def _trace_context(payload: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    trace_id = payload.get("_trace_id")
    traceparent = payload.get("_traceparent")
    if not isinstance(trace_id, str):
        trace_id = None
    if not isinstance(traceparent, str):
        traceparent = None
    return trace_id, traceparent


def _elapsed_ms(started_at: float) -> int:
    return int(round((time.monotonic() - started_at) * 1000))


def _safe_url_for_log(url: str) -> str:
    if not isinstance(url, str):
        return "<invalid-url>"
    trimmed = url.strip()
    if not trimmed:
        return "<empty-url>"
    parsed = urlsplit(trimmed)
    if parsed.scheme and parsed.netloc:
        path = parsed.path or "/"
        return f"{parsed.scheme}://{parsed.netloc}{path}"
    return trimmed.split("?", 1)[0].split("#", 1)[0]

def _get_bearer_token(environ: Dict[str, Any]) -> Optional[str]:
    auth = environ.get("HTTP_AUTHORIZATION", "")
    if not auth:
        return None
    parts = auth.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1].strip()


def _require_auth(environ: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    token = os.environ.get("SANDBOX_COMPUTE_API_TOKEN", "")
    if not token:
        return None
    provided = _get_bearer_token(environ)
    if provided != token:
        return {"status": "error", "message": "Unauthorized."}
    return None


def _parse_json(environ: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    length_raw = environ.get("CONTENT_LENGTH") or "0"
    try:
        length = int(length_raw)
    except ValueError:
        length = 0
    body = environ["wsgi.input"].read(length) if length > 0 else b""
    if not body:
        return {}, None
    try:
        data = json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None, {"status": "error", "message": "Invalid JSON body."}
    if not isinstance(data, dict):
        return None, {"status": "error", "message": "JSON body must be an object."}
    return data, None


def _require_agent_id(payload: Dict[str, Any]) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    agent_id = payload.get("agent_id")
    if not isinstance(agent_id, str) or not agent_id.strip():
        return None, {"status": "error", "message": "Missing required parameter: agent_id"}
    return agent_id.strip(), None


def _session_update(state: str) -> Dict[str, Any]:
    return {"status": "ok", "state": state}


def _handle_deploy_or_resume(payload: Dict[str, Any]) -> Dict[str, Any]:
    started_at = time.monotonic()
    agent_id, error = _require_agent_id(payload)
    if error:
        return error
    agent_root = _agent_workspace(agent_id)
    proxy_env_updated = _store_proxy_env(agent_root, payload)
    trace_id, _traceparent = _trace_context(payload)
    logger.info(
        "Sandbox deploy_or_resume agent=%s status=ok duration_ms=%s proxy_env_updated=%s trace_id=%s",
        agent_id,
        _elapsed_ms(started_at),
        proxy_env_updated,
        trace_id,
    )
    return _session_update("running")


def _handle_run_command(payload: Dict[str, Any]) -> Dict[str, Any]:
    start = time.time()
    agent_id, error = _require_agent_id(payload)
    if error:
        return error
    agent_root = _agent_workspace(agent_id)
    _store_proxy_env(agent_root, payload)
    command = payload.get("command")
    if not isinstance(command, str) or not command.strip():
        return {"status": "error", "message": "Missing required parameter: command"}
    if payload.get("interactive") is True:
        return {"status": "error", "message": "Interactive sessions are not supported yet."}
    cwd = payload.get("cwd")
    if isinstance(cwd, str) and cwd.strip():
        cwd_path, _ = _normalize_workspace_path(agent_root, cwd)
        if cwd_path is None:
            return {"status": "error", "message": "Invalid cwd path."}
        cwd = str(cwd_path)
    else:
        cwd = None
    env = payload.get("env") if isinstance(payload.get("env"), dict) else None
    timeout = _normalize_timeout(
        payload.get("timeout"),
        default=_run_command_timeout_seconds(),
    )

    try:
        result = subprocess.run(
            command,
            shell=True,
            cwd=cwd or None,
            env=_sandbox_env(agent_root, env),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        duration_ms = int((time.time() - start) * 1000)
        trace_id, _traceparent = _trace_context(payload)
        logger.exception(
            "Sandbox run_command timed out agent=%s command=%s cwd=%s duration_ms=%s trace_id=%s",
            agent_id,
            command,
            cwd,
            duration_ms,
            trace_id,
        )
        return {"status": "error", "message": "Command timed out."}
    except OSError as exc:
        duration_ms = int((time.time() - start) * 1000)
        trace_id, _traceparent = _trace_context(payload)
        logger.exception(
            "Sandbox run_command failed to start agent=%s command=%s cwd=%s duration_ms=%s trace_id=%s",
            agent_id,
            command,
            cwd,
            duration_ms,
            trace_id,
        )
        return {"status": "error", "message": f"Command failed to start: {exc}"}

    stdout, stderr = _truncate_streams(result.stdout or "", result.stderr or "")
    response = {
        "status": "ok" if result.returncode == 0 else "error",
        "exit_code": result.returncode,
        "stdout": stdout,
        "stderr": stderr,
    }
    duration_ms = int((time.time() - start) * 1000)
    trace_id, _traceparent = _trace_context(payload)
    logger.info(
        "Sandbox run_command agent=%s command=%s cwd=%s status=%s exit_code=%s duration_ms=%s trace_id=%s result=%s",
        agent_id,
        command,
        cwd,
        response.get("status"),
        response.get("exit_code"),
        duration_ms,
        trace_id,
        json.dumps(response, sort_keys=True, ensure_ascii=True),
    )
    return response


def _handle_python_exec(payload: Dict[str, Any]) -> Dict[str, Any]:
    start = time.time()
    agent_id, error = _require_agent_id(payload)
    if error:
        return error
    agent_root = _agent_workspace(agent_id)
    _store_proxy_env(agent_root, payload)
    code = payload.get("code")
    if not isinstance(code, str) or not code.strip():
        return {"status": "error", "message": "Missing required parameter: code"}
    timeout = _normalize_timeout(
        payload.get("timeout_seconds"),
        default=_python_default_timeout_seconds(),
        maximum=_python_max_timeout_seconds(),
    )

    try:
        result = subprocess.run(
            [sys.executable, "-c", code],
            env=_sandbox_env(agent_root),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        duration_ms = int((time.time() - start) * 1000)
        trace_id, _traceparent = _trace_context(payload)
        logger.exception(
            "Sandbox python_exec timed out agent=%s duration_ms=%s trace_id=%s",
            agent_id,
            duration_ms,
            trace_id,
        )
        return {"status": "error", "message": "Python execution timed out."}
    except OSError as exc:
        duration_ms = int((time.time() - start) * 1000)
        trace_id, _traceparent = _trace_context(payload)
        logger.exception(
            "Sandbox python_exec failed to start agent=%s duration_ms=%s trace_id=%s",
            agent_id,
            duration_ms,
            trace_id,
        )
        return {"status": "error", "message": f"Python execution failed to start: {exc}"}

    stdout, stderr = _truncate_streams(result.stdout or "", result.stderr or "")
    response = {
        "status": "ok" if result.returncode == 0 else "error",
        "exit_code": result.returncode,
        "stdout": stdout,
        "stderr": stderr,
    }
    return response


def _write_file(agent_root: Path, rel_path: str, content: bytes, overwrite: bool) -> Optional[Dict[str, Any]]:
    full_path, normalized = _normalize_workspace_path(agent_root, rel_path)
    if full_path is None:
        return {"status": "error", "message": "Invalid file_path."}
    if full_path.exists() and not overwrite:
        return {"status": "error", "message": "File already exists. Use overwrite=true to replace it."}

    existing_bytes = 0
    if full_path.exists():
        try:
            existing_bytes = full_path.stat().st_size
        except OSError:
            existing_bytes = 0

    capacity_error = _ensure_capacity(agent_root, len(content), existing_bytes=existing_bytes)
    if capacity_error:
        return capacity_error

    full_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(full_path, "wb") as handle:
            handle.write(content)
    except OSError as exc:
        logger.exception("Failed to write file to workspace path=%s", normalized or rel_path)
        return {"status": "error", "message": f"Failed to write file: {exc}"}
    return {"status": "ok", "export_path": normalized}


def _handle_create_file(agent_root: Path, payload: Dict[str, Any]) -> Dict[str, Any]:
    content = payload.get("content")
    if not isinstance(content, str) or not content.strip():
        return {"status": "error", "message": "Missing required parameter: content"}
    mime_type = payload.get("mime_type")
    if not isinstance(mime_type, str) or not mime_type.strip():
        return {"status": "error", "message": "Missing required parameter: mime_type"}
    file_path = payload.get("file_path")
    if not isinstance(file_path, str) or not file_path.strip():
        return {"status": "error", "message": "Missing required parameter: file_path"}
    file_path = file_path.strip()
    overwrite = payload.get("overwrite") is True
    if not Path(file_path).suffix:
        base_type = mime_type.split(";", 1)[0].strip().lower()
        guessed = mimetypes.guess_extension(base_type or "") or ""
        if guessed:
            file_path = f"{file_path}{guessed}"
    response = _write_file(agent_root, file_path, content.encode("utf-8"), overwrite)
    if response.get("status") != "ok":
        return response
    response["mime_type"] = mime_type
    return response


def _blocked_html_assets(html: str) -> bool:
    lowered = html.lower()
    return "http://" in lowered or "https://" in lowered or "file://" in lowered


_TOKEN_PATTERN = re.compile(r"\$\[([^\]]+)\]")


def _substitute_workspace_tokens(html: str, agent_root: Path) -> str:
    def _replace(match: re.Match) -> str:
        token = match.group(1)
        full_path, _normalized = _normalize_workspace_path(agent_root, token)
        if full_path is None or not full_path.exists():
            return match.group(0)
        try:
            content = full_path.read_bytes()
        except OSError:
            return match.group(0)
        mime_type = mimetypes.guess_type(full_path.name)[0] or "application/octet-stream"
        encoded = base64.b64encode(content).decode("utf-8")
        return f"data:{mime_type};base64,{encoded}"

    return _TOKEN_PATTERN.sub(_replace, html)


def _handle_create_pdf(agent_root: Path, payload: Dict[str, Any]) -> Dict[str, Any]:
    html = payload.get("html")
    if not isinstance(html, str) or not html.strip():
        return {"status": "error", "message": "Missing required parameter: html"}
    file_path = payload.get("file_path")
    if not isinstance(file_path, str) or not file_path.strip():
        return {"status": "error", "message": "Missing required parameter: file_path"}
    file_path = file_path.strip()
    if not Path(file_path).suffix:
        file_path = f"{file_path}.pdf"
    overwrite = payload.get("overwrite") is True

    html = _substitute_workspace_tokens(html, agent_root)

    if _blocked_html_assets(html):
        return {
            "status": "error",
            "message": "HTML contains blocked asset references (http/https/file).",
        }

    try:
        from weasyprint import HTML
    except Exception as exc:
        return {"status": "error", "message": f"PDF renderer unavailable: {exc}"}

    try:
        pdf_bytes = HTML(string=html, base_url=str(agent_root)).write_pdf()
    except Exception as exc:
        return {"status": "error", "message": f"Failed to render PDF: {exc}"}

    response = _write_file(agent_root, file_path, pdf_bytes, overwrite)
    if response.get("status") != "ok":
        return response
    response["mime_type"] = "application/pdf"
    return response


def _handle_tool_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    start = time.time()
    agent_id, error = _require_agent_id(payload)
    if error:
        return error
    tool_name = payload.get("tool_name")
    if not isinstance(tool_name, str) or not tool_name.strip():
        return {"status": "error", "message": "Missing required parameter: tool_name"}
    params = payload.get("params") if isinstance(payload.get("params"), dict) else {}
    agent_root = _agent_workspace(agent_id)
    _store_proxy_env(agent_root, payload)

    if tool_name == "python_exec":
        params = dict(params)
        params["agent_id"] = agent_id
        response = _handle_python_exec(params)
        duration_ms = int((time.time() - start) * 1000)
        trace_id, _traceparent = _trace_context(payload)
        logger.info(
            "Sandbox tool_request agent=%s tool=%s status=%s duration_ms=%s trace_id=%s result=%s",
            agent_id,
            tool_name,
            response.get("status"),
            duration_ms,
            trace_id,
            json.dumps(response, sort_keys=True, ensure_ascii=True),
        )
        return response
    if tool_name == "create_file":
        response = _handle_create_file(agent_root, params)
        duration_ms = int((time.time() - start) * 1000)
        trace_id, _traceparent = _trace_context(payload)
        logger.info(
            "Sandbox tool_request agent=%s tool=%s status=%s duration_ms=%s trace_id=%s result=%s",
            agent_id,
            tool_name,
            response.get("status"),
            duration_ms,
            trace_id,
            json.dumps(response, sort_keys=True, ensure_ascii=True),
        )
        return response
    if tool_name == "create_pdf":
        response = _handle_create_pdf(agent_root, params)
        duration_ms = int((time.time() - start) * 1000)
        trace_id, _traceparent = _trace_context(payload)
        logger.info(
            "Sandbox tool_request agent=%s tool=%s status=%s duration_ms=%s trace_id=%s result=%s",
            agent_id,
            tool_name,
            response.get("status"),
            duration_ms,
            trace_id,
            json.dumps(response, sort_keys=True, ensure_ascii=True),
        )
        return response

    response = {
        "status": "error",
        "error_code": "sandbox_unsupported_tool",
        "message": f"Sandbox tool '{tool_name}' is not supported yet.",
    }
    duration_ms = int((time.time() - start) * 1000)
    trace_id, _traceparent = _trace_context(payload)
    logger.info(
        "Sandbox tool_request agent=%s tool=%s status=%s duration_ms=%s trace_id=%s result=%s",
        agent_id,
        tool_name,
        response.get("status"),
        duration_ms,
        trace_id,
        json.dumps(response, sort_keys=True, ensure_ascii=True),
    )
    return response


def _handle_sync_filespace(payload: Dict[str, Any]) -> Dict[str, Any]:
    started_at = time.monotonic()
    agent_id, error = _require_agent_id(payload)
    if error:
        return error
    trace_id, _traceparent = _trace_context(payload)
    direction = payload.get("direction")
    if not isinstance(direction, str) or not direction.strip():
        return {"status": "error", "message": "Missing required parameter: direction"}
    direction = direction.strip().lower()
    agent_root = _agent_workspace(agent_id)
    proxy_env_updated = _store_proxy_env(agent_root, payload)
    manifest_load_started = time.monotonic()
    manifest = _load_manifest(agent_root)
    manifest_load_ms = _elapsed_ms(manifest_load_started)

    if direction == "push":
        since = _parse_since(payload.get("since"))
        scan_started_at = time.monotonic()
        changes: list[Dict[str, Any]] = []
        seen_paths: set[str] = set()
        scanned_files = 0
        uploaded_files = 0
        uploaded_bytes = 0
        deleted_count = 0
        for path in _iter_workspace_files(agent_root):
            scanned_files += 1
            rel = "/" + path.relative_to(agent_root).as_posix()
            seen_paths.add(rel)
            try:
                stat = path.stat()
            except OSError:
                continue
            mtime = stat.st_mtime
            if since is not None and mtime <= since:
                continue
            try:
                content = path.read_bytes()
            except OSError:
                continue
            mime_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
            checksum_sha256 = _checksum_bytes(content)
            changes.append(
                {
                    "path": rel,
                    "content_b64": base64.b64encode(content).decode("utf-8"),
                    "mime_type": mime_type,
                    "checksum_sha256": checksum_sha256,
                }
            )
            manifest["files"][rel] = {
                "mtime": mtime,
                "size": stat.st_size,
                "checksum_sha256": checksum_sha256,
            }
            manifest["deleted"].pop(rel, None)
            uploaded_files += 1
            uploaded_bytes += len(content)

        deleted = manifest.get("deleted", {})
        tracked = set(manifest.get("files", {}).keys())
        for rel in sorted(tracked - seen_paths):
            deleted_at = time.time()
            if since is not None and deleted_at <= since:
                continue
            changes.append({"path": rel, "is_deleted": True})
            deleted[rel] = {"deleted_at": deleted_at}
            manifest["files"].pop(rel, None)
            deleted_count += 1
        manifest["deleted"] = deleted
        _save_manifest(agent_root, manifest)
        response = {
            "status": "ok",
            "changes": changes,
            "sync_timestamp": datetime.now(timezone.utc).isoformat(),
        }
        logger.info(
            (
                "Sandbox sync_filespace agent=%s direction=push status=ok "
                "changes=%s uploaded_files=%s uploaded_bytes=%s deleted=%s scanned_files=%s "
                "scan_ms=%s manifest_load_ms=%s total_ms=%s since_set=%s proxy_env_updated=%s trace_id=%s"
            ),
            agent_id,
            len(changes),
            uploaded_files,
            uploaded_bytes,
            deleted_count,
            scanned_files,
            _elapsed_ms(scan_started_at),
            manifest_load_ms,
            _elapsed_ms(started_at),
            since is not None,
            proxy_env_updated,
            trace_id,
        )
        return response

    if direction == "pull":
        entries = payload.get("files") or payload.get("changes") or []
        if not isinstance(entries, list):
            return {"status": "error", "message": "files must be a list."}
        pull_started_at = time.monotonic()
        skipped = 0
        conflicts = 0
        downloaded_files = 0
        downloaded_bytes = 0
        download_total_ms = 0
        inline_content_files = 0
        deleted_entries = 0
        checksum_skips = 0
        proxy_env = _proxy_env_from_manifest(agent_root)
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            path = entry.get("path")
            full_path, normalized = _normalize_workspace_path(agent_root, path)
            if full_path is None or not normalized:
                continue
            entry_updated_at = _parse_entry_updated_at(entry)
            is_deleted = bool(entry.get("is_deleted"))
            if is_deleted:
                deleted_entries += 1
                if entry_updated_at is not None and full_path.exists():
                    try:
                        local_mtime = full_path.stat().st_mtime
                    except OSError:
                        local_mtime = None
                    if local_mtime is not None and local_mtime > entry_updated_at:
                        conflicts += 1
                        skipped += 1
                        continue
                try:
                    if full_path.exists():
                        full_path.unlink()
                except OSError:
                    continue
                manifest["files"].pop(normalized, None)
                deleted_at = entry_updated_at if entry_updated_at is not None else time.time()
                manifest.setdefault("deleted", {})[normalized] = {"deleted_at": deleted_at}
                continue

            remote_checksum = _normalize_checksum(entry.get("checksum_sha256"))
            local_meta = manifest.get("files", {}).get(normalized)
            if remote_checksum and full_path.exists():
                local_checksum = _resolve_local_checksum(full_path, local_meta)
                if local_checksum == remote_checksum:
                    try:
                        local_stat = full_path.stat()
                    except OSError:
                        local_stat = None
                    manifest["files"][normalized] = {
                        "mtime": local_stat.st_mtime if local_stat else time.time(),
                        "size": local_stat.st_size if local_stat else 0,
                        "updated_at": entry.get("updated_at"),
                        "checksum_sha256": remote_checksum,
                    }
                    manifest["deleted"].pop(normalized, None)
                    skipped += 1
                    checksum_skips += 1
                    continue

            content = _decode_content(entry)
            if content is None and entry.get("download_url"):
                download_started_at = time.monotonic()
                try:
                    content = _download_file(
                        entry["download_url"],
                        entry.get("size_bytes"),
                        proxy_env,
                    )
                except RuntimeError as exc:
                    message = str(exc)
                    logger.warning(
                        (
                            "Sandbox sync_filespace agent=%s direction=pull status=error "
                            "message=%s entries=%s skipped=%s conflicts=%s download_ms=%s "
                            "manifest_load_ms=%s total_ms=%s proxy_env_updated=%s trace_id=%s"
                        ),
                        agent_id,
                        message,
                        len(entries),
                        skipped,
                        conflicts,
                        download_total_ms,
                        manifest_load_ms,
                        _elapsed_ms(started_at),
                        proxy_env_updated,
                        trace_id,
                    )
                    return {"status": "error", "message": message}
                downloaded_files += 1
                downloaded_bytes += len(content)
                download_total_ms += _elapsed_ms(download_started_at)
            elif content is not None:
                inline_content_files += 1
            if content is None:
                message = f"Missing content for {normalized}"
                logger.warning(
                    (
                        "Sandbox sync_filespace agent=%s direction=pull status=error "
                        "message=%s entries=%s skipped=%s conflicts=%s downloaded_files=%s "
                        "manifest_load_ms=%s total_ms=%s proxy_env_updated=%s trace_id=%s"
                    ),
                    agent_id,
                    message,
                    len(entries),
                    skipped,
                    conflicts,
                    downloaded_files,
                    manifest_load_ms,
                    _elapsed_ms(started_at),
                    proxy_env_updated,
                    trace_id,
                )
                return {"status": "error", "message": message}

            existing_bytes = 0
            if full_path.exists():
                try:
                    existing_bytes = full_path.stat().st_size
                except OSError:
                    existing_bytes = 0
            if entry_updated_at is not None and full_path.exists():
                try:
                    local_mtime = full_path.stat().st_mtime
                except OSError:
                    local_mtime = None
                if local_mtime is not None and local_mtime > entry_updated_at:
                    conflicts += 1
                    skipped += 1
                    continue

            capacity_error = _ensure_capacity(agent_root, len(content), existing_bytes=existing_bytes)
            if capacity_error:
                logger.warning(
                    (
                        "Sandbox sync_filespace agent=%s direction=pull status=error "
                        "message=%s entries=%s skipped=%s conflicts=%s downloaded_files=%s "
                        "download_ms=%s manifest_load_ms=%s total_ms=%s proxy_env_updated=%s trace_id=%s"
                    ),
                    agent_id,
                    capacity_error.get("message"),
                    len(entries),
                    skipped,
                    conflicts,
                    downloaded_files,
                    download_total_ms,
                    manifest_load_ms,
                    _elapsed_ms(started_at),
                    proxy_env_updated,
                    trace_id,
                )
                return capacity_error

            full_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with open(full_path, "wb") as handle:
                    handle.write(content)
            except OSError as exc:
                logger.exception("Failed to write synced file agent=%s path=%s", agent_id, normalized)
                return {"status": "error", "message": f"Failed to write {normalized}: {exc}"}
            written_checksum = remote_checksum or _checksum_bytes(content)
            manifest["files"][normalized] = {
                "mtime": full_path.stat().st_mtime,
                "size": full_path.stat().st_size,
                "updated_at": entry.get("updated_at"),
                "checksum_sha256": written_checksum,
            }
            manifest["deleted"].pop(normalized, None)

        _save_manifest(agent_root, manifest)
        response = {
            "status": "ok",
            "applied": len(entries) - skipped,
            "skipped": skipped,
            "conflicts": conflicts,
            "sync_timestamp": datetime.now(timezone.utc).isoformat(),
        }
        logger.info(
            (
                "Sandbox sync_filespace agent=%s direction=pull status=ok "
                "entries=%s applied=%s skipped=%s conflicts=%s deleted_entries=%s "
                "checksum_skips=%s inline_files=%s downloaded_files=%s downloaded_bytes=%s download_ms=%s "
                "pull_ms=%s manifest_load_ms=%s total_ms=%s proxy_env_updated=%s trace_id=%s"
            ),
            agent_id,
            len(entries),
            response.get("applied"),
            skipped,
            conflicts,
            deleted_entries,
            checksum_skips,
            inline_content_files,
            downloaded_files,
            downloaded_bytes,
            download_total_ms,
            _elapsed_ms(pull_started_at),
            manifest_load_ms,
            _elapsed_ms(started_at),
            proxy_env_updated,
            trace_id,
        )
        return response

    message = "Invalid sync direction."
    logger.warning(
        "Sandbox sync_filespace agent=%s direction=%s status=error message=%s total_ms=%s trace_id=%s",
        agent_id,
        direction,
        message,
        _elapsed_ms(started_at),
        trace_id,
    )
    return {"status": "error", "message": message}


def _download_file(url: str, expected_size: Optional[int], proxy_env: Optional[Dict[str, str]]) -> bytes:
    started_at = time.monotonic()
    safe_url = _safe_url_for_log(url)
    max_bytes = _workspace_max_bytes()
    if expected_size and max_bytes > 0 and expected_size > max_bytes:
        raise RuntimeError("Download exceeds workspace size limit.")
    proxies = None
    if proxy_env:
        http_proxy = proxy_env.get("HTTP_PROXY")
        https_proxy = proxy_env.get("HTTPS_PROXY") or http_proxy
        if http_proxy or https_proxy:
            proxies = {
                "http": http_proxy or "",
                "https": https_proxy or "",
            }
    try:
        with requests.get(url, stream=True, timeout=30, proxies=proxies) as response:
            response.raise_for_status()
            data = bytearray()
            for chunk in response.iter_content(chunk_size=1024 * 256):
                if not chunk:
                    continue
                data.extend(chunk)
                if max_bytes > 0 and len(data) > max_bytes:
                    raise RuntimeError("Downloaded file exceeds workspace size limit.")
            result = bytes(data)
            logger.info(
                "Sandbox download_file url=%s status=ok bytes=%s duration_ms=%s via_proxy=%s",
                safe_url,
                len(result),
                _elapsed_ms(started_at),
                bool(proxies),
            )
            return result
    except requests.RequestException as exc:
        logger.exception(
            "Sandbox download_file url=%s status=error duration_ms=%s via_proxy=%s",
            safe_url,
            _elapsed_ms(started_at),
            bool(proxies),
        )
        raise RuntimeError(f"Failed to download file: {exc}") from exc


def _handle_terminate(payload: Dict[str, Any]) -> Dict[str, Any]:
    started_at = time.monotonic()
    agent_id, error = _require_agent_id(payload)
    if error:
        return error
    agent_root = _agent_workspace(agent_id)
    proxy_env_updated = _store_proxy_env(agent_root, payload)
    trace_id, _traceparent = _trace_context(payload)
    logger.info(
        "Sandbox terminate agent=%s status=ok duration_ms=%s proxy_env_updated=%s trace_id=%s",
        agent_id,
        _elapsed_ms(started_at),
        proxy_env_updated,
        trace_id,
    )
    return _session_update("stopped")


class GobiiStdioTransport(FastMCPStdioTransport):
    """Custom stdio transport that guarantees an errlog with a real fileno."""

    def __init__(
        self,
        command: str,
        args: list[str],
        env: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
        keep_alive: Optional[bool] = None,
    ):
        super().__init__(command=command, args=args, env=env, cwd=cwd, keep_alive=keep_alive)
        self._errlog_fallback = None

    def _resolve_errlog(self):
        for candidate in (getattr(sys, "__stderr__", None), sys.stderr):
            if candidate and hasattr(candidate, "fileno"):
                return candidate
        if self._errlog_fallback is None:
            self._errlog_fallback = open(os.devnull, "w")
        return self._errlog_fallback

    async def connect(self, **session_kwargs):
        if self._connect_task is not None:
            return

        errlog = self._resolve_errlog()

        async def _connect_task():
            try:
                async with contextlib.AsyncExitStack() as stack:
                    server_params = StdioServerParameters(
                        command=self.command,
                        args=self.args,
                        env=self.env,
                        cwd=self.cwd,
                    )
                    transport = await stack.enter_async_context(
                        stdio_client(server_params, errlog=errlog)
                    )
                    read_stream, write_stream = transport
                    self._session = await stack.enter_async_context(
                        ClientSession(read_stream, write_stream, **session_kwargs)
                    )

                    logger.debug("Stdio transport connected")
                    self._ready_event.set()

                    await self._stop_event.wait()
            finally:
                self._session = None
                logger.debug("Stdio transport disconnected")

        self._connect_task = asyncio.create_task(_connect_task())
        await self._ready_event.wait()

        if self._connect_task.done():
            exception = self._connect_task.exception()
            if exception is not None:
                raise exception

    async def disconnect(self):
        await super().disconnect()
        self._cleanup_errlog()

    async def close(self):
        await super().close()
        self._cleanup_errlog()

    def _cleanup_errlog(self):
        if self._errlog_fallback:
            try:
                self._errlog_fallback.close()
            except OSError:
                pass
            self._errlog_fallback = None


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


def _coerce_str_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if item is not None]
    return []


def _coerce_str_dict(value: Any) -> Dict[str, str]:
    if not isinstance(value, dict):
        return {}
    cleaned: Dict[str, str] = {}
    for key, val in value.items():
        if key is None or val is None:
            continue
        cleaned[str(key)] = str(val)
    return cleaned


def _parse_mcp_server_payload(payload: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    raw = payload.get("server")
    if not isinstance(raw, dict):
        return None, {"status": "error", "message": "Missing MCP server definition."}

    config_id = raw.get("config_id") or raw.get("id") or payload.get("server_id")
    if not isinstance(config_id, str) or not config_id.strip():
        return None, {"status": "error", "message": "Missing MCP server config id."}

    name = raw.get("name") or ""
    if not isinstance(name, str):
        name = str(name)

    command = raw.get("command") or ""
    if not isinstance(command, str):
        command = str(command)
    command = command.strip()

    url = raw.get("url") or ""
    if not isinstance(url, str):
        url = str(url)
    url = url.strip()

    args = _coerce_str_list(raw.get("command_args") or raw.get("args") or [])
    env = _coerce_str_dict(raw.get("env") or raw.get("environment") or {})
    headers = _coerce_str_dict(raw.get("headers") or {})

    if not command and not url:
        return None, {"status": "error", "message": "MCP server must include a command or URL."}

    return {
        "config_id": config_id.strip(),
        "name": name.strip(),
        "command": command,
        "args": args,
        "url": url,
        "env": env,
        "headers": headers,
    }, None


def _normalize_mcp_result(result: Any) -> Any:
    if isinstance(result, (dict, list, str, int, float, bool)) or result is None:
        return result
    if hasattr(result, "model_dump"):
        try:
            return result.model_dump()
        except Exception:
            logger.debug("Failed to serialize MCP result via model_dump()", exc_info=True)
    if hasattr(result, "dict"):
        try:
            return result.dict()
        except Exception:
            logger.debug("Failed to serialize MCP result via dict()", exc_info=True)
    if hasattr(result, "__dict__"):
        return result.__dict__
    return str(result)


async def _call_mcp_tool(runtime: Dict[str, Any], tool_name: str, params: Dict[str, Any]) -> Any:
    if runtime.get("url"):
        transport = StreamableHttpTransport(url=runtime["url"], headers=runtime["headers"])
    else:
        transport = GobiiStdioTransport(
            command=runtime["command"],
            args=runtime["args"],
            env=runtime["env"],
        )

    client = Client(transport)
    async with client:
        timeout = _mcp_timeout_seconds()
        return await asyncio.wait_for(client.call_tool(tool_name, params), timeout=timeout)


async def _discover_mcp_tools(runtime: Dict[str, Any]) -> list[Dict[str, Any]]:
    if runtime.get("url"):
        transport = StreamableHttpTransport(url=runtime["url"], headers=runtime["headers"])
    else:
        transport = GobiiStdioTransport(
            command=runtime["command"],
            args=runtime["args"],
            env=runtime["env"],
        )

    client = Client(transport)
    async with client:
        tools = await client.list_tools()

    serialized: list[Dict[str, Any]] = []
    for tool in tools or []:
        name = getattr(tool, "name", None) or (tool.get("name") if isinstance(tool, dict) else None)
        if not name:
            continue
        description = getattr(tool, "description", None) or ""
        parameters = getattr(tool, "inputSchema", None)
        if parameters is None and isinstance(tool, dict):
            parameters = tool.get("inputSchema") or tool.get("input_schema")
        if runtime.get("name") == "pipedream":
            full_name = name
        else:
            full_name = f"mcp_{runtime.get('name')}_{name}" if runtime.get("name") else name
        serialized.append(
            {
                "full_name": full_name,
                "server_name": runtime.get("name") or "",
                "tool_name": name,
                "description": description or f"{name} from {runtime.get('name') or 'MCP server'}",
                "parameters": parameters or {"type": "object", "properties": {}},
            }
        )
    return serialized


def _handle_mcp_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    start = time.time()
    agent_id, error = _require_agent_id(payload)
    if error:
        return error
    agent_root = _agent_workspace(agent_id)
    _store_proxy_env(agent_root, payload)

    runtime, runtime_error = _parse_mcp_server_payload(payload)
    if runtime_error:
        return runtime_error
    proxy_env = _proxy_env_from_manifest(agent_root)
    if proxy_env:
        runtime["env"] = {**runtime.get("env", {}), **proxy_env}

    tool_name = payload.get("tool_name")
    if not isinstance(tool_name, str) or not tool_name.strip():
        return {"status": "error", "message": "Missing required parameter: tool_name"}
    params = payload.get("params") if isinstance(payload.get("params"), dict) else {}

    try:
        result = asyncio.run(_call_mcp_tool(runtime, tool_name.strip(), params))
    except Exception as exc:
        duration_ms = int((time.time() - start) * 1000)
        trace_id, _traceparent = _trace_context(payload)
        logger.exception(
            "Sandbox mcp_request failed agent=%s tool=%s duration_ms=%s trace_id=%s",
            agent_id,
            tool_name,
            duration_ms,
            trace_id,
        )
        response = {"status": "error", "message": str(exc)}
        logger.info(
            "Sandbox mcp_request agent=%s tool=%s status=%s duration_ms=%s trace_id=%s result=%s",
            agent_id,
            tool_name,
            response.get("status"),
            duration_ms,
            trace_id,
            json.dumps(response, sort_keys=True, ensure_ascii=True),
        )
        return response

    response = {"status": "ok", "result": _normalize_mcp_result(result)}
    duration_ms = int((time.time() - start) * 1000)
    trace_id, _traceparent = _trace_context(payload)
    logger.info(
        "Sandbox mcp_request agent=%s tool=%s status=%s duration_ms=%s trace_id=%s result=%s",
        agent_id,
        tool_name,
        response.get("status"),
        duration_ms,
        trace_id,
        json.dumps(response, sort_keys=True, ensure_ascii=True),
    )
    return response


def _handle_discover_mcp_tools(payload: Dict[str, Any]) -> Dict[str, Any]:
    started_at = time.monotonic()
    runtime, runtime_error = _parse_mcp_server_payload(payload)
    if runtime_error:
        return runtime_error
    trace_id, _traceparent = _trace_context(payload)
    try:
        tools = asyncio.run(_discover_mcp_tools(runtime))
    except Exception as exc:
        logger.exception(
            "Sandbox discover_mcp_tools failed server_id=%s duration_ms=%s trace_id=%s",
            runtime.get("config_id"),
            _elapsed_ms(started_at),
            trace_id,
        )
        return {"status": "error", "message": str(exc)}

    response = {
        "status": "ok",
        "tools": tools,
        "server_id": runtime.get("config_id"),
    }
    logger.info(
        "Sandbox discover_mcp_tools server_id=%s status=ok tools=%s duration_ms=%s trace_id=%s",
        runtime.get("config_id"),
        len(tools),
        _elapsed_ms(started_at),
        trace_id,
    )
    return response


_ROUTES: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = {
    "/sandbox/compute/deploy_or_resume": _handle_deploy_or_resume,
    "/sandbox/compute/run_command": _handle_run_command,
    "/sandbox/compute/tool_request": _handle_tool_request,
    "/sandbox/compute/mcp_request": _handle_mcp_request,
    "/sandbox/compute/sync_filespace": _handle_sync_filespace,
    "/sandbox/compute/terminate": _handle_terminate,
    "/sandbox/compute/discover_mcp_tools": _handle_discover_mcp_tools,
}


def application(environ: Dict[str, Any], start_response: Callable) -> list[bytes]:
    started_at = time.monotonic()
    path = environ.get("PATH_INFO", "") or ""
    method = environ.get("REQUEST_METHOD", "GET").upper()

    if path.rstrip("/") == "/healthz":
        return _json_response(start_response, "200 OK", {"status": "ok"})

    if method != "POST":
        logger.warning(
            "Sandbox request rejected path=%s method=%s http_status=405 duration_ms=%s",
            path,
            method,
            _elapsed_ms(started_at),
        )
        return _json_response(start_response, "405 Method Not Allowed", {"status": "error", "message": "POST only."})

    auth_error = _require_auth(environ)
    if auth_error:
        logger.warning(
            "Sandbox request rejected path=%s method=%s http_status=401 duration_ms=%s",
            path,
            method,
            _elapsed_ms(started_at),
        )
        return _json_response(start_response, "401 Unauthorized", auth_error)

    payload, parse_error = _parse_json(environ)
    if parse_error:
        logger.warning(
            "Sandbox request rejected path=%s method=%s http_status=400 reason=parse_error duration_ms=%s",
            path,
            method,
            _elapsed_ms(started_at),
        )
        return _json_response(start_response, "400 Bad Request", parse_error)
    if payload is None:
        logger.warning(
            "Sandbox request rejected path=%s method=%s http_status=400 reason=invalid_payload duration_ms=%s",
            path,
            method,
            _elapsed_ms(started_at),
        )
        return _json_response(start_response, "400 Bad Request", {"status": "error", "message": "Invalid request."})
    traceparent, trace_id = _extract_traceparent(environ)
    if traceparent:
        payload["_traceparent"] = traceparent
    if trace_id:
        payload["_trace_id"] = trace_id

    handler = _ROUTES.get(path.rstrip("/"))
    if not handler:
        logger.warning(
            "Sandbox request rejected path=%s method=%s http_status=404 trace_id=%s duration_ms=%s",
            path,
            method,
            trace_id,
            _elapsed_ms(started_at),
        )
        return _json_response(start_response, "404 Not Found", {"status": "error", "message": "Unknown endpoint."})

    agent_id = payload.get("agent_id")
    if not isinstance(agent_id, str):
        agent_id = None
    elif not agent_id.strip():
        agent_id = None
    else:
        agent_id = agent_id.strip()

    try:
        result = handler(payload)
    except Exception:
        logger.exception(
            "Sandbox compute request failed path=%s method=%s agent=%s trace_id=%s duration_ms=%s",
            path,
            method,
            agent_id,
            trace_id,
            _elapsed_ms(started_at),
        )
        return _json_response(
            start_response,
            "200 OK",
            {"status": "error", "message": "Sandbox compute request failed."},
        )

    status = "unknown"
    if isinstance(result, dict):
        maybe_status = result.get("status")
        if isinstance(maybe_status, str) and maybe_status:
            status = maybe_status

    logger.info(
        (
            "Sandbox request completed path=%s method=%s agent=%s status=%s "
            "trace_id=%s duration_ms=%s payload_bytes=%s"
        ),
        path,
        method,
        agent_id,
        status,
        trace_id,
        _elapsed_ms(started_at),
        environ.get("CONTENT_LENGTH", "0"),
    )

    return _json_response(start_response, "200 OK", result)
