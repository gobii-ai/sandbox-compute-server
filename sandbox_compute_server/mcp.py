import asyncio
import contextlib
import json
import logging
import os
import sys
import threading
import time
from typing import Any, Dict, Optional, Tuple

from fastmcp import Client
from fastmcp.client.transports import StdioTransport as FastMCPStdioTransport
from fastmcp.client.transports import StreamableHttpTransport
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from sandbox_compute_server.config import _agent_workspace, _mcp_timeout_seconds
from sandbox_compute_server.manifest import _proxy_env_from_manifest, _store_proxy_env
from sandbox_compute_server.mcp_remote import (
    default_config_dir,
    normalize_mcp_remote_args,
)
from sandbox_compute_server.workspace import _elapsed_ms, _require_agent_id, _trace_context

logger = logging.getLogger(__name__)


class MCPRemoteAuthRequired(RuntimeError):
    """Raised when mcp-remote requests user authorization in bridge mode."""

    def __init__(self, event: Dict[str, Any]):
        super().__init__("MCP remote authorization required.")
        self.event = event


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
        self._stderr_reader = None
        self._stderr_writer = None
        self._stderr_sink = None
        self._stderr_thread = None
        self._auth_event: Optional[Dict[str, Any]] = None
        self._auth_lock = threading.Lock()

    def _resolve_errlog(self):
        if self._stderr_writer is not None:
            return self._stderr_writer

        sink = None
        for candidate in (getattr(sys, "__stderr__", None), sys.stderr):
            if candidate and hasattr(candidate, "write"):
                sink = candidate
                break
        if sink is None:
            if self._errlog_fallback is None:
                self._errlog_fallback = open(os.devnull, "w")
            sink = self._errlog_fallback

        read_fd, write_fd = os.pipe()
        self._stderr_reader = os.fdopen(read_fd, "r", buffering=1, encoding="utf-8", errors="replace")
        self._stderr_writer = os.fdopen(write_fd, "w", buffering=1, encoding="utf-8", errors="replace")
        self._stderr_sink = sink
        self._stderr_thread = threading.Thread(target=self._drain_stderr, name="mcp-stderr-drain", daemon=True)
        self._stderr_thread.start()
        return self._stderr_writer

    def _drain_stderr(self):
        if self._stderr_reader is None:
            return
        try:
            for line in self._stderr_reader:
                if line:
                    self._capture_auth_event(line)
                    if self._stderr_sink:
                        try:
                            self._stderr_sink.write(line)
                            self._stderr_sink.flush()
                        except OSError:
                            pass
        except OSError:
            return

    def _capture_auth_event(self, line: str):
        marker = "MCP_REMOTE_AUTH_URL "
        index = line.find(marker)
        if index < 0:
            return
        payload = line[index + len(marker) :].strip()
        if not payload:
            return
        try:
            event = json.loads(payload)
        except json.JSONDecodeError:
            logger.debug("Failed parsing MCP_REMOTE_AUTH_URL payload: %s", payload)
            return
        if not isinstance(event, dict):
            return
        with self._auth_lock:
            self._auth_event = event

    def pop_auth_event(self) -> Optional[Dict[str, Any]]:
        with self._auth_lock:
            if not self._auth_event:
                return None
            event = dict(self._auth_event)
            self._auth_event = None
            return event

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
            except Exception:
                self._ready_event.set()
                raise
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
        if self._stderr_writer:
            try:
                self._stderr_writer.close()
            except OSError:
                pass
            self._stderr_writer = None
        if self._stderr_thread and self._stderr_thread.is_alive():
            self._stderr_thread.join(timeout=0.2)
        self._stderr_thread = None
        if self._stderr_reader:
            try:
                self._stderr_reader.close()
            except OSError:
                pass
            self._stderr_reader = None
        self._stderr_sink = None
        if self._errlog_fallback:
            try:
                self._errlog_fallback.close()
            except OSError:
                pass
            self._errlog_fallback = None


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


def _coerce_bridge_dict(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    bridge: Dict[str, Any] = {}
    for key in (
        "auth_mode",
        "redirect_url",
        "poll_url",
        "notify_url",
        "auth_session_id",
    ):
        raw = value.get(key)
        if raw is None:
            continue
        text = str(raw).strip()
        if text:
            bridge[key] = text
    for key in ("poll_interval_seconds", "auth_timeout_seconds"):
        raw = value.get(key)
        if raw is None:
            continue
        text = str(raw).strip()
        if not text:
            continue
        try:
            bridge[key] = int(text)
        except ValueError:
            logger.debug("Ignoring invalid bridge option %s=%s", key, text)
    return bridge


def _normalize_mcp_remote_runtime(runtime: Dict[str, Any], *, agent_id: Optional[str] = None) -> Dict[str, Any]:
    command = str(runtime.get("command") or "").strip()
    args = [str(arg) for arg in runtime.get("args") or []]
    bridge = _coerce_bridge_dict(runtime.get("mcp_remote_bridge"))
    is_remote, normalized_args = normalize_mcp_remote_args(command, args, bridge)
    if not is_remote:
        return runtime

    normalized = dict(runtime)
    normalized["args"] = normalized_args

    env = _coerce_str_dict(runtime.get("env") or {})
    config_id = str(runtime.get("config_id") or "").strip()
    env.setdefault("MCP_REMOTE_CONFIG_DIR", default_config_dir(config_id, agent_id=agent_id))
    normalized["env"] = env
    return normalized


def _parse_mcp_server_payload(
    payload: Dict[str, Any],
    *,
    agent_id: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
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

    bridge = _coerce_bridge_dict(raw.get("mcp_remote_bridge") or payload.get("mcp_remote_bridge"))

    if not command and not url:
        return None, {"status": "error", "message": "MCP server must include a command or URL."}

    runtime = {
        "config_id": config_id.strip(),
        "name": name.strip(),
        "command": command,
        "args": args,
        "url": url,
        "env": env,
        "headers": headers,
        "mcp_remote_bridge": bridge,
    }
    return _normalize_mcp_remote_runtime(runtime, agent_id=agent_id), None


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


def _extract_auth_url(event: Dict[str, Any]) -> str:
    for key in ("authorization_url", "auth_url", "url"):
        value = event.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _action_required_response(event: Dict[str, Any]) -> Dict[str, Any]:
    connect_url = _extract_auth_url(event)
    message = "Authorization required. Open the provided connect URL to continue."
    response: Dict[str, Any] = {
        "status": "action_required",
        "message": message,
        "result": message,
        "auth": event,
    }
    if connect_url:
        response["connect_url"] = connect_url
    session_id = event.get("session_id")
    if isinstance(session_id, str) and session_id.strip():
        response["auth_session_id"] = session_id.strip()
    return response


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
    try:
        async with client:
            timeout = _mcp_timeout_seconds()
            return await asyncio.wait_for(client.call_tool(tool_name, params), timeout=timeout)
    except Exception as exc:
        if isinstance(transport, GobiiStdioTransport):
            auth_event = transport.pop_auth_event()
            if auth_event:
                raise MCPRemoteAuthRequired(auth_event) from exc
        raise


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
    try:
        async with client:
            tools = await client.list_tools()
    except Exception as exc:
        if isinstance(transport, GobiiStdioTransport):
            auth_event = transport.pop_auth_event()
            if auth_event:
                raise MCPRemoteAuthRequired(auth_event) from exc
        raise

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

    runtime, runtime_error = _parse_mcp_server_payload(payload, agent_id=agent_id)
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
    except MCPRemoteAuthRequired as auth_required:
        duration_ms = int((time.time() - start) * 1000)
        trace_id, _traceparent = _trace_context(payload)
        response = _action_required_response(auth_required.event)
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
    except MCPRemoteAuthRequired as auth_required:
        response = _action_required_response(auth_required.event)
        response["server_id"] = runtime.get("config_id")
        logger.info(
            "Sandbox discover_mcp_tools server_id=%s status=%s duration_ms=%s trace_id=%s",
            runtime.get("config_id"),
            response.get("status"),
            _elapsed_ms(started_at),
            trace_id,
        )
        return response
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
