import asyncio
import contextlib
import json
import logging
import os
import sys
import time
from typing import Any, Dict, Optional, Tuple

from fastmcp import Client
from fastmcp.client.transports import StdioTransport as FastMCPStdioTransport
from fastmcp.client.transports import StreamableHttpTransport
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from sandbox_compute_server.config import _agent_workspace, _mcp_timeout_seconds
from sandbox_compute_server.manifest import _proxy_env_from_manifest, _store_proxy_env
from sandbox_compute_server.workspace import _elapsed_ms, _require_agent_id, _trace_context

logger = logging.getLogger(__name__)


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
