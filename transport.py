"""
QQ Bot WebSocket + REST 传输层

QQ Bot API 分为两个通道:
WebSocket (WSS): 接收事件推送 (Dispatch)、维持心跳 (Heartbeat/ACK)、鉴权 (Identify)、断线重连 (Resume)
REST API (HTTPS): 发送消息、上传媒体等出站操作

本模块的 QQBotTransportClient 同时管理这两个通道。

Made BY Galeros

"""

from __future__ import annotations

from typing import Any, Callable, Coroutine, Dict, Optional, Set

import asyncio
import base64
import contextlib
import json
import time

try:
    from aiohttp import ClientSession, ClientTimeout, WSMsgType

    AIOHTTP_AVAILABLE = True
except ImportError:
    ClientSession = None
    ClientTimeout = None
    WSMsgType = None
    AIOHTTP_AVAILABLE = False

from .config import QQBotConnectionConfig
from .constants import (
    QQBOT_TOKEN_URL,
    OP_DISPATCH,
    OP_HEARTBEAT,
    OP_IDENTIFY,
    OP_RESUME,
    OP_RECONNECT,
    OP_INVALID_SESSION,
    OP_HELLO,
    OP_HEARTBEAT_ACK,
    WSS_ERR_INVALID_OPCODE,
    WSS_ERR_INVALID_PAYLOAD,
    WSS_ERR_INVALID_SESSION,
    WSS_ERR_SEQ_MISMATCH,
    WSS_ERR_RATE_LIMITED,
    WSS_ERR_SESSION_EXPIRED,
    WSS_ERR_INVALID_SHARD,
    WSS_ERR_SHARD_OVERLOAD,
    WSS_ERR_INVALID_VERSION,
    WSS_ERR_INVALID_INTENT,
    WSS_ERR_INTENT_NO_PERM,
    WSS_ERR_INTERNAL_START,
    WSS_ERR_INTERNAL_END,
    WSS_ERR_BOT_DISABLED,
    WSS_ERR_BOT_BANNED,
)

# 心跳默认值
_DEFAULT_HEARTBEAT_MS = 41250
# Rate limit 重试最大次数
_MAX_RATE_LIMIT_RETRIES = 3


class QQBotTransportClient:
    """QQ Bot WebSocket 客户端 + REST API 客户端

    连接生命周期:
    1. WSS 连接 -> Hello(op=10) -> Identify(op=2) -> Ready(Dispatch)
    2. 心跳循环 (op=1/11)
    3. Dispatch 事件 -> 回调 on_dispatch
    4. 断开 -> 自动重连 (Resume 优先)

    """

    def __init__(
        self,
        logger: Any,
        on_connection_opened: Callable[[], Coroutine[Any, Any, None]],
        on_connection_closed: Callable[[], Coroutine[Any, Any, None]],
        on_dispatch: Callable[[Dict[str, Any], str], Coroutine[Any, Any, None]],
    ) -> None:
        """初始化传输客户端

        Args:
            logger: 插件日志对象。
            on_connection_opened: WSS 鉴权完成后的回调。
            on_connection_closed: WSS 断开后的回调。
            on_dispatch: 收到 Dispatch 事件时的回调 ``(event_data, event_type)``。

        """
        self._logger = logger
        self._on_connection_opened = on_connection_opened
        self._on_connection_closed = on_connection_closed
        self._on_dispatch = on_dispatch

        self._config: Optional[QQBotConnectionConfig] = None
        self._connection_task: Optional[asyncio.Task[None]] = None
        self._ws: Any = None
        self._send_lock = asyncio.Lock()
        self._stop_requested: bool = False
        self._connection_active: bool = False

        # Token 管理
        self._access_token: str = ""
        self._token_expires_at: float = 0.0
        self._token_lock = asyncio.Lock()

        # Session 状态 (用于 Resume)
        self._session_id: str = ""
        self._last_received_seq: int = 0
        self._last_processed_seq: int = 0

        # 心跳
        self._heartbeat_interval_ms: int = _DEFAULT_HEARTBEAT_MS
        self._heartbeat_task: Optional[asyncio.Task[None]] = None
        self._last_heartbeat_ack_at: float = 0.0

        # REST session
        self._rest_session: Any = None

        # 后台任务追踪
        self._background_tasks: Set[asyncio.Task[Any]] = set()

    # -- 类方法 --

    @classmethod
    def is_available(cls) -> bool:
        """判断当前环境是否安装了 aiohttp """
        return AIOHTTP_AVAILABLE

    # -- 配置 & 启动 --

    def configure(self, config: QQBotConnectionConfig) -> None:
        """更新传输层配置

        Args:
            config: QQ Bot 连接配置
        
        """
        self._config = config

    async def start(self) -> None:
        """启动 WSS 连接循环"""
        if not self.is_available():
            raise RuntimeError("QQ Bot 适配器依赖 aiohttp，当前环境未安装")
        if self._config is None:
            raise RuntimeError("QQ Bot 适配器尚未配置 qqbot")
        if self._connection_task is not None and not self._connection_task.done():
            return

        self._stop_requested = False
        self._connection_task = asyncio.create_task(
            self._connection_loop(), name="qqbot_adapter.connection"
        )

    async def stop(self) -> None:
        """停止连接并清理所有资源"""
        self._stop_requested = True
        connection_task = self._connection_task
        self._connection_task = None

        await self._stop_heartbeat()

        ws = self._ws
        if ws is not None and not ws.closed:
            with contextlib.suppress(Exception):
                await ws.close()
        self._ws = None

        if connection_task is not None:
            connection_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await connection_task

        await self._cancel_background_tasks()

        rest_session = self._rest_session
        if rest_session is not None:
            with contextlib.suppress(Exception):
                await rest_session.close()
            self._rest_session = None

        await self._notify_connection_closed()

    # ==== REST API ====

    async def _ensure_token(self) -> str:
        """获取或刷新 access_token，带锁防并发。

        Returns:
            str: 当前有效的 access_token。

        Raises:
            RuntimeError: 当 token 获取失败时抛出。
        """
        async with self._token_lock:
            now = time.time()
            if self._access_token and (now + (self._config.token_refresh_before_sec if self._config else 300)) < self._token_expires_at:
                return self._access_token

            config = self._config
            if config is None:
                raise RuntimeError("QQ Bot 尚未配置")

            self._logger.debug("正在获取 QQ Bot access_token...")
            try:
                async with self._get_rest_session() as session:
                    async with session.post(
                        QQBOT_TOKEN_URL,
                        json={"appId": config.app_id, "clientSecret": config.app_secret},
                        timeout=ClientTimeout(total=15),
                    ) as resp:
                        data = await resp.json()
            except Exception as exc:
                raise RuntimeError(f"获取 QQ Bot access_token 失败: {exc}") from exc

            access_token = str(data.get("access_token") or "").strip()
            expires_in = data.get("expires_in", 7200)
            if not access_token:
                raise RuntimeError(f"QQ Bot API 未返回 access_token: {data}")

            self._access_token = access_token
            self._token_expires_at = now + int(expires_in)
            self._logger.info(
                "QQ Bot access_token 已更新，有效期 %d 秒",
                int(expires_in),
            )
            return self._access_token

    def _build_auth_header(self) -> Dict[str, str]:
        """构造 Authorization 请求头。

        Returns:
            Dict[str, str]: 含 ``Authorization`` 头的字典。
        """
        return {"Authorization": f"QQBot {self._access_token}"}

    async def post_c2c_message(
        self,
        openid: str,
        content: str = "",
        msg_type: int = 0,
        msg_id: str = "",
        media: Optional[Dict[str, Any]] = None,
        msg_seq: int = 0,
    ) -> Dict[str, Any]:
        """发送 C2C 私聊消息。

        POST /v2/users/{openid}/messages

        Args:
            openid: 接收者 user_openid。
            content: 消息文本（msg_type=0 时使用）。
            msg_type: 消息类型 (0=text, 7=media)。
            msg_id: 被回复的消息 ID（可选）。
            media: 媒体文件信息（msg_type=7 时使用）。
            msg_seq: 消息序号。

        Returns:
            Dict[str, Any]: API 响应。
        """
        await self._ensure_token()
        url = f"{self._config.base_url()}/v2/users/{openid}/messages"
        body: Dict[str, Any] = {"msg_type": msg_type}
        if content:
            body["content"] = content
        if msg_id:
            body["msg_id"] = msg_id
        if media:
            body["media"] = media
        if msg_seq:
            body["msg_seq"] = msg_seq
        return await self._post_json(url, body)

    async def post_group_message(
        self,
        group_openid: str,
        content: str = "",
        msg_type: int = 0,
        msg_id: str = "",
        media: Optional[Dict[str, Any]] = None,
        msg_seq: int = 0,
    ) -> Dict[str, Any]:
        """发送群聊消息。

        POST /v2/groups/{group_openid}/messages

        Args:
            group_openid: 目标群 openid。
            content: 消息文本。
            msg_type: 消息类型。
            msg_id: 被回复的消息 ID。
            media: 媒体文件信息。
            msg_seq: 消息序号。

        Returns:
            Dict[str, Any]: API 响应。
        """
        await self._ensure_token()
        url = f"{self._config.base_url()}/v2/groups/{group_openid}/messages"
        body: Dict[str, Any] = {"msg_type": msg_type}
        if content:
            body["content"] = content
        if msg_id:
            body["msg_id"] = msg_id
        if media:
            body["media"] = media
        if msg_seq:
            body["msg_seq"] = msg_seq
        return await self._post_json(url, body)

    async def upload_media(
        self,
        file_type: int,
        file_data: bytes,
        *,
        is_group: bool,
        target_openid: str,
    ) -> Dict[str, Any]:
        """上传媒体文件。

        C2C: POST /v2/users/{openid}/files
        群:  POST /v2/groups/{group_openid}/files

        Args:
            file_type: 文件类型 (1=image, 2=video, 3=voice)。
            file_data: 文件二进制数据。
            is_group: 是否为群聊上传。
            target_openid: C2C 时为 user_openid，群聊时为 group_openid。

        Returns:
            Dict[str, Any]: 含 ``file_info`` 的 API 响应。
        """
        await self._ensure_token()
        base_url = self._config.base_url()
        if is_group:
            url = f"{base_url}/v2/groups/{target_openid}/files"
        else:
            url = f"{base_url}/v2/users/{target_openid}/files"

        body: Dict[str, Any] = {
            "file_type": file_type,
            "file_data": base64.b64encode(file_data).decode("utf-8"),
            "srv_send_msg": False,
        }
        return await self._post_json(url, body)

    def _get_rest_session(self) -> Any:
        """获取或创建 REST API 的 aiohttp session。"""
        if self._rest_session is None or self._rest_session.closed:
            self._rest_session = ClientSession()
        return self._rest_session

    async def _post_json(self, url: str, body: Dict[str, Any]) -> Dict[str, Any]:
        """发送 JSON POST 请求，带重试和错误处理。"""
        headers = self._build_auth_header()
        headers["Content-Type"] = "application/json"

        for attempt in range(1 + _MAX_RATE_LIMIT_RETRIES):
            try:
                async with self._get_rest_session() as session:
                    async with session.post(
                        url,
                        json=body,
                        headers=headers,
                        timeout=ClientTimeout(total=self._config.action_timeout_sec if self._config else 15),
                    ) as resp:
                        data = await resp.json()
            except Exception as exc:
                self._logger.error("QQ Bot REST API 请求异常 (%s): %s", url, exc)
                if attempt < _MAX_RATE_LIMIT_RETRIES:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return {"success": False, "error": str(exc)}

            if resp.status == 429:
                retry_after = float(resp.headers.get("Retry-After", 2 ** attempt))
                self._logger.warning("QQ Bot API 限流 (429)，%d 秒后重试 (第 %d 次)", int(retry_after), attempt + 1)
                if attempt < _MAX_RATE_LIMIT_RETRIES:
                    await asyncio.sleep(retry_after)
                    continue

            if resp.status >= 500:
                if attempt < _MAX_RATE_LIMIT_RETRIES:
                    await asyncio.sleep(2 ** attempt)
                    continue

            if 200 <= resp.status < 300:
                return data

            self._logger.error("QQ Bot API 错误 (%d): %s", resp.status, data)
            return {"success": False, "error": str(data), "status_code": resp.status}

        return {"success": False, "error": "Max retries exceeded"}

    # ==== WSS 连接循环 ====

    async def _connection_loop(self) -> None:
        """外层重连循环。"""
        assert ClientSession is not None
        assert ClientTimeout is not None

        while not self._stop_requested:
            config = self._config
            if config is None:
                return

            ws_url = config.ws_url()
            self._logger.info("QQ Bot 开始连接 WSS: %s", ws_url)

            try:
                timeout = ClientTimeout(total=None, connect=15)
                async with ClientSession(timeout=timeout) as session:
                    async with session.ws_connect(ws_url) as ws:
                        self._ws = ws
                        self._logger.info("QQ Bot WSS 已连接: %s", ws_url)
                        reason = await self._gateway_loop(ws)
                        self._logger.warning("QQ Bot WSS 断开: %s，原因: %s", ws_url, reason)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._logger.warning(
                    "QQ Bot WSS 连接异常: %s；%s 秒后重连",
                    exc,
                    config.reconnect_delay_sec,
                )
            finally:
                self._ws = None
                await self._stop_heartbeat()
                await self._notify_connection_closed()

            if self._stop_requested:
                break
            self._logger.info("QQ Bot 将在 %.1f 秒后重连", config.reconnect_delay_sec)
            await asyncio.sleep(config.reconnect_delay_sec)

    async def _gateway_loop(self, ws: Any) -> str:
        """WSS 协议主循环。

        流程:
        1. 等待 Hello (op=10)
        2. 发送 Identify (op=2) 或 Resume (op=6)
        3. 进入事件循环处理 Dispatch、Heartbeat、Reconnect、Invalid Session、Hello 与 Heartbeat ACK

        Args:
            ws: aiohttp ClientWebSocketResponse。

        Returns:
            str: 断开原因。
        """
        assert WSMsgType is not None

        reason = "未知原因"
        try:
            async for ws_message in ws:
                if ws_message.type == WSMsgType.CLOSE or ws_message.type == WSMsgType.CLOSED:
                    reason = self._describe_close(ws)
                    break
                if ws_message.type == WSMsgType.ERROR:
                    reason = self._describe_error(ws)
                    break
                if ws_message.type != WSMsgType.TEXT:
                    continue

                payload = self._parse_json(ws_message.data)
                if payload is None:
                    continue

                op = int(payload.get("op", -1))
                d = payload.get("d")
                s = payload.get("s")
                t = payload.get("t", "")

                if s is not None:
                    self._last_received_seq = int(s)

                self._logger.debug(
                    "QQ Bot 收到帧: op=%s, t=%s, s=%s", op, t or "-", s
                )

                if op == OP_HELLO:
                    # 服务端下发心跳间隔 (毫秒)
                    if isinstance(d, dict):
                        self._heartbeat_interval_ms = int(d.get("heartbeat_interval", _DEFAULT_HEARTBEAT_MS))
                    self._logger.debug("收到 Hello，心跳间隔 %d ms", self._heartbeat_interval_ms)

                    if self._session_id:
                        # 有 session_id，尝试 Resume
                        await self._send_resume()
                    else:
                        await self._send_identify()
                elif op == OP_DISPATCH:
                    if t == "READY":
                        session_id = ""
                        if isinstance(d, dict):
                            session_id = str(d.get("session_id") or "")
                        self._session_id = session_id
                        self._logger.info("QQ Bot Ready (session=%s...)", session_id[:8] if session_id else "")
                        # 启动心跳
                        await self._start_heartbeat()
                        # 通知上层
                        await self._notify_connection_opened()
                    else:
                        if t == "RESUMED":
                            self._logger.info("QQ Bot 会话恢复成功，启动心跳")
                            await self._start_heartbeat()
                            await self._notify_connection_opened()
                        # 普通事件
                        if isinstance(d, dict):
                            event_seq = self._last_received_seq
                            self._create_background_task(
                                self._dispatch_and_record_seq(d, t, event_seq),
                                f"qqbot_adapter.dispatch.{t}",
                            )

                elif op == OP_HEARTBEAT:
                    # 服务端要求回复心跳 (发送 op=1 with latest seq)
                    await self._send_heartbeat()

                elif op == OP_HEARTBEAT_ACK:
                    self._last_heartbeat_ack_at = time.time()
                    self._logger.debug("QQ Bot 收到心跳 ACK (op=11)")

                elif op == OP_RECONNECT:
                    self._logger.warning("QQ Bot 服务端要求重连 (op=7)，关闭当前连接重新握手")
                    reason = "服务端要求重连 (op=7)"
                    # 关闭当前连接；保留 session_id 以便下一次 Hello 后优先 Resume。
                    break

                elif op == OP_INVALID_SESSION:
                    self._logger.warning(
                        "QQ Bot 收到 op=9 Invalid Session 原始负载: %s",
                        json.dumps(payload, ensure_ascii=False),
                    )
                    # 读取 QQ 服务端返回的错误详情
                    code = 0
                    error_msg = ""
                    if isinstance(d, dict):
                        code = int(d.get("code") or 0)
                        error_msg = str(d.get("message") or d.get("error") or "")
                    elif isinstance(d, str):
                        error_msg = d

                    # 按官方文档分类处理:
                    #   4008/4009 → 可 Resume
                    #   4006/4007/4900~4913 → 需重新 Identify
                    #   4914/4915 → 不可重连（下架/封禁）
                    #   其他 → 清空 session_id，下一次连接重新 Identify
                    if code in (WSS_ERR_RATE_LIMITED, WSS_ERR_SESSION_EXPIRED):
                        self._logger.info(
                            "QQ Bot 连接可恢复错误 (%d)，将保留会话并 Resume", code
                        )
                        reason = f"可恢复连接错误 ({code})"
                        break
                    elif code == WSS_ERR_INVALID_SESSION or code == WSS_ERR_SEQ_MISMATCH:
                        self._logger.warning("QQ Bot 会话/seq 无效 (%d)，重新 Identify", code)
                        self._session_id = ""
                        reason = f"会话无效 ({code})"
                        break
                    elif WSS_ERR_INTERNAL_START <= code <= WSS_ERR_INTERNAL_END:
                        self._logger.warning("QQ Bot 内部错误 (%d)，断开重连", code)
                        self._session_id = ""
                        reason = f"内部错误 ({code})"
                        break
                    elif code in (WSS_ERR_INVALID_OPCODE, WSS_ERR_INVALID_PAYLOAD,
                                  WSS_ERR_INVALID_SHARD, WSS_ERR_SHARD_OVERLOAD,
                                  WSS_ERR_INVALID_VERSION, WSS_ERR_INVALID_INTENT,
                                  WSS_ERR_INTENT_NO_PERM):
                        self._logger.error(
                            "QQ Bot WSS 配置错误 (code=%d): %s "
                            "(intents=%d, shard=[%d/%d], env=%s)",
                            code, error_msg,
                            self._config.intents if self._config else 0,
                            self._config.shard_index if self._config else 0,
                            self._config.shard_count if self._config else 1,
                            "沙箱" if (self._config and self._config.use_sandbox) else "生产",
                        )
                        reason = f"配置错误 ({code})"
                        self._stop_requested = True
                        break
                    elif code in (WSS_ERR_BOT_DISABLED, WSS_ERR_BOT_BANNED):
                        self._logger.error(
                            "QQ Bot 已被%s (code=%d)，无法继续",
                            "下架" if code == WSS_ERR_BOT_DISABLED else "封禁",
                            code,
                        )
                        reason = f"Bot 不可用 ({code})"
                        self._stop_requested = True
                        break
                    else:
                        self._logger.warning(
                            "QQ Bot 会话无效 (op=9, code=%d): %s "
                            "(intents=%d, shard=[%d/%d], env=%s)",
                            code, error_msg,
                            self._config.intents if self._config else 0,
                            self._config.shard_index if self._config else 0,
                            self._config.shard_count if self._config else 1,
                            "沙箱" if (self._config and self._config.use_sandbox) else "生产",
                        )
                        self._session_id = ""
                        reason = f"会话无效 (code={code})"
                        break

                else:
                    self._logger.debug("未知 op=%d, t=%s", op, t)

            # async for 自然结束：aiohttp 对 CLOSE/CLOSING/CLOSED 会抛 StopAsyncIteration，
            # 这些帧不会进入上面的循环体，因此在这里补打服务端关连接时的 close_code。
            # 仅当 reason 仍是初始值（未走任何显式 break）时才补充，避免覆盖 op=7/op=9 的原因。
            if reason == "未知原因":
                close_code = getattr(ws, "close_code", None)
                self._logger.warning(
                    "QQ Bot WSS 迭代结束（对端关闭）: close_code=%s, 收到过READY=%s, 最近received_seq=%s",
                    close_code, self._connection_active, self._last_received_seq,
                )
                reason = f"对端关闭连接 (close_code={close_code}, ready={self._connection_active})"

        except asyncio.CancelledError:
            reason = "任务被取消"
        except Exception as exc:
            reason = f"异常: {exc}"

        return reason

    async def _dispatch_and_record_seq(
        self, event_data: Dict[str, Any], event_type: str, event_seq: int,
    ) -> None:
        """分发事件，并在处理完成后记录可用于 Resume 的序列号。"""
        await self._on_dispatch(event_data, event_type)
        if event_seq > self._last_processed_seq:
            self._last_processed_seq = event_seq

    # ==== WSS 协议操作 ====

    async def _send_json(self, payload: Dict[str, Any]) -> None:
        """通过 WSS 发送 JSON 消息，带锁防并发。"""
        ws = self._ws
        if ws is None or ws.closed:
            self._logger.warning("WSS 未连接，无法发送")
            return
        data = json.dumps(payload, ensure_ascii=False)
        async with self._send_lock:
            await ws.send_str(data)

    async def _send_identify(self) -> None:
        """发送 Identify (op=2)。

        payload:
        {
          "op": 2,
          "d": {
            "token": "QQBot {access_token}",
            "intents": <intents>,
            "shard": [<index>, <total>],
            "properties": {"$os": "linux", "$browser": "maibot", "$device": "maibot"}
          }
        }
        """
        try:
            await self._ensure_token()
        except RuntimeError as exc:
            self._logger.error("Identify 前获取 token 失败: %s", exc)
            return

        config = self._config
        if config is None:
            raise RuntimeError("QQ Bot 尚未配置")

        payload = {
            "op": OP_IDENTIFY,
            "d": {
                "token": f"QQBot {self._access_token}",
                "intents": config.intents,
                "shard": [config.shard_index, config.shard_count],
                "properties": {
                    "$os": "linux",
                    "$browser": "maibot",
                    "$device": "maibot",
                },
            },
        }
        self._logger.debug("发送 Identify (intents=%d, shard=[%d/%d])", config.intents, config.shard_index, config.shard_count)
        await self._send_json(payload)
        self._logger.info(
            "QQ Bot 已发送 Identify（access_token长度=%d, intents=%d, shard=[%d/%d], env=%s），等待 READY...",
            len(self._access_token), config.intents,
            config.shard_index, config.shard_count,
            "沙箱" if config.use_sandbox else "生产",
        )

    async def _send_resume(self) -> None:
        """发送 Resume (op=6)。

        {
          "op": 6,
          "d": {
            "token": "QQBot {AccessToken}",
            "session_id": "<session_id>",
            "seq": <last_seq>
          }
        }
        """
        if not self._session_id:
            self._logger.debug("无 session_id，回退到 Identify")
            await self._send_identify()
            return

        try:
            await self._ensure_token()
        except RuntimeError as exc:
            self._logger.error("Resume 前获取 token 失败: %s", exc)
            return

        payload = {
            "op": OP_RESUME,
            "d": {
                "token": f"QQBot {self._access_token}",
                "session_id": self._session_id,
                "seq": self._last_processed_seq,
            },
        }
        self._logger.info(
            "发送 Resume (session=%s..., seq=%d)",
            self._session_id[:8],
            self._last_processed_seq,
        )
        await self._send_json(payload)

    async def _send_heartbeat(self) -> None:
        """发送 Heartbeat (op=1)。

        {"op": 1, "d": <last_seq>}
        """
        payload = {
            "op": OP_HEARTBEAT,
            "d": self._last_received_seq if self._last_received_seq > 0 else None,
        }
        await self._send_json(payload)

    # ==== 心跳管理 ====

    async def _start_heartbeat(self) -> None:
        """启动心跳定时循环。"""
        await self._stop_heartbeat()
        self._last_heartbeat_ack_at = time.time()
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name="qqbot_adapter.heartbeat"
        )

    async def _stop_heartbeat(self) -> None:
        """停止心跳循环。"""
        task = self._heartbeat_task
        self._heartbeat_task = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _heartbeat_loop(self) -> None:
        """心跳定时循环。

        每半个 Hello 心跳周期发送一次 Heartbeat (op=1)。
        若超过 2.5 个心跳周期未收到 ACK，则关闭连接以触发重连。
        """
        interval_s = self._heartbeat_interval_ms / 1000.0
        while not self._stop_requested:
            await asyncio.sleep(max(interval_s * 0.5, 1.0))
            ws = self._ws
            if ws is None or ws.closed:
                return

            elapsed_since_ack = time.time() - self._last_heartbeat_ack_at
            if elapsed_since_ack > interval_s * 2.5:
                self._logger.warning(
                    "QQ Bot 心跳超时: %d 秒未收到 ACK",
                    int(elapsed_since_ack),
                )
                # 触发重连
                with contextlib.suppress(Exception):
                    await ws.close()
                return

            await self._send_heartbeat()

    # ==== 工具函数 ====

    def _create_background_task(self, coro: Coroutine[Any, Any, Any], name: str) -> asyncio.Task[Any]:
        """创建并追踪后台任务。"""
        task = asyncio.create_task(coro, name=name)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    async def _cancel_background_tasks(self) -> None:
        """取消所有后台任务。"""
        tasks = list(self._background_tasks)
        for t in tasks:
            t.cancel()
        if tasks:
            with contextlib.suppress(Exception):
                await asyncio.gather(*tasks, return_exceptions=True)
        self._background_tasks.clear()

    async def _notify_connection_opened(self) -> None:
        """通知上层 WSS 已就绪。"""
        if self._connection_active:
            return
        self._connection_active = True
        try:
            await self._on_connection_opened()
        except Exception as exc:
            self._logger.warning("连接就绪回调失败: %s", exc)

    async def _notify_connection_closed(self) -> None:
        """通知上层 WSS 已断开。"""
        if not self._connection_active:
            return
        self._connection_active = False
        try:
            await self._on_connection_closed()
        except Exception as exc:
            self._logger.warning("断连回调失败: %s", exc)

    def _parse_json(self, data: Any) -> Optional[Dict[str, Any]]:
        """安全解析 JSON。"""
        try:
            result = json.loads(str(data))
        except Exception as exc:
            self._logger.warning("QQ Bot JSON 解析失败: %s", exc)
            return None
        return result if isinstance(result, dict) else None

    @staticmethod
    def _describe_close(ws: Any) -> str:
        """描述 CLOSE 帧信息。"""
        code = getattr(ws, "close_code", None)
        return f"收到 CLOSE 帧 (code={code})" if code else "收到 CLOSE 帧"

    def _describe_error(self, ws: Any) -> str:
        """描述 ERROR 帧信息。"""
        exc = ws.exception()
        return f"WebSocket 错误 (exception={exc})" if exc else "WebSocket 错误"
