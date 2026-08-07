from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urljoin

import aiohttp

logger = logging.getLogger(__name__)


try:
    from gateway.config import Platform, PlatformConfig  # type: ignore
    from gateway.platforms.base import (  # type: ignore
        BasePlatformAdapter,
        MessageEvent,
        MessageType,
        SendResult,
    )
except Exception:
    Platform = lambda name: name  # type: ignore
    PlatformConfig = Any  # type: ignore

    class MessageType:  # pragma: no cover - local fallback
        TEXT = "text"
        COMMAND = "command"

    @dataclass
    class SendResult:  # pragma: no cover - local fallback
        success: bool
        message_id: Optional[str] = None
        error: Optional[str] = None

    @dataclass
    class MessageEvent:  # pragma: no cover - local fallback
        text: str
        message_type: str
        source: Any
        raw_message: Dict[str, Any]
        message_id: Optional[str] = None
        reply_to_message_id: Optional[str] = None
        user_id: Optional[str] = None
        user_name: Optional[str] = None

    class BasePlatformAdapter:  # pragma: no cover - local fallback
        def __init__(self, config: Any, platform: str = "nextcloud") -> None:
            self.config = config
            self.platform = platform

        def build_source(self, **kwargs: Any) -> Dict[str, Any]:
            return kwargs

        async def handle_message(self, event: MessageEvent) -> None:
            return None

        def _mark_disconnected(self) -> None:
            return None


@dataclass
class PendingApproval:
    room_id: str
    requester_user_id: str
    future: asyncio.Future[bool]


@dataclass
class NextcloudRuntimeConfig:
    base_url: str
    username: str
    app_password: str
    bot_handle: str
    require_mention_in_groups: bool
    context_message_limit: int
    poll_interval_seconds: float
    allowed_rooms: set[str] = field(default_factory=set)
    attachment_tmp_dir: str = ""
    hitl_require_requester: bool = True


class NextcloudTalkPlatform(BasePlatformAdapter):
    """Nextcloud Talk adapter with WebSocket-first transport and polling fallback."""

    approve_reactions = {"✅", "👍"}
    reject_reactions = {"❌", "👎"}

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("nextcloud"))
        self.runtime = self._build_runtime_config(config)
        self._session: Optional[aiohttp.ClientSession] = None
        self._stop_event = asyncio.Event()
        self._ws_task: Optional[asyncio.Task[None]] = None
        self._polling_task: Optional[asyncio.Task[None]] = None
        self._poll_cursor_by_room: Dict[str, str] = {}
        self._pending_approvals: Dict[str, PendingApproval] = {}

    @staticmethod
    def _as_bool(value: Any, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _as_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _as_float(value: Any, default: float) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _build_runtime_config(self, config: PlatformConfig) -> NextcloudRuntimeConfig:
        extra = getattr(config, "extra", {}) or {}
        base_url = str(extra.get("base_url") or os.getenv("NEXTCLOUD_BASE_URL", "")).rstrip("/")
        username = str(extra.get("username") or os.getenv("NEXTCLOUD_USERNAME", ""))
        app_password = str(
            extra.get("app_password")
            or getattr(config, "token", "")
            or os.getenv("NEXTCLOUD_APP_PASSWORD", "")
        )
        bot_handle_raw = str(extra.get("bot_handle") or os.getenv("NEXTCLOUD_BOT_HANDLE", "")).strip()
        bot_handle = bot_handle_raw or f"@{username}"
        require_mention = self._as_bool(
            extra.get("require_mention_in_groups") or os.getenv("NEXTCLOUD_REQUIRE_MENTION_IN_GROUPS"),
            True,
        )
        context_limit = self._as_int(
            extra.get("context_message_limit") or os.getenv("NEXTCLOUD_CONTEXT_MESSAGE_LIMIT"),
            20,
        )
        poll_interval = self._as_float(
            extra.get("poll_interval_seconds") or os.getenv("NEXTCLOUD_POLL_INTERVAL_SECONDS"),
            3.0,
        )
        allowed_rooms_raw = extra.get("allowed_rooms") or os.getenv("NEXTCLOUD_ALLOWED_ROOMS", "")
        if isinstance(allowed_rooms_raw, list):
            allowed_rooms = {str(item).strip() for item in allowed_rooms_raw if str(item).strip()}
        else:
            allowed_rooms = {room.strip() for room in str(allowed_rooms_raw).split(",") if room.strip()}
        attachment_tmp_dir = str(extra.get("attachment_tmp_dir") or os.getenv("NEXTCLOUD_ATTACHMENT_TMP_DIR", "")).strip()
        hitl_require_requester = self._as_bool(
            extra.get("hitl_require_requester") or os.getenv("NEXTCLOUD_HITL_REQUIRE_REQUESTER"),
            True,
        )
        return NextcloudRuntimeConfig(
            base_url=base_url,
            username=username,
            app_password=app_password,
            bot_handle=bot_handle,
            require_mention_in_groups=require_mention,
            context_message_limit=max(1, context_limit),
            poll_interval_seconds=max(0.5, poll_interval),
            allowed_rooms=allowed_rooms,
            attachment_tmp_dir=attachment_tmp_dir,
            hitl_require_requester=hitl_require_requester,
        )

    def _authorization_header(self) -> str:
        return aiohttp.encode_basic_auth(self.runtime.username, self.runtime.app_password)

    def _ocs_headers(self) -> Dict[str, str]:
        return {
            "Authorization": self._authorization_header(),
            "OCS-APIRequest": "true",
            "Accept": "application/json",
        }

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    def _ws_url(self) -> str:
        # Placeholder endpoint; tune for concrete HPB deployment.
        return f"{self.runtime.base_url}/apps/spreed/ws"

    def _ocs_url(self, path: str) -> str:
        return urljoin(f"{self.runtime.base_url}/ocs/v2.php/apps/spreed/api/v1/", path.lstrip("/"))

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not self.runtime.base_url or not self.runtime.username or not self.runtime.app_password:
            raise RuntimeError("Nextcloud adapter not configured: base_url/username/app_password required")
        self._stop_event.clear()
        await self._ensure_session()
        ws_connected = await self._connect_websocket_once()
        if not ws_connected:
            self._start_polling_loop()
        return True

    async def disconnect(self) -> None:
        self._stop_event.set()
        tasks = [task for task in (self._ws_task, self._polling_task) if task]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._ws_task = None
        self._polling_task = None
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None
        self._mark_disconnected()

    async def start(self) -> None:
        await self.connect()

    async def stop(self) -> None:
        await self.disconnect()

    async def _connect_websocket_once(self) -> bool:
        session = await self._ensure_session()
        try:
            ws = await session.ws_connect(self._ws_url(), headers=self._ocs_headers(), heartbeat=30)
        except Exception as exc:
            logger.warning("Nextcloud: websocket unavailable, falling back to polling: %s", exc)
            return False
        self._ws_task = asyncio.create_task(self._websocket_loop(ws))
        return True

    async def _websocket_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        try:
            async for msg in ws:
                if self._stop_event.is_set():
                    return
                if msg.type == aiohttp.WSMsgType.TEXT:
                    payload = json.loads(msg.data)
                    await self.handle_incoming_event(payload)
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                    break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Nextcloud websocket loop ended unexpectedly: %s", exc)
        finally:
            if not self._stop_event.is_set():
                self._start_polling_loop()

    def _start_polling_loop(self) -> None:
        if self._polling_task and not self._polling_task.done():
            return
        self._polling_task = asyncio.create_task(self._polling_loop())

    async def _polling_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                room_ids = await self._list_joined_rooms()
                for room_id in room_ids:
                    if self.runtime.allowed_rooms and room_id not in self.runtime.allowed_rooms:
                        continue
                    events = await self._fetch_room_events(room_id)
                    for event in events:
                        await self.handle_incoming_event(event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Nextcloud polling error: %s", exc)
            await asyncio.sleep(self.runtime.poll_interval_seconds)

    async def _list_joined_rooms(self) -> List[str]:
        data = await self._ocs_get("room")
        if isinstance(data, list):
            return [str(room.get("token", "") or room.get("id", "")) for room in data if room]
        return []

    async def _fetch_room_events(self, room_id: str) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"limit": 50}
        if room_id in self._poll_cursor_by_room:
            params["lastKnownMessageId"] = self._poll_cursor_by_room[room_id]
        data = await self._ocs_get(f"chat/{room_id}", params=params)
        events: List[Dict[str, Any]] = []
        if isinstance(data, list):
            for event in data:
                normalized = dict(event)
                normalized.setdefault("room_id", room_id)
                events.append(normalized)
            if data:
                last_id = str(data[-1].get("id", ""))
                if last_id:
                    self._poll_cursor_by_room[room_id] = last_id
        return events

    async def _ocs_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        session = await self._ensure_session()
        async with session.get(self._ocs_url(path), params=params or {}, headers=self._ocs_headers()) as resp:
            body = await resp.json()
        self._raise_for_ocs_error(path, body)
        return self._ocs_data(body)

    async def _ocs_post(self, path: str, data: Dict[str, Any]) -> Any:
        session = await self._ensure_session()
        async with session.post(self._ocs_url(path), data=data, headers=self._ocs_headers()) as resp:
            body = await resp.json()
        self._raise_for_ocs_error(path, body)
        return self._ocs_data(body)

    @staticmethod
    def _ocs_data(body: Dict[str, Any]) -> Any:
        return body.get("ocs", {}).get("data")

    @staticmethod
    def _raise_for_ocs_error(path: str, body: Dict[str, Any]) -> None:
        meta = body.get("ocs", {}).get("meta", {})
        status = str(meta.get("status", "ok")).lower()
        status_code = int(meta.get("statuscode", 100))
        if status != "ok" or status_code >= 400:
            message = meta.get("message", "unknown OCS error")
            raise RuntimeError(f"Nextcloud OCS request failed for {path}: {status_code} {message}")

    async def handle_incoming_event(self, event: Dict[str, Any]) -> None:
        event_type = str(event.get("eventType", event.get("type", "message"))).lower()
        if "reaction" in event_type:
            await self._handle_reaction(event)
            return
        await self._handle_chat_message(event)

    async def _handle_chat_message(self, event: Dict[str, Any]) -> None:
        room_id = str(event.get("room_id") or event.get("token") or "")
        if not room_id:
            return
        if self.runtime.allowed_rooms and room_id not in self.runtime.allowed_rooms:
            return

        message_id = str(event.get("id") or event.get("message_id") or "")
        sender_id = str(
            event.get("actorId")
            or event.get("actor_id")
            or event.get("sender")
            or event.get("userId")
            or ""
        )
        if not sender_id or sender_id == self.runtime.username:
            return

        body = str(event.get("message") or event.get("text") or "")
        attachments = event.get("attachments") or event.get("files") or []
        participant_count = await self._resolve_participant_count(room_id, event)
        if not self._should_trigger(body, participant_count):
            return

        context_messages: List[Dict[str, Any]] = []
        if participant_count > 2:
            context_messages = await self.fetch_last_messages(
                room_id,
                limit=self.runtime.context_message_limit,
            )

        attachment_paths: List[str] = []
        for attachment in attachments:
            path = await self._download_attachment_from_metadata(attachment)
            if path:
                attachment_paths.append(path)

        msg_type = MessageType.COMMAND if body.strip().startswith("/") else MessageType.TEXT
        source = self.build_source(
            chat_id=room_id,
            chat_name=room_id,
            chat_type="dm" if participant_count <= 2 else "group",
            user_id=sender_id,
            user_name=sender_id,
            message_id=message_id or None,
        )

        event_payload = dict(event)
        event_payload["context_messages"] = context_messages
        event_payload["attachment_paths"] = attachment_paths
        msg_event = MessageEvent(
            text=body,
            message_type=msg_type,
            source=source,
            raw_message=event_payload,
            message_id=message_id or None,
            user_id=sender_id,
            user_name=sender_id,
        )
        await self.handle_message(msg_event)

    def _should_trigger(self, body: str, participant_count: int) -> bool:
        if participant_count <= 2:
            return True
        if not self.runtime.require_mention_in_groups:
            return True
        handle = self.runtime.bot_handle.strip()
        if not handle:
            return False
        pattern = rf"(?<!\w){re.escape(handle)}(?!\w)"
        return re.search(pattern, body, flags=re.IGNORECASE) is not None

    async def _resolve_participant_count(self, room_id: str, event: Dict[str, Any]) -> int:
        if "participant_count" in event:
            return int(event["participant_count"])
        participants = event.get("participants")
        if isinstance(participants, list):
            return len(participants)
        api_participants = await self._ocs_get(f"room/{room_id}/participants")
        if isinstance(api_participants, list):
            return len(api_participants)
        return 3

    async def fetch_last_messages(self, room_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        data = await self._ocs_get(f"chat/{room_id}", params={"limit": limit})
        if not isinstance(data, list):
            return []
        messages: List[Dict[str, Any]] = []
        for item in data[-limit:]:
            messages.append(
                {
                    "id": str(item.get("id", "")),
                    "sender_id": str(item.get("actorId", item.get("actor_id", ""))),
                    "text": str(item.get("message", item.get("text", ""))),
                    "timestamp": item.get("timestamp"),
                }
            )
        return messages

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        return await self.send_message(chat_id, content, reply_to, metadata=metadata)

    async def send_message(
        self,
        room_id: str,
        text: str,
        reply_to_message_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if not text:
            return SendResult(success=True)
        payload: Dict[str, Any] = {"message": text}
        if reply_to_message_id:
            payload["replyTo"] = reply_to_message_id
        if metadata:
            payload.update(metadata)
        data = await self._ocs_post(f"chat/{room_id}", payload)
        message_id = None
        if isinstance(data, dict):
            message_id = str(data.get("id", "") or data.get("messageId", "") or "") or None
        return SendResult(success=True, message_id=message_id)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": chat_id, "type": "group"}

    async def _download_attachment_from_metadata(self, attachment: Dict[str, Any]) -> Optional[str]:
        file_id = attachment.get("id") or attachment.get("fileId")
        path = attachment.get("path") or attachment.get("filePath")
        url = attachment.get("url") or attachment.get("downloadUrl")
        return await self.download_attachment(file_id=file_id, remote_path=path, file_url=url)

    async def download_attachment(
        self,
        file_id: Optional[str] = None,
        remote_path: Optional[str] = None,
        file_url: Optional[str] = None,
    ) -> Optional[str]:
        session = await self._ensure_session()
        url = file_url
        if not url and remote_path:
            quoted_path = quote(remote_path.lstrip("/"))
            url = f"{self.runtime.base_url}/remote.php/dav/files/{quote(self.runtime.username)}/{quoted_path}"
        if not url and file_id:
            url = self._ocs_url(f"chat/file/{file_id}")
        if not url:
            return None

        tmp_dir = self.runtime.attachment_tmp_dir or tempfile.gettempdir()
        Path(tmp_dir).mkdir(parents=True, exist_ok=True)
        suffix = Path(str(remote_path or file_id or "attachment")).suffix
        with tempfile.NamedTemporaryFile(delete=False, dir=tmp_dir, suffix=suffix) as tmp:
            tmp_path = tmp.name
        try:
            async with session.get(url, headers=self._ocs_headers()) as resp:
                if resp.status >= 400:
                    raise RuntimeError(f"Attachment download failed with status {resp.status}")
                with open(tmp_path, "wb") as out:
                    out.write(await resp.read())
        except Exception:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
            raise
        return tmp_path

    async def request_human_approval(
        self,
        room_id: str,
        prompt_message_id: str,
        requester_user_id: str,
    ) -> bool:
        future: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self._pending_approvals[prompt_message_id] = PendingApproval(
            room_id=room_id,
            requester_user_id=requester_user_id,
            future=future,
        )
        return await future

    async def _handle_reaction(self, event: Dict[str, Any]) -> None:
        target_message_id = str(
            event.get("targetMessageId")
            or event.get("messageId")
            or event.get("objectId")
            or ""
        )
        if not target_message_id:
            return
        pending = self._pending_approvals.get(target_message_id)
        if not pending:
            return

        reactor_id = str(
            event.get("actorId")
            or event.get("actor_id")
            or event.get("sender")
            or event.get("userId")
            or ""
        )
        emoji = str(event.get("emoji") or event.get("reaction") or event.get("key") or "")
        if self.runtime.hitl_require_requester and reactor_id != pending.requester_user_id:
            logger.info(
                "Nextcloud: ignoring approval reaction from %s; requester is %s",
                reactor_id,
                pending.requester_user_id,
            )
            return
        if emoji in self.approve_reactions:
            if not pending.future.done():
                pending.future.set_result(True)
            self._pending_approvals.pop(target_message_id, None)
            return
        if emoji in self.reject_reactions:
            if not pending.future.done():
                pending.future.set_result(False)
            self._pending_approvals.pop(target_message_id, None)


def nextcloud_deps_present() -> bool:
    return True


def ensure_nextcloud_deps() -> bool:
    return True


def validate_nextcloud_config(config: PlatformConfig) -> bool:
    extra = getattr(config, "extra", {}) or {}
    base_url = extra.get("base_url") or os.getenv("NEXTCLOUD_BASE_URL", "")
    username = extra.get("username") or os.getenv("NEXTCLOUD_USERNAME", "")
    token = extra.get("app_password") or getattr(config, "token", "") or os.getenv("NEXTCLOUD_APP_PASSWORD", "")
    return bool(str(base_url).strip() and str(username).strip() and str(token).strip())


def env_enablement() -> dict | None:
    base_url = os.getenv("NEXTCLOUD_BASE_URL", "").strip()
    username = os.getenv("NEXTCLOUD_USERNAME", "").strip()
    app_password = os.getenv("NEXTCLOUD_APP_PASSWORD", "").strip()
    if not (base_url and username and app_password):
        return None
    seed = {
        "base_url": base_url,
        "username": username,
        "app_password": app_password,
    }
    home = os.getenv("NEXTCLOUD_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = {
            "chat_id": home,
            "name": os.getenv("NEXTCLOUD_HOME_CHANNEL_NAME", "Nextcloud Home"),
        }
    return seed


def apply_yaml_config(yaml_cfg: dict, platform_cfg: dict) -> dict | None:
    if "base_url" in platform_cfg and not os.getenv("NEXTCLOUD_BASE_URL"):
        os.environ["NEXTCLOUD_BASE_URL"] = str(platform_cfg["base_url"])
    if "username" in platform_cfg and not os.getenv("NEXTCLOUD_USERNAME"):
        os.environ["NEXTCLOUD_USERNAME"] = str(platform_cfg["username"])
    if "app_password" in platform_cfg and not os.getenv("NEXTCLOUD_APP_PASSWORD"):
        os.environ["NEXTCLOUD_APP_PASSWORD"] = str(platform_cfg["app_password"])
    if "require_mention_in_groups" in platform_cfg and not os.getenv("NEXTCLOUD_REQUIRE_MENTION_IN_GROUPS"):
        os.environ["NEXTCLOUD_REQUIRE_MENTION_IN_GROUPS"] = str(platform_cfg["require_mention_in_groups"]).lower()
    if "allowed_rooms" in platform_cfg and not os.getenv("NEXTCLOUD_ALLOWED_ROOMS"):
        allowed_rooms = platform_cfg["allowed_rooms"]
        if isinstance(allowed_rooms, list):
            allowed_rooms = ",".join(str(v) for v in allowed_rooms)
        os.environ["NEXTCLOUD_ALLOWED_ROOMS"] = str(allowed_rooms)
    return None


def _build_adapter(config: PlatformConfig) -> NextcloudTalkPlatform:
    return NextcloudTalkPlatform(config)


async def standalone_send(
    pconfig: PlatformConfig,
    chat_id: str,
    content: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
    **_: Any,
) -> Dict[str, Any]:
    adapter = NextcloudTalkPlatform(pconfig)
    try:
        await adapter.connect()
        result = await adapter.send(chat_id, content, reply_to=thread_id)
        if result.success:
            return {"success": True, "message_id": result.message_id}
        return {"error": result.error or "unknown send error"}
    finally:
        await adapter.disconnect()


def register(ctx: Any) -> None:
    """Hermes plugin entry point."""
    ctx.register_platform(
        name="nextcloud",
        label="Nextcloud Talk",
        adapter_factory=_build_adapter,
        check_fn=nextcloud_deps_present,
        ensure_deps_fn=ensure_nextcloud_deps,
        validate_config=validate_nextcloud_config,
        is_connected=validate_nextcloud_config,
        required_env=[
            "NEXTCLOUD_BASE_URL",
            "NEXTCLOUD_USERNAME",
            "NEXTCLOUD_APP_PASSWORD",
        ],
        install_hint="pip install aiohttp",
        env_enablement_fn=env_enablement,
        apply_yaml_config_fn=apply_yaml_config,
        cron_deliver_env_var="NEXTCLOUD_HOME_CHANNEL",
        standalone_sender_fn=standalone_send,
        allowed_users_env="NEXTCLOUD_ALLOWED_USERS",
        allow_all_env="NEXTCLOUD_ALLOW_ALL_USERS",
        platform_hint=(
            "You are chatting via Nextcloud Talk. "
            "In group rooms, mention the bot handle to trigger responses."
        ),
        max_message_length=16000,
        emoji="☁️",
        allow_update_command=True,
    )
