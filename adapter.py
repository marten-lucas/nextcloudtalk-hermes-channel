from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .attachments import NextcloudAttachmentManager
from .client import NextcloudTalkClient
from .hitl import HITLManager
from .identity import NextcloudIdentityManager
from .presence import NextcloudPresenceManager
from .signaling import NextcloudSignalingManager

logger = logging.getLogger(__name__)

try:
    from gateway.config import Platform, PlatformConfig  # type: ignore
    from gateway.platforms.base import (  # type: ignore
        BasePlatformAdapter,
        MessageEvent,
        MessageType,
        SendResult,
    )
    from gateway.session import build_session_key  # type: ignore
except Exception:
    Platform = lambda name: name  # type: ignore
    PlatformConfig = Any  # type: ignore
    build_session_key = None  # type: ignore

    class MessageType:
        TEXT = "text"
        COMMAND = "command"

    @dataclass
    class SendResult:
        success: bool
        message_id: Optional[str] = None
        error: Optional[str] = None

    @dataclass
    class MessageEvent:
        text: str
        message_type: str
        source: Any
        raw_message: Dict[str, Any]
        message_id: Optional[str] = None
        reply_to_message_id: Optional[str] = None
        user_id: Optional[str] = None
        user_name: Optional[str] = None

    class BasePlatformAdapter:
        def __init__(self, config: Any, platform: str = "nextcloud") -> None:
            self.config = config
            self.platform = platform

        def build_source(self, **kwargs: Any) -> Dict[str, Any]:
            return kwargs

        async def handle_message(self, event: MessageEvent) -> None:
            return None

        async def cancel_session_processing(self, session_key: str, **_: Any) -> None:
            return None

        def _mark_disconnected(self) -> None:
            return None


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
    """Refactored Nextcloud Talk Adapter mit modularer Submodul-Struktur."""

    supports_status_text = True

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("nextcloud"))
        extra = getattr(config, "extra", {}) or {}

        base_url = str(extra.get("base_url") or os.getenv("NEXTCLOUD_BASE_URL", "")).rstrip("/")
        username = str(extra.get("username") or os.getenv("NEXTCLOUD_USERNAME", ""))
        app_password = str(
            extra.get("app_password")
            or getattr(config, "token", "")
            or os.getenv("NEXTCLOUD_APP_PASSWORD", "")
        )

        self.runtime = NextcloudRuntimeConfig(
            base_url=base_url,
            username=username,
            app_password=app_password,
            bot_handle=str(extra.get("bot_handle") or os.getenv("NEXTCLOUD_BOT_HANDLE", "")).strip()
            or f"@{username}",
            require_mention_in_groups=str(
                extra.get("require_mention_in_groups")
                or os.getenv("NEXTCLOUD_REQUIRE_MENTION_IN_GROUPS", "true")
            ).lower()
            in {"1", "true", "yes"},
            context_message_limit=int(
                extra.get("context_message_limit") or os.getenv("NEXTCLOUD_CONTEXT_MESSAGE_LIMIT", 20)
            ),
            poll_interval_seconds=float(
                extra.get("poll_interval_seconds") or os.getenv("NEXTCLOUD_POLL_INTERVAL_SECONDS", 3.0)
            ),
        )

        self.client = NextcloudTalkClient(base_url, username, app_password)
        self.identity_mgr = NextcloudIdentityManager(self.client, cache_ttl_seconds=120)
        self.hitl_mgr = HITLManager(enforce_requester_only=self.runtime.hitl_require_requester)
        self.presence_mgr = NextcloudPresenceManager(self.client)
        self.attachment_mgr = NextcloudAttachmentManager(
            self.client, tmp_dir=self.runtime.attachment_tmp_dir
        )
        self.signaling_mgr = NextcloudSignalingManager(self.client, self.handle_incoming_event)

        self._stop_event = asyncio.Event()
        self._polling_task: Optional[asyncio.Task[None]] = None
        self._poll_cursor_by_room: Dict[str, str] = {}
        self._poll_bootstrapped_rooms: set[str] = set()

    @property
    def is_connected(self) -> bool:
        return not self._stop_event.is_set()

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        self._stop_event.clear()
        await self.client.ensure_session()
        await self.presence_mgr.set_presence_status("online")
        await self.presence_mgr.clear_custom_status_message(force=True)

        self._polling_task = asyncio.create_task(self._polling_loop())
        logger.info("Nextcloud Talk: Adapter erfolgreich verbunden.")
        return True

    async def disconnect(self) -> None:
        self._stop_event.set()
        if self._polling_task:
            self._polling_task.cancel()
        await self.presence_mgr.set_presence_status("offline")
        await self.client.close()
        self._mark_disconnected()

    async def _polling_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                rooms = await self.client.ocs_get(
                    "apps/spreed/api/v4/room", params={"includeStatus": "true"}
                )
                if isinstance(rooms, list):
                    for room in rooms:
                        room_id = str(room.get("token", "") or room.get("id", ""))
                        if room_id:
                            events = await self._fetch_room_events(room_id)
                            for event in events:
                                await self.handle_incoming_event(event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Nextcloud Polling Fehler: %s", exc)
            await asyncio.sleep(self.runtime.poll_interval_seconds)

    async def _fetch_room_events(self, room_id: str) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"lookIntoFuture": 0, "limit": 50}
        if room_id in self._poll_cursor_by_room:
            params["lookIntoFuture"] = 1
            params["lastKnownMessageId"] = self._poll_cursor_by_room[room_id]

        data = await self.client.ocs_get(f"apps/spreed/api/v1/chat/{room_id}", params=params)
        events: List[Dict[str, Any]] = []
        if isinstance(data, list):
            for event in data:
                normalized = dict(event)
                normalized.setdefault("room_id", room_id)
                events.append(normalized)
                self._poll_cursor_by_room[room_id] = str(event.get("id", ""))
        return events

    async def handle_incoming_event(self, event: Dict[str, Any]) -> None:
        sender_id = str(event.get("actorId") or event.get("actor_id") or event.get("sender") or "")
        if not sender_id or sender_id == self.runtime.username:
            return

        # 1. Gruppen mit TTL-Cache laden
        groups = await self.identity_mgr.get_user_groups(sender_id)

        # 2. ContextVars für Outbound HTTP MCP/Honcho injizieren
        self.identity_mgr.set_contextvars_identity(sender_id, groups)

        room_id = str(event.get("room_id") or event.get("token") or "")
        body = str(event.get("message") or event.get("text") or "")

        source = self.build_source(
            chat_id=room_id,
            chat_name=room_id,
            chat_type="group",
            user_id=sender_id,
            user_name=sender_id,
        )
        if isinstance(source, dict):
            source["extra_headers"] = {
                "X-On-Behalf-Of": sender_id,
                "X-User-Groups": ",".join(groups),
            }

        msg_event = MessageEvent(
            text=body,
            message_type=MessageType.TEXT,
            source=source,
            raw_message=event,
            message_id=str(event.get("id") or ""),
            user_id=sender_id,
            user_name=sender_id,
        )
        await self.handle_message(msg_event)

    async def send_message(
        self,
        room_id: str,
        text: str,
        reply_to_message_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        payload: Dict[str, Any] = {"message": text}
        if reply_to_message_id:
            payload["replyTo"] = reply_to_message_id
        data = await self.client.ocs_post(f"apps/spreed/api/v1/chat/{room_id}", payload)
        msg_id = str(data.get("id", "")) if isinstance(data, dict) else None
        return SendResult(success=True, message_id=msg_id)


def validate_nextcloud_config(config: PlatformConfig) -> bool:
    extra = getattr(config, "extra", {}) or {}
    base_url = extra.get("base_url") or os.getenv("NEXTCLOUD_BASE_URL", "")
    username = extra.get("username") or os.getenv("NEXTCLOUD_USERNAME", "")
    token = (
        extra.get("app_password")
        or getattr(config, "token", "")
        or os.getenv("NEXTCLOUD_APP_PASSWORD", "")
    )
    return bool(str(base_url).strip() and str(username).strip() and str(token).strip())


def check_is_connected(adapter_or_config: Any) -> bool:
    if hasattr(adapter_or_config, "is_connected"):
        return bool(adapter_or_config.is_connected)
    return validate_nextcloud_config(adapter_or_config)


def _build_adapter(config: PlatformConfig) -> NextcloudTalkPlatform:
    return NextcloudTalkPlatform(config)


def register(ctx: Any) -> None:
    """Hermes Platform Plugin Registration Entrypoint."""
    ctx.register_platform(
        name="nextcloud",
        label="Nextcloud Talk",
        adapter_factory=_build_adapter,
        check_fn=lambda: True,
        validate_config=validate_nextcloud_config,
        is_connected=check_is_connected,
        required_env=[
            "NEXTCLOUD_BASE_URL",
            "NEXTCLOUD_USERNAME",
            "NEXTCLOUD_APP_PASSWORD",
        ],
        allowed_users_env="NEXTCLOUD_ALLOWED_USERS",
        allow_all_env="NEXTCLOUD_ALLOW_ALL_USERS",
        cron_deliver_env_var="NEXTCLOUD_HOME_CHANNEL",
        max_message_length=16000,
        emoji="☁️",
    )