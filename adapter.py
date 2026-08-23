from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import tempfile
from datetime import datetime, timezone
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
    from gateway.session import build_session_key  # type: ignore
except Exception:
    Platform = lambda name: name  # type: ignore
    PlatformConfig = Any  # type: ignore
    build_session_key = None  # type: ignore

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

        async def cancel_session_processing(self, session_key: str, **_: Any) -> None:
            return None

        def set_status_text(self, chat_id: str, text: Optional[str]) -> None:
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


@dataclass
class NextcloudSignalingSettings:
    server: str
    hello_auth_params: Dict[str, Any]
    signaling_mode: str = ""
    user_id: str = ""


class NextcloudTalkPlatform(BasePlatformAdapter):
    """Nextcloud Talk adapter with WebSocket-first transport and polling fallback."""

    supports_status_text = True
    approve_reactions = {"✅", "👍"}
    reject_reactions = {"❌", "👎"}
    cancel_reactions = {"⛔"}
    gateway_lifecycle_notices = {
        "gateway restarting": "offline",
        "gateway online": "online",
    }

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("nextcloud"))
        self.runtime = self._build_runtime_config(config)
        self._session: Optional[aiohttp.ClientSession] = None
        self._stop_event = asyncio.Event()
        self._ws_task: Optional[asyncio.Task[None]] = None
        self._polling_task: Optional[asyncio.Task[None]] = None
        self._room_ws_tasks: Dict[str, asyncio.Task[None]] = {}
        self._poll_cursor_by_room: Dict[str, str] = {}
        self._poll_bootstrapped_rooms: set[str] = set()
        self._pending_approvals: Dict[str, PendingApproval] = {}
        self._active_room_sessions: Dict[str, str] = {}
        self._message_index: Dict[str, Dict[str, Any]] = {}
        self._message_session_keys: Dict[str, Dict[str, str]] = {}
        self._session_reset_noted_rooms: set[str] = set()
        self._status_text: Dict[str, str] = {}
        self._current_presence_state: Optional[str] = None
        self._current_custom_status: Optional[tuple[Optional[str], str]] = None
        # --- ERWEITERUNG: User-Gruppen-Cache ---
        self._user_groups_cache: Dict[str, List[str]] = {}

    async def _get_user_groups(self, user_id: str) -> List[str]:
        """Return Nextcloud groups using the documented Provisioning API."""
        user_id = str(user_id or "").strip()
        if not user_id:
            return []

        if user_id in self._user_groups_cache:
            return list(self._user_groups_cache[user_id])

        try:
            session = await self._ensure_session()
            encoded_user_id = quote(user_id, safe="")
            path = f"users/{encoded_user_id}/groups"
            async with session.get(
                self._cloud_ocs_url(path),
                params={"format": "json"},
                headers=self._ocs_headers(),
            ) as resp:
                content_type = resp.headers.get("Content-Type", "")
                if "application/json" in content_type:
                    body = await resp.json()
                else:
                    body = {
                        "ocs": {
                            "meta": {
                                "status": "ok",
                                "statuscode": resp.status,
                            },
                            "data": {},
                        }
                    }

            self._raise_for_ocs_error(f"cloud/{path}", body)
            data = self._ocs_data(body)

            groups: List[str] = []
            if isinstance(data, dict):
                raw_groups = data.get("groups", [])
                if isinstance(raw_groups, dict):
                    raw_groups = raw_groups.get("element", [])
                if isinstance(raw_groups, list):
                    groups = [str(g).strip() for g in raw_groups if str(g).strip()]
                elif raw_groups:
                    groups = [str(raw_groups).strip()]
            elif isinstance(data, list):
                groups = [str(g).strip() for g in data if str(g).strip()]

            self._user_groups_cache[user_id] = groups
            logger.debug("Nextcloud: groups for user %s: %s", user_id, groups)
            return list(groups)
        except Exception as exc:
            logger.warning("Konnte Gruppen für User %s nicht abfragen: %s", user_id, exc)
            return []

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

    def _talk_url(self, path: str) -> str:
        return urljoin(f"{self.runtime.base_url}/ocs/v2.php/", path.lstrip("/"))

    def _cloud_ocs_url(self, path: str) -> str:
        """Build a URL for the core Cloud Provisioning OCS API."""
        return urljoin(f"{self.runtime.base_url}/ocs/v1.php/cloud/", path.lstrip("/"))

    @classmethod
    def _normalized_notice_text(cls, text: str) -> str:
        return " ".join(text.split()).strip().lower().rstrip(".!")

    @classmethod
    def _gateway_lifecycle_state(cls, text: str) -> Optional[str]:
        return cls.gateway_lifecycle_notices.get(cls._normalized_notice_text(text))

    @classmethod
    def _categorize_gateway_message(cls, text: str) -> tuple[str, dict[str, Any]]:
        if not text:
            return ("forward", {})
        
        normalized = " ".join(text.split()).strip().lower()
        
        if "gateway restarting" in normalized:
            return ("lifecycle", {"state": "offline", "text": text})
        if "gateway online" in normalized and "hermes is back" in normalized:
            return ("lifecycle", {"state": "online", "text": text})
        if "draining" in normalized and "active" in normalized and "agent" in normalized:
            return ("lifecycle", {"state": "draining", "text": text})
        
        if text.strip().startswith("⚠️"):
            return ("error", {"text": text})
        
        error_patterns = [
            "processing stopped",
            "no response was generated",
            "session too large",
            "interrupted before processing",
            "authentication failed",
            "provider.*failed",
            "provider.*rejected",
            "tool.*failed",
            "no response after",
        ]
        if any(pattern in normalized for pattern in error_patterns):
            return ("error", {"text": text})
        
        suppress_patterns = [
            "gateway.*queued",
            "compressing context",
            "compression timed out",
            "compression aborted",
            "working —",
            "subagent working",
            "steer failed",
        ]
        if any(pattern in normalized for pattern in suppress_patterns):
            return ("suppress", {})
        
        return ("forward", {"text": text})

    @staticmethod
    def _status_api_path(path: str) -> str:
        base = "apps/user_status/api/v1/user_status"
        suffix = path.strip("/")
        return f"{base}/{suffix}" if suffix else base

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not self.runtime.base_url or not self.runtime.username or not self.runtime.app_password:
            raise RuntimeError("Nextcloud adapter not configured: base_url/username/app_password required")
        self._stop_event.clear()
        await self._ensure_session()
        ws_connected = await self._connect_websocket_once()
        if not ws_connected:
            self._start_polling_loop()
        else:
            self._start_polling_loop()
        await self._set_presence_status("online")
        await self._clear_custom_status_message(force=True)
        return True

    async def disconnect(self) -> None:
        self._stop_event.set()
        tasks = [task for task in (self._ws_task, self._polling_task) if task]
        tasks.extend(task for task in self._room_ws_tasks.values() if task)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._ws_task = None
        self._polling_task = None
        self._room_ws_tasks = {}
        if self._session and not self._session.closed:
            await self._clear_custom_status_message(force=True)
            await self._set_presence_status("offline")
            for room_id in list(self._active_room_sessions):
                await self._leave_room_active(room_id)
            await self._session.close()
        self._session = None
        self._mark_disconnected()

    async def start(self) -> None:
        await self.connect()

    async def stop(self) -> None:
        await self.disconnect()

    async def _connect_websocket_once(self) -> bool:
        room_ids = await self._list_joined_rooms()
        started_any = False
        for room_id in room_ids:
            if self.runtime.allowed_rooms and room_id not in self.runtime.allowed_rooms:
                continue
            settings = await self._get_signaling_settings(room_id)
            if not settings:
                continue
            task = asyncio.create_task(self._room_signaling_loop(room_id, settings))
            self._room_ws_tasks[room_id] = task
            started_any = True
        if not started_any:
            logger.warning("Nextcloud: websocket signaling unavailable, falling back to polling")
        return started_any

    async def _room_signaling_loop(self, room_id: str, settings: NextcloudSignalingSettings) -> None:
        session = await self._ensure_session()
        try:
            async with session.ws_connect(self._signaling_ws_url(settings.server), heartbeat=30) as ws:
                await self._signaling_hello(ws, settings)
                session_id = self._active_room_sessions.get(room_id)
                if not session_id:
                    session_id = await self._mark_room_active(room_id)
                if not session_id:
                    raise RuntimeError(f"Nextcloud signaling join missing session id for room {room_id}")
                await self._signaling_join_room(ws, room_id, session_id, settings.user_id)
                async for msg in ws:
                    if self._stop_event.is_set():
                        return
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                            break
                        continue
                    payload = json.loads(msg.data)
                    if payload.get("type") == "event":
                        event = payload.get("event") or {}
                        if isinstance(event, dict) and event.get("target") in {"room", "participants"}:
                            events = await self._fetch_room_events(room_id)
                            for event_payload in events:
                                await self.handle_incoming_event(event_payload)
                    elif payload.get("type") == "room":
                        events = await self._fetch_room_events(room_id)
                        for event_payload in events:
                            await self.handle_incoming_event(event_payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not self._stop_event.is_set():
                logger.warning("Nextcloud websocket room loop ended unexpectedly for %s: %s", room_id, exc)
                self._start_polling_loop()
        finally:
            self._room_ws_tasks.pop(room_id, None)

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
        data = await self._ocs_get("apps/spreed/api/v4/room", params={"includeStatus": "true"})
        if isinstance(data, list):
            return [str(room.get("token", "") or room.get("id", "")) for room in data if room]
        return []

    async def _fetch_room_events(self, room_id: str) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"lookIntoFuture": 0, "limit": 50}
        if room_id not in self._poll_bootstrapped_rooms:
            data = await self._ocs_get(f"apps/spreed/api/v1/chat/{room_id}", params=params)
            latest_id = self._latest_message_id(data if isinstance(data, list) else [])
            if latest_id:
                self._poll_cursor_by_room[room_id] = latest_id
            self._poll_bootstrapped_rooms.add(room_id)
            return []

        if room_id in self._poll_cursor_by_room:
            params["lookIntoFuture"] = 1
            params["lastKnownMessageId"] = self._poll_cursor_by_room[room_id]
        data = await self._ocs_get(f"apps/spreed/api/v1/chat/{room_id}", params=params)
        events: List[Dict[str, Any]] = []
        if isinstance(data, list):
            for event in data:
                normalized = dict(event)
                normalized.setdefault("room_id", room_id)
                events.append(normalized)
            latest_id = self._latest_message_id(data)
            if latest_id:
                self._poll_cursor_by_room[room_id] = latest_id
        return events

    @staticmethod
    def _latest_message_id(messages: List[Dict[str, Any]]) -> Optional[str]:
        numeric_ids: List[int] = []
        fallback_ids: List[str] = []
        for message in messages:
            raw = str(message.get("id", "")).strip()
            if not raw:
                continue
            fallback_ids.append(raw)
            try:
                numeric_ids.append(int(raw))
            except ValueError:
                continue
        if numeric_ids:
            return str(max(numeric_ids))
        return fallback_ids[0] if fallback_ids else None

    async def _ocs_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        session = await self._ensure_session()
        query = {"format": "json"}
        if params:
            query.update(params)
        async with session.get(self._talk_url(path), params=query, headers=self._ocs_headers()) as resp:
            if resp.status == 304:
                return []
            body = await resp.json()
        self._raise_for_ocs_error(path, body)
        return self._ocs_data(body)

    async def _ocs_post(self, path: str, data: Dict[str, Any]) -> Any:
        return await self._ocs_request("post", path, data=data)

    async def _ocs_put(self, path: str, data: Dict[str, Any]) -> Any:
        return await self._ocs_request("put", path, data=data)

    async def _ocs_delete(self, path: str) -> Any:
        return await self._ocs_request("delete", path)

    async def _ocs_request(
        self,
        method: str,
        path: str,
        *,
        data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        session = await self._ensure_session()
        query = {"format": "json"}
        if params:
            query.update(params)
        request = getattr(session, method)
        async with request(
            self._talk_url(path),
            params=query,
            data=data,
            headers=self._ocs_headers(),
        ) as resp:
            content_type = resp.headers.get("Content-Type", "")
            if "application/json" in content_type:
                body = await resp.json()
            else:
                raw_text = await resp.text()
                body = {"ocs": {"meta": {"status": "ok", "statuscode": resp.status}, "data": raw_text}}
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

        actor_type = str(event.get("actorType") or event.get("actor_type") or "").strip().lower()
        if actor_type and actor_type != "users":
            logger.debug("Nextcloud: ignoring non-user actor message in room %s (actor_type=%s)", room_id, actor_type)
            return

        event_type = str(event.get("eventType", event.get("type", "message"))).lower()
        is_edit = "edit" in event_type
        is_delete = "delete" in event_type or "remove" in event_type
        if (event.get("systemMessage") or event.get("system_message")) and not is_delete:
            logger.debug("Nextcloud: ignoring system message in room %s", room_id)
            return

        message_id = str(event.get("id") or event.get("message_id") or event.get("messageId") or "")
        sender_id = str(
            event.get("actorId")
            or event.get("actor_id")
            or event.get("sender")
            or event.get("userId")
            or ""
        )
        if not sender_id or sender_id == self.runtime.username:
            return
        if sender_id.lower() in {"system", "changelog", "sample"}:
            logger.debug("Nextcloud: ignoring reserved sender %s in room %s", sender_id, room_id)
            return

        original_message_id = str(
            event.get("messageId")
            or event.get("objectId")
            or event.get("referenceId")
            or message_id
        ).strip() or message_id
        original_record = self._message_index.get(original_message_id, {})
        body = str(event.get("message") or event.get("text") or "")
        trigger_text = body
        if await self._handle_reaction_fallback_from_message(event, sender_id, body):
            return
        attachments = self._extract_attachments(event)
        timestamp_source = original_record.get("timestamp") or event.get("timestamp") or event.get("datetime")
        time_label = self._format_event_time(timestamp_source)
        if is_edit:
            body = f"Vergangene Nachricht von {time_label} wurde geaendert zu:\n{body.strip()}".strip()
        elif is_delete:
            body = f"Nachricht von {time_label} wurde geloescht."
            trigger_text = str(original_record.get("text") or "")
        if not body.strip() and not attachments:
            logger.debug("Nextcloud: ignoring empty user message in room %s", room_id)
            return
        participant_count = await self._resolve_participant_count(room_id, event)
        if not self._should_trigger(trigger_text or body, participant_count):
            return

        await self._mark_room_active(room_id)

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

        body = self._normalize_nextcloud_command(body)

        # --- ERWEITERUNG START: Gruppen abfragen ---
        user_groups = await self._get_user_groups(sender_id)
        groups_header_str = ",".join(user_groups)
        logger.warning("RBAC Header gesetzt: User=%s, Groups=%s", sender_id, groups_header_str)
        # --- ERWEITERUNG ENDE ---

        msg_type = MessageType.COMMAND if body.strip().startswith("/") else MessageType.TEXT
        # Hermes' current BasePlatformAdapter.build_source() does not accept
        # transport-specific extra_headers. Keep the SessionSource compatible
        # with the core adapter contract and attach optional metadata afterwards.
        source = self.build_source(
            chat_id=room_id,
            chat_name=room_id,
            chat_type="dm" if participant_count <= 2 else "group",
            user_id=sender_id,
            user_name=sender_id,
            message_id=message_id or None,
        )
        try:
            source.extra_headers = {
                "X-On-Behalf-Of": sender_id,
                "X-User-Groups": groups_header_str,
            }
        except Exception:
            logger.debug("Nextcloud: SessionSource does not allow extra_headers")
        session_key = self._build_gateway_session_key(source)
        if session_key:
            self._message_session_keys[original_message_id] = {
                "session_key": session_key,
                "requester_user_id": sender_id,
                "chat_id": room_id,
            }
        if not is_delete:
            self._message_index[original_message_id] = {
                "room_id": room_id,
                "sender_id": sender_id,
                "text": str(event.get("message") or event.get("text") or ""),
                "timestamp": event.get("timestamp") or event.get("datetime"),
            }
        reset_note = await self._fresh_session_note(source, room_id, original_message_id)
        if reset_note:
            body = f"{reset_note}\n\n{body}"

        event_payload = dict(event)
        event_payload["context_messages"] = context_messages
        event_payload["attachment_paths"] = attachment_paths
        event_payload["original_message_id"] = original_message_id
        event_payload["is_edit_event"] = is_edit
        event_payload["is_delete_event"] = is_delete
        event_payload["user_groups"] = list(user_groups)
        msg_event = MessageEvent(
            text=body,
            message_type=msg_type,
            source=source,
            raw_message=event_payload,
            message_id=message_id or None,
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
        api_participants = await self._ocs_get(f"apps/spreed/api/v4/room/{room_id}/participants")
        if isinstance(api_participants, list):
            return len(api_participants)
        if "participant_count" in event:
            return int(event["participant_count"])
        participants = event.get("participants")
        if isinstance(participants, list):
            return len(participants)
        return 3

    async def fetch_last_messages(self, room_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        data = await self._ocs_get(f"apps/spreed/api/v1/chat/{room_id}", params={"lookIntoFuture": 0, "limit": limit})
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

    @staticmethod
    def _format_event_time(raw_timestamp: Any) -> str:
        if raw_timestamp in (None, ""):
            return "unbekannter Zeit"
        try:
            numeric = float(raw_timestamp)
            if numeric > 1_000_000_000_000:
                numeric /= 1000.0
            return datetime.fromtimestamp(numeric, tz=timezone.utc).astimezone().strftime("%H:%M")
        except (TypeError, ValueError, OSError):
            return str(raw_timestamp)

    @staticmethod
    def _fallback_known_commands() -> set[str]:
        return {
            "approve",
            "background",
            "deny",
            "help",
            "model",
            "new",
            "reload-mcp",
            "reset",
            "resume",
            "status",
            "stop",
        }

    @classmethod
    def _resolve_known_command(cls, name: str) -> Optional[str]:
        token = str(name or "").strip().lower()
        if not token:
            return None
        candidates = [token]
        hyphenated = token.replace("_", "-")
        if hyphenated != token:
            candidates.append(hyphenated)
        try:
            from hermes_cli.commands import is_gateway_known_command  # type: ignore

            for candidate in candidates:
                if is_gateway_known_command(candidate):
                    return candidate
        except Exception:
            pass
        try:
            from agent.skill_commands import get_skill_commands  # type: ignore

            skill_commands = get_skill_commands() or {}
            for candidate in candidates:
                if f"/{candidate}" in skill_commands:
                    return candidate
        except Exception:
            pass
        for candidate in candidates:
            if candidate in cls._fallback_known_commands():
                return candidate
        return None

    @classmethod
    def _normalize_nextcloud_command(cls, text: str) -> str:
        if not text.startswith("!"):
            return text
        match = re.match(r"^!([A-Za-z][A-Za-z0-9_-]*)(?=$|\s)(.*)$", text, flags=re.DOTALL)
        if not match:
            return text
        resolved = cls._resolve_known_command(match.group(1))
        if resolved is None:
            return text
        return f"/{resolved}{match.group(2) or ''}"

    def _build_gateway_session_key(self, source: Any) -> Optional[str]:
        if not callable(build_session_key):
            return None
        try:
            return build_session_key(
                source,
                group_sessions_per_user=getattr(self.config, "extra", {}).get("group_sessions_per_user", True),
                thread_sessions_per_user=getattr(self.config, "extra", {}).get("thread_sessions_per_user", False),
            )
        except Exception:
            return None

    async def _fresh_session_note(self, source: Any, room_id: str, current_message_id: str) -> Optional[str]:
        runner = getattr(self, "gateway_runner", None)
        if runner is None or room_id in self._session_reset_noted_rooms:
            return None
        session_key = self._build_gateway_session_key(source)
        if not session_key:
            return None
        try:
            if runner._peek_session_state(session_key) is not None:
                return None
        except Exception:
            return None
        recent_messages = await self.fetch_last_messages(room_id, limit=2)
        prior_messages = [item for item in recent_messages if str(item.get("id") or "") != current_message_id]
        if not prior_messages:
            return None
        self._session_reset_noted_rooms.add(room_id)
        return (
            "[System note: Hermes session in this existing chat was reset. "
            "Earlier room history was not reloaded automatically; please summarize older context if it matters.]"
        )

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
        
        category, details = self._categorize_gateway_message(text)
        
        if category == "lifecycle":
            state = details.get("state")
            if state == "offline":
                await self._set_custom_status_message("Gateway restarting", "🔄")
                await self._set_presence_status("offline")
            elif state == "online":
                await self._set_presence_status("online")
                await self._clear_custom_status_message()
            elif state == "draining":
                msg = details.get("text", "Gateway draining")
                await self._set_custom_status_message(msg[:140], "⏸️")
            return SendResult(success=True)
        
        elif category == "error":
            if not reply_to_message_id:
                await self._set_custom_status_message("Fehler", "⚠️")
                return SendResult(success=True)
            
            error_text = details.get("text", text)
            formatted = f"🚫 **Fehler**\n\n{error_text}"
            await self._mark_room_active(room_id)
            payload: Dict[str, Any] = {
                "message": formatted,
                "replyTo": reply_to_message_id,
            }
            if metadata:
                payload.update(metadata)
            data = await self._ocs_post(f"apps/spreed/api/v1/chat/{room_id}", payload)
            message_id = None
            if isinstance(data, dict):
                message_id = str(data.get("id", "") or data.get("messageId", "") or "") or None
            return SendResult(success=True, message_id=message_id)
        
        elif category == "suppress":
            return SendResult(success=True)
        
        else:  # category == "forward"
            await self._mark_room_active(room_id)
            payload: Dict[str, Any] = {"message": text}
            if reply_to_message_id:
                payload["replyTo"] = reply_to_message_id
            if metadata:
                payload.update(metadata)
            data = await self._ocs_post(f"apps/spreed/api/v1/chat/{room_id}", payload)
            message_id = None
            if isinstance(data, dict):
                message_id = str(data.get("id", "") or data.get("messageId", "") or "") or None
            return SendResult(success=True, message_id=message_id)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": chat_id, "type": "group"}

    async def send_typing(self, chat_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        await self._mark_room_active(chat_id)
        live_text = self._status_text.get(str(chat_id))
        if live_text:
            await self._set_custom_status_message(live_text, "💬")
        else:
            await self._set_custom_status_message("Antwort wird geschrieben", "✍️")
        await self._emit_typing_state(chat_id, True)

    async def stop_typing(self, chat_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        await self._emit_typing_state(chat_id, False)

    async def on_processing_start(self, event: MessageEvent) -> None:
        self.set_status_text(event.source.chat_id, "Liest Kontext")
        await self._set_presence_status("online")
        await self._set_custom_status_message("Liest Kontext", "📖")

    async def on_processing_complete(self, event: MessageEvent, outcome: Any) -> None:
        self.set_status_text(event.source.chat_id, None)
        await self._set_presence_status("online")
        await self._clear_custom_status_message()

    async def send_or_update_status(
        self,
        chat_id: str,
        status_key: str,
        content: str,
        *,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        message, icon = self._map_progress_status(status_key, content)
        if message:
            self.set_status_text(chat_id, message)
            await self._set_custom_status_message(message, icon)
        return SendResult(success=True)

    @staticmethod
    def _map_progress_status(status_key: str, content: str) -> tuple[Optional[str], Optional[str]]:
        normalized_key = str(status_key or "").strip().lower()
        normalized_content = " ".join(str(content or "").split()).strip()
        normalized_lower = normalized_content.lower()
        if "context" in normalized_lower:
            return "Liest Kontext", "📖"
        if normalized_key == "_thinking" or normalized_lower.startswith("💬 "):
            return "Denkt nach", "🤔"
        if normalized_key.startswith("tool.") or "tool" in normalized_key:
            return "Fuehrt Werkzeuge aus", "🛠️"
        if normalized_content:
            return (normalized_content[:80], "💬")
        return None, None

    async def _set_presence_status(self, state: str) -> None:
        normalized = str(state or "").strip().lower()
        if normalized == self._current_presence_state:
            return
        await self._ocs_put(self._status_api_path("status"), {"statusType": normalized})
        self._current_presence_state = normalized

    async def _set_custom_status_message(self, message: str, status_icon: Optional[str] = None) -> None:
        normalized_message = " ".join(str(message or "").split()).strip()
        normalized_icon = status_icon or None
        new_state = (normalized_icon, normalized_message)
        if not normalized_message or new_state == self._current_custom_status:
            return
        payload: Dict[str, Any] = {
            "message": normalized_message[:140],
        }
        if normalized_icon is not None:
            payload["statusIcon"] = normalized_icon
        await self._ocs_put(self._status_api_path("message/custom"), payload)
        self._current_custom_status = new_state

    async def _clear_custom_status_message(self, *, force: bool = False) -> None:
        if self._current_custom_status is None and not force:
            return
        await self._ocs_delete(self._status_api_path("message"))
        self._current_custom_status = None

    async def _download_attachment_from_metadata(self, attachment: Dict[str, Any]) -> Optional[str]:
        file_id = attachment.get("id") or attachment.get("fileId")
        path = attachment.get("path") or attachment.get("filePath")
        url = attachment.get("url") or attachment.get("downloadUrl")
        return await self.download_attachment(file_id=file_id, remote_path=path, file_url=url)

    @staticmethod
    def _extract_attachments(event: Dict[str, Any]) -> List[Dict[str, Any]]:
        direct = event.get("attachments") or event.get("files") or []
        attachments: List[Dict[str, Any]] = []
        if isinstance(direct, list):
            for item in direct:
                if isinstance(item, dict):
                    attachments.append(item)

        message_parameters = event.get("messageParameters") or event.get("parameters") or {}
        if isinstance(message_parameters, dict):
            for param in message_parameters.values():
                if not isinstance(param, dict):
                    continue
                param_type = str(param.get("type") or "").lower()
                if param_type != "file":
                    continue
                attachment: Dict[str, Any] = {}
                if param.get("id") is not None:
                    attachment["id"] = param.get("id")
                if param.get("path"):
                    attachment["path"] = param.get("path")
                if param.get("link"):
                    attachment["url"] = param.get("link")
                if attachment:
                    attachments.append(attachment)
        return attachments

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
            url = self._talk_url(f"apps/spreed/api/v1/chat/{file_id}")
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

        reactor_id = str(
            event.get("actorId")
            or event.get("actor_id")
            or event.get("sender")
            or event.get("userId")
            or ""
        )
        emoji = str(event.get("emoji") or event.get("reaction") or event.get("key") or "")
        if emoji in self.cancel_reactions:
            session_info = self._message_session_keys.get(target_message_id)
            if session_info and reactor_id == session_info.get("requester_user_id"):
                await self.cancel_session_processing(session_info["session_key"])
            return
        pending = self._pending_approvals.get(target_message_id)
        if not pending:
            return
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

    async def _handle_reaction_fallback_from_message(
        self,
        event: Dict[str, Any],
        sender_id: str,
        body: str,
    ) -> bool:
        emoji = body.strip()
        if emoji not in self.approve_reactions and emoji not in self.reject_reactions:
            return False

        target_message_id = str(
            event.get("referenceId")
            or event.get("replyTo")
            or event.get("parentMessageId")
            or event.get("targetMessageId")
            or ""
        ).strip()
        if not target_message_id:
            return False
        if target_message_id not in self._pending_approvals:
            return False

        await self._handle_reaction(
            {
                "targetMessageId": target_message_id,
                "actorId": sender_id,
                "emoji": emoji,
            }
        )
        return True

    async def _mark_room_active(self, room_id: str) -> Optional[str]:
        data = await self._ocs_post(f"apps/spreed/api/v4/room/{room_id}/participants/active", {"force": True})
        session_id: Optional[str] = None
        if isinstance(data, dict):
            session_id = str(data.get("sessionId") or data.get("sessionid") or "").strip() or None
        if session_id:
            self._active_room_sessions[room_id] = session_id
        return session_id

    async def _leave_room_active(self, room_id: str) -> None:
        session_id = self._active_room_sessions.pop(room_id, None)
        if not session_id:
            return
        session = await self._ensure_session()
        try:
            async with session.delete(
                self._talk_url(f"apps/spreed/api/v4/room/{room_id}/participants/active"),
                headers=self._ocs_headers(),
            ) as resp:
                if resp.status >= 400:
                    logger.warning("Nextcloud: could not mark room %s inactive: %s", room_id, resp.status)
        except Exception as exc:
            logger.warning("Nextcloud: could not mark room %s inactive: %s", room_id, exc)

    async def _get_signaling_settings(self, room_id: str) -> Optional[NextcloudSignalingSettings]:
        data = await self._ocs_get(f"apps/spreed/api/v3/signaling/settings", params={"token": room_id})
        if not isinstance(data, dict):
            return None
        server = str(data.get("server") or "").strip()
        hello_auth_params = data.get("helloAuthParams") or {}
        if not server or not isinstance(hello_auth_params, dict):
            return None
        return NextcloudSignalingSettings(
            server=server,
            hello_auth_params=hello_auth_params,
            signaling_mode=str(data.get("signalingMode") or ""),
            user_id=str(data.get("userId") or ""),
        )

    @staticmethod
    def _signaling_ws_url(server: str) -> str:
        url = server.strip()
        if url.startswith("https://"):
            url = "wss://" + url[len("https://") :]
        elif url.startswith("http://"):
            url = "ws://" + url[len("http://") :]
        if url.endswith("/"):
            url = url[:-1]
        return f"{url}/spreed"

    async def _emit_typing_state(self, room_id: str, typing: bool) -> None:
        settings = await self._get_signaling_settings(room_id)
        if not settings:
            return

        session_id = self._active_room_sessions.get(room_id)
        if not session_id:
            session_id = await self._mark_room_active(room_id)
        if not session_id:
            return

        session = await self._ensure_session()
        signal_type = "startedTyping" if typing else "stoppedTyping"

        async def _send_once(current_session_id: str) -> None:
            async with session.ws_connect(
                self._signaling_ws_url(settings.server),
                heartbeat=30,
            ) as ws:
                await self._signaling_hello(ws, settings)
                await self._signaling_join_room(
                    ws,
                    room_id,
                    current_session_id,
                    settings.user_id,
                )
                logger.info(
                    "Nextcloud: emitting typing signal for room %s (%s)",
                    room_id,
                    signal_type,
                )
                await ws.send_json(
                    {
                        "type": "message",
                        "message": {
                            "recipient": {"type": "room"},
                            "data": {"type": signal_type},
                        },
                    }
                )

        try:
            await asyncio.wait_for(_send_once(session_id), timeout=5)
        except Exception as first_exc:
            # A cached Talk participant session can become stale. This is
            # especially visible in direct chats because signaling rejects
            # the old session with no_such_room. Drop it, create a fresh
            # active session and retry once.
            logger.debug(
                "Nextcloud: typing join failed for %s, refreshing active session: %s",
                room_id,
                first_exc,
            )
            self._active_room_sessions.pop(room_id, None)
            try:
                fresh_session_id = await self._mark_room_active(room_id)
                if not fresh_session_id or fresh_session_id == session_id:
                    raise first_exc
                await asyncio.wait_for(_send_once(fresh_session_id), timeout=5)
            except Exception as exc:
                logger.warning(
                    "Nextcloud: failed to send typing state for room %s: %s",
                    room_id,
                    exc,
                )

    async def _signaling_hello(self, ws: aiohttp.ClientWebSocketResponse, settings: NextcloudSignalingSettings) -> None:
        hello_version = "2.0" if settings.hello_auth_params.get("2.0") else "1.0"
        await ws.send_json(
            {
                "type": "hello",
                "hello": {
                    "version": hello_version,
                    "auth": {
                        "url": self._talk_url("apps/spreed/api/v3/signaling/backend"),
                        "params": settings.hello_auth_params[hello_version],
                    },
                },
            }
        )
        while True:
            msg = await ws.receive()
            if msg.type == aiohttp.WSMsgType.TEXT:
                payload = json.loads(msg.data)
                if payload.get("type") == "welcome":
                    continue
                if payload.get("type") == "hello":
                    return
                if payload.get("type") == "error":
                    raise RuntimeError(f"Nextcloud signaling hello failed: {payload}")
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                raise RuntimeError("Nextcloud signaling websocket closed during hello")

    async def _signaling_join_room(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        room_id: str,
        session_id: str,
        user_id: str = "",
    ) -> None:
        await ws.send_json(
            {
                "type": "room",
                "room": {
                    "roomid": room_id,
                    "sessionid": session_id,
                    **({"userid": user_id} if user_id else {}),
                },
            }
        )
        while True:
            msg = await ws.receive()
            if msg.type == aiohttp.WSMsgType.TEXT:
                payload = json.loads(msg.data)
                if payload.get("type") == "room":
                    return
                if payload.get("type") == "error":
                    raise RuntimeError(f"Nextcloud signaling room join failed: {payload}")
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                raise RuntimeError("Nextcloud signaling websocket closed during room join")


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
