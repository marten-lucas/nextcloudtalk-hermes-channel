from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class NextcloudSignalingSettings:
    server: str
    hello_auth_params: Dict[str, Any]
    signaling_mode: str = ""
    user_id: str = ""


class NextcloudSignalingManager:
    """Handhabt WebSocket (HPB) Signaling-Verbindungen und Fallback-Polling."""

    def __init__(self, client: Any, event_handler: Any):
        self.client = client
        self.event_handler = event_handler
        self._active_room_sessions: Dict[str, str] = {}

    async def get_signaling_settings(self, room_id: str) -> Optional[NextcloudSignalingSettings]:
        data = await self.client.ocs_get("apps/spreed/api/v3/signaling/settings", params={"token": room_id})
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

    async def mark_room_active(self, room_id: str) -> Optional[str]:
        data = await self.client.ocs_post(f"apps/spreed/api/v4/room/{room_id}/participants/active", {"force": True})
        session_id = str(data.get("sessionId") or data.get("sessionid") or "").strip() if isinstance(data, dict) else None
        if session_id:
            self._active_room_sessions[room_id] = session_id
        return session_id

    async def leave_room_active(self, room_id: str) -> None:
        session_id = self._active_room_sessions.pop(room_id, None)
        if not session_id:
            return
        session = await self.client.ensure_session()
        try:
            async with session.delete(
                self.client.talk_url(f"apps/spreed/api/v4/room/{room_id}/participants/active"),
                headers=self.client.ocs_headers(),
            ) as resp:
                if resp.status >= 400:
                    logger.warning("Nextcloud: Konnte Raum %s nicht inaktiv setzen: %s", room_id, resp.status)
        except Exception as exc:
            logger.warning("Nextcloud: Konnte Raum %s nicht inaktiv setzen: %s", room_id, exc)