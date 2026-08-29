from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class NextcloudPresenceManager:
    """Steuert Presence-Status, Typings und Custom-Status-Nachrichten."""

    def __init__(self, client: Any):
        self.client = client
        self._current_presence_state: Optional[str] = None
        self._current_custom_status: Optional[tuple[Optional[str], str]] = None
        self._status_text: Dict[str, str] = {}

    def set_status_text(self, chat_id: str, text: Optional[str]) -> None:
        if text:
            self._status_text[str(chat_id)] = text
        else:
            self._status_text.pop(str(chat_id), None)

    async def set_presence_status(self, state: str) -> None:
        normalized = str(state or "").strip().lower()
        if normalized == self._current_presence_state:
            return
        await self.client.ocs_put("apps/user_status/api/v1/user_status/status", {"statusType": normalized})
        self._current_presence_state = normalized

    async def set_custom_status_message(self, message: str, status_icon: Optional[str] = None) -> None:
        normalized_message = " ".join(str(message or "").split()).strip()
        new_state = (status_icon, normalized_message)
        if not normalized_message or new_state == self._current_custom_status:
            return

        payload: Dict[str, Any] = {"message": normalized_message[:140]}
        if status_icon:
            payload["statusIcon"] = status_icon

        await self.client.ocs_put("apps/user_status/api/v1/user_status/message/custom", payload)
        self._current_custom_status = new_state

    async def clear_custom_status_message(self, *, force: bool = False) -> None:
        if self._current_custom_status is None and not force:
            return
        await self.client.ocs_delete("apps/user_status/api/v1/user_status/message")
        self._current_custom_status = None