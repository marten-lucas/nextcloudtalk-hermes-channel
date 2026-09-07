from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Talk-Typing verfällt serverseitig nach ~15 s; wir erneuern alle 5 s.
_TYPING_REFRESH_INTERVAL = 5.0


class NextcloudPresenceManager:
    """Steuert Presence-Status, Typings und Custom-Status-Nachrichten."""

    def __init__(self, client: Any):
        self.client = client
        self._current_presence_state: Optional[str] = None
        self._current_custom_status: Optional[tuple[Optional[str], str]] = None
        self._status_text: Dict[str, str] = {}
        # Typing-Refresh-Tasks pro Raum (send_typing/stop_typing Contract).
        self._typing_tasks: Dict[str, asyncio.Task] = {}
        # Referenzzähler für aktive Turns — Presence busy <-> online.
        self._busy_refs: int = 0

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

    async def set_busy(self) -> None:
        """Markiert einen aktiven Turn (Referenzgezählt). Presence -> busy.
        Der busy-Status ist ein NC-User-Status (online + away=False) mit
        Custom-Message-Verwaltung durch send_or_update_status."""
        self._busy_refs += 1

    async def clear_busy(self) -> None:
        """Turn beendet — zurück auf online, wenn kein Turn mehr aktiv."""
        self._busy_refs = max(0, self._busy_refs - 1)

    @property
    def is_busy(self) -> bool:
        return self._busy_refs > 0

    async def send_typing(self, chat_id: str) -> None:
        """Startet/erneuert den Typing-Indikator für einen Raum.

        Der Gateway-Loop (_keep_typing) ruft alle ~2 s; wir starten einen
        Refresh-Task, der alle 5 s den Typing-Status erneuert (Talk verfällt
        nach ~15 s). Mehrfachaufrufe für denselben Raum idempotent.
        """
        room_id = str(chat_id or "").strip()
        if not room_id:
            return
        existing = self._typing_tasks.get(room_id)
        if existing and not existing.done():
            return
        self._typing_tasks[room_id] = asyncio.create_task(
            self._typing_refresh_loop(room_id)
        )

    async def stop_typing(self, chat_id: str) -> None:
        """Beendet den Typing-Indikator für einen Raum."""
        room_id = str(chat_id or "").strip()
        task = self._typing_tasks.pop(room_id, None)
        if task is None:
            return
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    async def _typing_refresh_loop(self, room_id: str) -> None:
        """Erneuert den Typing-Status periodisch; bricht bei OCS-Fehlern ab."""
        try:
            while True:
                try:
                    await self.client.ocs_post(
                        f"apps/spreed/api/v1/room/{room_id}/typing",
                        {},
                    )
                except Exception as exc:
                    # 404 = alter Talk ohne Typing-Endpoint — still weg.
                    logger.debug("Typing-Status für Raum %s nicht gesetzt: %s", room_id, exc)
                    return
                await asyncio.sleep(_TYPING_REFRESH_INTERVAL)
        except asyncio.CancelledError:
            raise

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