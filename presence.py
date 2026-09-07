from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class NextcloudPresenceManager:
    """Steuert Presence-Status, Typings und Custom-Status-Nachrichten.

    Typing läuft über das HPB-Signaling (startedTyping/stoppedTyping WS-
    Messages) — Talk 23 hat keinen OCS-Typing-Endpoint. Die Signaling-
    Events verfallen serverseitig nach ~10 s; der Gateway-Loop erneuert
    alle ~2 s sowieso, wir frischen innerhalb eines kurzlebigen Tasks auf.
    """

    # Signaling-Typing verfällt nach ~10 s — Refresh etwas früher.
    _TYPING_TTL = 8.0

    def __init__(self, client: Any, signaling_mgr: Any = None):
        self.client = client
        self.signaling_mgr = signaling_mgr
        self._current_presence_state: Optional[str] = None
        self._current_custom_status: Optional[tuple[Optional[str], str]] = None
        self._status_text: Dict[str, str] = {}
        # Typing-Renew-Tasks pro Raum (send_typing/stop_typing Contract).
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
        """Markiert einen aktiven Turn (Referenzgezählt)."""
        self._busy_refs += 1

    async def clear_busy(self) -> None:
        """Turn beendet — Referenz freigeben."""
        self._busy_refs = max(0, self._busy_refs - 1)

    @property
    def is_busy(self) -> bool:
        return self._busy_refs > 0

    async def send_typing(self, chat_id: str) -> None:
        """Startet/erneuert den Typing-Indikator für einen Raum.

        Sofortiges Signaling-Event + Renew-Task (TTL 8 s), damit der
        Indikator während des gesamten Turns sichtbar bleibt, ohne dass
        jeder Gateway-Loop-Tick ein WS-Handshake auslöst.
        """
        room_id = str(chat_id or "").strip()
        if not room_id or self.signaling_mgr is None:
            return
        # Sofortiges Event (schnelles Feedback)
        try:
            await self.signaling_mgr.emit_typing_state(room_id, True)
        except Exception as exc:
            logger.debug("Typing-Event fehlgeschlagen für Raum %s: %s", room_id, exc)
            return
        # Renew-Task nur wenn keiner läuft
        existing = self._typing_tasks.get(room_id)
        if existing and not existing.done():
            return
        self._typing_tasks[room_id] = asyncio.create_task(
            self._typing_renew_loop(room_id)
        )

    async def stop_typing(self, chat_id: str) -> None:
        """Beendet den Typing-Indikator für einen Raum."""
        room_id = str(chat_id or "").strip()
        task = self._typing_tasks.pop(room_id, None)
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        if self.signaling_mgr is None:
            return
        try:
            await self.signaling_mgr.emit_typing_state(room_id, False)
        except Exception as exc:
            logger.debug("Typing-Stop fehlgeschlagen für Raum %s: %s", room_id, exc)

    async def _typing_renew_loop(self, room_id: str) -> None:
        """Erneuert das Typing-Event alle TTL-Sekunden bis stop_typing."""
        try:
            while True:
                await asyncio.sleep(self._TYPING_TTL)
                if self.signaling_mgr is None:
                    return
                await self.signaling_mgr.emit_typing_state(room_id, True)
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