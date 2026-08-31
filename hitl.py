from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class PendingApproval:
    room_id: str
    requester_user_id: str
    future: asyncio.Future[bool]


class HITLManager:
    """Verwaltet Reaktionen für Human-in-the-Loop Bestätigungen."""

    approve_reactions = {"✅", "👍"}
    reject_reactions = {"❌", "👎"}
    cancel_reactions = {"⛔"}

    def __init__(self, enforce_requester_only: bool = True):
        self.enforce_requester_only = enforce_requester_only
        self._pending_approvals: Dict[str, PendingApproval] = {}

    async def request_approval(self, room_id: str, prompt_message_id: str, requester_user_id: str) -> bool:
        future: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self._pending_approvals[prompt_message_id] = PendingApproval(
            room_id=room_id,
            requester_user_id=requester_user_id,
            future=future,
        )
        return await future

    async def handle_reaction(self, event: Dict[str, Any], cancel_callback: Any = None) -> None:
        target_message_id = str(
            event.get("targetMessageId") or event.get("messageId") or event.get("objectId") or ""
        )
        if not target_message_id:
            return

        reactor_id = str(
            event.get("actorId") or event.get("actor_id") or event.get("sender") or event.get("userId") or ""
        )
        emoji = str(event.get("emoji") or event.get("reaction") or event.get("key") or "")

        if emoji in self.cancel_reactions and cancel_callback:
            await cancel_callback(target_message_id, reactor_id)
            return

        pending = self._pending_approvals.get(target_message_id)
        if not pending:
            return

        if self.enforce_requester_only and reactor_id != pending.requester_user_id:
            logger.info("Nextcloud: Reaktionen von %s ignoriert; Requester ist %s", reactor_id, pending.requester_user_id)
            return

        if emoji in self.approve_reactions:
            if not pending.future.done():
                pending.future.set_result(True)
            self._pending_approvals.pop(target_message_id, None)
        elif emoji in self.reject_reactions:
            if not pending.future.done():
                pending.future.set_result(False)
            self._pending_approvals.pop(target_message_id, None)

    def is_fallback_emoji_reply(self, body: str) -> bool:
        """True, wenn die Nachricht ein nacktes Approval/Reject-Emoji ist."""
        return body.strip() in self.approve_reactions or body.strip() in self.reject_reactions

    def resolve_fallback_target(self, event: Dict[str, Any]) -> Optional[str]:
        """Ermittelt die Ziel-Message-ID eines Emoji-Replies aus Chat-Nachricht."""
        target = str(
            event.get("referenceId")
            or event.get("replyTo")
            or event.get("parentMessageId")
            or event.get("targetMessageId")
            or ""
        ).strip()
        if target and target in self._pending_approvals:
            return target
        return None