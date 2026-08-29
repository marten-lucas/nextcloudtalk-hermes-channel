from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional
from urllib.parse import quote

logger = logging.getLogger(__name__)


class NextcloudIdentityManager:
    """Verwaltet Gruppenzuweisungen mit TTL-Cache und koppelt Identitäten an ContextVars."""

    def __init__(self, client: Any, cache_ttl_seconds: int = 120):
        self.client = client
        self.cache_ttl_seconds = cache_ttl_seconds
        self._cache: Dict[str, tuple[float, List[str]]] = {}

    async def get_user_groups(self, user_id: str) -> List[str]:
        user_id = str(user_id or "").strip()
        if not user_id:
            return []

        now = time.time()
        if user_id in self._cache:
            timestamp, cached_groups = self._cache[user_id]
            if now - timestamp < self.cache_ttl_seconds:
                return list(cached_groups)

        try:
            encoded_user_id = quote(user_id, safe="")
            path = f"users/{encoded_user_id}/groups"
            body = await self.client.cloud_ocs_get(path)
            data = body.get("ocs", {}).get("data", {}) if isinstance(body, dict) else {}

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

            self._cache[user_id] = (now, groups)
            logger.debug("Nextcloud: Gruppen für User %s (TTL %ss): %s", user_id, self.cache_ttl_seconds, groups)
            return list(groups)
        except Exception as exc:
            logger.warning("Konnte Gruppen für User %s nicht abfragen: %s", user_id, exc)
            return []

    def set_contextvars_identity(self, user_id: str, groups: List[str]) -> None:
        """Koppelt den Sender direkt an hermes-x-on-behalf contextvars."""
        try:
            from hermes_x_on_behalf.plugin import set_identity_context  # type: ignore
            groups_str = ",".join(groups) if groups else None
            set_identity_context(user_id, groups_str)
        except ImportError:
            pass