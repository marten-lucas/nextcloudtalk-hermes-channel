import logging
from typing import Dict, List, Optional, Set
import time

logger = logging.getLogger(__name__)


class NextcloudIdentityManager:
    """Manages identity mapping and group caching for Nextcloud users."""

    def __init__(self, client, cache_ttl_seconds: int = 120):
        self.client = client
        self.cache_ttl_seconds = cache_ttl_seconds
        self._group_cache: Dict[str, tuple[float, Set[str]]] = {}

    async def get_user_groups(self, user_id: str) -> Set[str]:
        """Retrieves user groups with TTL caching and graceful fallback for OCS 998."""
        if not user_id:
            return set()

        now = time.time()
        if user_id in self._group_cache:
            timestamp, groups = self._group_cache[user_id]
            if now - timestamp < self.cache_ttl_seconds:
                return groups

        try:
            # get_user_groups auf dem client nutzen (nutzt intern "get" statt "GET")
            if hasattr(self.client, "get_user_groups"):
                groups_list = await self.client.get_user_groups(user_id)
                groups = set(groups_list) if isinstance(groups_list, (list, set)) else set()
            else:
                response = await self.client._ocs_request(
                    "get", f"/cloud/users/{user_id}/groups"
                )
                if isinstance(response, dict):
                    data = response.get("ocs", {}).get("data", {})
                    groups_list = data.get("groups", []) if isinstance(data, dict) else []
                    groups = set(groups_list)
                else:
                    groups = set()

            self._group_cache[user_id] = (now, groups)
            return groups

        except Exception as e:
            err_str = str(e)
            if "998" in err_str:
                logger.debug(
                    f"User '{user_id}' ist kein regulärer Nextcloud-User oder besitzt keine Gruppen (OCS 998)."
                )
                groups = set()
                self._group_cache[user_id] = (now, groups)
                return groups

            logger.warning(f"Konnte Gruppen für User {user_id} nicht abfragen: {e}")
            return set()

    def set_contextvars_identity(self, user_id: str, groups: Set[str]) -> None:
        """Sets context variables for outgoing calls if needed."""
        pass

    def clear_contextvars_identity(self) -> None:
        """Clears context variables after processing."""
        pass

    def clear_cache(self) -> None:
        """Clears the internal group cache."""
        self._group_cache.clear()
