import logging
from typing import Dict, List, Optional, Set
import time

logger = logging.getLogger(__name__)

# Lazy-Import der ContextVars aus hermes-x-on-behalf (optional installiert)
_xonbehalf_vars: Optional[tuple] = None


def _get_xonbehalf_vars() -> Optional[tuple]:
    """Lädt (current_user_id, current_user_groups) aus hermes-x-on-behalf, falls verfügbar."""
    global _xonbehalf_vars
    if _xonbehalf_vars is not None:
        return _xonbehalf_vars
    try:
        from hermes_x_on_behalf.plugin import current_user_id, current_user_groups
        _xonbehalf_vars = (current_user_id, current_user_groups)
    except Exception:
        try:
            # Fallback: Plugin-Verzeichnis liegt als Schwesterprojekt im Workspace
            import importlib.util, os, sys
            plugin_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "hermes-x-on-behalf",
            )
            if os.path.isdir(plugin_path):
                pkg = type(sys)("hermes_x_on_behalf")
                pkg.__path__ = [plugin_path]
                sys.modules.setdefault("hermes_x_on_behalf", pkg)
                plugin_mod = importlib.import_module("hermes_x_on_behalf.plugin")
                _xonbehalf_vars = (plugin_mod.current_user_id, plugin_mod.current_user_groups)
            else:
                _xonbehalf_vars = (None, None)
        except Exception as exc:
            logger.debug(f"hermes-x-on-behalf ContextVars nicht verfügbar: {exc}")
            _xonbehalf_vars = (None, None)
    return _xonbehalf_vars


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
            # Bevorzugt: cloud_ocs_get (Provisioning API v1, korrekter Endpunkt)
            if hasattr(self.client, "cloud_ocs_get"):
                data = await self.client.cloud_ocs_get(f"users/{user_id}/groups")
                groups_list = data.get("groups", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
                groups = set(groups_list) if isinstance(groups_list, (list, set)) else set()
            elif hasattr(self.client, "get_user_groups"):
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
            status_code = getattr(e, "status_code", None)
            # OCS 998 = User existiert nicht / kein regulärer User → keine Gruppen
            if "998" in err_str or status_code == 998:
                logger.debug(
                    f"User '{user_id}' ist kein regulärer Nextcloud-User oder besitzt keine Gruppen (OCS 998)."
                )
                groups = set()
                self._group_cache[user_id] = (now, groups)
                return groups

            logger.warning(f"Konnte Gruppen für User {user_id} nicht abfragen: {e}")
            return set()

    def set_contextvars_identity(self, user_id: str, groups: Set[str]) -> None:
        """Setzt die ContextVars von hermes-x-on-behalf für die HTTP-Header-Injektion."""
        vars_pair = _get_xonbehalf_vars()
        if vars_pair is None or vars_pair[0] is None:
            return
        current_user_id, current_user_groups = vars_pair
        try:
            current_user_id.set(str(user_id) if user_id else None)
            current_user_groups.set(",".join(sorted(groups)) if groups else None)
        except Exception as exc:
            logger.debug(f"Konnte Identity-ContextVars nicht setzen: {exc}")

    def clear_contextvars_identity(self) -> None:
        """Räumt die Identity-ContextVars auf (z. B. nach Session-Ende)."""
        vars_pair = _get_xonbehalf_vars()
        if vars_pair is None or vars_pair[0] is None:
            return
        current_user_id, current_user_groups = vars_pair
        try:
            current_user_id.set(None)
            current_user_groups.set(None)
        except Exception as exc:
            logger.debug(f"Konnte Identity-ContextVars nicht leeren: {exc}")

    def clear_cache(self) -> None:
        """Clears the internal group cache."""
        self._group_cache.clear()
