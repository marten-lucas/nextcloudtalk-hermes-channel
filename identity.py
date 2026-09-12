from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Dict, List, Optional, Set

if TYPE_CHECKING:
    from hermes_x_on_behalf import PrincipalContext

logger = logging.getLogger(__name__)


def _get_xonbehalf():
    """Lädt das hermes-x-on-behalf-Paket, falls verfügbar (optional dependency).

    Sucht in dieser Reihenfolge:
    1. ``hermes_x_on_behalf`` (direkt installiert)
    2. ``hermes_plugins.hermes_x_on_behalf`` (Hermes-Plugin-Loader)
    3. Schwester-Verzeichnis ``hermes-x-on-behalf`` (Workspace-Layout)
    """
    for module_name in ("hermes_x_on_behalf", "hermes_plugins.hermes_x_on_behalf"):
        try:
            import importlib

            mod = importlib.import_module(module_name)
            if hasattr(mod, "PrincipalContext"):
                return mod
        except Exception:
            continue
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
            return importlib.import_module("hermes_x_on_behalf")
        return None
    except Exception as exc:
        logger.debug(f"hermes-x-on-behalf nicht verfügbar: {exc}")
        return None


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
                # cloud_ocs_get returns the FULL OCS body ({"ocs": {"meta":..., "data": {...}}}),
                # unlike ocs_get which unwraps to body["ocs"]["data"]. Also tolerate a client
                # that already unwraps to the data dict directly.
                if isinstance(data, dict):
                    if "ocs" in data:
                        data = data.get("ocs", {}).get("data", {})
                    groups_list = data.get("groups", []) if isinstance(data, dict) else []
                elif isinstance(data, list):
                    groups_list = data
                else:
                    groups_list = []
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

    def build_principal(
        self,
        user_id: str,
        groups: Set[str],
        room_id: str,
        is_group_chat: bool,
        conversation_description: Optional[str] = None,
    ):
        """Baut einen PrincipalContext für eine eingehende Nachricht.

        1:1-Chat: keine Raum-Scopes (conversation_id bleibt leer).
        Gruppenraum: room_id als conversation_id (+ optionale Memory-Tag-
        Beschreibung für das deterministische Scope-Routing).
        """
        xob = _get_xonbehalf()
        if xob is None or not user_id:
            return None
        try:
            conversation_id = f"talk:room:{room_id}" if (is_group_chat and room_id) else None
            return xob.PrincipalContext.interactive(
                user_id=str(user_id),
                groups=groups,
                conversation_id=conversation_id,
                channel="nextcloud-talk",
                conversation_description=conversation_description,
            )
        except Exception as exc:
            logger.debug(f"Konnte PrincipalContext nicht bauen: {exc}")
            return None

    def principal_context(self, principal):
        """Context-Manager mit Token-basiertem Set/Reset (leak-proof)."""
        xob = _get_xonbehalf()
        return xob.principal_context(principal)

    def principal_headers(self, principal) -> Dict[str, str]:
        """Leitet die Propagation-Header aus dem PrincipalContext ab."""
        xob = _get_xonbehalf()
        if xob is None or principal is None:
            return {}
        try:
            return xob.principal_to_headers(principal)
        except Exception:
            return {}

    def clear_cache(self) -> None:
        """Clears the internal group cache."""
        self._group_cache.clear()
