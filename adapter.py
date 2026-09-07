from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .attachments import NextcloudAttachmentManager
from .client import NextcloudOCSException, NextcloudTalkClient
from .hitl import HITLManager
from .identity import NextcloudIdentityManager
from .outbound import categorize_gateway_message
from .presence import NextcloudPresenceManager
from .signaling import NextcloudSignalingManager


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

    class MessageType:
        TEXT = "text"
        COMMAND = "command"

    @dataclass
    class SendResult:
        success: bool
        message_id: Optional[str] = None
        error: Optional[str] = None

    @dataclass
    class MessageEvent:
        text: str
        message_type: str
        source: Any
        raw_message: Dict[str, Any]
        message_id: Optional[str] = None
        reply_to_message_id: Optional[str] = None
        user_id: Optional[str] = None
        user_name: Optional[str] = None

    class BasePlatformAdapter:
        def __init__(self, config: Any, platform: str = "nextcloud") -> None:
            self.config = config
            self.platform = platform

        def build_source(self, **kwargs: Any) -> Dict[str, Any]:
            return kwargs

        async def handle_message(self, event: MessageEvent) -> None:
            return None

        async def cancel_session_processing(
            self,
            session_key: str,
            **_: Any,
        ) -> None:
            return None

        def _mark_disconnected(self) -> None:
            return None


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


class NextcloudTalkPlatform(BasePlatformAdapter):
    """Refactored Nextcloud Talk Adapter mit modularer Submodul-Struktur."""

    supports_status_text = True

    @staticmethod
    def _resolve_platform():
        """Platform-Member robust auflösen (auch ohne Plugin-Discovery).

        ``Platform("nextcloud")`` erzeugt nur dann dynamisch ein Pseudo-
        Member, wenn das Plugin gebündelt ist oder die Registry den Namen
        kennt. In Test- oderisolierteren Kontexten ist beides nicht der
        Fall — dann fällt die Auflösung auf "matrix" (immer vorhanden)
        zurück; das Tag dient nur der Adapter-Kennzeichnung.
        """
        try:
            return Platform("nextcloud")
        except ValueError:
            try:
                from gateway.platform_registry import platform_registry

                if platform_registry.is_registered("nextcloud"):
                    return Platform("nextcloud")
            except Exception:
                pass
            return Platform("matrix")

    def __init__(self, config: PlatformConfig):
        super().__init__(config, self._resolve_platform())

        extra = getattr(config, "extra", {}) or {}

        base_url = str(
            extra.get("base_url")
            or os.getenv("NEXTCLOUD_BASE_URL", "")
        ).rstrip("/")

        username = str(
            extra.get("username")
            or os.getenv("NEXTCLOUD_USERNAME", "")
        )

        app_password = str(
            extra.get("app_password")
            or getattr(config, "token", "")
            or os.getenv("NEXTCLOUD_APP_PASSWORD", "")
        )

        self.runtime = NextcloudRuntimeConfig(
            base_url=base_url,
            username=username,
            app_password=app_password,
            bot_handle=str(
                extra.get("bot_handle")
                or os.getenv("NEXTCLOUD_BOT_HANDLE", "")
            ).strip()
            or f"@{username}",
            require_mention_in_groups=str(
                extra.get("require_mention_in_groups")
                or os.getenv(
                    "NEXTCLOUD_REQUIRE_MENTION_IN_GROUPS",
                    "true",
                )
            ).lower()
            in {"1", "true", "yes"},
            context_message_limit=int(
                extra.get("context_message_limit")
                or os.getenv(
                    "NEXTCLOUD_CONTEXT_MESSAGE_LIMIT",
                    20,
                )
            ),
            poll_interval_seconds=float(
                extra.get("poll_interval_seconds")
                or os.getenv(
                    "NEXTCLOUD_POLL_INTERVAL_SECONDS",
                    3.0,
                )
            ),
            allowed_rooms={
                room.strip()
                for room in str(
                    extra.get("allowed_rooms")
                    or os.getenv("NEXTCLOUD_ALLOWED_ROOMS", "")
                ).split(",")
                if room.strip()
            },
            attachment_tmp_dir=str(
                extra.get("attachment_tmp_dir")
                or os.getenv("NEXTCLOUD_ATTACHMENT_TMP_DIR", "")
            ),
            hitl_require_requester=str(
                extra.get("hitl_require_requester")
                or os.getenv("NEXTCLOUD_HITL_REQUIRE_REQUESTER", "true")
            ).lower()
            in {"1", "true", "yes"},
        )

        self.client = NextcloudTalkClient(
            base_url,
            username,
            app_password,
        )

        self.identity_mgr = NextcloudIdentityManager(
            self.client,
            cache_ttl_seconds=120,
        )

        self.hitl_mgr = HITLManager(
            enforce_requester_only=self.runtime.hitl_require_requester
        )

        self.presence_mgr = NextcloudPresenceManager(
            self.client
        )

        self.attachment_mgr = NextcloudAttachmentManager(
            self.client,
            tmp_dir=self.runtime.attachment_tmp_dir,
        )

        self.signaling_mgr = NextcloudSignalingManager(
            self.client,
            self.handle_incoming_event,
        )

        self._stop_event = asyncio.Event()
        self._polling_task: Optional[asyncio.Task[None]] = None
        self._poll_cursor_by_room: Dict[str, str] = {}
        self._poll_bootstrapped_rooms: set[str] = set()
        self._message_index: Dict[str, Dict[str, Any]] = {}
        self._message_session_keys: Dict[str, Dict[str, str]] = {}
        self._session_reset_noted_rooms: set[str] = set()
        self._sent_message_ids: List[str] = []
        self._room_ws_tasks: Dict[str, asyncio.Task[None]] = {}
        # Cache für Raum-Metadaten (readOnly-Flag), TTL-gesteuert
        self._room_meta_cache: Dict[str, Dict[str, Any]] = {}
        self._room_meta_cache_ts: Dict[str, float] = {}
        self._room_meta_ttl_seconds: float = 300.0

        # ── Empfangs-Überwachung (Resilience) ──
        # Letzter empfangener Event/Poll pro Raum (time.monotonic).
        self._last_rx: Dict[str, float] = {}
        # Watchdog-Task: prükt periodisch, ob Empfangspfade gestorben
        # oder gestalled sind, und startet sie neu.
        self._watchdog_task: Optional[asyncio.Task[None]] = None
        # Periodisches Low-Frequency-Polling läuft IMMER als Sicherheitsschicht
        # unter dem WebSocket-Pfad (Dedupe über _poll_cursor/_message_index).
        self._lfp_enabled: bool = str(
            os.getenv("NEXTCLOUD_LOWFREQ_POLLING", "true")
        ).lower() in {"1", "true", "yes"}
        self._lfp_interval_seconds: float = 60.0
        # Ein WS-Empfang gilt nach so vielen Sekunden ohne Event als stalled.
        self._rx_stall_seconds: float = float(
            os.getenv("NEXTCLOUD_RX_STALL_SECONDS", "180")
        )


    @property
    def is_connected(self) -> bool:
        """True nur, wenn mindestens ein Empfangspfad lebendig ist.

        Ein gestarteter, aber längst abgestorbener WS-Task oder ein toter
        Polling-Task darf nicht als „verbunden" gemeldet werden — sonst
        verhindert der Status Reconnects und täuscht Dashboard/Gateway.
        """
        if self._stop_event.is_set():
            return False
        if self._polling_task and not self._polling_task.done():
            return True
        return any(
            task and not task.done()
            for task in self._room_ws_tasks.values()
        )

    async def connect(
        self,
        *,
        is_reconnect: bool = False,
    ) -> bool:
        self._stop_event.clear()

        await self.client.ensure_session()

        ws_connected = await self._connect_websocket_once()

        if not ws_connected:
            logger.info("Nextcloud Talk: WebSocket nicht verfügbar, nutze Polling-Fallback.")
            self._start_polling_loop()
        else:
            logger.info("Nextcloud Talk: WebSocket-Signaling gestartet.")

        # Dauerhaftes Low-Frequency-Polling als Sicherheitsschicht unter
        # dem WS-Pfad (Dedupe verhindert Doppelzustellung). Fängt Nachrichten
        # auf, die ein still gestorbenes WS-Signaling verpasst.
        if self._lfp_enabled:
            self._start_polling_loop()

        # Watchdog: überwacht WS-Tasks und Empfangsaktivität.
        self._start_watchdog()

        await self.presence_mgr.set_presence_status(
            "online"
        )

        await self.presence_mgr.clear_custom_status_message(
            force=True
        )

        logger.info(
            "Nextcloud Talk: Adapter erfolgreich verbunden."
        )

        return True

    async def disconnect(self) -> None:
        self._stop_event.set()

        tasks = [
            task
            for task in (
                self._polling_task,
                self._watchdog_task,
                *self._room_ws_tasks.values(),
            )
            if task
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._polling_task = None
        self._watchdog_task = None
        self._room_ws_tasks = {}

        await self.presence_mgr.set_presence_status(
            "offline"
        )

        await self.client.close()

        self._mark_disconnected()

    def _start_polling_loop(self) -> None:
        if self._polling_task and not self._polling_task.done():
            return
        self._polling_task = asyncio.create_task(
            self._polling_loop()
        )

    async def _connect_websocket_once(self) -> bool:
        """Versucht, WebSocket-Signaling für alle erlaubten Räume zu starten."""
        try:
            room_ids = await self._list_joined_rooms()
        except Exception as exc:
            logger.warning("Nextcloud Talk: Raumliste für Signaling fehlgeschlagen: %s", exc)
            return False

        started_any = False
        for room_id in room_ids:
            if self.runtime.allowed_rooms and room_id not in self.runtime.allowed_rooms:
                continue
            settings = await self.signaling_mgr.get_signaling_settings(room_id)
            if not settings:
                continue
            task = asyncio.create_task(
                self._supervised_room_loop(
                    room_id,
                    settings,
                )
            )
            self._room_ws_tasks[room_id] = task
            started_any = True

        return started_any

    async def _supervised_room_loop(
        self,
        room_id: str,
        settings: Any,
    ) -> None:
        """Supervidiert den WS-Signaling-Loop eines Raums mit Auto-Reconnect.

        Der innere Loop endet bei WS-Close/Timeout still — ohne Supervisor
        würde der Empfangspfad verlöschen, während der Adapter weiter
        „verbunden" meldet. Hier wird der Loop alle ``_rx_stall_seconds``-
        unabhängigen 5 s nach Ende neu gestartet, bis der Adapter stoppt.
        """
        backoff = 5.0
        while not self._stop_event.is_set():
            try:
                await self.signaling_mgr.room_signaling_loop(
                    room_id,
                    settings,
                    self._fetch_room_events,
                    self._stop_event,
                )
                if self._stop_event.is_set():
                    return
                logger.warning(
                    "Nextcloud Talk: WS für Raum %s endete — Reconnect in %.0fs",
                    room_id,
                    backoff,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._stop_event.is_set():
                    return
                logger.warning(
                    "Nextcloud Talk: WS für Raum %s crashte: %s — Reconnect in %.0fs",
                    room_id,
                    exc,
                    backoff,
                )
            await asyncio.sleep(backoff)

    def _start_watchdog(self) -> None:
        if self._watchdog_task and not self._watchdog_task.done():
            return
        self._watchdog_task = asyncio.create_task(self._watchdog_loop())

    async def _watchdog_loop(self) -> None:
        """Erkennt tote/gestallte Empfangspfade und startet sie neu.

        Kriterien pro Raum:
        - WS-Task done → sofort neu starten
        - Kein RX seit ``_rx_stall_seconds`` (WS lebt, HPB liefert nichts) →
          Task killen und neu starten (Supervisor übernimmt den Reconnect).
        """
        while not self._stop_event.is_set():
            try:
                await asyncio.sleep(60.0)
                if self._stop_event.is_set():
                    return

                now = time.monotonic()
                for room_id, task in list(self._room_ws_tasks.items()):
                    if self._stop_event.is_set():
                        return

                    if task.done():
                        logger.warning(
                            "Nextcloud Talk: Watchdog — WS-Task für Raum %s ist done; starte neu.",
                            room_id,
                        )
                        await self._restart_room_ws(room_id)
                        continue

                    last_rx = self._last_rx.get(room_id)
                    if (
                        last_rx is not None
                        and (now - last_rx) > self._rx_stall_seconds
                    ):
                        logger.warning(
                            "Nextcloud Talk: Watchdog — kein Empfang in Raum %s seit %.0fs; starte WS neu.",
                            room_id,
                            now - last_rx,
                        )
                        await self._restart_room_ws(room_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Nextcloud Talk: Watchdog-Fehler: %s", exc)

    async def _restart_room_ws(self, room_id: str) -> None:
        old = self._room_ws_tasks.get(room_id)
        if old and not old.done():
            old.cancel()
            try:
                await old
            except (asyncio.CancelledError, Exception):
                pass
        settings = None
        try:
            settings = await self.signaling_mgr.get_signaling_settings(room_id)
        except Exception as exc:
            logger.warning(
                "Nextcloud Talk: Signaling-Settings für Raum %s nicht abrufbar: %s",
                room_id,
                exc,
            )
        if not settings:
            # Raum ggf. nicht mehr vorhanden — Task-Eintrag entfernen.
            self._room_ws_tasks.pop(room_id, None)
            self._last_rx.pop(room_id, None)
            return
        self._room_ws_tasks[room_id] = asyncio.create_task(
            self._supervised_room_loop(room_id, settings)
        )
        self._last_rx[room_id] = time.monotonic()

    async def _list_joined_rooms(self) -> List[str]:
        data = await self.client.ocs_get(
            "apps/spreed/api/v4/room",
            params={"includeStatus": "true"},
        )
        if isinstance(data, list):
            return [
                str(room.get("token", "") or room.get("id", ""))
                for room in data
                if room
            ]
        return []

    async def _polling_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                rooms = await self.client.ocs_get(
                    "apps/spreed/api/v4/room",
                    params={"includeStatus": "true"},
                )

                if isinstance(rooms, list):
                    for room in rooms:
                        room_id = str(
                            room.get("token", "")
                            or room.get("id", "")
                        )

                        if not room_id:
                            continue

                        if self.runtime.allowed_rooms and room_id not in self.runtime.allowed_rooms:
                            continue

                        # RX-Zeitstempel auch für Polls pflegen — der Watchdog
                        # behandelt Polling als gültigen Empfangspfad.
                        self._last_rx[room_id] = time.monotonic()

                        events = await self._fetch_room_events(
                            room_id
                        )

                        for event in events:
                            await self.handle_incoming_event(
                                event
                            )

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                logger.warning(
                    "Nextcloud Polling Fehler: %s",
                    exc,
                )

            await asyncio.sleep(
                self.runtime.poll_interval_seconds
            )

    async def _fetch_room_events(
        self,
        room_id: str,
    ) -> List[Dict[str, Any]]:
        # Bootstrap: erster Poll pro Raum setzt nur den Cursor auf die
        # neueste Message und dispatcht keine Altlasten (Backlog-Skip).
        if room_id not in self._poll_bootstrapped_rooms:
            data = await self.client.ocs_get(
                f"apps/spreed/api/v1/chat/{room_id}",
                params={"lookIntoFuture": 0, "limit": 50},
            )
            latest_id = self._latest_message_id(data if isinstance(data, list) else [])
            if latest_id:
                self._poll_cursor_by_room[room_id] = latest_id
            self._poll_bootstrapped_rooms.add(room_id)
            return []

        params: Dict[str, Any] = {
            "lookIntoFuture": 0,
            "limit": 50,
        }

        if room_id in self._poll_cursor_by_room:
            params["lookIntoFuture"] = 1
            params["lastKnownMessageId"] = (
                self._poll_cursor_by_room[room_id]
            )

        data = await self.client.ocs_get(
            f"apps/spreed/api/v1/chat/{room_id}",
            params=params,
        )

        events: List[Dict[str, Any]] = []

        if isinstance(data, list):
            for event in data:
                normalized = dict(event)
                normalized.setdefault(
                    "room_id",
                    room_id,
                )

                events.append(normalized)

            # Cursor auf die HÖCHSTE Message-ID setzen (Talk liefert
            # absteigende Reihenfolge; die letzte in der Liste ist die
            # älteste — siehe Fix f9f057b).
            latest_id = self._latest_message_id(data)
            if latest_id:
                self._poll_cursor_by_room[room_id] = latest_id

        return events

    @staticmethod
    def _latest_message_id(messages: List[Dict[str, Any]]) -> Optional[str]:
        """Ermittelt die höchste Message-ID (Talk liefert absteigende Reihenfolge)."""
        best: Optional[int] = None
        best_id: Optional[str] = None
        for item in messages:
            try:
                numeric = int(str(item.get("id", "")).strip() or 0)
            except (TypeError, ValueError):
                continue
            if best is None or numeric > best:
                best = numeric
                best_id = str(item.get("id", ""))
        return best_id

    async def handle_incoming_event(
        self,
        event: Dict[str, Any],
    ) -> None:
        # RX-Zeitstempel für den Watchdog (Stall-Erkennung).
        room_id = str(event.get("room_id") or "")
        if room_id:
            self._last_rx[room_id] = time.monotonic()

        # Dedupe: WS-Pfad und Low-Frequency-Polling können dieselbe Nachricht
        # liefern (Cursor setzen unterschiedlich). Verarbeitung nur einmalig
        # pro Message-ID — erste Zustellung gewinnt.
        dedupe_id = str(
            event.get("id")
            or event.get("message_id")
            or event.get("messageId")
            or ""
        )
        if dedupe_id:
            if not hasattr(self, "_dispatched_ids"):
                self._dispatched_ids: Dict[str, float] = {}
            now_mono = time.monotonic()
            first_seen = self._dispatched_ids.get(dedupe_id)
            if first_seen is not None:
                return
            self._dispatched_ids[dedupe_id] = now_mono
            # Ring-Limit: alte IDs entfernen (Speicher begrenzen)
            if len(self._dispatched_ids) > 5000:
                cutoff = now_mono - 3600.0
                self._dispatched_ids = {
                    k: v for k, v in self._dispatched_ids.items() if v > cutoff
                }

        event_type = str(event.get("eventType", event.get("type", "message"))).lower()

        # Reaction-Events an HITL-Manager dispatchen
        if "reaction" in event_type or str(event.get("type", "")).lower() == "reaction":
            await self._handle_reaction(event)
            return

        # 1. Native Nextcloud Actor-Type & SystemMessage Flags auswerten
        actor_type = str(event.get("actorType") or event.get("actor_type") or "").strip().lower()
        if actor_type and actor_type != "users":
            logger.debug(f"Nextcloud: Ignoriere Nachricht von Nicht-User Actor (actor_type={actor_type})")
            return

        is_edit = "edit" in event_type
        is_delete = "delete" in event_type or "remove" in event_type
        if (event.get("systemMessage") or event.get("system_message")) and not is_delete:
            logger.debug("Nextcloud: Ignoriere native SystemMessage.")
            return

        sender_id = str(
            event.get("actorId")
            or event.get("actor_id")
            or event.get("sender")
            or event.get("userId")
            or ""
        )

        # 2. Ignoriere eigene Nachrichten, leere Absender sowie reservierte System-Accounts
        if (
            not sender_id
            or sender_id == self.runtime.username
            or sender_id.lower() in {"system", "changelog", "sample"}
        ):
            return

        room_id = str(
            event.get("room_id")
            or event.get("token")
            or ""
        )
        if not room_id:
            return

        if self.runtime.allowed_rooms and room_id not in self.runtime.allowed_rooms:
            return

        body = str(
            event.get("message")
            or event.get("text")
            or ""
        )

        # 3. Ignoriere systemgenerierte Nachrichten-Muster & Platzhalter
        if (
            "{actor}" in body
            or "Das System hat" in body
            or "Gesprächseinstellungen verwalten" in body
            or "Unterhaltungsinformationen bearbeiten" in body
        ):
            logger.debug(f"Nextcloud: Ignoriere automatische System-Textnachricht: {body[:30]}...")
            return

        # 4. Message-ID-Korrelation (Edit/Delete beziehen sich auf Original)
        message_id = str(event.get("id") or event.get("message_id") or event.get("messageId") or "")
        original_message_id = str(
            event.get("messageId")
            or event.get("objectId")
            or event.get("referenceId")
            or message_id
        ).strip() or message_id
        original_record = self._message_index.get(original_message_id, {})

        # 5. Emoji-Reply-Fallback für HITL (✅/❌ als Chat-Nachricht statt Reaction)
        if await self._handle_reaction_fallback_from_message(event, sender_id, body):
            return

        # 6. Attachments extrahieren
        attachments = self.attachment_mgr.extract_attachments(event)

        trigger_text = body
        timestamp_source = original_record.get("timestamp") or event.get("timestamp") or event.get("datetime")
        time_label = self._format_event_time(timestamp_source)

        # 7. Edit/Delete-Semantik
        if is_edit:
            body = f"Vergangene Nachricht von {time_label} wurde geaendert zu:\n{body.strip()}".strip()
        elif is_delete:
            body = f"Nachricht von {time_label} wurde geloescht."
            trigger_text = str(original_record.get("text") or "")

        # 8. Leere Nachrichten ohne Attachments ignorieren
        if not body.strip() and not attachments:
            logger.debug("Nextcloud: Ignoriere leere Nutzer-Nachricht in Raum %s", room_id)
            return

        # 9. Participant-Count & Mention-Gating (Loop-/Flut-Prävention in Gruppen)
        participant_count = await self._resolve_participant_count(room_id, event)
        if not self._should_trigger(trigger_text or body, participant_count):
            return

        # 10. Kontext-Abruf für Gruppenräume
        context_messages: List[Dict[str, Any]] = []
        if participant_count > 2:
            context_messages = await self.fetch_last_messages(
                room_id,
                limit=self.runtime.context_message_limit,
            )

        # 11. Attachments herunterladen
        attachment_paths: List[str] = []
        for attachment in attachments:
            path = await self._download_attachment_from_metadata(attachment)
            if path:
                attachment_paths.append(path)

        # 12. Command-Normalisierung (!cmd -> /cmd)
        body = self._normalize_nextcloud_command(body)

        groups = await self.identity_mgr.get_user_groups(
            sender_id
        )

        # Memory-Tag aus der Raum-Beschreibung (Konvention: [memory:team:...])
        # — skalierbar für neue Räume ohne Plugin-Konfig. Fail-open: Bei API-
        # Fehlern bleibt die Beschreibung None (→ fallback_scope).
        room_meta = await self._get_room_meta(room_id) or {}
        room_description = str(room_meta.get("description") or "") or None

        principal = self.identity_mgr.build_principal(
            user_id=sender_id,
            groups=groups,
            room_id=room_id,
            is_group_chat=participant_count > 2,
            conversation_description=room_description,
        )

        source = self.build_source(
            chat_id=room_id,
            chat_name=room_id,
            chat_type="dm" if participant_count <= 2 else "group",
            user_id=sender_id,
            user_name=sender_id,
        )

        headers = self.identity_mgr.principal_headers(principal) or {
            "X-On-Behalf-Of": sender_id,
            "X-User-Groups": ",".join(groups),
        }
        # extra_headers plattformneutral transportieren:
        # - dict-Sources (Test-Fallback-Base) → Key-Setzung
        # - reale SessionSource-Objekte (Hermes >= 0.20) → Attribut
        #   (Dataclass ist nicht frozen; das x-on-behalf-Plugin liest
        #   getattr(source, "extra_headers"))
        if isinstance(source, dict):
            source["extra_headers"] = headers
        else:
            try:
                setattr(source, "extra_headers", headers)
            except Exception:
                logger.warning(
                    "Nextcloud: Konnte extra_headers nicht an SessionSource hängen."
                )

        # 13. Session-Key-Korrelation für Cancel/HITL
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

        # 14. Fresh-Session-Reset-Note
        reset_note = await self._fresh_session_note(source, room_id, original_message_id)
        if reset_note:
            body = f"{reset_note}\n\n{body}"

        event_payload = dict(event)
        event_payload["context_messages"] = context_messages
        event_payload["attachment_paths"] = attachment_paths
        event_payload["original_message_id"] = original_message_id
        event_payload["is_edit_event"] = is_edit
        event_payload["is_delete_event"] = is_delete
        event_payload["user_groups"] = list(groups)

        msg_event = MessageEvent(
            text=body,
            message_type=MessageType.COMMAND if body.strip().startswith("/") else MessageType.TEXT,
            source=source,
            raw_message=event_payload,
            message_id=message_id or None,
            user_id=sender_id,
            user_name=sender_id,
        )

        # Principal-Context als lokaler with-Block um die komplette
        # Agent-Bearbeitung (Token-Reset im finally — kein Identity-Leak,
        # kein shared state bei parallelen Events). Presence busy für die
        # Turn-Dauer, zurück auf online danach.
        await self.mark_turn_started()
        try:
            if principal is not None:
                with self.identity_mgr.principal_context(principal):
                    await self.handle_message(msg_event)
            else:
                await self.handle_message(msg_event)
        finally:
            await self.mark_turn_finished()
            # Typing-Indikator spätestens jetzt beenden (safety net — der
            # Gateway ruft stop_typing normalerweise selbst).
            try:
                await self.presence_mgr.stop_typing(room_id)
            except Exception:
                pass

    async def _get_room_meta(self, room_id: str) -> Optional[Dict[str, Any]]:
        """Raum-Metadaten (Existenz + readOnly) mit TTL-Cache abrufen.

        Liefert ein leeres Dict, wenn der Raum nicht existiert (HTTP 404) —
        in dem Fall darf nicht gesendet werden. Liefert None bei anderen
        Fehlern (Prüfung nicht möglich, fail-open).
        """
        import time as _time

        now = _time.monotonic()
        cached_ts = self._room_meta_cache_ts.get(room_id)
        if cached_ts is not None and (now - cached_ts) < self._room_meta_ttl_seconds:
            return self._room_meta_cache.get(room_id)

        try:
            meta = await self.client.ocs_get(f"apps/spreed/api/v4/room/{room_id}")
        except NextcloudOCSException as exc:
            if exc.status_code == 404:
                logger.warning(
                    "[Nextcloud Talk] Raum '%s' existiert nicht (mehr) — Senden übersprungen.",
                    room_id,
                )
                self._room_meta_cache[room_id] = {}
                self._room_meta_cache_ts[room_id] = now
                return {}
            logger.warning(
                "[Nextcloud Talk] Raum-Metadaten für '%s' nicht abrufbar (HTTP %s): %s",
                room_id,
                exc.status_code,
                exc.message,
            )
            return None
        except Exception as exc:
            logger.warning(
                "[Nextcloud Talk] Raum-Metadaten für '%s' nicht abrufbar: %s",
                room_id,
                exc,
            )
            return None

        if not isinstance(meta, dict) or not meta:
            return None

        self._room_meta_cache[room_id] = meta
        self._room_meta_cache_ts[room_id] = now
        return meta

    async def _can_send_to_room(self, room_id: str) -> tuple[bool, Optional[str]]:
        """Prüft Existenz und Sendeberechtigung eines Raums, bevor gepostet wird.

        Returns:
            (ok, reason): ok=False mit reason, wenn das Senden abgebrochen
            werden soll. Bei unklarer Prüfung (API-Fehler) wird ok=True
            zurückgegeben, damit der POST es weiterhin versuchen darf.
        """
        meta = await self._get_room_meta(room_id)
        if meta is None:
            # Prüfung nicht möglich — POST darf es versuchen (fail-open)
            return True, None
        if not meta:
            # 404: Raum existiert nicht (mehr)
            return False, f"Room '{room_id}' does not exist (404)"

        # readOnly: 0 = beschreibbar, 1 = nur Moderatoren, 2 = nur Admins.
        # participantType: 1 = Owner, 2 = Moderator, 3 = User, 4 = Guest.
        read_only = int(meta.get("readOnly") or 0)
        participant_type = int(meta.get("participantType") or 0)
        is_moderator = participant_type in (1, 2)

        if read_only and not is_moderator:
            return False, (
                f"Room '{room_id}' is read-only (readOnly={read_only}) "
                f"and bot is not a moderator (participantType={participant_type})"
            )

        return True, None

    def _should_trigger(self, body: str, participant_count: int) -> bool:
        """Mention-Gating: DMs (<=2) immer, Gruppen nur bei Mention (konfigurierbar)."""
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
        """Teilnehmerzahl: API-first, Event-Fallback, Default 3 (sicher = Gruppe)."""
        try:
            api_participants = await self.client.ocs_get(
                f"apps/spreed/api/v4/room/{room_id}/participants"
            )
        except Exception:
            api_participants = None
        if isinstance(api_participants, list):
            return len(api_participants)
        if "participant_count" in event:
            try:
                return int(event["participant_count"])
            except (TypeError, ValueError):
                pass
        participants = event.get("participants")
        if isinstance(participants, list):
            return len(participants)
        return 3

    async def fetch_last_messages(self, room_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        data = await self.client.ocs_get(
            f"apps/spreed/api/v1/chat/{room_id}",
            params={"lookIntoFuture": 0, "limit": limit},
        )
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

    async def _handle_reaction(self, event: Dict[str, Any]) -> None:
        """Reaktion (✅/👍/❌/👎/⛔) auf Approval-Prompt oder laufende Session."""
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

        if emoji in HITLManager.cancel_reactions:
            session_info = self._message_session_keys.get(target_message_id)
            if session_info and reactor_id == session_info.get("requester_user_id"):
                await self.cancel_session_processing(session_info["session_key"])
            return

        await self.hitl_mgr.handle_reaction(
            event,
            cancel_callback=self._hitl_cancel_callback,
        )

    async def _hitl_cancel_callback(self, target_message_id: str, reactor_id: str) -> None:
        session_info = self._message_session_keys.get(target_message_id)
        if session_info and reactor_id == session_info.get("requester_user_id"):
            await self.cancel_session_processing(session_info["session_key"])

    async def _handle_reaction_fallback_from_message(
        self,
        event: Dict[str, Any],
        sender_id: str,
        body: str,
    ) -> bool:
        """Emoji-Reply-Fallback: nacktes ✅/❌ als Chat-Nachricht statt Reaction."""
        emoji = body.strip()
        if not self.hitl_mgr.is_fallback_emoji_reply(emoji):
            return False

        target_message_id = self.hitl_mgr.resolve_fallback_target(event)
        if not target_message_id:
            return False

        await self._handle_reaction(
            {
                "targetMessageId": target_message_id,
                "actorId": sender_id,
                "emoji": emoji,
            }
        )
        return True

    async def _download_attachment_from_metadata(self, attachment: Dict[str, Any]) -> Optional[str]:
        file_id = attachment.get("id") or attachment.get("fileId")
        path = attachment.get("path") or attachment.get("filePath")
        url = attachment.get("url") or attachment.get("downloadUrl")
        try:
            return await self.attachment_mgr.download_attachment(
                file_id=file_id,
                remote_path=path,
                file_url=url,
            )
        except Exception as exc:
            logger.warning("Nextcloud: Attachment-Download fehlgeschlagen: %s", exc)
            return None

    async def request_human_approval(
        self,
        room_id: str,
        prompt_message_id: str,
        requester_user_id: str,
    ) -> bool:
        """HITL-Contract: wartet auf Reaction des Requesters (kein Timeout)."""
        return await self.hitl_mgr.request_approval(
            room_id=room_id,
            prompt_message_id=prompt_message_id,
            requester_user_id=requester_user_id,
        )


    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Hermes platform contract: metadata for a Talk room."""
        meta = await self._get_room_meta(chat_id) or {}
        return {
            "id": chat_id,
            "target": chat_id,
            "type": "nextcloud_talk_room",
            "name": str(meta.get("displayName") or meta.get("name") or chat_id),
            "participant_count": meta.get("participantCount"),
            "read_only": meta.get("readOnly"),
        }

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Hermes platform contract: send a message to a Talk room."""
        return await self.send_message(
            room_id=chat_id,
            text=content,
            reply_to_message_id=reply_to,
            metadata=metadata,
        )

    async def send_message(
        self,
        room_id: str,
        text: str,
        reply_to_message_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if not text or not text.strip():
            logger.warning(f"Senden einer leeren Nachricht an Raum '{room_id}' abgebrochen.")
            return SendResult(success=False, error="Empty message")

        # Loop-Prävention: ausgehende Nachrichten kategorisieren
        category, details = categorize_gateway_message(text)

        if category == "lifecycle":
            state = details.get("state")
            if state == "offline":
                await self.presence_mgr.set_custom_status_message("Gateway restarting", "🔄")
                await self.presence_mgr.set_presence_status("offline")
            elif state == "online":
                await self.presence_mgr.set_presence_status("online")
                await self.presence_mgr.clear_custom_status_message()
            elif state == "draining":
                msg = details.get("text", "Gateway draining")
                await self.presence_mgr.set_custom_status_message(msg[:140], "⏸️")
            return SendResult(success=True)

        if category == "suppress":
            return SendResult(success=True)

        if category == "error" and not reply_to_message_id:
            await self.presence_mgr.set_custom_status_message("Fehler", "⚠️")
            return SendResult(success=True)

        if category == "error":
            text = f"🚫 **Fehler**\n\n{details.get('text', text)}"

        payload: Dict[str, Any] = {
            "message": text,
        }

        if reply_to_message_id:
            payload["replyTo"] = reply_to_message_id

        if metadata:
            payload.update(metadata)

        # Existenz- und Rechte-Prüfung vor dem POST (verhindert 403/404-Noise)
        can_send, deny_reason = await self._can_send_to_room(room_id)
        if not can_send:
            logger.warning(
                "[Nextcloud Talk] Senden an Raum '%s' abgelehnt: %s",
                room_id,
                deny_reason,
            )
            return SendResult(success=False, error=deny_reason)

        try:
            await self.signaling_mgr.mark_room_active(room_id)

            data = await self.client.ocs_post(
                f"apps/spreed/api/v1/chat/{room_id}",
                payload,
            )

            msg_id = (
                str(data.get("id", ""))
                if isinstance(data, dict)
                else None
            )

            # Echo-Ring-Puffer: eigene gesendete Message-IDs merken
            if msg_id:
                self._sent_message_ids.append(msg_id)
                del self._sent_message_ids[:-50]

            return SendResult(
                success=True,
                message_id=msg_id,
            )

        except NextcloudOCSException as e:
            if e.status_code == 403:
                logger.error(
                    f"[Nextcloud Talk] Keine Sendeberechtigung im Raum '{room_id}' (HTTP 403)."
                )
            elif e.status_code == 400:
                logger.error(
                    f"[Nextcloud Talk] Ungültiger Payload beim Senden an Raum '{room_id}' (HTTP 400)."
                )
            else:
                logger.error(
                    f"[Nextcloud Talk] OCS-Fehler {e.status_code} beim Senden an Raum '{room_id}': {e.message}"
                )

            return SendResult(
                success=False,
                error=f"OCS Error {e.status_code}: {e.message}",
            )

        except Exception as exc:
            logger.exception(
                f"[Nextcloud Talk] Unerwarteter Fehler beim Senden an Raum '{room_id}': {exc}"
            )
            return SendResult(
                success=False,
                error=str(exc),
            )

    async def send_or_update_status(
        self,
        chat_id: str,
        status_key: str,
        content: str,
        *,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Status-Contract: Fortschritt als Custom-Presence statt Chat-Nachricht."""
        message, icon = self._map_progress_status(status_key, content)
        if message:
            self.presence_mgr.set_status_text(chat_id, message)
            await self.presence_mgr.set_custom_status_message(message, icon)
        return SendResult(success=True)

    # ── Typing-Indikator (Gateway-Contract) ─────────────────────────────

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Typing-Indikator im Talk-Raum. Der Gateway-Loop (_keep_typing) ruft
        alle ~2 s; der Presence-Manager hält pro Raum einen Refresh-Task (Talk
        verfällt nach ~15 s). Fehler werden geschluckt — Typing darf den
        Antwortpfad niemals blockieren."""
        try:
            await self.presence_mgr.send_typing(chat_id)
        except Exception:
            logger.debug("send_typing fehlgeschlagen für Raum %s", chat_id, exc_info=True)

    async def stop_typing(self, chat_id: str) -> None:
        """Beendet den Typing-Indikator (Turn-Ende, Fehler, Finalisierung)."""
        try:
            await self.presence_mgr.stop_typing(chat_id)
        except Exception:
            logger.debug("stop_typing fehlgeschlagen für Raum %s", chat_id, exc_info=True)

    async def mark_turn_started(self) -> None:
        """Presence-Marker: ein Turn ist aktiv (Referenzgezählt)."""
        try:
            await self.presence_mgr.set_busy()
        except Exception:
            logger.debug("set_busy fehlgeschlagen", exc_info=True)

    async def mark_turn_finished(self) -> None:
        """Presence-Marker: Turn beendet — zurück auf online, wenn letzter."""
        try:
            await self.presence_mgr.clear_busy()
        except Exception:
            logger.debug("clear_busy fehlgeschlagen", exc_info=True)

    @staticmethod
    def _map_progress_status(status_key: str, content: str) -> tuple[Optional[str], Optional[str]]:
        normalized_key = str(status_key or "").strip().lower()
        normalized_content = " ".join(str(content or "").split()).strip()
        normalized_lower = normalized_content.lower()
        if "context" in normalized_lower:
            return "Liest Kontext", "📖"
        if normalized_key == "_thinking" or normalized_lower.startswith("💬 "):
            return "Denkt nach", "🤔"
        # Token-Generierung: der letzte API-Call streamt die Antwort — das
        # Custom-Status zeigt „Antwortet ✍️" während Typing im Chat läuft.
        if normalized_key in ("_generating", "_responding", "llm.generating"):
            return "Antwortet", "✍️"
        if normalized_key.startswith("tool.") or "tool" in normalized_key:
            return "Fuehrt Werkzeuge aus", "🛠️"
        if normalized_content:
            return (normalized_content[:80], "💬")
        return None, None


# ---------------------------------------------------------------------------
# Plugin-Registrierung (Hermes PluginManager)
# ---------------------------------------------------------------------------


def _load_dotenv_fallback() -> Dict[str, str]:
    """Liest ~/.hermes/.env als Fallback ein (einmalig gecached).

    Der Gateway-Prozess lädt die .env via dotenv in os.environ — aber
    Status-Konsumenten (Dashboard, `hermes status`) sind separate Prozesse.
    Damit is_connected()/check_is_connected() dort dieselbe Antwort liefern
    wie zur Gateway-Startzeit, wird die .env direkt gelesen.
    """
    global _DOTENV_CACHE
    if _DOTENV_CACHE is not None:
        return _DOTENV_CACHE
    values: Dict[str, str] = {}
    try:
        env_path = Path.home() / ".hermes" / ".env"
        if env_path.is_file():
            for line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, raw = line.partition("=")
                key = key.strip()
                raw = raw.strip().strip('"').strip("'")
                if key and raw:
                    values.setdefault(key, raw)
    except Exception:
        pass
    _DOTENV_CACHE = values
    return values


_DOTENV_CACHE: Optional[Dict[str, str]] = None


def _env(*names: str) -> str:
    dotenv = _load_dotenv_fallback()
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
        value = dotenv.get(name, "").strip()
        if value:
            return value
    return ""


def validate_talk_config(config: Any) -> bool:
    """Prüft eine PlatformConfig auf vollständige Nextcloud-Zugangsdaten."""
    extra = getattr(config, "extra", {}) or {}
    base_url = str(extra.get("base_url") or os.getenv("NEXTCLOUD_BASE_URL", "")).strip()
    username = str(extra.get("username") or os.getenv("NEXTCLOUD_USERNAME", "")).strip()
    app_password = str(
        extra.get("app_password")
        or getattr(config, "token", "")
        or os.getenv("NEXTCLOUD_APP_PASSWORD", "")
    ).strip()
    return bool(base_url and username and app_password)


def validate_talk_config_from_env() -> bool:
    """Env-only Enablement-Check (ohne PlatformConfig)."""
    return bool(
        _env("NEXTCLOUD_BASE_URL")
        and _env("NEXTCLOUD_USERNAME")
        and _env("NEXTCLOUD_APP_PASSWORD")
    )


def env_enablement() -> Optional[Dict[str, Any]]:
    """Auto-Populate für `hermes gateway status` bei env-only Setups."""
    if not validate_talk_config_from_env():
        return None
    try:
        poll = float(_env("NEXTCLOUD_POLL_INTERVAL_SECONDS") or 3.0)
    except ValueError:
        poll = 3.0
    return {
        "base_url": _env("NEXTCLOUD_BASE_URL"),
        "username": _env("NEXTCLOUD_USERNAME"),
        "app_password": _env("NEXTCLOUD_APP_PASSWORD"),
        "bot_handle": _env("NEXTCLOUD_BOT_HANDLE") or f"@{_env('NEXTCLOUD_USERNAME')}",
        "require_mention_in_groups": _env(
            "NEXTCLOUD_REQUIRE_MENTION_IN_GROUPS", "true"
        ).lower()
        in {"1", "true", "yes"},
        "context_message_limit": int(_env("NEXTCLOUD_CONTEXT_MESSAGE_LIMIT") or 20),
        "poll_interval_seconds": max(1.0, poll),
        "allowed_rooms": _env("NEXTCLOUD_ALLOWED_ROOMS"),
        "attachment_tmp_dir": _env("NEXTCLOUD_ATTACHMENT_TMP_DIR"),
        "hitl_require_requester": _env(
            "NEXTCLOUD_HITL_REQUIRE_REQUESTER", "true"
        ).lower()
        in {"1", "true", "yes"},
    }


def check_is_connected(adapter_or_config: Any) -> bool:
    """Verbindungsanzeige für Dashboard/Status.

    Wird vom Gateway mit einem PlatformConfig (noch nicht verbunden) ODER mit
    der aktiven Adapter-Instanz aufgerufen. Stehen die Credentials (wie in
    diesem Deployment) nur in der .env und nicht in der PlatformConfig, fällt
    die Prüfung auf die Env-Validierung zurück — sonst filtert das Dashboard
    die Plattform fälschlich aus der Status-Anzeige heraus.
    """
    if hasattr(adapter_or_config, "is_connected"):
        return bool(adapter_or_config.is_connected)
    if validate_talk_config(adapter_or_config):
        return True
    # Credentials kommen aus der .env (env_enablement-Pfad) — PlatformConfig
    # trägt sie nicht. Gleiche Prüfung wie beim Gateway-Start.
    return validate_talk_config_from_env()


def _build_adapter(config: PlatformConfig) -> NextcloudTalkPlatform:
    return NextcloudTalkPlatform(config)


def register(ctx: Any) -> None:
    """Registriert die Nextcloud-Talk-Plattform beim Hermes Gateway."""
    ctx.register_platform(
        name="nextcloud",
        label="Nextcloud Talk",
        adapter_factory=_build_adapter,
        check_fn=validate_talk_config_from_env,
        validate_config=validate_talk_config,
        is_connected=check_is_connected,
        env_enablement_fn=env_enablement,
        required_env=[
            "NEXTCLOUD_BASE_URL",
            "NEXTCLOUD_USERNAME",
            "NEXTCLOUD_APP_PASSWORD",
        ],
        cron_deliver_env_var="NEXTCLOUD_HOME_CHANNEL",
        max_message_length=32000,
        emoji="💬",
    )
