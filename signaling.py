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
        self._room_sockets: Dict[str, aiohttp.ClientWebSocketResponse] = {}
        self._pending_typing: Dict[str, bool] = {}

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

    async def emit_typing_state(self, room_id: str, typing: bool) -> None:
        """Send typing state on the room's persistent signaling connection.

        A second websocket using the same Nextcloud room session displaces the
        receive socket on this HPB, so typing updates must share that socket.
        Keep only the latest state when the room is reconnecting.
        """
        ws = self._room_sockets.get(room_id)
        if ws is None or ws.closed:
            self._pending_typing[room_id] = bool(typing)
            return
        try:
            await self._send_typing_state(ws, typing)
        except Exception:
            self._pending_typing[room_id] = bool(typing)
            raise

    async def _send_typing_state(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        typing: bool,
    ) -> None:
        signal_type = "startedTyping" if typing else "stoppedTyping"
        await ws.send_json({
            "type": "message",
            "message": {
                "recipient": {"type": "room"},
                "data": {"type": signal_type},
            },
        })

    async def _flush_pending_typing(
        self,
        room_id: str,
        ws: aiohttp.ClientWebSocketResponse,
    ) -> None:
        typing = self._pending_typing.pop(room_id, None)
        if typing is not None:
            try:
                await self._send_typing_state(ws, typing)
            except Exception:
                self._pending_typing[room_id] = typing
                raise

    async def mark_room_active(self, room_id: str) -> Optional[str]:
        data = await self.client.ocs_post(f"apps/spreed/api/v4/room/{room_id}/participants/active", {"force": True})
        session_id = str(data.get("sessionId") or data.get("sessionid") or "").strip() if isinstance(data, dict) else None
        if session_id:
            self._active_room_sessions[room_id] = session_id
        return session_id

    @staticmethod
    def signaling_ws_url(server: str) -> str:
        url = server.strip()
        if url.startswith("https://"):
            url = "wss://" + url[len("https://"):]
        elif url.startswith("http://"):
            url = "ws://" + url[len("http://"):]
        if url.endswith("/"):
            url = url[:-1]
        return f"{url}/spreed"

    async def room_signaling_loop(
        self,
        room_id: str,
        settings: NextcloudSignalingSettings,
        fetch_room_events: Any,
        stop_event: Any,
        on_receive: Any = None,
    ) -> None:
        """Receive room signals without blocking on Nextcloud's long-poll API."""
        session = await self.client.ensure_session()
        ws: Optional[aiohttp.ClientWebSocketResponse] = None
        fetch_task: Optional[asyncio.Task] = None
        try:
            async with session.ws_connect(
                self.signaling_ws_url(settings.server),
                heartbeat=30,
            ) as ws:
                await self._hello(ws, settings)
                # IMMER eine frische Session holen: HPB invalidiert die alte
                # Session-ID, sobald ihre WS-Verbindung stirbt. Wiederverwendung
                # der gecachten ID schlägt beim Room-Join mit dem irreführenden
                # Fehler 'no_such_room' / 'The user is not invited to this
                # room.' fehl — Endlos-Reconnect-Loop (Fix 2026-09-09).
                session_id = await self.mark_room_active(room_id)
                if not session_id:
                    raise RuntimeError(f"Nextcloud signaling join missing session id for room {room_id}")
                await self._join_room(ws, room_id, session_id, settings.user_id)
                self._room_sockets[room_id] = ws
                await self._flush_pending_typing(room_id, ws)

                fetch_requested = asyncio.Event()

                async def fetch_after_signal() -> None:
                    while not stop_event.is_set():
                        await fetch_requested.wait()
                        fetch_requested.clear()
                        if stop_event.is_set():
                            return
                        try:
                            events = await fetch_room_events(room_id)
                            for event_payload in events:
                                await self.event_handler(event_payload)
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            logger.warning(
                                "Nextcloud Talk: Signaling-Fetch für Raum %s fehlgeschlagen: %s",
                                room_id,
                                exc,
                            )

                fetch_task = asyncio.create_task(fetch_after_signal())
                async for msg in ws:
                    if stop_event.is_set():
                        return
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        if msg.type in (
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.CLOSING,
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            logger.warning(
                                "Nextcloud signaling socket closed for room %s "
                                "(frame=%s, code=%s).",
                                room_id,
                                msg.type.name,
                                ws.close_code,
                            )
                            break
                        continue
                    if on_receive is not None:
                        on_receive(room_id)
                    payload = json.loads(msg.data)
                    if payload.get("type") == "event":
                        event = payload.get("event") or {}
                        if isinstance(event, dict) and event.get("target") in {"room", "participants"}:
                            fetch_requested.set()
                    elif payload.get("type") == "room":
                        fetch_requested.set()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not stop_event.is_set():
                err_str = str(exc)
                if "closing transport" in err_str or "closed" in err_str:
                    logger.debug("Nextcloud websocket transport closed for %s: %s", room_id, exc)
                else:
                    logger.warning("Nextcloud websocket room loop ended for %s: %s", room_id, exc)
        finally:
            if fetch_task is not None:
                fetch_task.cancel()
                await asyncio.gather(fetch_task, return_exceptions=True)
            if ws is not None and self._room_sockets.get(room_id) is ws:
                self._room_sockets.pop(room_id, None)

    async def _hello(self, ws: aiohttp.ClientWebSocketResponse, settings: NextcloudSignalingSettings) -> None:
        hello_version = "2.0" if settings.hello_auth_params.get("2.0") else "1.0"
        await ws.send_json(
            {
                "type": "hello",
                "hello": {
                    "version": hello_version,
                    "auth": {
                        "url": self.client.talk_url("apps/spreed/api/v3/signaling/backend"),
                        "params": settings.hello_auth_params[hello_version],
                    },
                },
            }
        )
        while True:
            msg = await ws.receive()
            if msg.type == aiohttp.WSMsgType.TEXT:
                payload = json.loads(msg.data)
                if payload.get("type") == "welcome":
                    continue
                if payload.get("type") == "hello":
                    return
                if payload.get("type") == "error":
                    raise RuntimeError(f"Nextcloud signaling hello failed: {payload}")
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                raise RuntimeError("Nextcloud signaling websocket closed during hello")

    async def _join_room(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        room_id: str,
        session_id: str,
        user_id: str = "",
    ) -> None:
        await ws.send_json(
            {
                "type": "room",
                "room": {
                    "roomid": room_id,
                    "sessionid": session_id,
                    **({"userid": user_id} if user_id else {}),
                },
            }
        )
        while True:
            msg = await ws.receive()
            if msg.type == aiohttp.WSMsgType.TEXT:
                payload = json.loads(msg.data)
                if payload.get("type") == "room":
                    return
                if payload.get("type") == "error":
                    raise RuntimeError(f"Nextcloud signaling room join failed: {payload}")
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                raise RuntimeError("Nextcloud signaling websocket closed during room join")