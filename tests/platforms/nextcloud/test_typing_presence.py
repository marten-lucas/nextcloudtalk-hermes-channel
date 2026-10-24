"""Tests für Typing-Indikator und Busy-Presence im Talk-Adapter.

Verträge:
- send_typing/stop_typing überschreiben die Base-No-ops und steuern
  Refresh-Tasks pro Raum (Talk verfällt nach ~15 s, Refresh alle 5 s).
- Mehrfachaufrufe idempotent (Gateway-Loop ruft alle ~2 s).
- Busy-Presence referenzgezählt: mark_turn_started/finished koppeln
  Turn-Lebensdauer an die Presence-Verwaltung.
- Fehler in Typing-Aufrufen blockieren den Antwortpfad nie.
"""
import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from .test_adapter_contracts import (  # noqa: F401  (Bootstrap-Nebeneffekt)
    TestableNextcloudTalkPlatform,
    make_config,
)
from _ncplugin_under_test.client import NextcloudTalkClient
from _ncplugin_under_test.presence import NextcloudPresenceManager
from _ncplugin_under_test.signaling import NextcloudSignalingManager


def make_adapter() -> TestableNextcloudTalkPlatform:
    return TestableNextcloudTalkPlatform(
        make_config(base_url="https://nc.local", username="hermes", app_password="pw")
    )


class UserStatusOcsContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_client_puts_core_ocs_payload_as_json(self):
        requests = []

        class Response:
            status = 200
            headers = {"Content-Type": "application/json"}

            async def json(self):
                return {"ocs": {"meta": {"status": "ok", "statuscode": 200}, "data": {}}}

        class ResponseContext:
            async def __aenter__(self):
                return Response()

            async def __aexit__(self, *_args):
                return False

        class Session:
            closed = False

            def request(self, method, url, **kwargs):
                requests.append((method, url, kwargs))
                return ResponseContext()

        client = NextcloudTalkClient("https://nc.local", "hermes", "pw")
        client._session = Session()
        payload = {"statusType": "busy"}

        await client.ocs_put_core(
            "apps/user_status/api/v1/user_status/status",
            payload,
        )

        method, url, kwargs = requests[0]
        self.assertEqual(method, "PUT")
        self.assertEqual(
            url,
            "https://nc.local/ocs/v2.php/apps/user_status/api/v1/user_status/status",
        )
        self.assertEqual(kwargs["json"], payload)
        self.assertNotIn("data", kwargs)

    async def test_talk_client_uses_json_for_core_ocs_status_updates(self):
        requests = []

        class Response:
            status = 200
            headers = {"Content-Type": "application/json"}

            async def json(self):
                return {"ocs": {"meta": {"status": "ok", "statuscode": 200}, "data": {}}}

        class ResponseContext:
            async def __aenter__(self):
                return Response()

            async def __aexit__(self, *_args):
                return False

        class Session:
            closed = False

            def request(self, method, url, **kwargs):
                requests.append((method, url, kwargs))
                return ResponseContext()

        client = NextcloudTalkClient("https://nc.local", "hermes", "pw")
        client._session = Session()
        payload = {"message": "Antwortet", "statusIcon": "✍️"}

        await client.ocs_put_core(
            "apps/user_status/api/v1/user_status/message/custom",
            payload,
        )

        method, url, kwargs = requests[0]
        self.assertEqual(method, "PUT")
        self.assertEqual(
            url,
            "https://nc.local/ocs/v2.php/apps/user_status/api/v1/user_status/message/custom",
        )
        self.assertEqual(kwargs["json"], payload)
        self.assertNotIn("data", kwargs)

    async def test_presence_uses_deck_compatible_core_ocs_json_calls(self):
        client = type("MockClient", (), {})()
        client.ocs_put_core = AsyncMock(return_value={})
        client.ocs_delete_core = AsyncMock(return_value={})
        presence = NextcloudPresenceManager(client)

        await presence.set_presence_status("busy")
        await presence.set_custom_status_message("Antwortet", "✍️")
        await presence.clear_custom_status_message()

        client.ocs_put_core.assert_any_await(
            "apps/user_status/api/v1/user_status/status",
            {"statusType": "busy"},
        )
        client.ocs_put_core.assert_any_await(
            "apps/user_status/api/v1/user_status/message/custom",
            {"message": "Antwortet", "statusIcon": "✍️"},
        )
        client.ocs_delete_core.assert_awaited_once_with(
            "apps/user_status/api/v1/user_status/message"
        )


class SignalingFetchNonblockingTests(unittest.IsolatedAsyncioTestCase):
    async def test_long_chat_fetch_does_not_block_ws_receive_loop(self):
        import json
        import aiohttp
        from _ncplugin_under_test.signaling import NextcloudSignalingSettings

        queue = asyncio.Queue()
        received_frames = []
        fetch_started = asyncio.Event()
        release_fetch = asyncio.Event()
        second_fetch = asyncio.Event()
        fetch_count = 0

        class FakeMessage:
            def __init__(self, payload=None, msg_type=aiohttp.WSMsgType.TEXT):
                self.type = msg_type
                self.data = json.dumps(payload) if payload is not None else ""

        class FakeWebSocket:
            closed = False

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                self.closed = True

            async def send_json(self, payload):
                if payload.get("type") == "hello":
                    await queue.put(FakeMessage({"type": "hello"}))
                elif payload.get("type") == "room":
                    await queue.put(FakeMessage({"type": "room"}))
                    await queue.put(FakeMessage({"type": "event", "event": {"target": "room"}}))

            async def receive(self):
                return await queue.get()

            def __aiter__(self):
                return self

            async def __anext__(self):
                return await self.receive()

        websocket = FakeWebSocket()

        class FakeSession:
            def ws_connect(self, *_args, **_kwargs):
                return websocket

        class FakeClient:
            async def ensure_session(self):
                return FakeSession()

            def talk_url(self, path):
                return f"https://nc.local/ocs/v2.php/{path}"

            async def ocs_post(self, *_args, **_kwargs):
                return {"sessionId": "session-1"}

        async def fetch_room_events(_room_id):
            nonlocal fetch_count
            fetch_count += 1
            if fetch_count == 1:
                fetch_started.set()
                await release_fetch.wait()
            elif fetch_count == 2:
                second_fetch.set()
            return []

        async def handle_event(_event):
            return None

        manager = NextcloudSignalingManager(FakeClient(), handle_event)
        stop_event = asyncio.Event()

        def on_receive(_room_id):
            received_frames.append(True)

        task = asyncio.create_task(
            manager.room_signaling_loop(
                "room1",
                NextcloudSignalingSettings(
                    server="https://signal.local",
                    hello_auth_params={"1.0": {"token": "test"}},
                    user_id="hermes",
                ),
                fetch_room_events,
                stop_event,
                on_receive=on_receive,
            )
        )
        try:
            await asyncio.wait_for(fetch_started.wait(), timeout=0.5)
            await queue.put(FakeMessage({"type": "event", "event": {"target": "room"}}))
            await asyncio.wait_for(
                self._wait_for_frames(received_frames, 2),
                timeout=0.5,
            )
            self.assertEqual(fetch_count, 1)
            release_fetch.set()
            await asyncio.wait_for(second_fetch.wait(), timeout=0.5)
        finally:
            release_fetch.set()
            stop_event.set()
            await queue.put(FakeMessage(msg_type=aiohttp.WSMsgType.CLOSE))
            await asyncio.wait_for(task, timeout=0.5)

    @staticmethod
    async def _wait_for_frames(received_frames, count):
        while len(received_frames) < count:
            await asyncio.sleep(0)


class PersistentTypingWebSocketTests(unittest.IsolatedAsyncioTestCase):
    async def test_typing_reuses_room_websocket(self):
        client = type("MockClient", (), {})()
        client.ensure_session = AsyncMock()
        client.ocs_get = AsyncMock()
        manager = NextcloudSignalingManager(client, AsyncMock())
        websocket = type("MockWebSocket", (), {"closed": False})()
        websocket.send_json = AsyncMock()
        manager._room_sockets["room1"] = websocket

        await manager.emit_typing_state("room1", True)

        client.ensure_session.assert_not_awaited()
        client.ocs_get.assert_not_awaited()
        websocket.send_json.assert_awaited_once_with(
            {
                "type": "message",
                "message": {
                    "recipient": {"type": "room"},
                    "data": {"type": "startedTyping"},
                },
            }
        )

    async def test_typing_during_reconnect_flushes_latest_state(self):
        client = type("MockClient", (), {})()
        client.ensure_session = AsyncMock()
        manager = NextcloudSignalingManager(client, AsyncMock())
        await manager.emit_typing_state("room1", True)
        await manager.emit_typing_state("room1", False)

        websocket = type("MockWebSocket", (), {"closed": False})()
        websocket.send_json = AsyncMock()
        await manager._flush_pending_typing("room1", websocket)

        self.assertNotIn("room1", manager._pending_typing)
        websocket.send_json.assert_awaited_once_with(
            {
                "type": "message",
                "message": {
                    "recipient": {"type": "room"},
                    "data": {"type": "stoppedTyping"},
                },
            }
        )
        client.ensure_session.assert_not_awaited()


class TypingContractTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.adapter = make_adapter()

    async def test_send_typing_emits_signaling_event(self):
        await self.adapter.send_typing("room1")
        # Sofortiges Signaling-Event gefeuert (startedTyping)
        self.assertIn(("room1", True), self.adapter.mock_signaling.typing_events)
        await self.adapter.stop_typing("room1")

    async def test_stop_typing_emits_stopped_event(self):
        await self.adapter.send_typing("room1")
        await self.adapter.stop_typing("room1")
        self.assertIn(("room1", False), self.adapter.mock_signaling.typing_events)

    async def test_send_typing_starts_renew_task(self):
        await self.adapter.send_typing("room1")
        task = self.adapter.presence_mgr._typing_tasks.get("room1")
        self.assertIsNotNone(task)
        self.assertFalse(task.done())
        await self.adapter.stop_typing("room1")

    async def test_send_typing_idempotent_per_room(self):
        await self.adapter.send_typing("room1")
        first = self.adapter.presence_mgr._typing_tasks.get("room1")
        await self.adapter.send_typing("room1")  # Gateway-Loop ruft wieder
        second = self.adapter.presence_mgr._typing_tasks.get("room1")
        self.assertIs(first, second)
        await self.adapter.stop_typing("room1")

    async def test_stop_typing_cancels_task(self):
        await self.adapter.send_typing("room1")
        await self.adapter.stop_typing("room1")
        self.assertNotIn("room1", self.adapter.presence_mgr._typing_tasks)

    async def test_stop_typing_without_task_is_noop(self):
        await self.adapter.stop_typing("never-started")  # darf nicht werfen

    async def test_send_typing_swallows_signaling_errors(self):
        self.adapter.mock_signaling.emit_typing_state = AsyncMock(
            side_effect=RuntimeError("boom")
        )
        # Darf nicht werfen; kein Renew-Task ohne erfolgreiches Initial-Event
        await self.adapter.send_typing("room-err")
        await asyncio.sleep(0.05)

    async def test_empty_chat_id_ignored(self):
        await self.adapter.send_typing("")
        await self.adapter.stop_typing("")
        self.assertEqual(self.adapter.presence_mgr._typing_tasks, {})


class BusyPresenceContractTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.adapter = make_adapter()

    async def test_busy_refcount_up_down(self):
        mgr = self.adapter.presence_mgr
        self.assertFalse(mgr.is_busy)
        await self.adapter.mark_turn_started()
        self.assertTrue(mgr.is_busy)
        await self.adapter.mark_turn_started()  # zweiter paralleler Turn
        self.assertTrue(mgr.is_busy)
        await self.adapter.mark_turn_finished()
        self.assertTrue(mgr.is_busy)  # noch einer aktiv
        await self.adapter.mark_turn_finished()
        self.assertFalse(mgr.is_busy)

    async def test_clear_busy_never_negative(self):
        mgr = self.adapter.presence_mgr
        await self.adapter.mark_turn_finished()
        self.assertFalse(mgr.is_busy)

    async def test_last_turn_clears_global_custom_status(self):
        await self.adapter.mark_turn_started()
        await self.adapter.mark_turn_started()
        await self.adapter.send_or_update_status("room1", "_thinking", "")

        await self.adapter.mark_turn_finished()
        self.assertTrue(self.adapter.presence_mgr.is_busy)
        self.assertEqual(self.adapter.custom_status_clears, [])

        await self.adapter.mark_turn_finished()
        self.assertFalse(self.adapter.presence_mgr.is_busy)
        self.assertEqual(self.adapter.custom_status_clears, [True])
        self.assertEqual(self.adapter.presence_updates[-1], "online")

    async def test_stop_typing_clears_custom_status_without_turn_hooks(self):
        await self.adapter.send_or_update_status("room1", "_generating", "")

        await self.adapter.stop_typing("room1")

        self.assertEqual(self.adapter.custom_status_clears, [True])
        self.assertEqual(self.adapter.presence_updates[-1], "online")

    async def test_handle_incoming_event_sets_busy(self):
        adapter = make_adapter()
        adapter.mock_participants["room-busy"] = 2
        adapter.identity_mgr.get_user_groups = AsyncMock(return_value=[])

        states = []
        original_handle = adapter.handle_message

        async def spy_handle(event):
            states.append(adapter.presence_mgr.is_busy)
            await original_handle(event)

        adapter.handle_message = spy_handle
        await adapter.handle_incoming_event(
            {"room_id": "room-busy", "id": "m-busy-1", "actorId": "vorstand", "message": "Test"}
        )
        # Während handle_message lief, war busy aktiv
        self.assertEqual(states, [True])
        self.assertFalse(adapter.presence_mgr.is_busy)


class StatusMappingTests(unittest.IsolatedAsyncioTestCase):
    def test_generating_maps_to_answer_status(self):
        message, icon = TestableNextcloudTalkPlatform._map_progress_status("_generating", "")
        self.assertEqual((message, icon), ("Antwortet", "✍️"))

    def test_thinking_maps_to_thinking_status(self):
        message, icon = TestableNextcloudTalkPlatform._map_progress_status("_thinking", "")
        self.assertEqual((message, icon), ("Denkt nach", "🤔"))

    def test_tool_maps_to_tool_status(self):
        message, icon = TestableNextcloudTalkPlatform._map_progress_status("tool.terminal", "")
        self.assertEqual((message, icon), ("Fuehrt Werkzeuge aus", "🛠️"))
