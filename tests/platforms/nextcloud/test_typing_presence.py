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
from unittest.mock import AsyncMock, Mock, patch

from _ncplugin_under_test.client import NextcloudTalkClient
from _ncplugin_under_test.platform_wip import active_platform_turns
from _ncplugin_under_test.presence import NextcloudPresenceManager
from _ncplugin_under_test.signaling import NextcloudSignalingManager

from .test_adapter_contracts import (  # noqa: F401  (Bootstrap-Nebeneffekt)
    TestableNextcloudTalkPlatform,
    make_config,
)


def make_adapter() -> TestableNextcloudTalkPlatform:
    return TestableNextcloudTalkPlatform(
        make_config(base_url="https://nc.local", username="hermes", app_password="pw")
    )


class ClientSessionLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_rest_and_websocket_use_separate_connector_policies(self):
        client = NextcloudTalkClient("https://nc.local", "hermes", "pw")
        try:
            rest_session = await client.ensure_session()
            ws_session = await client.ensure_ws_session()

            self.assertIsNot(rest_session, ws_session)
            self.assertTrue(rest_session.connector.force_close)
            self.assertFalse(ws_session.connector.force_close)
            self.assertEqual(ws_session.connector._keepalive_timeout, 15.0)
        finally:
            await client.close()

    async def test_close_closes_both_sessions_and_allows_recreation(self):
        client = NextcloudTalkClient("https://nc.local", "hermes", "pw")
        rest_session = await client.ensure_session()
        ws_session = await client.ensure_ws_session()

        await client.close()

        self.assertTrue(rest_session.closed)
        self.assertTrue(ws_session.closed)
        self.assertIsNone(client._session)
        self.assertIsNone(client._ws_session)
        self.assertIsNot(await client.ensure_session(), rest_session)
        await client.close()


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


class SharedPlatformWipTests(unittest.IsolatedAsyncioTestCase):
    async def test_talk_typing_lifecycle_is_visible_to_deck_wip(self):
        adapter = make_adapter()
        adapter._room_status_idle_grace_seconds = 0
        adapter._ensure_speed_heartbeat = Mock()
        adapter._clear_idle_user_status = AsyncMock()
        adapter.presence_mgr.set_presence_status = AsyncMock()
        adapter.presence_mgr.set_busy = AsyncMock()
        adapter.presence_mgr.clear_busy = AsyncMock()
        adapter.presence_mgr.send_typing = AsyncMock()
        adapter.presence_mgr.stop_typing = AsyncMock()
        baseline = active_platform_turns(exclude_platform="deck")

        await adapter.send_typing("room-wip-test")
        self.assertEqual(active_platform_turns(exclude_platform="deck"), baseline + 1)

        await adapter.stop_typing("room-wip-test")
        await asyncio.sleep(0.01)
        self.assertEqual(active_platform_turns(exclude_platform="deck"), baseline)


class SignalingFetchNonblockingTests(unittest.IsolatedAsyncioTestCase):
    async def test_long_chat_fetch_does_not_block_ws_receive_loop(self):
        import json

        import aiohttp
        from _ncplugin_under_test.signaling import NextcloudSignalingSettings

        queue = asyncio.Queue()
        received_frames = []
        connected_rooms = []
        connect_options = {}
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
            def ws_connect(self, *_args, **kwargs):
                connect_options.update(kwargs)
                return websocket

        class FakeClient:
            async def ensure_ws_session(self):
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
                on_connected=connected_rooms.append,
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
            self.assertEqual(connected_rooms, ["room1"])
            self.assertEqual(connect_options["heartbeat"], 30.0)
            self.assertTrue(connect_options["autoping"])
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
        client.ensure_ws_session = AsyncMock()
        client.ocs_get = AsyncMock()
        manager = NextcloudSignalingManager(client, AsyncMock())
        websocket = type("MockWebSocket", (), {"closed": False})()
        websocket.send_json = AsyncMock()
        manager._room_sockets["room1"] = websocket

        await manager.emit_typing_state("room1", True)

        client.ensure_ws_session.assert_not_awaited()
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
        client.ensure_ws_session = AsyncMock()
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
        client.ensure_ws_session.assert_not_awaited()


class TypingContractTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.adapter = make_adapter()
        self.adapter._room_status_idle_grace_seconds = 0.01

    async def asyncTearDown(self):
        for room_id in list(self.adapter._active_turn_rooms):
            await self.adapter.stop_typing(room_id)
        await asyncio.sleep(0.02)

    async def test_send_typing_emits_signaling_and_sets_busy_presence(self):
        await self.adapter.send_typing("room1")
        self.assertIn(("room1", True), self.adapter.mock_signaling.typing_events)
        self.assertTrue(self.adapter.presence_mgr.is_busy)
        self.assertEqual(self.adapter.presence_updates[-1], "busy")
        await self.adapter.stop_typing("room1")
        await asyncio.sleep(0.02)
        self.assertFalse(self.adapter.presence_mgr.is_busy)
        self.assertEqual(self.adapter.presence_updates[-1], "online")

    async def test_stop_typing_emits_stopped_event(self):
        await self.adapter.send_typing("room1")
        await self.adapter.stop_typing("room1")
        self.assertIn(("room1", False), self.adapter.mock_signaling.typing_events)

    async def test_new_typing_tick_cancels_pending_status_release(self):
        await self.adapter.send_typing("room1")
        await self.adapter.stop_typing("room1")
        await asyncio.sleep(0.002)

        await self.adapter.send_typing("room1")
        await asyncio.sleep(0.02)

        self.assertTrue(self.adapter.presence_mgr.is_busy)
        self.assertIn("room1", self.adapter._active_turn_rooms)
        await self.adapter.stop_typing("room1")
        await asyncio.sleep(0.02)
        self.assertFalse(self.adapter.presence_mgr.is_busy)

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


class SpeedStatusContractTests(unittest.IsolatedAsyncioTestCase):
    def test_deck_speed_configuration_is_inherited_by_talk(self):
        with patch.dict(
            "os.environ",
            {
                "NEXTCLOUD_DECK_SPEED_ENABLED": "true",
                "NEXTCLOUD_DECK_SPEED_URL": "https://speed.example/speed",
            },
            clear=True,
        ):
            adapter = make_adapter()

        self.assertTrue(adapter.runtime.speed_enabled)
        self.assertEqual(
            adapter.runtime.speed_url,
            "https://speed.example/speed",
        )

    def test_talk_speed_setting_can_override_deck_default(self):
        with patch.dict(
            "os.environ",
            {
                "NEXTCLOUD_DECK_SPEED_ENABLED": "true",
                "NEXTCLOUD_DECK_SPEED_URL": "https://deck.example/speed",
                "NEXTCLOUD_TALK_SPEED_ENABLED": "false",
                "NEXTCLOUD_TALK_SPEED_URL": "https://talk.example/speed",
            },
            clear=True,
        ):
            adapter = make_adapter()

        self.assertFalse(adapter.runtime.speed_enabled)
        self.assertEqual(adapter.runtime.speed_url, "https://talk.example/speed")

    async def test_fetch_speed_reads_json_from_configured_sidecar(self):
        adapter = make_adapter()
        adapter.runtime.speed_url = "https://speed.example/speed"
        requests = []

        class Response:
            status = 200

            async def json(self, *, content_type=None):
                return {"phase": "generate", "tg": 11.87}

        class ResponseContext:
            async def __aenter__(self):
                return Response()

            async def __aexit__(self, *_args):
                return False

        class Session:
            closed = False

            def get(self, url, *, timeout):
                requests.append((url, timeout.total))
                return ResponseContext()

        adapter._speed_session = Session()

        speed = await adapter._fetch_speed()

        self.assertEqual(speed, {"phase": "generate", "tg": 11.87})
        self.assertEqual(requests, [("https://speed.example/speed", 0.4)])
        adapter._speed_session = None

    def test_speed_suffix_formats_generation_and_prompt_phases(self):
        adapter = make_adapter()

        self.assertEqual(
            adapter._speed_suffix({"phase": "generate", "tg": 11.87}),
            "11.9 t/s",
        )
        self.assertEqual(
            adapter._speed_suffix(
                {
                    "phase": "prompt",
                    "prompt_progress": 0.42,
                    "prompt_tps": 100.41,
                }
            ),
            "Kontext 42% · ⏱ 100 t/s",
        )
        self.assertEqual(adapter._speed_suffix({"phase": "idle"}), "")

    async def test_shared_speed_heartbeat_sets_busy_and_live_status(self):
        adapter = make_adapter()
        adapter.runtime.speed_enabled = True
        adapter.runtime.speed_url = "https://speed.example/speed"
        adapter._room_progress_status["room-speed"] = (
            "Team Talk",
            "Antwortet",
            "✍️",
        )
        speed_status_set = asyncio.Event()

        async def fetch_speed():
            return {"phase": "generate", "tg": 11.87}

        async def record_custom_status(message, icon=None):
            adapter.custom_status_updates.append((message, icon))
            if message == "Team Talk · Antwortet ⚡ 11.9 t/s":
                speed_status_set.set()

        adapter._fetch_speed = fetch_speed
        adapter._set_custom_status_message = record_custom_status

        await adapter.mark_turn_started("room-speed")
        await asyncio.wait_for(speed_status_set.wait(), timeout=1.0)

        self.assertIn("busy", adapter.presence_updates)
        self.assertEqual(
            adapter.custom_status_updates[-1],
            ("Team Talk · Antwortet ⚡ 11.9 t/s", "✍️"),
        )
        await adapter.mark_turn_finished("room-speed")
        self.assertEqual(adapter.presence_updates[-1], "online")
        self.assertIsNone(adapter._speed_heartbeat_task)

    async def test_parallel_turns_keep_busy_until_the_last_turn_finishes(self):
        adapter = make_adapter()

        await adapter.mark_turn_started("room-one")
        await adapter.mark_turn_started("room-two")
        await adapter.mark_turn_finished("room-one")

        self.assertTrue(adapter.presence_mgr.is_busy)
        self.assertNotEqual(adapter.presence_updates[-1], "online")
        self.assertEqual(adapter.custom_status_clears, [])

        await adapter.mark_turn_finished("room-two")

        self.assertFalse(adapter.presence_mgr.is_busy)
        self.assertEqual(adapter.presence_updates[-1], "online")
        self.assertEqual(adapter.custom_status_clears, [True])


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
        await self.adapter.mark_turn_started("room1")
        await self.adapter.mark_turn_started("room2")
        await self.adapter.send_or_update_status("room1", "_thinking", "")

        await self.adapter.mark_turn_finished("room1")
        self.assertTrue(self.adapter.presence_mgr.is_busy)
        self.assertEqual(self.adapter.custom_status_clears, [])

        await self.adapter.mark_turn_finished("room2")
        self.assertFalse(self.adapter.presence_mgr.is_busy)
        self.assertEqual(self.adapter.custom_status_clears, [True])
        self.assertEqual(self.adapter.presence_updates[-1], "online")

    async def test_stop_typing_clears_custom_status_without_turn_hooks(self):
        self.adapter._room_status_idle_grace_seconds = 0.01
        await self.adapter.send_or_update_status("room1", "_generating", "")
        self.assertTrue(self.adapter.presence_mgr.is_busy)

        await self.adapter.stop_typing("room1")
        await asyncio.sleep(0.02)

        self.assertFalse(self.adapter.presence_mgr.is_busy)
        self.assertEqual(self.adapter.custom_status_clears, [True])
        self.assertEqual(self.adapter.presence_updates[-1], "online")

    async def test_inbound_handoff_does_not_end_gateway_typing_lifecycle(self):
        adapter = make_adapter()
        adapter._room_status_idle_grace_seconds = 0.01
        adapter.mock_participants["room-busy"] = 2
        adapter.identity_mgr.get_user_groups = AsyncMock(return_value=[])

        states = []
        original_handle = adapter.handle_message

        async def spy_handle(event):
            await adapter.send_typing("room-busy")
            states.append(adapter.presence_mgr.is_busy)
            await original_handle(event)

        adapter.handle_message = spy_handle
        await adapter.handle_incoming_event(
            {"room_id": "room-busy", "id": "m-busy-1", "actorId": "vorstand", "message": "Test"}
        )
        self.assertEqual(states, [True])
        self.assertTrue(adapter.presence_mgr.is_busy)

        await adapter.stop_typing("room-busy")
        await asyncio.sleep(0.02)
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
