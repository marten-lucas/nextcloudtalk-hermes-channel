"""Resilience-Verträge des Talk-Adapters (Supervision/Watchdog/Status).

Deckt die Maßnahmen nach dem Empfangs-Ausfall vom 2026-09-06 ab:
1. WS-Loops laufen unter einem Supervisor mit Auto-Reconnect.
2. Der Watchdog startet beendete/geschlossene WS-Tasks neu; stille Räume bleiben bestehen.
3. Low-Frequency-Polling läuft dauerhaft als Sicherheitsschicht.
4. ``is_connected`` meldet nur lebendige Empfangspfade als verbunden.
"""
import asyncio
import time
import unittest
from unittest.mock import AsyncMock

# Bootstrap MUSS vor dem adapter-Import laufen (Namespace-Setup),
# daher wird die Testklasse aus dem Contracts-Modul zuerst importiert.
from .test_adapter_contracts import (
    TestableNextcloudTalkPlatform,
    make_config,
)

import adapter as _adapter_module  # noqa: E402  (nach Bootstrap)
from adapter import NextcloudTalkPlatform  # noqa: E402


def make_adapter() -> TestableNextcloudTalkPlatform:
    adapter = TestableNextcloudTalkPlatform(
        make_config(base_url="https://nc.local", username="hermes", app_password="pw")
    )
    adapter.mock_participants["room1"] = 2
    return adapter


class IsConnectedContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_not_connected_when_stop_event_set(self):
        adapter = make_adapter()
        adapter._stop_event.set()
        self.assertFalse(adapter.is_connected)

    async def test_not_connected_when_all_receive_tasks_done(self):
        adapter = make_adapter()
        adapter._stop_event.clear()
        done_task = asyncio.create_task(asyncio.sleep(0))
        await asyncio.sleep(0.01)  # Task laufen lassen → done
        adapter._room_ws_tasks["room1"] = done_task
        adapter._polling_task = None
        self.assertFalse(adapter.is_connected)

    async def test_connected_with_live_ws_task(self):
        adapter = make_adapter()
        adapter._stop_event.clear()
        adapter._polling_task = None
        live = asyncio.create_task(asyncio.sleep(10))
        try:
            adapter._room_ws_tasks["room1"] = live
            self.assertTrue(adapter.is_connected)
        finally:
            live.cancel()

    async def test_connected_with_live_polling_task_only(self):
        adapter = make_adapter()
        adapter._stop_event.clear()
        adapter._room_ws_tasks.clear()
        live = asyncio.create_task(asyncio.sleep(10))
        try:
            adapter._polling_task = live
            self.assertTrue(adapter.is_connected)
        finally:
            live.cancel()


class WatchdogContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_watchdog_restarts_done_task(self):
        adapter = make_adapter()
        adapter._stop_event.clear()
        done = asyncio.create_task(asyncio.sleep(0))
        await asyncio.sleep(0.01)
        adapter._room_ws_tasks["room1"] = done
        adapter._restart_room_ws = AsyncMock()

        # Eine Watchdog-Iteration manuell ausführen (Loop-Körper extrahiert):
        for room_id, task in list(adapter._room_ws_tasks.items()):
            if task.done():
                await adapter._restart_room_ws(room_id)
                continue
            ws = adapter.signaling_mgr.get_room_socket(room_id)
            if ws is not None and ws.closed:
                await adapter._restart_room_ws(room_id)

        adapter._restart_room_ws.assert_awaited_once_with("room1")

    async def test_watchdog_does_not_restart_idle_room_when_polling_is_active(self):
        adapter = make_adapter()
        adapter._stop_event.clear()
        live = asyncio.create_task(asyncio.sleep(10))
        adapter._room_ws_tasks["room1"] = live
        adapter._last_ws_rx["room1"] = time.monotonic() - 600
        adapter._last_poll_success["room1"] = time.monotonic()
        adapter.signaling_mgr.get_room_socket = lambda _room: type(
            "Socket", (), {"closed": False}
        )()
        adapter._restart_room_ws = AsyncMock()

        for room_id, task in list(adapter._room_ws_tasks.items()):
            if task.done():
                await adapter._restart_room_ws(room_id)
                continue
            ws = adapter.signaling_mgr.get_room_socket(room_id)
            if ws is not None and ws.closed:
                await adapter._restart_room_ws(room_id)

        adapter._restart_room_ws.assert_not_awaited()
        live.cancel()

    async def test_watchdog_restarts_explicitly_closed_socket(self):
        adapter = make_adapter()
        adapter._stop_event.clear()
        live = asyncio.create_task(asyncio.sleep(10))
        adapter._room_ws_tasks["room1"] = live
        adapter.signaling_mgr.get_room_socket = lambda _room: type(
            "Socket", (), {"closed": True}
        )()
        adapter._restart_room_ws = AsyncMock()

        for room_id, task in list(adapter._room_ws_tasks.items()):
            if task.done():
                await adapter._restart_room_ws(room_id)
                continue
            ws = adapter.signaling_mgr.get_room_socket(room_id)
            if ws is not None and ws.closed:
                await adapter._restart_room_ws(room_id)

        adapter._restart_room_ws.assert_awaited_once_with("room1")
        live.cancel()

    async def test_restart_room_ws_creates_supervised_task(self):
        adapter = make_adapter()
        adapter._stop_event.clear()
        adapter.signaling_mgr.get_signaling_settings = AsyncMock(
            return_value=object()  # truthy settings
        )
        started = []

        async def fake_loop(room_id, settings):
            started.append((room_id, settings))

        adapter._supervised_room_loop = fake_loop
        # Alten (done) Task installieren
        old = asyncio.create_task(asyncio.sleep(0))
        await asyncio.sleep(0.01)
        adapter._room_ws_tasks["room1"] = old

        await adapter._restart_room_ws("room1")

        self.assertIn("room1", adapter._room_ws_tasks)
        new_task = adapter._room_ws_tasks["room1"]
        self.assertIsNot(new_task, old)
        self.assertNotIn("room1", adapter._last_ws_rx)
        self.assertNotIn("room1", adapter._ws_connected_at)
        new_task.cancel()
        await asyncio.gather(new_task, return_exceptions=True)

    async def test_restart_aborts_when_room_settings_unavailable(self):
        adapter = make_adapter()
        adapter._stop_event.clear()
        adapter.signaling_mgr.get_signaling_settings = AsyncMock(return_value=None)
        old = asyncio.create_task(asyncio.sleep(0))
        await asyncio.sleep(0.01)
        adapter._room_ws_tasks["room1"] = old

        await adapter._restart_room_ws("room1")

        self.assertNotIn("room1", adapter._room_ws_tasks)
        self.assertNotIn("room1", adapter._last_rx)
        self.assertNotIn("room1", adapter._last_ws_rx)
        self.assertNotIn("room1", adapter._ws_connected_at)
        self.assertNotIn("room1", adapter._last_poll_success)


class SupervisedLoopContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_supervisor_jitters_backoff_and_resets_after_stability(self):
        from unittest.mock import call, patch

        import adapter as adapter_module

        adapter = make_adapter()
        adapter._stop_event.clear()
        adapter._ws_reconnect_stable_seconds = 0.005
        calls = []
        delays = []
        original_sleep = asyncio.sleep

        async def fake_inner(
            room_id, settings, fetch, stop_event, on_receive=None, on_connected=None
        ):
            calls.append(room_id)
            if on_connected is not None:
                on_connected(room_id)
            if len(calls) == 3:
                await original_sleep(0.01)
            elif len(calls) == 4:
                adapter._stop_event.set()

        async def capture_sleep(delay):
            delays.append(delay)

        def lower_jitter_bound(low, _high):
            return low

        adapter.signaling_mgr.room_signaling_loop = fake_inner
        with (
            patch.object(adapter_module.asyncio, "sleep", capture_sleep),
            patch.object(
                adapter_module.random, "uniform", side_effect=lower_jitter_bound
            ) as jitter,
        ):
            await asyncio.wait_for(
                adapter._supervised_room_loop("room1", object()),
                timeout=1.0,
            )

        self.assertEqual(calls, ["room1"] * 4)
        self.assertEqual(delays, [4.0, 8.0, 4.0])
        self.assertEqual(
            jitter.call_args_list,
            [call(4.0, 6.0), call(8.0, 12.0), call(4.0, 6.0)],
        )

    async def test_supervised_loop_caps_exponential_backoff(self):
        from unittest.mock import patch

        import adapter as adapter_module

        adapter = make_adapter()
        adapter._stop_event.clear()
        adapter._ws_reconnect_max_seconds = 10.0
        adapter._ws_reconnect_stable_seconds = 999.0
        calls = []
        delays = []

        async def fake_inner(
            room_id, settings, fetch, stop_event, on_receive=None, on_connected=None
        ):
            calls.append(room_id)
            if len(calls) == 5:
                adapter._stop_event.set()

        async def capture_sleep(delay):
            delays.append(delay)

        def lower_jitter_bound(low, _high):
            return low

        adapter.signaling_mgr.room_signaling_loop = fake_inner
        with (
            patch.object(adapter_module.asyncio, "sleep", capture_sleep),
            patch.object(
                adapter_module.random, "uniform", side_effect=lower_jitter_bound
            ),
        ):
            await asyncio.wait_for(
                adapter._supervised_room_loop("room1", object()),
                timeout=1.0,
            )

        self.assertEqual(calls, ["room1"] * 5)
        self.assertEqual(delays, [4.0, 8.0, 8.0, 8.0])

    async def test_supervised_loop_survives_inner_exception(self):
        from unittest.mock import patch

        import adapter as adapter_module

        adapter = make_adapter()
        adapter._stop_event.clear()
        calls = []

        async def fake_inner(
            room_id, settings, fetch, stop_event, on_receive=None, on_connected=None
        ):
            calls.append(room_id)
            if len(calls) >= 2:
                adapter._stop_event.set()
            raise RuntimeError("ws boom")

        async def no_wait(_delay):
            return None

        adapter.signaling_mgr.room_signaling_loop = fake_inner
        with patch.object(adapter_module.asyncio, "sleep", no_wait):
            await adapter._supervised_room_loop("room1", object())

        self.assertEqual(calls, ["room1", "room1"])


class DedupeContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_duplicate_message_id_processed_once(self):
        adapter = make_adapter()
        adapter.mock_participants["room-dedupe"] = 2
        adapter.identity_mgr.get_user_groups = AsyncMock(return_value=[])

        event = {
            "room_id": "room-dedupe",
            "id": "dup-1",
            "actorId": "vorstand",
            "message": "@hermes bitte merken",
        }
        await adapter.handle_incoming_event(event)
        await adapter.handle_incoming_event(event)  # Duplikat — muss ignoriert werden

        self.assertEqual(len(adapter.received_events), 1)


class EventDispatchContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_turn_does_not_block_receive_and_room_order_is_preserved(self):
        adapter = make_adapter()
        adapter._stop_event.clear()
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        processed = []

        async def slow_turn(event):
            processed.append(event["id"])
            if event["id"] == "first":
                first_started.set()
                await release_first.wait()

        adapter.handle_incoming_event = slow_turn
        first = {"room_id": "room1", "id": "first"}
        second = {"room_id": "room1", "id": "second"}
        try:
            await adapter._enqueue_incoming_event(first)
            await asyncio.wait_for(first_started.wait(), timeout=0.5)

            rx_before_second = adapter._last_rx["room1"]
            await asyncio.wait_for(
                adapter._enqueue_incoming_event(second), timeout=0.1
            )

            queue = adapter._event_queues["room1"]
            self.assertEqual(processed, ["first"])
            self.assertEqual(queue.qsize(), 1)
            self.assertGreaterEqual(adapter._last_rx["room1"], rx_before_second)

            release_first.set()
            await asyncio.wait_for(queue.join(), timeout=1.0)
            self.assertEqual(processed, ["first", "second"])
        finally:
            release_first.set()
            task = adapter._event_dispatch_tasks.get("room1")
            if task and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


class LowFrequencyPollingContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_connect_starts_polling_even_with_ws_success(self):
        adapter = make_adapter()
        adapter.connect_websocket_success = True
        adapter.presence_mgr.set_presence_status = AsyncMock()
        adapter.presence_mgr.clear_custom_status_message = AsyncMock()

        await adapter.connect()

        poll_started = ("start_polling",) in adapter.calls
        # Aufräumen
        await adapter.disconnect()
        self.assertTrue(poll_started)

    async def test_connect_starts_polling_on_ws_failure(self):
        adapter = make_adapter()
        adapter.connect_websocket_success = False
        adapter.presence_mgr.set_presence_status = AsyncMock()
        adapter.presence_mgr.clear_custom_status_message = AsyncMock()

        await adapter.connect()
        await adapter.disconnect()

        poll_started = ("start_polling",) in adapter.calls
        self.assertTrue(poll_started)
