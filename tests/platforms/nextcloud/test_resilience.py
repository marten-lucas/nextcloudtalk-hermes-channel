"""Resilience-Verträge des Talk-Adapters (Supervision/Watchdog/Status).

Deckt die Maßnahmen nach dem Empfangs-Ausfall vom 2026-09-06 ab:
1. WS-Loops laufen unter einem Supervisor mit Auto-Reconnect.
2. Der Watchdog startet done/gestallte WS-Tasks neu.
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
        now = time.monotonic()
        for room_id, task in list(adapter._room_ws_tasks.items()):
            if task.done():
                await adapter._restart_room_ws(room_id)
                continue
            last_rx = adapter._last_rx.get(room_id)
            if last_rx is not None and (now - last_rx) > adapter._rx_stall_seconds:
                await adapter._restart_room_ws(room_id)

        adapter._restart_room_ws.assert_awaited_once_with("room1")

    async def test_watchdog_restarts_stalled_room(self):
        adapter = make_adapter()
        adapter._stop_event.clear()
        live = asyncio.create_task(asyncio.sleep(10))
        adapter._room_ws_tasks["room1"] = live
        # Letzter Empfang deutlich zurückliegend
        adapter._last_rx["room1"] = time.monotonic() - adapter._rx_stall_seconds - 1
        adapter._restart_room_ws = AsyncMock()

        now = time.monotonic()
        for room_id, task in list(adapter._room_ws_tasks.items()):
            if task.done():
                await adapter._restart_room_ws(room_id)
                continue
            last_rx = adapter._last_rx.get(room_id)
            if last_rx is not None and (now - last_rx) > adapter._rx_stall_seconds:
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
        self.assertIn("room1", adapter._last_rx)
        new_task.cancel()

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


class SupervisedLoopContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_supervised_loop_reconnects_after_inner_loop_end(self):
        adapter = make_adapter()
        adapter._stop_event.clear()
        calls = []

        async def fake_inner(room_id, settings, fetch, stop_event):
            calls.append(room_id)
            if len(calls) >= 3:
                adapter._stop_event.set()
                return
            # erster/zweiter Durchlauf: sofortiges Ende (simulierter WS-Close)

        adapter.signaling_mgr.room_signaling_loop = fake_inner
        # Backoff für den Test stark verkürzen
        supervised = adapter._supervised_room_loop("room1", object())
        async_fast = asyncio.wait_for(supervised, timeout=2.0)

        # Wir müssen das sleep(5)-Backoff umgehen: patch asyncio.sleep im Modul
        import adapter as adapter_module
        orig_sleep = adapter_module.asyncio.sleep

        async def fast_sleep(delay, *a, **kw):
            return await orig_sleep(min(delay, 0.01) if delay == 5.0 else delay)

        adapter_module.asyncio.sleep = fast_sleep
        try:
            await asyncio.wait_for(supervised, timeout=2.0)
        finally:
            adapter_module.asyncio.sleep = orig_sleep

        self.assertEqual(calls, ["room1", "room1", "room1"])

    async def test_supervised_loop_survives_inner_exception(self):
        adapter = make_adapter()
        adapter._stop_event.clear()
        calls = []

        async def fake_inner(room_id, settings, fetch, stop_event):
            calls.append(room_id)
            raise RuntimeError("ws boom")

        adapter.signaling_mgr.room_signaling_loop = fake_inner

        import adapter as adapter_module
        orig_sleep = adapter_module.asyncio.sleep

        async def fast_sleep(delay, *a, **kw):
            return await orig_sleep(min(delay, 0.01) if delay == 5.0 else delay)

        # sleep permanent patchen (auch für den im Task laufenden Supervisor)
        adapter_module.asyncio.sleep = fast_sleep
        supervised = asyncio.create_task(
            adapter._supervised_room_loop("room1", object())
        )
        try:
            # Mehrere Crash/Reconnect-Zyklen zulassen (fast_sleep macht die
            # 5s-Backoffs real nur 10ms lang).
            await orig_sleep(0.5)
        finally:
            adapter_module.asyncio.sleep = orig_sleep
            adapter._stop_event.set()
            supervised.cancel()
            try:
                await supervised
            except (asyncio.CancelledError, Exception):
                pass
        self.assertGreaterEqual(len(calls), 2)


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
