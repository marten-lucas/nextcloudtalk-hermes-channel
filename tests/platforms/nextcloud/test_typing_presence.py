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


def make_adapter() -> TestableNextcloudTalkPlatform:
    return TestableNextcloudTalkPlatform(
        make_config(base_url="https://nc.local", username="hermes", app_password="pw")
    )


class TypingContractTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.adapter = make_adapter()
        self.sent: list = []
        self.adapter.client.ocs_post = AsyncMock(
            side_effect=lambda path, data=None: self.sent.append(path) or {"id": "x"}
        )

    async def test_send_typing_starts_refresh_task(self):
        await self.adapter.send_typing("room1")
        task = self.adapter.presence_mgr._typing_tasks.get("room1")
        self.assertIsNotNone(task)
        self.assertFalse(task.done())
        await self.adapter.stop_typing("room1")

    async def test_send_typing_refreshes_periodically(self):
        await self.adapter.send_typing("room1")
        # Loop posted at least one typing call immediately
        await asyncio.sleep(0.05)
        self.assertTrue(any("/typing" in p for p in self.sent))
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

    async def test_send_typing_swallows_ocs_errors(self):
        self.adapter.client.ocs_post = AsyncMock(side_effect=RuntimeError("boom"))
        # Darf nicht werfen; Loop beendet sich nach dem Fehler selbst
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
