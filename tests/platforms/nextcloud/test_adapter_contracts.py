import asyncio
import importlib
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

# Bootstrap: Das Plugin-Verzeichnis als Package laden und `adapter` als
# Top-Level-Alias bereitstellen, damit `import adapter` funktioniert,
# obwohl adapter.py relative Imports (from .client import ...) nutzt.
if "adapter" not in sys.modules:
    _pkg = types.ModuleType("_ncplugin_under_test")
    import os

    # tests/platforms/nextcloud/ -> 3 Ebenen hoch = Plugin-Root
    _plugin_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    _pkg.__path__ = [_plugin_root]
    sys.modules["_ncplugin_under_test"] = _pkg
    for _mod in ("attachments", "client", "hitl", "identity", "outbound", "presence", "signaling", "adapter"):
        importlib.import_module(f"_ncplugin_under_test.{_mod}")
    sys.modules["adapter"] = sys.modules["_ncplugin_under_test.adapter"]

import adapter as nextcloud_adapter_module
from adapter import NextcloudTalkPlatform


class _MockTalkClient:
    """Mock-Client, der die OCS-Aufrufe des Adapters und aller Manager abfängt.

    Ersetzt die früheren Adapter-Hooks _ocs_get/_ocs_post, die beim
    Manager-Refactor (v0.2.0) entfernt wurden.
    """

    def __init__(self, adapter):
        self.adapter = adapter

    def _record(self, method, path, *args):
        self.adapter.calls.append((method, path, *args))

    async def ocs_get(self, path, params=None):
        self._record("ocs_get", path, params)
        if path == "apps/spreed/api/v3/signaling/settings":
            return {
                "server": "",
                "helloAuthParams": {},
                "signalingMode": "standalone",
                "userId": "hermes",
            }
        if path == "apps/spreed/api/v4/room":
            return [{"token": rid} for rid in self.adapter.mock_joined_rooms]
        if path.endswith("/participants"):
            parts = path.split("/")
            room_id = parts[5] if len(parts) > 5 else ""
            count = self.adapter.mock_participants.get(room_id, 3)
            return [{"id": f"user-{i}"} for i in range(count)]
        if path.startswith("apps/spreed/api/v4/room/") and path.count("/") == 5:
            # Einzelraum-Metadaten (Existenz-/readOnly-Prüfung)
            parts = path.split("/")
            room_id = parts[5]
            if room_id in self.adapter.mock_room_meta:
                return self.adapter.mock_room_meta[room_id]
            return {"token": room_id, "readOnly": 0, "participantType": 3}
        if "/chat/" in path:
            return list(self.adapter.mock_room_messages)
        return []

    async def ocs_post(self, path, data=None):
        self._record("ocs_post", path, data)
        if path.endswith("/participants/active"):
            return {"sessionId": "session-1"}
        return {"id": "sent-1"}

    async def ocs_put(self, path, data=None):
        self._record("ocs_put", path, data)
        return {}

    async def ocs_delete(self, path):
        self._record("ocs_delete", path)
        return {}

    async def cloud_ocs_get(self, path, params=None):
        self._record("cloud_ocs_get", path, params)
        return {"groups": []}

    async def ensure_session(self):
        return None

    async def close(self):
        return None


class TestableNextcloudTalkPlatform(NextcloudTalkPlatform):
    def __init__(self, config):
        super().__init__(config)
        self.calls = []
        self.mock_room_messages = []
        self.mock_participants = {}
        self.mock_joined_rooms = []
        self.connect_websocket_success = True
        self.received_events = []
        self.presence_updates = []
        self.custom_status_updates = []
        self.custom_status_clears = []
        self.cancelled_sessions = []
        self.mock_room_meta = {}
        # Client durch Mock ersetzen (Adapter ruft seit v0.2.0
        # self.client.ocs_get/ocs_post direkt auf statt Adapter-Hooks)
        self.client = _MockTalkClient(self)
        # Manager neu verdrahten, damit sie den Mock-Client nutzen
        self.identity_mgr.client = self.client
        self.presence_mgr.client = self.client
        self.signaling_mgr.client = self.client
        self.attachment_mgr.client = self.client
        self.hitl_mgr.client = self.client
        self._hooked_presence()

    async def _connect_websocket_once(self) -> bool:
        self.calls.append(("connect_websocket",))
        return self.connect_websocket_success

    def _start_polling_loop(self) -> None:
        self.calls.append(("start_polling",))
        self._polling_task = asyncio.create_task(asyncio.sleep(0.001))

    async def _ocs_get(self, path, params=None):
        return await self.client.ocs_get(path, params)

    async def _ocs_post(self, path, data):
        return await self.client.ocs_post(path, data)

    async def _download_attachment_from_metadata(self, attachment):
        self.calls.append(("download_attachment", attachment))
        return "/tmp/mock-attachment"

    async def _set_presence_status(self, state):
        self.presence_updates.append(state)

    async def _set_custom_status_message(self, message, status_icon=None):
        self.custom_status_updates.append((message, status_icon))

    async def _clear_custom_status_message(self, force=False):
        self.custom_status_clears.append(force)

    def _hooked_presence(self):
        """Presence-Manager mit Hook-Delegation ersetzen (v0.1.22-Verhalten)."""
        adapter = self

        class _HookedPresenceManager(type(self.presence_mgr)):
            async def set_presence_status(state_self, state):
                await adapter._set_presence_status(state)

            async def set_custom_status_message(state_self, message, status_icon=None):
                await adapter._set_custom_status_message(message, status_icon)

            async def clear_custom_status_message(state_self, *, force=False):
                await adapter._clear_custom_status_message(force=force)

        self.presence_mgr = _HookedPresenceManager(self.client)

    async def cancel_session_processing(self, session_key, **kwargs):
        self.cancelled_sessions.append((session_key, kwargs))

    async def handle_message(self, event):
        self.received_events.append(event)


def make_config(**extra):
    return SimpleNamespace(extra=extra, token=None)


class NextcloudAdapterContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_group_room_requires_mention_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        adapter.mock_participants["room1"] = 3
        await adapter.handle_incoming_event(
            {"room_id": "room1", "id": "m1", "actorId": "kassier", "message": "Hallo zusammen"}
        )
        self.assertEqual(adapter.received_events, [])

        await adapter.handle_incoming_event(
            {"room_id": "room1", "id": "m2", "actorId": "kassier", "message": "@hermes bitte helfen"}
        )
        self.assertEqual(len(adapter.received_events), 1)
        self.assertEqual(adapter.received_events[0].source["user_id"], "kassier")

    async def test_identity_headers_use_human_source_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        adapter.mock_participants["room-identity"] = 2
        adapter.identity_mgr.get_user_groups = AsyncMock(
            return_value=["admin", "kiga_board"]
        )

        await adapter.handle_incoming_event(
            {"room_id": "room-identity", "id": "m-id", "actorId": "vorstand", "message": "Bitte helfen"}
        )

        self.assertEqual(len(adapter.received_events), 1)
        self.assertEqual("vorstand", adapter.received_events[0].source["user_id"])
        self.assertEqual("vorstand", adapter.received_events[0].source["extra_headers"]["X-On-Behalf-Of"])
        self.assertEqual("admin,kiga_board", adapter.received_events[0].source["extra_headers"]["X-User-Groups"])

    async def test_two_participant_room_always_triggers_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        adapter.mock_participants["room2"] = 2

        await adapter.handle_incoming_event(
            {"room_id": "room2", "id": "m1", "actorId": "vorstand", "message": "Ohne Mention"}
        )
        self.assertEqual(len(adapter.received_events), 1)
        self.assertEqual(adapter.received_events[0].source["chat_type"], "dm")

    async def test_group_room_uses_api_participant_count_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        adapter.mock_participants["room_api"] = 3
        await adapter.handle_incoming_event(
            {
                "room_id": "room_api",
                "id": "m-api-1",
                "actorId": "vorstand",
                "message": "Ohne Mention",
                "participants": [{"id": "vorstand"}, {"id": "ki_assistent"}],
            }
        )
        self.assertEqual(adapter.received_events, [])

    async def test_context_fetch_limit_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(
                base_url="https://nc.local",
                username="hermes",
                app_password="pw",
                context_message_limit=5,
            )
        )
        adapter.mock_participants["room3"] = 5
        adapter.mock_room_messages = [{"id": str(i), "actorId": "u", "message": f"m{i}"} for i in range(10)]

        await adapter.handle_incoming_event(
            {"room_id": "room3", "id": "m100", "actorId": "vorstand", "message": "@hermes context?"}
        )
        self.assertEqual(len(adapter.received_events), 1)
        self.assertEqual(len(adapter.received_events[0].raw_message["context_messages"]), 5)

    async def test_reply_to_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        result = await adapter.send_message("room4", "Antwort", "m-parent")
        self.assertTrue(result.success)
        last_post = [call for call in adapter.calls if call[0] == "ocs_post"][-1]
        self.assertEqual(last_post[1], "apps/spreed/api/v1/chat/room4")
        self.assertEqual(last_post[2]["replyTo"], "m-parent")

    async def test_send_marks_room_active_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        await adapter.send_message("room4", "Antwort")
        active_post = [call for call in adapter.calls if call[0] == "ocs_post" and call[1].endswith("/participants/active")]
        self.assertTrue(active_post)

    async def test_bang_command_alias_maps_to_gateway_command_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        adapter.mock_participants["room-bang"] = 2
        await adapter.handle_incoming_event(
            {"room_id": "room-bang", "id": "m-bang", "actorId": "vorstand", "message": "!stop bitte"}
        )
        self.assertEqual(adapter.received_events[0].text, "/stop bitte")
        self.assertEqual(adapter.received_events[0].message_type, "command")

    async def test_gateway_restarting_notice_updates_presence_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        result = await adapter.send_message("room4", "Gateway restarting")
        self.assertTrue(result.success)
        chat_posts = [call for call in adapter.calls if call[0] == "ocs_post" and "/chat/" in call[1]]
        self.assertEqual(chat_posts, [])
        self.assertEqual(adapter.presence_updates[-1], "offline")
        self.assertEqual(adapter.custom_status_updates[-1], ("Gateway restarting", "🔄"))

    async def test_gateway_online_notice_updates_presence_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        result = await adapter.send_message("room4", "gateway online")
        self.assertTrue(result.success)
        chat_posts = [call for call in adapter.calls if call[0] == "ocs_post" and "/chat/" in call[1]]
        self.assertEqual(chat_posts, [])
        self.assertEqual(adapter.presence_updates[-1], "online")
        self.assertTrue(adapter.custom_status_clears)

    async def test_source_uses_username_only_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        adapter.mock_participants["room-profile"] = 2
        await adapter.handle_incoming_event(
            {
                "room_id": "room-profile",
                "id": "m-profile",
                "actorId": "vorstand",
                "actorDisplayName": "Marten Lucas",
                "message": "Profiltest",
            }
        )
        source = adapter.received_events[0].source
        self.assertEqual(source["user_id"], "vorstand")
        self.assertEqual(source["user_name"], "vorstand")

    async def test_fresh_session_existing_chat_adds_reset_note_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        adapter.mock_participants["room-reset-note"] = 2
        adapter.mock_room_messages = [
            {"id": "1", "actorId": "vorstand", "message": "alt"},
            {"id": "2", "actorId": "vorstand", "message": "neu"},
        ]
        adapter.gateway_runner = SimpleNamespace(_peek_session_state=lambda session_key: None)
        original_build_session_key = nextcloud_adapter_module.build_session_key
        nextcloud_adapter_module.build_session_key = lambda source, **kwargs: "session:room-reset-note"
        try:
            await adapter.handle_incoming_event(
                {"room_id": "room-reset-note", "id": "2", "actorId": "vorstand", "message": "Aktuelle Frage"}
            )
        finally:
            nextcloud_adapter_module.build_session_key = original_build_session_key
        self.assertIn("was reset", adapter.received_events[0].text)

    async def test_attachment_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        adapter.mock_participants["room5"] = 2

        await adapter.handle_incoming_event(
            {
                "room_id": "room5",
                "id": "m-attach",
                "actorId": "vorstand",
                "message": "Bitte ansehen",
                "attachments": [{"id": "file-1"}],
            }
        )
        self.assertEqual(
            adapter.received_events[0].raw_message["attachment_paths"],
            ["/tmp/mock-attachment"],
        )

    async def test_attachment_from_message_parameters_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        adapter.mock_participants["room5"] = 2
        await adapter.handle_incoming_event(
            {
                "room_id": "room5",
                "id": "m-attach-param",
                "actorId": "vorstand",
                "message": "{file}",
                "messageParameters": {
                    "file": {"type": "file", "id": "file-2", "path": "/Dokumente/hermes-test.md"}
                },
            }
        )
        self.assertEqual(adapter.received_events[0].raw_message["attachment_paths"], ["/tmp/mock-attachment"])

    async def test_system_message_is_ignored_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        adapter.mock_participants["room_sys"] = 2
        await adapter.handle_incoming_event(
            {
                "room_id": "room_sys",
                "id": "m-sys",
                "actorId": "system",
                "actorType": "bots",
                "systemMessage": "conversation_created",
                "message": "Das System hat die Unterhaltung erstellt",
            }
        )
        self.assertEqual(adapter.received_events, [])

    async def test_edit_event_reenters_as_contextual_message_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        adapter.mock_participants["room-edit"] = 2
        await adapter.handle_incoming_event(
            {"room_id": "room-edit", "id": "m1", "actorId": "vorstand", "message": "alt"}
        )
        await adapter.handle_incoming_event(
            {
                "room_id": "room-edit",
                "id": "m1-edit",
                "messageId": "m1",
                "actorId": "vorstand",
                "eventType": "message_edit",
                "message": "neu",
                "timestamp": 1723101000,
            }
        )
        self.assertIn("wurde geaendert zu", adapter.received_events[-1].text)
        self.assertIn("neu", adapter.received_events[-1].text)

    async def test_delete_event_reenters_as_contextual_message_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        adapter.mock_participants["room-delete"] = 2
        await adapter.handle_incoming_event(
            {"room_id": "room-delete", "id": "m-del", "actorId": "vorstand", "message": "bitte loeschen"}
        )
        await adapter.handle_incoming_event(
            {
                "room_id": "room-delete",
                "id": "m-del-delete",
                "messageId": "m-del",
                "actorId": "vorstand",
                "eventType": "message_delete",
            }
        )
        self.assertIn("wurde geloescht", adapter.received_events[-1].text)

    async def test_empty_message_without_attachment_is_ignored_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        adapter.mock_participants["room_empty"] = 2
        await adapter.handle_incoming_event(
            {
                "room_id": "room_empty",
                "id": "m-empty",
                "actorId": "vorstand",
                "message": "",
            }
        )
        self.assertEqual(adapter.received_events, [])

    async def test_ws_fallback_to_polling_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        adapter.connect_websocket_success = False
        await adapter.connect()
        self.assertIn(("connect_websocket",), adapter.calls)
        self.assertIn(("start_polling",), adapter.calls)
        await adapter.disconnect()
        self.assertIn("online", adapter.presence_updates)
        self.assertIn("offline", adapter.presence_updates)

    async def test_polling_bootstrap_skips_existing_history_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        room_id = "room-bootstrap"
        calls = {"count": 0}

        async def fake_ocs_get(path, params=None):
            if path != f"apps/spreed/api/v1/chat/{room_id}":
                return []
            calls["count"] += 1
            if calls["count"] == 1:
                return [{"id": "1", "message": "old1"}, {"id": "2", "message": "old2"}]
            return [{"id": "3", "message": "new"}]

        adapter.client.ocs_get = fake_ocs_get  # type: ignore[method-assign]

        first = await adapter._fetch_room_events(room_id)
        second = await adapter._fetch_room_events(room_id)
        self.assertEqual(first, [])
        self.assertEqual(len(second), 1)
        self.assertEqual(second[0]["id"], "3")

    async def test_poll_cursor_uses_latest_message_id_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        room_id = "room-cursor"
        calls = {"count": 0, "params": []}

        async def fake_ocs_get(path, params=None):
            if path != f"apps/spreed/api/v1/chat/{room_id}":
                return []
            calls["count"] += 1
            calls["params"].append(dict(params or {}))
            if calls["count"] == 1:
                # Descending order as returned by Talk endpoint
                return [{"id": "12", "message": "newest"}, {"id": "10", "message": "older"}]
            return [{"id": "14", "message": "newer"}, {"id": "13", "message": "old"}]

        adapter.client.ocs_get = fake_ocs_get  # type: ignore[method-assign]
        first = await adapter._fetch_room_events(room_id)
        self.assertEqual(first, [])
        self.assertEqual(adapter._poll_cursor_by_room[room_id], "12")

        second = await adapter._fetch_room_events(room_id)
        self.assertEqual(len(second), 2)
        self.assertEqual(adapter._poll_cursor_by_room[room_id], "14")
        self.assertEqual(calls["params"][1].get("lookIntoFuture"), 1)
        self.assertEqual(calls["params"][1].get("lastKnownMessageId"), "12")

    async def test_hitl_requester_only_and_no_timeout_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        approval_task = asyncio.create_task(
            adapter.request_human_approval("room7", "prompt-1", "kassier")
        )
        await asyncio.sleep(0)
        self.assertFalse(approval_task.done(), "Approval must stay pending without timeout")

        await adapter.handle_incoming_event(
            {
                "type": "reaction",
                "targetMessageId": "prompt-1",
                "actorId": "fremder-user",
                "emoji": "✅",
            }
        )
        await asyncio.sleep(0)
        self.assertFalse(approval_task.done(), "Foreign reaction must be ignored")

        await adapter.handle_incoming_event(
            {
                "type": "reaction",
                "targetMessageId": "prompt-1",
                "actorId": "kassier",
                "emoji": "✅",
            }
        )
        self.assertTrue(await approval_task)

    async def test_hitl_fallback_reply_emoji_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        approval_task = asyncio.create_task(
            adapter.request_human_approval("room8", "prompt-2", "vorstand")
        )
        await asyncio.sleep(0)
        await adapter.handle_incoming_event(
            {
                "room_id": "room8",
                "id": "m-emoji",
                "actorId": "vorstand",
                "message": "✅",
                "referenceId": "prompt-2",
                "participant_count": 2,
            }
        )
        self.assertTrue(await approval_task)

    async def test_cancel_reaction_stops_matching_session_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        adapter._message_session_keys["prompt-9"] = {
            "session_key": "nextcloud:session:9",
            "requester_user_id": "vorstand",
            "chat_id": "room9",
        }
        await adapter.handle_incoming_event(
            {
                "type": "reaction",
                "targetMessageId": "prompt-9",
                "actorId": "vorstand",
                "emoji": "⛔",
            }
        )
        self.assertEqual(adapter.cancelled_sessions[0][0], "nextcloud:session:9")

    async def test_status_update_sets_custom_presence_text_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        await adapter.send_or_update_status("room10", "tool.started", "Running tool")
        self.assertEqual(adapter.custom_status_updates[-1], ("Fuehrt Werkzeuge aus", "🛠️"))


if __name__ == "__main__":
    unittest.main()
