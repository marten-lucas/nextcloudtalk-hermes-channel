import asyncio
import unittest
from types import SimpleNamespace

from adapter import NextcloudTalkPlatform


class TestableNextcloudTalkPlatform(NextcloudTalkPlatform):
    def __init__(self, config):
        super().__init__(config)
        self.calls = []
        self.mock_room_messages = []
        self.mock_participants = {}
        self.mock_joined_rooms = []
        self.connect_websocket_success = True
        self.received_events = []

    async def _connect_websocket_once(self) -> bool:
        self.calls.append(("connect_websocket",))
        return self.connect_websocket_success

    def _start_polling_loop(self) -> None:
        self.calls.append(("start_polling",))
        self._polling_task = asyncio.create_task(asyncio.sleep(0.001))

    async def _ocs_get(self, path, params=None):
        self.calls.append(("ocs_get", path, params))
        if path == "apps/spreed/api/v4/room":
            return [{"token": rid} for rid in self.mock_joined_rooms]
        if path.endswith("/participants"):
            parts = path.split("/")
            room_id = parts[5] if len(parts) > 5 else ""
            count = self.mock_participants.get(room_id, 3)
            return [{"id": f"user-{i}"} for i in range(count)]
        if "/chat/" in path:
            return list(self.mock_room_messages)
        return []

    async def _ocs_post(self, path, data):
        self.calls.append(("ocs_post", path, data))
        return {"id": "sent-1"}

    async def _download_attachment_from_metadata(self, attachment):
        self.calls.append(("download_attachment", attachment))
        return "/tmp/mock-attachment"

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
        self.assertEqual(adapter.received_events[0].user_id, "kassier")

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

    async def test_ws_fallback_to_polling_contract(self):
        adapter = TestableNextcloudTalkPlatform(
            make_config(base_url="https://nc.local", username="hermes", app_password="pw")
        )
        adapter.connect_websocket_success = False
        await adapter.connect()
        self.assertIn(("connect_websocket",), adapter.calls)
        self.assertIn(("start_polling",), adapter.calls)
        await adapter.disconnect()

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


if __name__ == "__main__":
    unittest.main()
