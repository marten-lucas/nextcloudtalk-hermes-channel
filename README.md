# Nextcloud Talk Platform Plugin for Hermes

Standalone Hermes platform plugin for **Nextcloud Talk**.

## What it does

- Connects Hermes to Nextcloud Talk as a regular bot user
- Uses **WebSocket first**, with **HTTP polling fallback**
- Triggers automatically in 1:1 chats and 2-person rooms
- Requires `@mention` in group rooms with more than 2 participants
- Fetches recent room context on demand
- Sends replies as `replyTo`
- Downloads attachments to a temp directory
- Propagates sender identity to Hermes / downstream MCP tools
- Supports HITL approval via reactions

## Repository layout

This repo is already packaged as a **standalone Hermes plugin**:

```text
.
├── __init__.py
├── adapter.py
├── plugin.yaml
└── tests/
```

Drop the folder into `~/.hermes/plugins/nextcloud-talk/` or add it through the Hermes Dashboard UI.

## Required environment variables

- `NEXTCLOUD_BASE_URL`
- `NEXTCLOUD_USERNAME`
- `NEXTCLOUD_APP_PASSWORD`

## Optional environment variables

- `NEXTCLOUD_ALLOWED_USERS`
- `NEXTCLOUD_ALLOW_ALL_USERS`
- `NEXTCLOUD_BOT_HANDLE`
- `NEXTCLOUD_REQUIRE_MENTION_IN_GROUPS`
- `NEXTCLOUD_CONTEXT_MESSAGE_LIMIT`
- `NEXTCLOUD_POLL_INTERVAL_SECONDS`
- `NEXTCLOUD_ALLOWED_ROOMS`
- `NEXTCLOUD_ATTACHMENT_TMP_DIR`
- `NEXTCLOUD_HITL_REQUIRE_REQUESTER`
- `NEXTCLOUD_HOME_CHANNEL`
- `NEXTCLOUD_HOME_CHANNEL_NAME`

## Local validation

Run the contract tests:

```bash
python -m unittest -q tests.platforms.nextcloud.test_adapter_contracts
python -m unittest discover -q
```

## Usage notes

- Use a dedicated bot account in Nextcloud.
- Prefer an app password over the account password.
- For groups, set `NEXTCLOUD_BOT_HANDLE` to the exact mention handle users should use.
- If WebSocket is not available, the adapter falls back to polling automatically.

## Status

This plugin is ready to be added via Hermes UI and then connected to a local Nextcloud instance for smoke testing.
