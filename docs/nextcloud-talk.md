# Nextcloud Talk Plugin Documentation

## Overview

This plugin integrates Hermes with **Nextcloud Talk** as a standalone platform adapter.
It is intended for installation via the Hermes plugin system or the Hermes Dashboard UI.

## Runtime behavior

### Room handling

- **1:1 chats**: every message triggers Hermes
- **2-participant rooms**: every message triggers Hermes
- **Group rooms (>2 participants)**: Hermes only reacts to messages containing the bot mention

### Context handling

When a group message triggers Hermes, the adapter loads recent room history on demand and forwards it as context.
The default limit is 20 messages and can be changed with `NEXTCLOUD_CONTEXT_MESSAGE_LIMIT`.

### Replies

Hermes replies in the same room and uses Nextcloud reply metadata so the response is shown as a direct reply.

### Attachments

Incoming attachment metadata is detected and downloaded into a temp directory before being passed to Hermes.

### HITL approvals

The adapter supports approval/rejection via reactions:

- Approve: `✅`, `👍`
- Reject: `❌`, `👎`

Only the original requester may resolve the action when requester enforcement is enabled.

## Transport

1. Try WebSocket connection first
2. If that fails, start polling
3. Continue retrying transparently

## Configuration reference

### Required

| Variable | Meaning |
| --- | --- |
| `NEXTCLOUD_BASE_URL` | Nextcloud base URL |
| `NEXTCLOUD_USERNAME` | Bot username |
| `NEXTCLOUD_APP_PASSWORD` | Bot app password |

### Optional

| Variable | Meaning |
| --- | --- |
| `NEXTCLOUD_BOT_HANDLE` | Mention handle used in group rooms |
| `NEXTCLOUD_REQUIRE_MENTION_IN_GROUPS` | Require mention in rooms with >2 participants |
| `NEXTCLOUD_CONTEXT_MESSAGE_LIMIT` | Recent messages to fetch on group trigger |
| `NEXTCLOUD_POLL_INTERVAL_SECONDS` | Polling interval when WS is unavailable |
| `NEXTCLOUD_ALLOWED_ROOMS` | Optional comma-separated room allowlist |
| `NEXTCLOUD_ALLOWED_USERS` | Optional comma-separated sender allowlist |
| `NEXTCLOUD_ALLOW_ALL_USERS` | Allow all users (dev only) |
| `NEXTCLOUD_ATTACHMENT_TMP_DIR` | Temp dir for downloaded files |
| `NEXTCLOUD_HITL_REQUIRE_REQUESTER` | Only requester may approve/reject |
| `NEXTCLOUD_HOME_CHANNEL` | Default room for cron delivery |
| `NEXTCLOUD_HOME_CHANNEL_NAME` | Display name for home channel |

## Installation

1. Copy the repo to `~/.hermes/plugins/nextcloud-talk/`
2. Add the plugin in the Hermes Dashboard
3. Fill the environment variables
4. Restart Hermes / reload plugins

## Verification

Use the built-in tests:

```bash
python -m unittest -q tests.platforms.nextcloud.test_adapter_contracts
python -m unittest discover -q
```

## Notes

- The plugin is standalone and does not require changes to Hermes core.
- The transport endpoints are intentionally isolated in `adapter.py` for easy tuning against your Nextcloud instance.
