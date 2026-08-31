# Nextcloud Talk Platform Plugin for Hermes

Standalone Hermes platform plugin for **Nextcloud Talk** integration.

## Features

- ✅ Connects Hermes as a regular Nextcloud bot user
- ✅ **WebSocket-first** transport (HPB signaling) with **HTTP polling fallback**
- ✅ Automatic triggers in 1:1 chats and 2-person rooms
- ✅ `@mention`-based triggering in group rooms (>2 participants)
- ✅ Room allowlist (`NEXTCLOUD_ALLOWED_ROOMS`)
- ✅ On-demand context fetching (last N messages) for group triggers
- ✅ **Intelligent outbound message categorization** (lifecycle, error, suppress, forward)
- ✅ Edit/Delete event handling (edits and deletions re-enter Hermes with context)
- ✅ `!command` → `/command` alias normalization for gateway commands
- ✅ Sends replies with Nextcloud `replyTo` metadata for visual context linking
- ✅ Multimodal support: downloads attachments (images, documents) to temp directory
- ✅ Sender identity propagation to Hermes and downstream MCP tools (`X-On-Behalf-Of`, `X-User-Groups`)
- ✅ Human-in-the-Loop (HITL) approvals via message reactions
- ✅ Custom presence and status signaling

## Repository layout

Standalone Hermes plugin, modular structure:

```text
.
├── __init__.py             # Plugin entrypoint (exports NextcloudTalkPlatform, register)
├── adapter.py              # Core platform adapter: inbound pipeline, outbound routing
├── client.py               # Nextcloud Talk OCS REST client (+ NextcloudOCSException)
├── identity.py             # User group lookup with TTL cache + ContextVars identity
├── hitl.py                 # HITL approval manager (reaction-based)
├── presence.py             # Presence & custom status manager
├── attachments.py          # Attachment extraction & download
├── signaling.py            # WebSocket (HPB) signaling manager
├── outbound.py             # Outbound message categorization (lifecycle/error/suppress/forward)
├── plugin.yaml             # Plugin metadata
├── docs/
│   ├── nextcloud-talk.md   # Extended documentation
│   └── restoration-plan.md # Refactor parity plan (historical)
└── tests/
    └── platforms/nextcloud/test_adapter_contracts.py
```

Installation: Copy to `~/.hermes/plugins/nextcloud-talk/` or install via Hermes Dashboard UI.

## Quick Start

### 1. Environment Setup

**Required variables:**

```bash
export NEXTCLOUD_BASE_URL="https://cloud.example.org"
export NEXTCLOUD_USERNAME="hermes"
export NEXTCLOUD_APP_PASSWORD="xxxx-yyyy-zzzz-wwww"  # Use app password, not account password
```

**Recommended optional variables:**

```bash
export NEXTCLOUD_BOT_HANDLE="@hermes"
export NEXTCLOUD_CONTEXT_MESSAGE_LIMIT="20"
export NEXTCLOUD_REQUIRE_MENTION_IN_GROUPS="true"
```

See [Configuration Reference](#configuration-reference) for complete list.

### 2. Installation

```bash
# Via Hermes Dashboard UI:
# 1. Navigate to Settings → Plugins
# 2. Click "Add Plugin"
# 3. Enter plugin path or repo clone
# 4. Fill required environment variables
# 5. Restart Hermes

# Or manually:
git clone <repo> ~/.hermes/plugins/nextcloud-talk/
cd ~/.hermes/plugins/nextcloud-talk/
# Set environment variables
hermes reload plugins
```

### 3. Verify Connection

```bash
# Check Hermes logs for connection success:
# ✓ nextcloud connected
# ✓ Using WebSocket|polling transport
```

## Message Handling

### Inbound (Nextcloud → Hermes)

The adapter processes incoming events through a filter and trigger pipeline:

1. **Reaction events** are routed to the HITL manager (✅/👍 approve, ❌/👎 reject, ⛔ cancel)
2. **Non-user actors** (`actorType != "users"`) are ignored
3. **Native system messages** (`systemMessage` flag) are ignored (except deletions)
4. **Own messages** (bot username) and reserved accounts (`system`, `changelog`, `sample`) are ignored
5. **Known system text patterns** (e.g. „Das System hat …", „{actor}") are ignored
6. **Room allowlist** (`NEXTCLOUD_ALLOWED_ROOMS`) is enforced
7. **Trigger gating**: 1:1 / 2-participant rooms always trigger; group rooms require `@mention` (configurable)
8. **Edit/Delete events** re-enter Hermes with contextual text („Nachricht wurde geändert zu …" / „… wurde geloescht.")
9. **Attachments** are extracted and downloaded; empty messages without attachments are ignored
10. **Group context**: last N messages are fetched and attached as `context_messages`
11. **Command normalization**: `!command` aliases are resolved to `/command` gateway commands
12. **Identity**: sender groups are resolved (TTL-cached) and injected as `X-On-Behalf-Of` / `X-User-Groups` headers plus ContextVars for downstream MCP tools

### Outbound (Hermes → Nextcloud)

Every outgoing message is categorized before sending:

#### Category A: **Lifecycle** (Status + Presence)

Gateway operational events trigger presence and custom status updates:

- `"Gateway restarting"` → State: `offline`, Custom Status: 🔄 "Gateway restarting"
- `"Gateway online — Hermes is back"` → State: `online`, Clear custom status
- `"Draining"` + `"active agent"` → State: `draining`, Custom Status: ⏸️

**Behavior:** Status and presence are updated in Nextcloud; no message sent to chat.

#### Category B: **Error** (Reply with Context)

Error messages are formatted and sent as replies to the triggering message:

- Messages starting with `⚠️`
- Patterns: `"processing stopped"`, `"no response"`, `"session too large"`, `"authentication failed"`, `"provider failed"`, `"tool failed"`, etc.

**Behavior:**
- Sent as reply to trigger message (visually linked via `replyTo`)
- Format: `🚫 **Fehler**\n\n{original_error_message}`
- If no trigger message exists: shown in custom status only (fallback)

#### Category C: **Suppress** (Silent Handling)

Queue and progress messages that clutter the chat are silently suppressed:

- `"gateway queued"`, `"compressing context"`, `"working —"`, `"subagent working"`, `"steer failed"`

**Behavior:** Message never reaches Nextcloud; `SendResult(success=True)` returned silently.

#### Category D: **Forward** (Send As-Is)

All other messages sent as normal bot responses:

**Behavior:** Sent to chat as-is with optional `replyTo` if available; room is marked active.

---

## Configuration Reference

### Required environment variables

| Variable | Description | Example |
| --- | --- | --- |
| `NEXTCLOUD_BASE_URL` | Nextcloud instance URL | `https://cloud.example.org` |
| `NEXTCLOUD_USERNAME` | Bot account username | `hermes` |
| `NEXTCLOUD_APP_PASSWORD` | Bot app password | `xxxx-yyyy-zzzz-wwww` |

### Optional environment variables

| Variable | Default | Description |
| --- | --- | --- |
| `NEXTCLOUD_BOT_HANDLE` | `@{NEXTCLOUD_USERNAME}` | Mention handle for group rooms |
| `NEXTCLOUD_REQUIRE_MENTION_IN_GROUPS` | `true` | Require bot mention in rooms with >2 participants |
| `NEXTCLOUD_CONTEXT_MESSAGE_LIMIT` | `20` | Recent messages to fetch on group trigger |
| `NEXTCLOUD_POLL_INTERVAL_SECONDS` | `3` | Polling interval when WebSocket unavailable |
| `NEXTCLOUD_ALLOWED_USERS` | (none) | Comma-separated allowed user IDs (allowlist) |
| `NEXTCLOUD_ALLOW_ALL_USERS` | `false` | Allow all users (dev/testing only) |
| `NEXTCLOUD_ALLOWED_ROOMS` | (none) | Comma-separated allowed room tokens (allowlist) |
| `NEXTCLOUD_ATTACHMENT_TMP_DIR` | (system temp) | Directory for temporary attachment downloads |
| `NEXTCLOUD_HITL_REQUIRE_REQUESTER` | `true` | Only original requester can approve/reject reactions |
| `NEXTCLOUD_HOME_CHANNEL` | (none) | Default room ID for cron/scheduled delivery |
| `NEXTCLOUD_HOME_CHANNEL_NAME` | (none) | Display name for home channel |

### Room Behavior

- **1:1 chats**: Every message triggers Hermes (no mention required)
- **2-participant rooms**: Every message triggers Hermes (no mention required)
- **Group rooms (>2 participants)**: Only messages with bot mention trigger Hermes (disable via `NEXTCLOUD_REQUIRE_MENTION_IN_GROUPS=false`)

### HITL Approvals

Tool execution confirmations use message reactions:

- **Approve**: `✅`, `👍`
- **Reject**: `❌`, `👎`
- **Cancel**: `⛔` (stops the running session)

Only the original message sender can approve/reject (when `NEXTCLOUD_HITL_REQUIRE_REQUESTER=true`).

---

## Testing

### Run unit tests

```bash
python -m unittest discover -s tests -q
```

### Manual smoke testing

1. Invite the bot user to a 1:1 or group room
2. Send a message (1:1) or mention the bot (group room)
3. Verify bot responds in the same room with `replyTo` context
4. Verify gateway notices (restart/queued/compression) do **not** appear in the chat
5. React with ✅/❌ on a HITL prompt and verify approval flow

---

## Best Practices

- **Use app passwords**: Create a dedicated app password for the bot account instead of using the account password
- **Dedicated bot user**: Use a separate Nextcloud user (e.g., "hermes") for the bot
- **Custom status**: The plugin manages bot presence and custom status automatically; don't manually change it
- **Message history**: The plugin does not permanently store chat history; context is fetched on-demand when group rooms trigger
- **Attachments**: Downloaded to temp directory; cleanup is handled by the OS temp file mechanism
- **WebSocket**: If WebSocket is unavailable (firewall, network), the adapter automatically falls back to HTTP polling

---

## Architecture Notes

The plugin is structurally similar to the [Matrix platform plugin](https://github.com/NousResearch/hermes-agent/tree/main/plugins/platforms/matrix) but tailored for Nextcloud Talk:

- **Transport abstraction**: WebSocket (HPB signaling) vs. polling handled transparently, with polling as safety-net alongside WebSocket
- **Identity mapping**: Sender user ID passed to Hermes for context-aware tool execution
- **MCP integration**: User context flows to downstream Nextcloud MCP Server for access-controlled tool execution
- **Outbound filtering**: All gateway-internal noise is categorized and kept out of the chat (loop prevention)

For extended documentation, see [docs/nextcloud-talk.md](docs/nextcloud-talk.md).

## Version

- **Plugin version**: 0.2.1
- **Hermes compatibility**: v0.3.0+
- **Status**: Production-ready for Nextcloud 25+

## Troubleshooting

### Plugin shows "inactive"

Check config file permissions on server:
```bash
ls -la /etc/hermes/config.yaml
# Should be: hermes:hermes with mode 660 (rw-rw----)
```

If owned by root, run:
```bash
sudo chown hermes:hermes /etc/hermes/config.yaml
sudo chmod 660 /etc/hermes/config.yaml
```

### Transport errors in logs

Look for `fallback to polling` or `Connecting via HTTP polling` messages. This is normal if WebSocket is unavailable.

### Bot doesn't respond to mentions

1. Verify bot is in the room
2. Use exact handle: `@hermes` (check `NEXTCLOUD_BOT_HANDLE` env var)
3. Check Hermes logs for trigger gating entries
4. Ensure the room token is in `NEXTCLOUD_ALLOWED_ROOMS` if an allowlist is configured

### Bot spams the chat with internal messages

This indicates outbound categorization is not active. Verify `outbound.py` is present and `send_message` routes through `categorize_gateway_message()`. Gateway notices (restart, queued, compression) must never appear as chat messages.
