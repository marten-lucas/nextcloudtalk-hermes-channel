# Nextcloud Talk Plugin — Technical Documentation

## Overview

The Nextcloud Talk plugin integrates Hermes Agent with Nextcloud Talk using a custom platform adapter. It is installed as a standalone plugin via the Hermes plugin system.

**Version**: 0.1.23  
**Architecture**: Python async adapter using WebSocket + HTTP polling  
**Design goal**: Updatefat (no dependencies on Hermes core patches)

---

## Runtime Behavior

### Room Handling

The plugin implements context-aware message filtering:

| Room Type | Trigger Condition | Behavior |
| --- | --- | --- |
| **1:1 chat** | Every message | Process immediately |
| **2-participant room** | Every message | Process immediately |
| **Group room** (>2 participants) | Message contains bot mention | Fetch last N messages for context |

### Message Flow

```
User sends message in Nextcloud Talk
    ↓
Adapter detects via WebSocket or polling
    ↓
_categorize_gateway_message() → (category, details)
    ↓
Route by category:
    ├─ "lifecycle"  → Update presence + custom status
    ├─ "error"      → Format with 🚫 header + replyTo
    ├─ "suppress"   → Return silently (no message sent)
    └─ "forward"    → Send response as-is
    ↓
Response posted to same room with optional replyTo
```

### Context Handling

When a group room message triggers the bot:

1. Adapter loads last **N** recent messages from room (default: 20)
2. Formats as conversation history with sender names and timestamps
3. Passes full context to Hermes along with current message
4. Hermes processes in full conversational context
5. Response sent back to same room

**Configurable via:** `NEXTCLOUD_CONTEXT_MESSAGE_LIMIT`

### Replies

Responses use Nextcloud Talk's `replyTo` metadata:

- **Visual linking**: Response shown as a direct reply to the trigger message
- **Preserved across deletions**: Reply metadata remains valid even if trigger is deleted
- **Used by all categories**: Error messages, normal responses all link via `replyTo`

### Attachments

Incoming message attachments are detected and processed:

1. Metadata extracted from message payload
2. Files downloaded via WebDAV to temp directory
3. Paths passed to Hermes as attachment context
4. Hermes vision/multimodal pipeline processes (if applicable)
5. Temp files cleaned up by OS (configurable dir)

**Configurable via:** `NEXTCLOUD_ATTACHMENT_TMP_DIR`

### Message Categorization (v0.1.23)

The adapter categorizes all messages from Hermes gateway using pattern-based detection:

#### Category A: Lifecycle

**Patterns:**
- `"gateway restarting"` → State change to `offline`
- `"gateway online — hermes is back"` → State change to `online`
- `"draining"` + `"active"` + `"agent"` → State change to `draining`

**Handling:**
```python
if category == "lifecycle":
    await self._set_custom_status_message(status_text, emoji)
    await self._set_presence_status(state)
    # No chat message sent
```

**Why:** Gateway lifecycle events are administrative; they clutter chat and belong in bot presence.

#### Category B: Error

**Patterns:**
- Starts with `⚠️` emoji
- Contains: `"processing stopped"`, `"no response"`, `"session too large"`, `"auth failed"`, `"provider failed"`, `"tool failed"`, etc.

**Handling:**
```python
if category == "error":
    if reply_to_message_id:
        formatted = f"🚫 **Fehler**\n\n{error_text}"
        await self._ocs_post(..., {"message": formatted, "replyTo": reply_to_message_id})
    else:
        await self._set_custom_status_message("Fehler", "⚠️")
```

**Why:** Errors should link visually to the user's message; formatting with header + emoji improves UX.

#### Category C: Suppress

**Patterns:**
- `"gateway queued"`, `"compressing context"`, `"compression timed out"`, `"working —"`, `"subagent working"`, `"steer failed"`

**Handling:**
```python
if category == "suppress":
    return SendResult(success=True)  # Silent return
```

**Why:** Queue/progress messages are internal; they don't add value in chat and create noise. Suppress entirely.

#### Category D: Forward

**Patterns:** All other messages (default fallback)

**Handling:**
```python
else:  # "forward"
    await self._ocs_post(..., {"message": text, "replyTo": reply_to_message_id})
```

**Why:** Safe default; any unknown message type is sent as-is. Never loses information.

### Identify & Personalization

The plugin extracts sender user ID and propagates it downstream:

```
Message received → Extract sender_user_id
    ↓
Pass to Hermes via message context
    ↓
Hermes passes to MCP Server
    ↓
MCP Server executes tools in context of that specific user
```

**Example:** User "alice@example.org" says "Check my emails"  
→ MCP Server fetches Alice's emails (not Bob's)  
→ Response personalized to Alice

### Human-in-the-Loop (HITL)

Tool execution approvals use message reactions:

**Workflow:**
1. Bot sends message with pending tool execution
2. Bot waits for user reaction on that message
3. User reacts: `✅` (approve) or `❌` (reject)
4. Adapter detects reaction → resolves HITL confirmation
5. Tool execution proceeds or is canceled

**Approval emoji:**
- ✅ Checkmark
- 👍 Thumbs up

**Rejection emoji:**
- ❌ Cross
- 👎 Thumbs down

**Permissions (if `NEXTCLOUD_HITL_REQUIRE_REQUESTER=true`):**
- Only the original message sender can approve/reject
- Reactions from others are ignored (safety feature)

---

## Transport

The plugin implements dual-mode transport for reliability:

### WebSocket (Primary)

1. Tries to connect to Nextcloud High-Performance Backend
2. Listens for real-time `spreed_room_message` events
3. Processes events immediately (low latency)
4. Connection managed with automatic reconnect on failure

**Advantages:** Instant message delivery, bidirectional, efficient

### HTTP Polling (Fallback)

If WebSocket fails or is not available:

1. Falls back to polling the Nextcloud OCS Spreed API
2. Periodically calls `GET /ocs/v2.php/apps/spreed/api/v1/chat/{roomId}`
3. Compares message list with previous fetch to detect new messages
4. Processes deltas as if received via WebSocket

**Poll interval:** Configurable via `NEXTCLOUD_POLL_INTERVAL_SECONDS` (default: 3 seconds)

**Fallback behavior:** Automatic, transparent (no user intervention needed)

### Error Handling & Retry

- Connection failures trigger exponential backoff
- Transient errors (network hiccup) retried automatically
- Persistent failures logged and surfaced (e.g., auth failure)
- Plugin status shows as "connected" or "error" in Hermes

---

## Configuration Reference

### Required

| Variable | Meaning | Example |
| --- | --- | --- |
| `NEXTCLOUD_BASE_URL` | Nextcloud base URL | `https://cloud.example.org` |
| `NEXTCLOUD_USERNAME` | Bot account username | `hermes` |
| `NEXTCLOUD_APP_PASSWORD` | Bot app password (not account password) | `xxxx-yyyy-zzzz-wwww` |

### Optional (with defaults)

| Variable | Default | Meaning |
| --- | --- | --- |
| `NEXTCLOUD_BOT_HANDLE` | `@{NEXTCLOUD_USERNAME}` | Mention handle in group rooms |
| `NEXTCLOUD_REQUIRE_MENTION_IN_GROUPS` | `true` | Require mention in rooms >2 participants |
| `NEXTCLOUD_CONTEXT_MESSAGE_LIMIT` | `20` | Recent messages to fetch on group trigger |
| `NEXTCLOUD_POLL_INTERVAL_SECONDS` | `3` | Polling interval (seconds) when WS unavailable |
| `NEXTCLOUD_HITL_REQUIRE_REQUESTER` | `true` | Only original sender can approve/reject |

### Optional (access control)

| Variable | Meaning |
| --- | --- |
| `NEXTCLOUD_ALLOWED_USERS` | Comma-separated user IDs allowed to trigger bot (allowlist) |
| `NEXTCLOUD_ALLOW_ALL_USERS` | `true` to allow all users (dev/testing only) |
| `NEXTCLOUD_ALLOWED_ROOMS` | Comma-separated room IDs allowed to trigger bot (allowlist) |

### Optional (storage & delivery)

| Variable | Meaning |
| --- | --- |
| `NEXTCLOUD_ATTACHMENT_TMP_DIR` | Directory for temporary attachment downloads (default: `/tmp/nc_hermes`) |
| `NEXTCLOUD_HOME_CHANNEL` | Default room ID for cron/scheduled message delivery |
| `NEXTCLOUD_HOME_CHANNEL_NAME` | Display name for home channel |

---

## Installation

### Via Hermes Dashboard

1. Navigate to **Settings** → **Plugins**
2. Click **Add Plugin**
3. Enter plugin path (e.g., `/home/user/.hermes/plugins/nextcloud-talk/`)
4. Fill in required environment variables:
   - `NEXTCLOUD_BASE_URL`
   - `NEXTCLOUD_USERNAME`
   - `NEXTCLOUD_APP_PASSWORD`
5. Click **Save & Activate**
6. Restart Hermes or reload plugins

### Manual Installation

```bash
# Clone or copy plugin
git clone <repo> ~/.hermes/plugins/nextcloud-talk/

# Set environment variables (in .env or shell)
export NEXTCLOUD_BASE_URL="https://cloud.example.org"
export NEXTCLOUD_USERNAME="hermes"
export NEXTCLOUD_APP_PASSWORD="xxxx-yyyy-zzzz-wwww"

# Restart Hermes
hermes restart
# OR reload plugins
hermes plugins reload
```

---

## Verification

### Check Logs

```bash
# Monitor live logs
hermes logs -f

# Look for:
# ✓ nextcloud connected
# ✓ Using WebSocket|polling transport
```

### Smoke Test

1. Invite bot user to a 1:1 room
2. Send a test message
3. Verify bot responds in same room
4. Check response has `replyTo` linking to your message

### Contract Tests

```bash
python -m unittest -q tests.platforms.nextcloud.test_adapter_contracts
python -m unittest discover -q
```

---

## Design Decisions

### Why pattern-based categorization?

- **Resilient to wording changes**: If Hermes message text changes slightly, pattern still matches
- **No i18n dependencies**: Works across Hermes versions; no need to patch translation keys
- **Updatefat**: Plugin survives Hermes updates without manual rework
- **Extensible**: Add new pattern → handled automatically

### Why on-demand context?

- **Privacy**: Hermes doesn't retain chat history by default; loaded only when needed
- **Scalability**: No memory overhead for inactive rooms
- **Consent**: Context only loaded when user explicitly triggers bot

### Why custom status for lifecycle events?

- **Visibility without chat pollution**: Lifecycle changes visible in Nextcloud presence without clogging chat
- **Platform convention**: Presence status is standard way to signal bot availability
- **Non-intrusive**: Users can see bot state without scrolling through messages

---

## Troubleshooting

### "Provider authentication failed"

**Cause:** Nextcloud credentials invalid or Nextcloud unreachable

**Fix:**
```bash
# Verify credentials
curl -u hermes:xxxx-yyyy-zzzz-wwww https://cloud.example.org/ocs/v2.php/apps/spreed/api/v1/chat?format=json

# Check env vars
echo $NEXTCLOUD_BASE_URL
echo $NEXTCLOUD_USERNAME
```

### "Connection refused" / "WebSocket not available"

**Cause:** Nextcloud High-Performance Backend not enabled or firewall blocking WebSocket

**Fix:** Plugin automatically falls back to polling. This is normal and expected.

### Bot shows "inactive"

**Cause:** Config file permission issue (on server deployments)

**Fix:**
```bash
sudo chown hermes:hermes /etc/hermes/config.yaml
sudo chmod 660 /etc/hermes/config.yaml
```

On production servers, a systemd timer can enforce permissions automatically. See deployment guide.

### Bot doesn't respond to mentions

**Check:**
1. Bot is in the room
2. Exact mention handle matches `NEXTCLOUD_BOT_HANDLE` (default: `@hermes`)
3. Room has >2 participants AND message includes bot mention
4. Sender user ID not blocklisted in `NEXTCLOUD_ALLOWED_USERS`
5. Room not blocklisted in `NEXTCLOUD_ALLOWED_ROOMS`

**Example working mention:** `@hermes, what is 2+2?` (in group room)

### Attachment downloads failing

**Cause:** Nextcloud WebDAV endpoint unreachable or temp dir not writable

**Fix:**
```bash
# Verify temp dir exists and is writable
mkdir -p /tmp/nc_hermes
chmod 770 /tmp/nc_hermes

# Or set custom dir
export NEXTCLOUD_ATTACHMENT_TMP_DIR="/var/tmp/nc_hermes"
```

---

## Performance Notes

- **Message latency**: <100ms via WebSocket, <3s via polling (default interval)
- **Context fetch**: ~200-500ms for last 20 messages (network dependent)
- **Memory**: Minimal; context loaded on-demand and released after processing
- **Storage**: No persistent chat history; attachments cleaned up by OS temp mechanism

---

## Security Considerations

- **App Password**: Always use Nextcloud app password, not account password
- **Dedicated bot user**: Use separate Nextcloud account for bot
- **HITL requester checking**: Only original sender can approve tool execution (if enabled)
- **No audit logging**: Plugin does not log chat contents; pass-through only
- **Identity propagation**: Sender user ID included in Hermes context (needed for personalized tool execution)

---

## Notes

- The plugin is standalone and does not modify Hermes core.
- Transport endpoints are abstracted in `adapter.py` for easy tuning.
- Message categorization uses case-insensitive substring matching for robustness.
- Attach metadata via Hermes message objects; Nextcloud Talk payload updated accordingly.
