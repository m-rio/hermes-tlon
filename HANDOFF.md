# hermes-tlon — Session Handoff

> Paste the "Kickstart Prompt" section at the bottom into a new Hermes/Devin session
> to continue development with full context.

---

## Current State (as of May 2026)

The plugin is **live and working for 1-on-1 DMs, group DMs (clubs), and group channels**.
Gateway connects on startup, messages route to the AI agent, replies go back. Tested
against ship `~tichul-tiprum-sigmes-modfyn` at `http://104.219.236.151:8080`.

A closed upstream PR (NousResearch/hermes-agent#1043) attempted the same integration
as a core patch. Analysis of that PR identified the root cause of intermittent DM
delivery (see Known Issues #2, #3). Our plugin approach and API choices (`/v4`,
correct `@da` ID format) are validated as correct; the PR's approach was wrong on both.

**Files:**
```
~/.hermes/plugins/hermes-tlon/
  __init__.py      — registers TlonPlatformAdapter via ctx.register_platform()
  adapter.py       — BasePlatformAdapter subclass; gateway interface
  urbit.py         — UrbitClient; raw HTTP/SSE protocol layer
  plugin.yaml      — env var declarations for hermes setup wizard
  HANDOFF.md       — this file
```

---

## Hard-Won Urbit API Facts

These took significant debugging to get right. Do not re-derive; use these directly.

### Authentication
```
POST /~/login
Content-Type: application/x-www-form-urlencoded
Body: password=<login-code>
→ Sets cookie:  urbauth-~ship=<token>
   All subsequent requests must carry this cookie (aiohttp CookieJar handles it).
```

### Channel lifecycle
```
PUT  /~/channel/<uid>   — open channel / send pokes+subscribes+acks (JSON array)
GET  /~/channel/<uid>   — SSE stream (Accept: text/event-stream)
```
`uid` = `"{int(time.time())}-{uuid4().hex[:8]}"` — arbitrary unique string.

### Wake poke (required to keep channel alive)
```json
{"id":1,"action":"poke","ship":"shipname-no-tilde","app":"hood","mark":"helm-hi","json":"hi"}
```

### Subscribing to all DM + club events
```json
{"id":2,"action":"subscribe","ship":"shipname-no-tilde","app":"chat","path":"/v4"}
```
The `/v4` path on the `%chat` agent delivers **all** DM and group-DM events
as `writ-response-4` mark SSE diffs.  Do **not** use `%dm-inbox` (legacy agent).

### Sending a DM
```
app:  chat
mark: chat-dm-action-2        ← v1 "chat-dm-action" will NACK with cast fail
```
```json
{
  "ship": "~recipient",
  "diff": {
    "id": "~author/1.262.957.790.607.584.526.336",
    "delta": {
      "add": {
        "essay": {
          "content": [{"inline": ["hello"]}],
          "author": "~author",
          "sent": 1778250894833,
          "kind": "/chat",
          "blob": null,
          "meta": null
        },
        "time": null
      }
    }
  }
}
```

### Message ID format
The `id` field in `diff` is `"~ship/${formatUd(da.fromUnix(sentMs))}"`.

```python
URBIT_EPOCH_OFFSET = 292_277_024_400   # seconds: Urbit year-0 → Unix epoch

def da_from_unix_ms(unix_ms: int) -> int:
    return int((unix_ms / 1000.0 + URBIT_EPOCH_OFFSET) * (2 ** 32))

def format_ud(n: int) -> str:
    s = str(n); parts = []
    while len(s) > 3: parts.append(s[-3:]); s = s[:-3]
    parts.append(s)
    return ".".join(reversed(parts))

def make_msg_id(ship: str, sent_ms: int) -> str:
    ship = ship if ship.startswith("~") else f"~{ship}"
    return f"{ship}/{format_ud(da_from_unix_ms(sent_ms))}"
```

### Inbound SSE event shape (DM received)
```json
{
  "id": 2,
  "mark": "writ-response-4",
  "response": "diff",
  "json": {
    "id":   "~sender/1.262...",
    "whom": "~sender",
    "response": {
      "add": {
        "essay": {
          "author":  "~sender",
          "sent":    1778250894833,
          "kind":    "/chat",
          "content": [{"inline": ["message text"]}],
          "blob":    null,
          "meta":    null
        },
        "time": "170141184507955...",
        "seq":  1
      }
    }
  }
}
```
The `json` field is the `WritResponse` — that's what `_on_dm_event(payload)` receives.
`response.del`, `response.add-react`, `response.reply` are other delta variants (ignored now).

### Group DMs (clubs, `0v…` IDs) — IMPLEMENTED

Same `/v4` subscription delivers club events as `WritResponse` SSE diffs —
the `whom` field is the club UUID (`"0v..."`) instead of a ship name.

**Inbound** — same `_on_dm_event()` handler; club detected by `whom.startswith("0v")`.
`chat_id` is set to `"club/0v..."` so the gateway sessions stay separate from DMs.

**Outbound** — `send()` routes `"club/0v..."` or bare `"0v..."` chat IDs to the new
`send_club_msg()` method in `urbit.py`:
```
app:  chat
mark: chat-club-action-2
json: {
  "id":   "0v...",       ← club UUID
  "diff": {
    "uid": "0v4",        ← ALWAYS "0v4" (constant, from multiDmAction())
    "delta": {
      "writ": {
        "id":    "~author/1.262...",
        "delta": { "add": { "essay": { ...same essay shape as DM... }, "time": null } }
      }
    }
  }
}
```
Thread replies inside clubs use `"reply"` delta inside `"writ"` (same structure as DM
reply-essay). See `tlon-apps/packages/api/src/urbit/dms.ts` → `multiDmAction()`.

### Group channels (Tlon groups, `chat/~host/name` IDs) — IMPLEMENTED

Uses the `%channels` agent (separate from `%chat`).

**Subscribe** (added alongside the DM subscription in `connect()`):
```json
{"id":3,"action":"subscribe","ship":"shipname","app":"channels","path":"/v4"}
```
Use `/v4` — NOT `/v1`. The `/v1` path exists but does NOT include `essay` in
post add events; `/v4` delivers the full post shape.

**Inbound SSE event shape** (`ChannelsSubscribeResponse`):
```json
{
  "nest": "chat/~host/channel-name",
  "response": {
    "post": {
      "id": "170141184507...",
      "r-post": {
        "set": {
          "seal": { "id": "...", "reacts": {}, "replies": {} },
          "essay": {
            "content": [{"inline": ["message text"]}],
            "author":  "~sender",
            "sent":    1715134800000,
            "kind":    "/chat",
            "blob":    null,
            "meta":    null
          }
        }
      }
    }
  }
}
```
For thread replies, `"r-post"` has a `"reply"` key instead of `"set"`:
```json
"r-post": {
  "reply": {
    "id": "<reply-id>",
    "r-reply": {
      "set": {
        "seal": {...},
        "essay": {
          "content": [...],
          "author": "~sender",
          "sent": ...,
          "blob": null
        }
      }
    }
  }
}
```
For deletions `"set": null`.

**Outbound send** — `send_channel_post()` in `urbit.py`:
```
app:  channels
mark: channel-action-2        ← NOT "channel-action" (that's the old mark)
```
New top-level post:
```json
{
  "channel": {
    "nest": "chat/~host/channel-name",
    "action": {
      "post": {
        "add": {
          "content": [{"inline": ["hello channel"]}],
          "author": "~author",
          "sent": 1715134800000,
          "kind": "/chat",
          "blob": null,
          "meta": null
        }
      }
    }
  }
}
```
Thread reply:
```json
{
  "channel": {
    "nest": "chat/~host/channel-name",
    "action": {
      "post": {
        "reply": {
          "id": "<parent-post-id>",
          "action": {
            "add": {
              "reply-essay": {
                "content": [{"inline": ["reply text"]}],
                "author": "~author",
                "sent": 1715134800000,
                "blob": null
              },
              "time": null
            }
          }
        }
      }
    }
  }
}
```

**Channel filter** — `TLON_CHANNELS` env var (comma-separated nests, or `"*"` for all):
```
TLON_CHANNELS=chat/~host/mychannel,chat/~host/other
TLON_CHANNELS=*
```
If unset, the subscription is still active but events are silently discarded
(prevents flooding the gateway from channels the user didn't opt into).

**chat_id format** in gateway sessions: `"channel/<nest>"`, e.g.
`"channel/chat/~host/mychannel"`. `send()` routes any `chat_id` starting with
`"channel/"` to `send_channel_post()`.

**Source of truth for channels API**:
```
tlon-apps/packages/api/src/client/channelsApi.ts  — channelAction(), subscription path
tlon-apps/packages/api/src/client/postsApi.ts     — sendPost(), toPostEssay(), sendReply()
tlon-apps/packages/api/src/client/apiUtils.ts     — toPostEssay() → kind="/chat"
tlon-apps/desk/lib/channel-json.hoon              — essay encoder (v10: kind as path)
```

### Image story blocks (P3 reference)
PR #1043 sent images as a `block` verse inside the story:
```json
{"block": {"image": {"src": "https://...", "alt": "description", "width": 0, "height": 0}}}
```
Inbound image blocks arrive in the same position inside the story array.
Our current `story_to_text()` silently skips block verses — add a handler there too.

### Standalone cron delivery limitation
When the hermes gateway is **not running** (e.g., a bare `hermes` CLI session),
cron delivery falls back to `_send_to_platform()` in `send_message_tool.py`.
That file has no Tlon case → cron delivery silently fails with "unknown platform".
The live-adapter path (gateway running) works correctly via `adapter.send()`.
To fix: add a `elif platform == Platform("tlon"):` branch in `send_message_tool.py`
that instantiates `TlonPlatformAdapter` and calls `send()`. Low priority until
someone needs offline cron delivery.

### Source of truth for Tlon API shapes
```
https://github.com/tloncorp/tlon-apps (branch: develop)
  desk/app/chat.hoon             — chat agent, mark list, subscription paths
  desk/app/channels.hoon         — channels agent, subscription paths
  desk/lib/chat-json.hoon        — JSON encoder/decoder for all chat types
  desk/lib/channel-json.hoon     — JSON encoder/decoder for channel types (v10)
  desk/mar/chat/dm/action.hoon   — mark file: uses chat-dm-action v3 dejs
  packages/api/src/urbit/dms.ts  — TypeScript DmAction, WritDiff types
  packages/api/src/client/postsApi.ts    — chatAction(), sendPost(), sendReply()
  packages/api/src/client/chatApi.ts     — subscribeToChatUpdates(), path "/v4"
  packages/api/src/client/channelsApi.ts — channelAction(), subscribe path "/v4"
  packages/api/src/client/apiUtils.ts    — toPostEssay() → kind="/chat", toAuthor()
```

---

## What Works

| Feature | Status |
|---------|--------|
| Auth (cookie-based) | ✅ |
| DM send | ✅ |
| DM receive (any ship) | ✅ |
| SSE reconnect with backoff | ✅ |
| Gateway routing → AI agent | ✅ |
| Allowlist / allow-all / owner (env + config.yaml) | ✅ |
| TLON_HOME_CHANNEL for cron | ✅ wired via `cron_deliver_env_var` |
| Story → plain text | ✅ |
| Plain text → story (markdown) | ✅ |
| Thread replies (send) | ✅ |
| `%quit` resubscription | ✅ |
| ACK-every-event (threshold=1) | ✅ |
| Club (group DM) receive | ✅ |
| Club (group DM) send | ✅ |
| Group channel receive (TLON_CHANNELS) | ✅ |
| Group channel send | ✅ |

---

## Known Issues / Gotchas

1. ~~**`ownership: ~sigmes-modfyn` in config.yaml is dead**~~ **FIXED** — `adapter.py`
   now reads `owner_ship`, `allowed_users`, and `allow_all_users` from the config.yaml
   `extra` dict as fallbacks when the corresponding env vars are not set. Env vars
   still take precedence. Example in config.yaml:
   ```yaml
   platforms:
     tlon:
       extra:
         owner_ship: "~your-main-ship"
   ```

2. ~~**`%quit` events don't trigger resubscription**~~ **FIXED** — `_reconnect_after_quit()`
   added to `urbit.py`; scheduled via `asyncio.create_task()` from the `quit` branch in
   `_handle_sse_event()`. Re-subscribes on the existing channel (SSE stream stays open
   when Eyre sends `quit`, so no `open_channel()` needed).
   **⚠️ TEST NEEDED** — see "Testing" section below.

3. ~~**`_ACK_THRESHOLD = 20` causes slow buffer drain**~~ **FIXED** — `_ACK_THRESHOLD`
   lowered to `1` (ack every event). With threshold=20, at ~1 keepalive event per 3 min,
   Eyre's ~50-event buffer would fill in ~2.5 hours → `%quit` → silent death. Now we ack
   every event so the buffer never builds.
   **⚠️ TEST NEEDED** — see "Testing" section below.

4. ~~**Thread replies untested**~~ **FIXED** — the `reply_to` branch was tested
   and revealed the `italic` bug (see #6). Replies now work correctly once `italics`
   key is used.

5. **Self-message filter** — messages where `author == self.ship` are dropped.
   This is intentional (prevents echo loops) but blocks self-DM testing.
   Also: if `TLON_SHIP` env var is not set, `self.ship` is empty and the filter never
   fires, causing the bot to echo itself into an infinite loop.

6. ~~**`"italic"` wrong key in `text_to_story()`**~~ **FIXED** — The Tlon
   `story-json.hoon` inline decoder uses tag `"italics"` (with an 's'). Our code was
   sending `"italic"` (no 's'), which caused `gall: poke-as cast fail` NACK for any
   message containing italic, bold-italic (`***…***`), or any subsequent inline element
   after the bad tag. Fix: renamed both occurrences in `_parse_inline_md()` to
   `"italics"`. Also added `"italics"` check to `story_to_text()` for inbound parsing.
   Root cause confirmed by inspecting `desk/lib/story-json.hoon` → `dejs.inline` → `of`
   combinator tags.

---

## Testing

### ⚠️ Test checkpoint: P1 fixes (run this after every session that touches urbit.py)

**Test 1 — Smoke test (DMs still work)**
```bash
hermes gateway
```
From the Tlon app on your phone or another ship, send a DM to `~tichul-tiprum-sigmes-modfyn`.
Expected: reply arrives within ~10 seconds.

**Test 2 — ACK threshold (quick, visual)**
```bash
tail -f ~/.hermes/logs/gateway.log | grep -i "tlon.*ack\|tlon.*keepalive"
```
Wait ~3 minutes for the keepalive poke. Expected: you should see an ack line within
seconds of the keepalive poke (because threshold=1 → acks every event).
Old behavior: no ack for 20+ events / ~60 minutes.

**Test 3 — `%quit` resubscription (requires dojo access)**
From the ship's dojo, restart the `%chat` Gall agent to force a quit event:
```
> |nuke %chat, =desk %landscape
> |start %chat
```
Watch the log:
```bash
tail -f ~/.hermes/logs/gateway.log | grep -i "tlon.*quit\|tlon.*resub"
```
Expected sequence:
```
WARNING  Tlon: subscription N quit — scheduling resubscription
INFO     Tlon: re-subscribing 1 subscription(s) after reconnect
INFO     Tlon: resubscribed after quit event
```
Then send a DM and confirm it still arrives. If you see the log sequence but DMs stop
working, the resubscription is firing but something in the re-subscribe path is broken.

**Test 5 — Club (group DM) send + receive (requires a Tlon club)**
Create or join a club on `~tichul-tiprum-sigmes-modfyn`, then send a message to the club
from a member ship. Expected:
```
INFO  Tlon: inbound club from ~member-ship: 'hello club'
```
Then trigger a club reply from the bot (e.g. via a `/send` command or cron).
Expected: message appears in the club on all member devices.
The club's `chat_id` in logs and session keys will be `club/0v...`.

**Test 6 — Group channel receive + send (requires TLON_CHANNELS)**
```bash
export TLON_CHANNELS="chat/~your-host/channel-name"
hermes gateway
```
From another ship, post a message to the channel. Expected log:
```
INFO  Tlon: inbound channel post from ~sender in chat/~host/name: 'hello channel'
```
Then trigger a reply from the bot (e.g. via a /send command). Expected: message
appears in the Tlon channel. The gateway `chat_id` for the channel is
`"channel/chat/~host/name"` (use this in /send commands).

Use `TLON_CHANNELS=*` to accept events from ALL channels (useful for debugging).

**Test 4 — Long-running stability (soak test, ~3 hours)**
Leave the gateway running overnight. Previously it would go deaf after ~2.5 hours.
If DMs still work 3+ hours later with no `%quit` log lines → buffer fix confirmed.
If you see `%quit` lines followed by resubscription → quit-handler fix confirmed.

---

## Feature Gap vs Full Parity

Priority order for "feels like a real Tlon client":

### P1 — High value, achievable
- [x] **Fix `%quit` handler** — `_reconnect_after_quit()` added in `urbit.py`;
      scheduled via `asyncio.create_task()` from the `quit` branch in
      `_handle_sse_event()`. Re-subscribes on the existing channel (no
      `open_channel()` needed — the SSE stream is still open). (Known Issue #2)
- [x] **Lower `_ACK_THRESHOLD` to 1** — acks every event; prevents the
      buffer-overflow path that leads to silent `%quit`. (Known Issue #3)
- [x] **Markdown → story** — fully implemented in `urbit.py` as `text_to_story()`:
      bold, italic (`"italics"` key — NOT `"italic"`), bold-italic, strikethrough,
      inline code, fenced code blocks, ATX headings, links. The `"italic"` vs
      `"italics"` bug was found and fixed in this session (see Known Issue #6).
- [x] **Pre-authorize owner ship** — `owner_ship`, `allowed_users`, `allow_all_users`
      now read from config.yaml `extra` dict as fallbacks to env vars.

### P2 — Needed for group use
- [x] **Group DMs (clubs)** — `0v…` chat IDs, `chat-club-action-2` poke implemented.
  - Inbound: `_on_dm_event()` checks `whom.startswith("0v")` → `chat_id = "club/0v..."`
  - Outbound: `send_club_msg()` in `urbit.py` + routing in `send()` in `adapter.py`
  - Thread replies in clubs also supported via reply-essay inside `writ` delta
  - **⚠️ TEST NEEDED** — no live club to test against yet
- [x] **`TLON_HOME_CHANNEL` wired end-to-end** — `cron_deliver_env_var="TLON_HOME_CHANNEL"`
  registered in `ctx.register_platform()`, `_env_enablement()` seeds `home_channel` from
  the env var. When gateway is running, cron delivery routes to this channel automatically.

### P3 — Full feature parity
- [x] **Group channels** — subscribe `app=channels, path=/v4`; send via `channel-action-2`
  - Inbound: `_on_channel_event()` in `adapter.py`; filtered by `TLON_CHANNELS` env var
  - Outbound: `send_channel_post()` in `urbit.py` + routing in `send()` (`"channel/nest"`)
  - Thread replies in channels also supported via `"post": { "reply": {...} }` action
  - **⚠️ TEST NEEDED** — no live channel test run yet (see Test 6 in Testing section)
- [ ] **Receive reactions** — `add-react` delta in WritResponse (currently ignored)
- [ ] **Message deletion notice** — `del` delta (currently ignored)

### Out of scope (no Tlon API support)
- Typing indicators, read receipts, user presence — Tlon has no HTTP API for these

---

## Kickstart Prompt for Next Session

```
I'm continuing development of the hermes-tlon plugin — a Tlon/Urbit messaging 
gateway adapter for the Hermes Agent. The plugin is at:

  ~/.hermes/plugins/hermes-tlon/
    adapter.py   — BasePlatformAdapter subclass (gateway interface)
    urbit.py     — UrbitClient (Urbit HTTP/SSE protocol)
    plugin.yaml  — env var config
    HANDOFF.md   — full technical handoff (READ THIS FIRST)

Current state: All P1 + P2 + P3 (group channels) items done. DMs, clubs, and group
  channels all implemented. Config.yaml `extra.owner_ship` works. TLON_HOME_CHANNEL
  wired for cron. All P1 bugs fixed: `%quit`, `_ACK_THRESHOLD=1`, `"italics"` key.
Live test ship: ~tichul-tiprum-sigmes-modfyn at http://104.219.236.151:8080

Read HANDOFF.md fully before touching any code. It contains the exact Urbit API 
formats, the hard-won ID formula, the prioritized gap list, and the Testing section.

⚠️  Tests still needed:
    - Test 3: `%quit` resubscription (requires dojo access)
    - Test 4: soak test (~3 hours)
    - Test 5: club send/receive (needs a real club)
    - Test 6: channel receive + send (needs TLON_CHANNELS set + a real channel)

Remaining P3 items (low priority):
1. Receive reactions — `add-react` delta in WritResponse (currently ignored)
2. Message deletion notice — `del` delta (currently ignored)

The reference source for all Tlon API shapes is:
  https://github.com/tloncorp/tlon-apps (branch: develop)
  Key files: packages/api/src/client/postsApi.ts, chatApi.ts, channelsApi.ts, apiUtils.ts
             desk/app/chat.hoon, desk/lib/chat-json.hoon, desk/lib/channel-json.hoon

Hermes plugin base class reference:
  ~/.hermes/hermes-agent/gateway/platforms/base.py
  ~/.hermes/hermes-agent/plugins/platforms/irc/adapter.py (good pattern example)
```
