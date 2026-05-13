# hermes-tlon — Session Handoff

> Paste the "Kickstart Prompt" section at the bottom into a new Hermes/Devin session
> to continue development with full context.

---

## Current State (as of May 2026)

The plugin is **live and working for 1-on-1 DMs**. Gateway connects on startup,
messages route to the AI agent, replies go back. Tested against ship
`~tichul-tiprum-sigmes-modfyn` at `http://104.219.236.151:8080`.

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

### Group DMs (clubs, `0v…` IDs) — NOT YET IMPLEMENTED
Same `/v4` subscription delivers club events as `chat-club-action-2` mark.
Send poke uses:
```
app:  chat
mark: chat-club-action-2
json: { "id": "0v...", "diff": { "uid": "0v4", "delta": { "writ": { "id": ..., "delta": ... } } } }
```
See `tlon-apps/packages/api/src/urbit/dms.ts` → `multiDmAction()`.

### Group channels (Tlon groups, `chat/~host/name` IDs) — NOT YET IMPLEMENTED
These use the `%channels` agent (not `%chat`) with `channels-action` mark.
Subscribe: `app=channels, path=/v1` (delivers `channel-response` events).
Source: `tlon-apps/packages/api/src/client/channelsApi.ts`.

### Group channel send (P3 reference, from PR #1043)
When implementing group channel send (P3), use `channel-action` poke (not
`channels-action` — one important difference from the subscription mark):
```
app:  channels
mark: channel-action
json: {
  "nest": "chat/~host/channel-name",
  "action": {
    "post": {
      "add": {
        "essay": { ...same essay shape as DM... },
        "revision": 0
      }
    }
  }
}
```
PR #1043 source: `gateway/platforms/tlon.py` →
`https://github.com/wca4a/hermes-agent/blob/feature/tlon-adapter/gateway/platforms/tlon.py`

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
  desk/app/chat.hoon             — agent, mark list, subscription paths
  desk/lib/chat-json.hoon        — JSON encoder/decoder for all chat types
  desk/mar/chat/dm/action.hoon   — mark file: uses chat-dm-action v3 dejs
  packages/api/src/urbit/dms.ts  — TypeScript DmAction, WritDiff types
  packages/api/src/client/postsApi.ts  — chatAction(), sendPost() — exact send format
  packages/api/src/client/chatApi.ts   — subscribeToChatUpdates(), path "/v4"
  packages/api/src/client/apiUtils.ts  — formatUd(), da.fromUnix() usage
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
| Allowlist / allow-all / owner | ✅ (env vars) |
| TLON_HOME_CHANNEL for cron | ✅ (declared, untested) |
| Story → plain text | ✅ |
| Plain text → story (markdown) | ✅ |
| Thread replies (send) | ✅ (untested against live ship) |
| `%quit` resubscription | ✅ |
| ACK-every-event (threshold=1) | ✅ |

---

## Known Issues / Gotchas

1. **`ownership: ~sigmes-modfyn` in config.yaml is dead** — not read by adapter or gateway.
   To pre-authorize a ship without pairing, set `TLON_ALLOWED_USERS=~ship` in `.env`.

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
- [ ] **Pre-authorize owner ship** — read `tlon.ownership` from config and skip pairing

### P2 — Needed for group use
- [ ] **Group DMs (clubs)** — `0v…` chat IDs, `chat-club-action-2` poke
  - Inbound: already arrives on `/v4` as `ClubAction` events (need to parse)
  - Outbound: `multiDmAction()` format from `dms.ts`
- [ ] **`TLON_HOME_CHANNEL` wired end-to-end** — test cron delivery to a Tlon channel

### P3 — Full feature parity
- [ ] **Group channels** — `%channels` agent, `channels-action` mark, `/v1` subscription
  - Separate subscription from DMs (different agent entirely)
  - Chat ID format: `chat/~host/channel-name`
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

Current state: DMs work end-to-end (send + receive + gateway routing to AI).
  All P1 bugs FIXED: `%quit` resubscription; `_ACK_THRESHOLD=1`; markdown → story
  with correct `"italics"` key (bug found by live test — see Known Issue #6).
Live test ship: ~tichul-tiprum-sigmes-modfyn at http://104.219.236.151:8080

Read HANDOFF.md fully before touching any code. It contains the exact Urbit API 
formats, the hard-won ID formula, the prioritized gap list, and the Testing section.

⚠️  BEFORE writing any new code, ask the user to run the P1 test checkpoint
    from the Testing section (Tests 1–4). Tests 1-2 have passed. Test 3 (`%quit`
    resubscription) and Test 4 (soak) are not yet verified.

Today's goals (pick from P1/P2 in HANDOFF.md — all P1 items are done):

1. Pre-authorize owner ship from config (skip pairing for tlon.ownership) [P1 remaining]
2. Group DMs (clubs) — inbound parsing + outbound send [P2]
3. Wire TLON_HOME_CHANNEL end-to-end and test cron delivery [P2]
4. Group channels — subscribe to %channels agent, send via channel-action [P3]

The reference source for all Tlon API shapes is:
  https://github.com/tloncorp/tlon-apps (branch: develop)
  Key files: packages/api/src/client/postsApi.ts, chatApi.ts, apiUtils.ts
             desk/app/chat.hoon, desk/lib/chat-json.hoon

Hermes plugin base class reference:
  ~/.hermes/hermes-agent/gateway/platforms/base.py
  ~/.hermes/hermes-agent/plugins/platforms/irc/adapter.py (good pattern example)
```
