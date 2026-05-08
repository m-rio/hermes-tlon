# hermes-tlon — Session Handoff

> Paste the "Kickstart Prompt" section at the bottom into a new Hermes/Devin session
> to continue development with full context.

---

## Current State (as of May 2026)

The plugin is **live and working for 1-on-1 DMs**. Gateway connects on startup,
messages route to the AI agent, replies go back. Tested against ship
`~tichul-tiprum-sigmes-modfyn` at `http://104.219.236.151:8080`.

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
| Plain text → story | ✅ |
| Thread replies (send) | ✅ (untested against live ship) |

---

## Known Issues / Gotchas

1. **`ownership: ~sigmes-modfyn` in config.yaml is dead** — not read by adapter or gateway.
   To pre-authorize a ship without pairing, set `TLON_ALLOWED_USERS=~ship` in `.env`.

2. **`_resubscribe_all()` is a no-op** — after a network disconnect the SSE reconnects
   and re-opens a new channel, but subscriptions are not re-sent. The adapter calls
   `open_channel()` + `_resubscribe_all()` on reconnect but the latter does nothing.
   Fix: store `(app, path, handler)` tuples and re-issue them in `_resubscribe_all()`.

3. **Thread replies untested** — the `reply_to` branch in `send_dm()` builds a
   `ReplyDelta` but has never been tested against a real ship.

4. **Self-message filter** — messages where `author == self.ship` are dropped.
   This is intentional (prevents echo loops) but blocks self-DM testing.

---

## Feature Gap vs Full Parity

Priority order for "feels like a real Tlon client":

### P1 — High value, achievable
- [ ] **Fix reconnect re-subscribe** — silent go-deaf after network blip
- [ ] **Markdown → story** — bold, italic, inline code, links in outbound messages
  - Tlon story format: `{"bold": [...inlines]}`, `{"italic": [...]}`, `{"inline-code": "text"}`
  - Simple regex/markdown parser → story verses is sufficient
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
Live test ship: ~tichul-tiprum-sigmes-modfyn at http://104.219.236.151:8080

Read HANDOFF.md fully before touching any code. It contains the exact Urbit API 
formats, the hard-won ID formula, and the prioritized gap list.

Today's goals (pick from P1/P2 in HANDOFF.md):

1. Fix reconnect re-subscribe (silent failure after network blip)
2. Add markdown → Tlon story conversion for outbound messages
3. Pre-authorize owner ship from config (skip pairing for tlon.ownership)
4. Group DMs (clubs) — inbound parsing + outbound send

The reference source for all Tlon API shapes is:
  https://github.com/tloncorp/tlon-apps (branch: develop)
  Key files: packages/api/src/client/postsApi.ts, chatApi.ts, apiUtils.ts
             desk/app/chat.hoon, desk/lib/chat-json.hoon

Hermes plugin base class reference:
  ~/.hermes/hermes-agent/gateway/platforms/base.py
  ~/.hermes/hermes-agent/plugins/platforms/irc/adapter.py (good pattern example)
```
