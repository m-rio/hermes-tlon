"""
Urbit HTTP channel client for hermes-tlon.

Implements the Urbit ship HTTP API used by Tlon:
  - Auth:       POST /~/login  (form-encoded password → Set-Cookie)
  - Channel:    PUT  /~/channel/<uid>  (open, poke, subscribe, ack)
  - SSE stream: GET  /~/channel/<uid>  (EventSource)

DM scope only (Phase 2).  Group channel support is Phase 3.

References:
  - openclaw-tlon src/urbit/sse-client.ts  (SSE lifecycle + ack logic)
  - openclaw-tlon src/urbit/channel-ops.ts  (poke/subscribe/ack payloads)
  - openclaw-tlon src/urbit/auth.ts          (login endpoint)
  - openclaw-tlon src/urbit/send.ts          (DM story + sendDm)
"""

import asyncio
import json
import logging
import time
import uuid
from typing import AsyncIterator, Callable, Dict, List, Optional

import aiohttp

logger = logging.getLogger("hermes.tlon.urbit")

# ---------------------------------------------------------------------------
# Urbit @da / @ud helpers
# ---------------------------------------------------------------------------

# Seconds between Urbit year-0 epoch and Unix epoch
_URBIT_EPOCH_OFFSET_SECS = 292_277_024_400


def da_from_unix_ms(unix_ms: int) -> int:
    """Convert a Unix millisecond timestamp to an Urbit @da atom.

    Urbit @da = (unix_seconds + EPOCH_OFFSET) * 2^32
    The low 32 bits represent sub-second precision; for whole-millisecond
    inputs the result is simply ((ms / 1000) + OFFSET) << 32.
    """
    da_secs = unix_ms / 1000.0 + _URBIT_EPOCH_OFFSET_SECS
    return int(da_secs * (2 ** 32))


def format_ud(n: int) -> str:
    """Format an integer as Urbit @ud notation (dot-separated thousands).

    e.g. 1234567 → "1.234.567"
    """
    s = str(n)
    parts: list[str] = []
    while len(s) > 3:
        parts.append(s[-3:])
        s = s[:-3]
    parts.append(s)
    return ".".join(reversed(parts))


def make_msg_id(ship: str, sent_ms: int) -> str:
    """Return a Tlon DM message id: "~ship/ud-da".

    Matches the format produced by Tlon's postsApi.ts:
      `${authorId}/${formatUd(da.fromUnix(sentAt).toString())}`
    """
    ship_full = ship if ship.startswith("~") else f"~{ship}"
    return f"{ship_full}/{format_ud(da_from_unix_ms(sent_ms))}"


# ---------------------------------------------------------------------------
# Story helpers
# ---------------------------------------------------------------------------

def text_to_story(text: str) -> list:
    """Convert plain text to a minimal Tlon story (list of verse objects).

    A story is a list of verses; the simplest form is a single inline verse
    wrapping the text as a plain string inline element.

    Structure: [{"inline": ["text content"]}]
    """
    if not text:
        return [{"inline": [""]}]

    verses = []
    for line in text.split("\n"):
        if line:
            verses.append({"inline": [line]})
        else:
            # Empty line → blank inline (preserves paragraph breaks)
            verses.append({"inline": [""]})
    return verses


def story_to_text(story: list) -> str:
    """Extract plain text from a Tlon story (list of verse objects).

    Handles the common inline types.  Unrecognised structures are skipped
    silently so we don't crash on rich content.
    """
    parts: list[str] = []

    def _inline(node) -> str:
        if isinstance(node, str):
            return node
        if not isinstance(node, dict):
            return ""
        if "text" in node:
            return node["text"]
        if "bold" in node:
            return "".join(_inline(i) for i in node["bold"])
        if "italic" in node:
            return "".join(_inline(i) for i in node["italic"])
        if "strike" in node:
            return "".join(_inline(i) for i in node["strike"])
        if "code" in node:
            return node["code"]
        if "link" in node:
            link = node["link"]
            return link.get("content", link.get("href", ""))
        if "ship" in node:
            return node["ship"]
        if "tag" in node:
            return f"#{node['tag']}"
        if "break" in node:
            return "\n"
        return ""

    for verse in story:
        if not isinstance(verse, dict):
            continue
        if "inline" in verse:
            parts.append("".join(_inline(i) for i in verse["inline"]))
        # block verses (images, code blocks, etc.) are ignored for plain text

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# UrbitClient
# ---------------------------------------------------------------------------

class UrbitClient:
    """Async client for a single Urbit ship's HTTP API.

    Lifecycle::

        client = UrbitClient(ship_url, ship, login_code)
        await client.authenticate()
        await client.open_channel()          # creates channel + sends helm-hi
        sub_id = await client.subscribe("dm-inbox", "/updates", handler)
        await client.open_sse_stream()       # starts background reader task
        ...
        # To send a DM:
        await client.send_dm("~recipient", "Hello!")
        ...
        await client.close()
    """

    # Ack every N events to keep the SSE channel healthy (mirrors openclaw)
    _ACK_THRESHOLD = 20

    def __init__(self, ship_url: str, ship: str, login_code: str):
        self.ship_url = ship_url.rstrip("/")
        # ship: canonical form is with "~"; the poke envelope uses without "~"
        self.ship = ship.strip()
        self._ship_no_tilde = self.ship.lstrip("~")
        self.login_code = login_code.strip()

        # aiohttp session — carries the auth cookie automatically
        self._session: Optional[aiohttp.ClientSession] = None
        self._authenticated: bool = False

        # Channel
        self._channel_id: str = ""
        self._channel_url: str = ""

        # Subscription registry: sub_id → callback
        self._sub_handlers: Dict[int, Callable] = {}
        self._next_sub_id: int = 1

        # SSE reader task
        self._sse_task: Optional[asyncio.Task] = None
        self._sse_closed: bool = False

        # Event ack tracking
        self._last_heard_event_id: int = -1
        self._last_acked_event_id: int = -1

        # Reconnect state
        self._reconnect_delay: float = 1.0

    # ── Session / auth ────────────────────────────────────────────────────

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            jar = aiohttp.CookieJar(unsafe=True)
            self._session = aiohttp.ClientSession(cookie_jar=jar)
        return self._session

    async def authenticate(self) -> bool:
        """POST /~/login — store auth cookie in the session jar."""
        session = await self._ensure_session()
        url = f"{self.ship_url}/~/login"
        try:
            async with session.post(
                url,
                data={"password": self.login_code},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                await resp.text()  # drain — finalises Set-Cookie on some ships
                if resp.status not in (200, 204, 302):
                    logger.error("Tlon auth failed: HTTP %s at %s", resp.status, url)
                    return False
                import yarl
                cookies = session.cookie_jar.filter_cookies(yarl.URL(self.ship_url))
                if not cookies:
                    logger.error("Tlon auth: no cookie in response — check URL and code")
                    return False
                self._authenticated = True
                logger.debug("Tlon: authenticated, cookie set for %s", self.ship_url)
                return True
        except Exception as exc:
            logger.exception("Tlon: authentication error: %s", exc)
            return False

    async def _reauth(self) -> bool:
        """Re-authenticate after a session expiry."""
        logger.info("Tlon: re-authenticating...")
        self._authenticated = False
        return await self.authenticate()

    # ── Channel lifecycle ─────────────────────────────────────────────────

    def _new_channel_id(self) -> str:
        return f"{int(time.time())}-{uuid.uuid4().hex[:8]}"

    async def open_channel(self) -> None:
        """Create a new channel and send the helm-hi wake poke.

        Mirrors openclaw's ensureUrbitChannelOpen → createUrbitChannel +
        wakeUrbitChannel.
        """
        if not self._authenticated:
            raise RuntimeError("Not authenticated — call authenticate() first")

        self._channel_id = self._new_channel_id()
        self._channel_url = f"{self.ship_url}/~/channel/{self._channel_id}"

        # helm-hi wake poke
        wake = [{
            "id": self._next_id(),
            "action": "poke",
            "ship": self._ship_no_tilde,
            "app": "hood",
            "mark": "helm-hi",
            "json": "Opening hermes-tlon channel",
        }]
        await self._put_channel(wake, context="open_channel")
        logger.debug("Tlon: channel %s opened", self._channel_id)

    # ── Subscribe ─────────────────────────────────────────────────────────

    async def subscribe(
        self,
        app: str,
        path: str,
        on_event: Optional[Callable] = None,
        on_err: Optional[Callable] = None,
        on_quit: Optional[Callable] = None,
    ) -> int:
        """Subscribe to a Gall agent path and return the sub_id.

        on_event(json) is called for each matching SSE event.
        """
        sub_id = self._next_sub_id
        self._next_sub_id += 1

        if on_event is not None:
            self._sub_handlers[sub_id] = on_event

        payload = [{
            "id": sub_id,
            "action": "subscribe",
            "ship": self._ship_no_tilde,
            "app": app,
            "path": path,
        }]
        await self._put_channel(payload, context=f"subscribe {app}{path}")
        logger.debug("Tlon: subscribed to %s%s (sub_id=%d)", app, path, sub_id)
        return sub_id

    # ── Poke ─────────────────────────────────────────────────────────────

    async def poke(self, app: str, mark: str, poke_json: object) -> int:
        """Send a poke to a Gall agent and return the poke_id."""
        poke_id = self._next_id()
        payload = [{
            "id": poke_id,
            "action": "poke",
            "ship": self._ship_no_tilde,
            "app": app,
            "mark": mark,
            "json": poke_json,
        }]
        await self._put_channel(payload, context=f"poke {app} {mark}")
        logger.debug("Tlon: poke sent to %s/%s (id=%d)", app, mark, poke_id)
        return poke_id

    # ── Ack ──────────────────────────────────────────────────────────────

    async def ack(self, event_id: int) -> None:
        """Acknowledge an SSE event so the ship doesn't re-send it."""
        self._last_acked_event_id = event_id
        payload = [{
            "id": self._next_id(),
            "action": "ack",
            "event-id": event_id,
        }]
        try:
            await self._put_channel(payload, context="ack")
        except Exception as exc:
            logger.debug("Tlon: ack failed for event %d: %s", event_id, exc)

    # ── SSE stream ────────────────────────────────────────────────────────

    async def open_sse_stream(self) -> None:
        """Start the background SSE reader task."""
        if self._sse_task and not self._sse_task.done():
            return
        self._sse_closed = False
        self._sse_task = asyncio.create_task(self._sse_loop())

    async def _sse_loop(self) -> None:
        """Background task: reads the SSE stream and dispatches events."""
        while not self._sse_closed:
            try:
                await self._read_sse_stream()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("Tlon: SSE stream error: %s — reconnecting in %.1fs",
                               exc, self._reconnect_delay)
                if self._sse_closed:
                    break
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, 30.0)

                # Re-open channel + re-subscribe on reconnect
                try:
                    if not self._authenticated:
                        await self._reauth()
                    await self.open_channel()
                    await self._resubscribe_all()
                except Exception as reopen_exc:
                    logger.error("Tlon: reconnect failed: %s", reopen_exc)

        logger.info("Tlon: SSE loop exited")

    async def _read_sse_stream(self) -> None:
        """Open the GET SSE stream and read until disconnected."""
        session = await self._ensure_session()
        logger.info("Tlon: opening SSE stream on %s", self._channel_url)

        async with session.get(
            self._channel_url,
            headers={"Accept": "text/event-stream"},
            timeout=aiohttp.ClientTimeout(sock_read=None),  # stream — no read timeout
        ) as resp:
            if resp.status != 200:
                raise RuntimeError(f"SSE GET returned HTTP {resp.status}")

            self._reconnect_delay = 1.0  # reset on successful connection
            logger.info("Tlon: SSE stream connected")

            buf = ""
            async for chunk in resp.content.iter_chunked(4096):
                if self._sse_closed:
                    break
                buf += chunk.decode("utf-8", errors="replace")
                while "\n\n" in buf:
                    raw_event, buf = buf.split("\n\n", 1)
                    self._handle_sse_event(raw_event)

    def _handle_sse_event(self, raw: str) -> None:
        """Parse and dispatch a single SSE event block."""
        data: Optional[str] = None
        event_id: Optional[int] = None

        for line in raw.splitlines():
            if line.startswith("id: "):
                try:
                    event_id = int(line[4:])
                except ValueError:
                    pass
            elif line.startswith("data: "):
                data = line[6:]

        if data is None:
            return

        # Track event ID and ack periodically
        if event_id is not None and event_id > self._last_heard_event_id:
            self._last_heard_event_id = event_id
            if event_id - self._last_acked_event_id >= self._ACK_THRESHOLD:
                asyncio.create_task(self.ack(event_id))

        try:
            parsed = json.loads(data)
        except json.JSONDecodeError:
            logger.debug("Tlon: non-JSON SSE data: %s", data[:200])
            return

        resp_type = parsed.get("response")

        if resp_type == "poke":
            if parsed.get("err"):
                logger.error("Tlon: poke NACK id=%s: %s",
                             parsed.get("id"), parsed.get("err"))
            else:
                logger.debug("Tlon: poke ACK id=%s", parsed.get("id"))
            return

        if resp_type == "quit":
            sub_id = parsed.get("id")
            logger.warning("Tlon: subscription %s quit — will resubscribe", sub_id)
            return

        # Subscription event — route to registered handler
        sub_id = parsed.get("id")
        payload = parsed.get("json")
        if payload is None:
            return

        if sub_id in self._sub_handlers:
            try:
                handler = self._sub_handlers[sub_id]
                result = handler(payload)
                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception as exc:
                logger.exception("Tlon: error in sub %d handler: %s", sub_id, exc)
        else:
            # Broadcast to all handlers (shouldn't happen with proper sub tracking)
            for handler in self._sub_handlers.values():
                try:
                    result = handler(payload)
                    if asyncio.iscoroutine(result):
                        asyncio.create_task(result)
                except Exception as exc:
                    logger.exception("Tlon: error in broadcast handler: %s", exc)

    # ── Re-subscribe after reconnect ──────────────────────────────────────

    async def _resubscribe_all(self) -> None:
        """Re-send all active subscriptions after a channel reconnect."""
        # Subscriptions are stored per-id; we need the app+path to re-subscribe.
        # For simplicity in Phase 2 (DM only), the adapter re-calls subscribe()
        # by closing and reopening the adapter.  This is handled in the adapter's
        # _listen_loop reconnect logic.
        pass

    # ── High-level DM send ────────────────────────────────────────────────

    async def send_dm(
        self,
        to_ship: str,
        text: str,
        reply_to: Optional[str] = None,
    ) -> str:
        """Send a plain-text DM to another Urbit ship.

        to_ship: ship name with or without "~" (e.g. "~sampel-palnet")
        text:    plain text message content
        reply_to: optional message ID to thread-reply to

        Returns a message_id string.
        Raises on auth or poke failure.
        """
        if not self._authenticated:
            ok = await self._reauth()
            if not ok:
                raise RuntimeError("Tlon: authentication failed — cannot send DM")

        # Ensure to_ship has the tilde
        if not to_ship.startswith("~"):
            to_ship = f"~{to_ship}"

        sent_at = int(time.time() * 1000)  # ms since Unix epoch
        story = text_to_story(text)

        # Full essay as expected by %chat agent (matches Tlon postsApi.ts sendPost)
        essay = {
            "content": story,
            "author": self.ship,
            "sent": sent_at,
            "kind": "/chat",
            "blob": None,
            "meta": None,
        }

        # Message ID format: "~author/ud-da"  (Tlon: formatUd(da.fromUnix(sent)))
        msg_id = make_msg_id(self.ship, sent_at)

        if reply_to:
            # Thread reply (ReplyDelta)
            reply_id = make_msg_id(self.ship, sent_at)
            dm_action_json = {
                "ship": to_ship,
                "diff": {
                    "id": reply_to,
                    "delta": {
                        "reply": {
                            "id": reply_id,
                            "meta": None,
                            "delta": {
                                "add": {
                                    "reply-essay": {
                                        "content": story,
                                        "author": self.ship,
                                        "sent": sent_at,
                                        "blob": None,
                                    },
                                    "time": None,
                                }
                            },
                        }
                    },
                },
            }
        else:
            # New top-level DM (WritDeltaAdd) — matches Tlon chatAction() in postsApi.ts
            dm_action_json = {
                "ship": to_ship,
                "diff": {
                    "id": msg_id,
                    "delta": {
                        "add": {
                            "essay": essay,
                            "time": None,
                        }
                    },
                },
            }

        # Agent: %chat, mark: chat-dm-action-2 (v2 — used by current Tlon clients)
        await self.poke("chat", "chat-dm-action-2", dm_action_json)

        logger.info("Tlon: DM sent to %s (msg %s)", to_ship, msg_id)
        return msg_id

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def close(self) -> None:
        """Cancel the SSE task and close the HTTP session."""
        self._sse_closed = True
        if self._sse_task and not self._sse_task.done():
            self._sse_task.cancel()
            try:
                await self._sse_task
            except asyncio.CancelledError:
                pass
        if self._session and not self._session.closed:
            await self._session.close()
        self._authenticated = False
        logger.debug("Tlon: client closed")

    # ── Internal helpers ──────────────────────────────────────────────────

    _id_counter: int = 0

    def _next_id(self) -> int:
        UrbitClient._id_counter += 1
        return UrbitClient._id_counter

    async def _put_channel(self, payload: list, context: str = "") -> None:
        """PUT the channel with a JSON payload.  Re-auths once on 403."""
        session = await self._ensure_session()
        for attempt in range(2):
            try:
                async with session.put(
                    self._channel_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status in (200, 204):
                        return
                    if resp.status == 403 and attempt == 0:
                        logger.warning("Tlon: 403 on %s — re-authenticating", context)
                        await self._reauth()
                        continue
                    body = await resp.text()
                    raise RuntimeError(
                        f"Tlon: PUT channel failed ({context}): "
                        f"HTTP {resp.status} — {body[:200]}"
                    )
            except aiohttp.ClientError as exc:
                if attempt == 0:
                    logger.warning("Tlon: network error on %s: %s — retrying", context, exc)
                    await asyncio.sleep(1)
                    continue
                raise
