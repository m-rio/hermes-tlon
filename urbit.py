"""
Urbit HTTP channel client for hermes-tlon.

Implements the Urbit ship HTTP API used by Tlon:
  - Auth:       POST /~/login  (form-encoded password → Set-Cookie)
  - Channel:    PUT  /~/channel/<uid>  (open, poke, subscribe, ack)
  - SSE stream: GET  /~/channel/<uid>  (EventSource)

Supports DMs, group DMs (clubs), and group channels (Phase 3).

References:
  - openclaw-tlon src/urbit/sse-client.ts  (SSE lifecycle + ack logic)
  - openclaw-tlon src/urbit/channel-ops.ts  (poke/subscribe/ack payloads)
  - openclaw-tlon src/urbit/auth.ts          (login endpoint)
  - openclaw-tlon src/urbit/send.ts          (DM story + sendDm)
"""

import asyncio
import json
import logging
import os
import re
import time
import uuid
from typing import Any, AsyncIterator, Callable, Dict, List, Optional

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


def ensure_ud_format(id_val: Any) -> str:
    """Ensure a Tlon post ID is in dot-formatted @ud notation.

    channel-action-2 decodes ``id`` fields with ``(se %ud)``, which requires
    dot-separated thousands notation (e.g. "170.141.184.507.966.284.013…").
    Inbound SSE events may deliver the same ID as a plain decimal string or
    integer; strip dots first so we never double-format an already-correct ID.
    """
    try:
        raw = str(id_val).strip().replace(".", "")
        return format_ud(int(raw))
    except (ValueError, TypeError):
        return str(id_val)


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

# ---------------------------------------------------------------------------
# Markdown → Tlon inline parser
# ---------------------------------------------------------------------------

# Inline markdown token regex (precedence: longest/most-specific first)
_MD_INLINE_RE = re.compile(
    r'\*\*\*(.+?)\*\*\*'              # group 1 : ***bold-italic***
    r'|\*\*(.+?)\*\*'                 # group 2 : **bold**
    r'|__(.+?)__'                     # group 3 : __bold__
    r'|\*([^*\n]+?)\*'                # group 4 : *italic*
    r'|_([^\s_][^_\n]*?[^\s_]|[^\s_])_'  # group 5 : _italic_
    r'|~~(.+?)~~'                     # group 6 : ~~strike~~
    r'|`([^`\n]+)`'                   # group 7 : `inline code`
    r'|\[([^\]\n]+)\]\(([^)\s]+)\)',  # groups 8,9: [text](url)
    re.DOTALL,
)

# Fenced code block (``` … ```)
_CODE_FENCE_RE = re.compile(r'^```[^\n]*$')

# ATX heading (# … ###### at start of a line)
_HEADING_RE = re.compile(r'^#{1,6}\s+(.*)')


def _parse_inline_md(text: str) -> list:
    """Recursively parse inline markdown into Tlon inline elements."""
    result: list = []
    last = 0
    for m in _MD_INLINE_RE.finditer(text):
        if m.start() > last:
            result.append(text[last:m.start()])
        g = m.groups()
        if g[0]:                        # ***bold italic***
            result.append({"bold": [{"italics": _parse_inline_md(g[0])}]})
        elif g[1] or g[2]:             # **bold** / __bold__
            result.append({"bold": _parse_inline_md(g[1] or g[2])})
        elif g[3] or g[4]:             # *italic* / _italic_
            result.append({"italics": _parse_inline_md(g[3] or g[4])})
        elif g[5]:                     # ~~strike~~
            result.append({"strike": _parse_inline_md(g[5])})
        elif g[6]:                     # `code`
            result.append({"inline-code": g[6]})
        elif g[7]:                     # [text](url)
            result.append({"link": {"href": g[8], "content": g[7]}})
        last = m.end()
    if last < len(text):
        result.append(text[last:])
    return result or [""]


def text_to_story(text: str) -> list:
    """Convert markdown text to a Tlon story (list of verse objects).

    Supported markdown:
      **bold** / __bold__        → {bold: [...]}
      *italic* / _italic_        → {italics: [...]}
      ***bold-italic***          → {bold: [{italics: [...]}]}
      ~~strike~~                 → {strike: [...]}
      `inline code`              → {"inline-code": "..."}
      [text](url)                → {link: {href, content}}
      ```...``` fenced blocks    → each line as {"inline-code": line}
      # Heading                  → {bold: [...]} on its own verse
      Blank lines                → blank inline (paragraph separator)
    """
    if not text:
        return [{"inline": [""]}]

    verses: list = []
    in_code_block = False
    code_buf: list[str] = []

    for line in text.split("\n"):
        # Fenced code block toggle
        if _CODE_FENCE_RE.match(line):
            if in_code_block:
                # Emit the collected block as a multi-line inline-code element
                if code_buf:
                    verses.append({"inline": [{"inline-code": "\n".join(code_buf)}]})
                code_buf = []
                in_code_block = False
            else:
                in_code_block = True
            continue

        if in_code_block:
            code_buf.append(line)
            continue

        if not line:
            verses.append({"inline": [""]})
            continue

        # ATX heading → bold text
        h = _HEADING_RE.match(line)
        if h:
            verses.append({"inline": [{"bold": _parse_inline_md(h.group(1))}]})
            continue

        verses.append({"inline": _parse_inline_md(line)})

    # Unclosed code fence
    if code_buf:
        verses.append({"inline": [{"inline-code": "\n".join(code_buf)}]})

    return verses or [{"inline": [""]}]


def story_to_text(story: list) -> str:
    """Extract plain text from a Tlon story (list of verse objects).

    Handles inline types (bold, italics, strike, code, link, ship, tag,
    blockquote) and block verses (image, cite/quote, code block).
    Unrecognised structures are skipped silently.
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
        if "italics" in node:
            return "".join(_inline(i) for i in node["italics"])
        if "italic" in node:  # legacy compat
            return "".join(_inline(i) for i in node["italic"])
        if "strike" in node:
            return "".join(_inline(i) for i in node["strike"])
        if "blockquote" in node:
            inner = "".join(_inline(i) for i in node["blockquote"])
            return f"> {inner}"
        # "inline-code" is the canonical Tlon key; "code" kept for compat
        if "inline-code" in node:
            return node["inline-code"]
        if "code" in node:
            return node["code"]
        if "link" in node:
            link = node["link"]
            return link.get("content", link.get("href", ""))
        if "ship" in node:
            s = node["ship"]
            return s if s.startswith("~") else f"~{s}"
        if "tag" in node:
            return f"#{node['tag']}"
        if "break" in node:
            return "\n"
        return ""

    def _block(b: dict) -> str:
        """Extract text from a block-type verse value."""
        if "image" in b:
            img = b["image"]
            alt = img.get("alt", "")
            src = img.get("src", "")
            return f"[image: {alt or src}]"
        if "cite" in b:
            return "[quoted message]"
        if "code" in b:
            code = b["code"]
            if isinstance(code, dict):
                lang = code.get("lang", "")
                body = code.get("code", "")
                return f"```{lang}\n{body}\n```"
            return str(code)
        return ""

    for verse in story:
        if not isinstance(verse, dict):
            continue
        if "inline" in verse:
            parts.append("".join(_inline(i) for i in verse["inline"]))
        elif "block" in verse:
            b = verse["block"]
            if isinstance(b, dict):
                text = _block(b)
                if text:
                    parts.append(text)

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

    # Ack every N events to keep the SSE channel healthy (mirrors openclaw).
    # Low value (1 = every event) prevents Eyre's unacked-event buffer from
    # filling up and triggering a %quit that silently kills subscriptions.
    _ACK_THRESHOLD = 1

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

        # Persistent subscription params for reconnect replay
        # Each entry: {"app": str, "path": str, "on_event": Callable|None,
        #              "on_err": Callable|None, "on_quit": Callable|None}
        self._subscription_params: List[Dict] = []

        # SSE reader task
        self._sse_task: Optional[asyncio.Task] = None
        self._sse_closed: bool = False

        # Event ack tracking
        self._last_heard_event_id: int = -1
        self._last_acked_event_id: int = -1

        # Reconnect state
        self._reconnect_delay: float = 1.0

        # Keepalive
        # Poke the ship every _KEEPALIVE_INTERVAL seconds so the SSE stream
        # always has traffic.  If no bytes arrive for _SSE_READ_TIMEOUT seconds
        # (> interval) we treat the stream as silently dead and reconnect.
        _ka_env = int(os.environ.get("TLON_KEEPALIVE_INTERVAL", "180"))
        self._KEEPALIVE_INTERVAL: float = max(30.0, float(_ka_env))
        self._SSE_READ_TIMEOUT: float = self._KEEPALIVE_INTERVAL * 2.5
        self._keepalive_task: Optional[asyncio.Task] = None

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

        # Remember params so we can replay after a reconnect
        self._subscription_params.append({
            "app": app, "path": path,
            "on_event": on_event, "on_err": on_err, "on_quit": on_quit,
        })

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

    async def scry(self, path: str) -> Any:
        """Scry a Gall agent path and return the parsed JSON response.

        path: e.g. "/contacts/v1/self" or "/groups/v1/groups"
        The auth cookie is sent automatically from the session cookie jar.
        Appends ".json" suffix if the path doesn't already have it.
        Uses the Eyre scry endpoint /~/scry/{path}.json.
        """
        if not self._authenticated:
            raise RuntimeError("Not authenticated — call authenticate() first")
        session = await self._ensure_session()
        full_path = path if path.endswith(".json") else f"{path}.json"
        # Eyre scry endpoint requires /~/scry/ prefix; without it, requests
        # hit the general HTTP handler and get redirected to Landscape HTML.
        url = f"{self.ship_url}/~/scry{full_path}"
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=30),
                allow_redirects=False,
            ) as resp:
                if resp.status == 404:
                    raise FileNotFoundError(f"Scry path not found: {path}")
                if resp.status != 200:
                    text = await resp.text()
                    raise RuntimeError(f"Scry failed: HTTP {resp.status} — {text[:200]}")
                return await resp.json(content_type=None)
        except aiohttp.ClientError as exc:
            raise RuntimeError(f"Scry request error: {exc}") from exc

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
        """Start the background SSE reader task and keepalive task."""
        if self._sse_task and not self._sse_task.done():
            return
        self._sse_closed = False
        self._sse_task = asyncio.create_task(self._sse_loop())
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())

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

    async def _keepalive_loop(self) -> None:
        """Periodically poke the ship to keep the SSE channel alive.

        Urbit ships can silently stop sending SSE events after a long idle
        period.  Sending a helm-hi poke every _KEEPALIVE_INTERVAL seconds
        guarantees at least one SSE event (the poke ack) per interval, so
        the read-timeout in _read_sse_stream can detect true dead connections.
        """
        await asyncio.sleep(self._KEEPALIVE_INTERVAL)
        while not self._sse_closed:
            if self._channel_url and self._authenticated:
                try:
                    wake = [{
                        "id": self._next_id(),
                        "action": "poke",
                        "ship": self._ship_no_tilde,
                        "app": "hood",
                        "mark": "helm-hi",
                        "json": "hermes-tlon keepalive",
                    }]
                    await self._put_channel(wake, context="keepalive")
                    logger.debug("Tlon: keepalive poke sent")
                except Exception as exc:
                    logger.warning("Tlon: keepalive poke failed: %s", exc)
            try:
                await asyncio.sleep(self._KEEPALIVE_INTERVAL)
            except asyncio.CancelledError:
                break

    async def _read_sse_stream(self) -> None:
        """Open the GET SSE stream and read until disconnected or timed out."""
        session = await self._ensure_session()
        logger.info("Tlon: opening SSE stream on %s", self._channel_url)

        async with session.get(
            self._channel_url,
            headers={"Accept": "text/event-stream"},
            timeout=aiohttp.ClientTimeout(sock_read=None),  # outer: no hard limit
        ) as resp:
            if resp.status != 200:
                raise RuntimeError(f"SSE GET returned HTTP {resp.status}")

            self._reconnect_delay = 1.0  # reset on successful connection
            logger.info("Tlon: SSE stream connected (idle timeout=%.0fs, keepalive=%.0fs)",
                        self._SSE_READ_TIMEOUT, self._KEEPALIVE_INTERVAL)

            buf = ""
            while not self._sse_closed:
                try:
                    chunk = await asyncio.wait_for(
                        resp.content.read(4096),
                        timeout=self._SSE_READ_TIMEOUT,
                    )
                except asyncio.TimeoutError:
                    raise RuntimeError(
                        f"SSE stream idle for >{self._SSE_READ_TIMEOUT:.0f}s — "
                        "assuming silent disconnect"
                    )
                if not chunk:
                    raise RuntimeError("SSE stream closed by server (empty read)")
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
            logger.warning("Tlon: subscription %s quit — scheduling resubscription", sub_id)
            asyncio.create_task(self._reconnect_after_quit())
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
        """Re-send all active subscriptions after a channel reconnect.

        Clears the handler map and re-issues each stored subscription so the
        new channel receives the correct sub_ids → handler mappings.
        """
        if not self._subscription_params:
            return

        # Clear stale id→handler mapping; new sub_ids will be assigned below
        self._sub_handlers.clear()

        # Snapshot the list and clear it so subscribe() can repopulate it
        # without duplicating entries.
        params_snapshot = list(self._subscription_params)
        self._subscription_params.clear()

        logger.info("Tlon: re-subscribing %d subscription(s) after reconnect",
                    len(params_snapshot))
        for p in params_snapshot:
            try:
                await self.subscribe(
                    app=p["app"],
                    path=p["path"],
                    on_event=p["on_event"],
                    on_err=p["on_err"],
                    on_quit=p["on_quit"],
                )
            except Exception as exc:
                logger.error("Tlon: failed to re-subscribe %s%s: %s",
                             p["app"], p["path"], exc)

    async def _reconnect_after_quit(self) -> None:
        """Re-subscribe after Eyre terminates a subscription with a quit event.

        Eyre sends ``response: "quit"`` when it kills a subscription due to:
          - event buffer overflow (most common; mitigated by low _ACK_THRESHOLD)
          - Gall agent restart / desk upgrade
          - ship reboot (in that case the SSE stream also closes, triggering
            the normal _sse_loop reconnect path independently)

        Eyre delivers the quit *over* the existing SSE stream, which stays
        open.  So _sse_loop never sees an exception and never runs its own
        reconnect logic.  This method bridges that gap: it re-subscribes on
        the existing channel so the bot doesn't silently go deaf.
        """
        await asyncio.sleep(0.25)  # brief pause so Eyre can settle
        try:
            await self._resubscribe_all()
            logger.info("Tlon: resubscribed after quit event")
        except Exception as exc:
            logger.error(
                "Tlon: resubscribe-after-quit failed: %s — "
                "will retry when the SSE stream next reconnects", exc
            )

    # ── High-level DM send ────────────────────────────────────────────────

    async def send_dm(
        self,
        to_ship: str,
        text: str,
        reply_to: Optional[str] = None,
        story: Optional[List] = None,
    ) -> str:
        """Send a plain-text DM to another Urbit ship.

        to_ship:  ship name with or without "~" (e.g. "~sampel-palnet")
        text:     plain text message content (ignored when story is provided)
        reply_to: optional message ID to thread-reply to
        story:    pre-built Tlon story (list of verse objects); when given,
                  skips text_to_story() conversion (used by send_image())

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
        story = story if story is not None else text_to_story(text)

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

    # ── High-level club (group DM) send ──────────────────────────────────

    async def send_club_msg(
        self,
        club_id: str,
        text: str,
        reply_to: Optional[str] = None,
        story: Optional[List] = None,
    ) -> str:
        """Send a message to a Tlon club (group DM / multi-DM).

        club_id:  club UUID, e.g. "0v3.abc12" (with or without leading "0v")
        text:     message content (markdown supported; ignored when story given)
        reply_to: optional message ID to thread-reply to
        story:    pre-built Tlon story; when given, skips text_to_story()

        Returns a message_id string.
        Raises on auth or poke failure.
        """
        if not self._authenticated:
            ok = await self._reauth()
            if not ok:
                raise RuntimeError("Tlon: authentication failed — cannot send club message")

        sent_at = int(time.time() * 1000)
        story = story if story is not None else text_to_story(text)
        msg_id = make_msg_id(self.ship, sent_at)

        essay = {
            "content": story,
            "author": self.ship,
            "sent": sent_at,
            "kind": "/chat",
            "blob": None,
            "meta": None,
        }

        if reply_to:
            # Thread reply inside a club
            reply_id = make_msg_id(self.ship, sent_at)
            writ_delta = {
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
            }
        else:
            # New top-level club message
            writ_delta = {
                "id": msg_id,
                "delta": {
                    "add": {
                        "essay": essay,
                        "time": None,
                    }
                },
            }

        club_action_json = {
            "id": club_id,
            "diff": {
                "uid": "0v4",   # constant — see multiDmAction() in Tlon dms.ts
                "delta": {
                    "writ": writ_delta,
                },
            },
        }

        await self.poke("chat", "chat-club-action-2", club_action_json)
        logger.info("Tlon: club message sent to %s (msg %s)", club_id, msg_id)
        return msg_id

    # ── High-level group channel send ─────────────────────────────────────

    async def send_channel_post(
        self,
        nest: str,
        text: str,
        reply_to: Optional[str] = None,
        story: Optional[List] = None,
    ) -> str:
        """Send a message to a Tlon group channel.

        nest:     channel nest ID, e.g. "chat/~host/channel-name"
        text:     message content (markdown supported; ignored when story given)
        reply_to: optional parent post ID for thread replies
        story:    pre-built Tlon story; when given, skips text_to_story()

        Returns a message_id string.
        Raises on auth or poke failure.
        """
        if not self._authenticated:
            ok = await self._reauth()
            if not ok:
                raise RuntimeError("Tlon: authentication failed — cannot send channel post")

        sent_at = int(time.time() * 1000)
        story = story if story is not None else text_to_story(text)
        msg_id = make_msg_id(self.ship, sent_at)

        if reply_to:
            # Thread reply inside a channel (PostActionReply).
            # channel-action-2 decodes id with (se %ud) — must be dot-formatted @ud.
            channel_action_json = {
                "channel": {
                    "nest": nest,
                    "action": {
                        "post": {
                            "reply": {
                                "id": ensure_ud_format(reply_to),
                                "action": {
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
                        }
                    },
                }
            }
        else:
            # New top-level post (PostActionAdd)
            essay = {
                "content": story,
                "author": self.ship,
                "sent": sent_at,
                "kind": "/chat",
                "blob": None,
                "meta": None,
            }
            channel_action_json = {
                "channel": {
                    "nest": nest,
                    "action": {
                        "post": {
                            "add": essay,
                        }
                    },
                }
            }

        # Agent: %channels, mark: channel-action-2 (current Tlon version)
        await self.poke("channels", "channel-action-2", channel_action_json)
        logger.info("Tlon: channel post sent to %s (msg %s)", nest, msg_id)
        return msg_id

    # ── Channel history (scry) ───────────────────────────────────────────

    async def fetch_channel_history(
        self, nest: str, count: int = 20
    ) -> list[dict]:
        """Scry recent channel posts for group dispatch context.

        Returns a list of dicts with keys: author, text, sent, id — oldest
        first, newest last. Uses the /channels/v4 scry surface (same as
        OpenClaw and the official Tlon adapter).
        """
        if count <= 0:
            return []
        try:
            payload = await self.scry(
                f"/channels/v4/{nest}/posts/newest/{count}/outline"
            )
        except Exception as exc:
            logger.debug("Tlon: channel history scry failed for %s: %s", nest, exc)
            return []

        if not isinstance(payload, list):
            return []

        entries: list[dict] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            # Outline posts have shape: {"post": {"id": ..., "essay": {...}}}
            post = item.get("post", item)
            essay = post.get("essay") or post
            if not isinstance(essay, dict):
                continue
            author = essay.get("author", "")
            if isinstance(author, dict):
                author = author.get("ship", "")
            author = str(author).strip()
            if author and not author.startswith("~"):
                author = f"~{author}"
            content = essay.get("content", [])
            text = story_to_text(content) if isinstance(content, list) else str(content)
            if not text.strip():
                continue
            try:
                sent = float(essay.get("sent") or 0)
            except (TypeError, ValueError):
                sent = 0.0
            post_id = str(post.get("id") or "")
            entries.append({
                "author": author,
                "text": text,
                "sent": sent,
                "id": post_id,
            })

        # Scry returns newest-first; reverse for chronological order
        entries.reverse()
        return entries

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def close(self) -> None:
        """Cancel the SSE + keepalive tasks and close the HTTP session."""
        self._sse_closed = True
        for task in (self._sse_task, self._keepalive_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
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
