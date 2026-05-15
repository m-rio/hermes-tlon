"""
Tlon/Urbit Platform Adapter for Hermes Agent.

Connects to an Urbit ship and relays DMs, group DMs (clubs), and group
channels to/from the Hermes agent via the gateway.

Configuration in config.yaml::

    gateway:
      platforms:
        tlon:
          enabled: true
          extra:
            ship_url: "https://your-ship.tlon.network"
            ship: "~sampel-palnet"
            login_code: "lidlut-tabwed-pillex-ridrup"

Or via environment variables (highest priority):
    TLON_SHIP_URL, TLON_SHIP, TLON_LOGIN_CODE,
    TLON_HOME_CHANNEL, TLON_OWNER_SHIP,
    TLON_ALLOWED_USERS, TLON_ALLOW_ALL_USERS,
    TLON_CHANNELS  (comma-separated channel nests, e.g. "chat/~host/name"; "*" = all)
"""

import asyncio
import datetime
import logging
import os
import re
import time
from typing import Any, Dict, Optional

logger = logging.getLogger("hermes.tlon.adapter")

from gateway.platforms.base import (
    BasePlatformAdapter,
    SendResult,
    MessageEvent,
    MessageType,
)
from gateway.config import Platform, PlatformConfig

from .urbit import UrbitClient, story_to_text


# ---------------------------------------------------------------------------
# Tlon ↔ Hermes command text helpers
# ---------------------------------------------------------------------------

# Tlon clients treat any message starting with "/" as a local client command
# and refuse to send it.  The gateway's approval prompts instruct users to
# type "/approve" etc. — we rewrite those in outbound text so users see the
# slash-free form ("approve", "deny", …) which we then re-add on the way in.

# Matches `/approve…` or `/deny…` when wrapped in backticks
_SLASH_IN_BACKTICK_RE = re.compile(r"(?<=`)/(?=(?:approve|deny)\b)")

def _strip_slashes_for_tlon(text: str) -> str:
    """Remove leading '/' from gateway command hints in backticks.

    Turns `` `/approve always` `` → `` `approve always` `` so Tlon users
    can type the command without the slash that their client would swallow.
    """
    return _SLASH_IN_BACKTICK_RE.sub("", text)


# Matches the exact bare approval replies a Tlon user would type
_BARE_APPROVAL_RE = re.compile(
    r"^(?:approve(?:\s+(?:all\s+)?(?:once|session|always))?|deny(?:\s+all)?)$",
    re.IGNORECASE,
)

def _restore_slash_for_gateway(text: str) -> str:
    """Prepend '/' to bare approval/deny replies so the gateway recognises them.

    Tlon users type "approve always" (no slash); the gateway expects "/approve
    always".  Only exact-match phrases are normalised so normal sentences are
    never accidentally rewritten.
    """
    stripped = text.strip()
    if _BARE_APPROVAL_RE.match(stripped):
        return "/" + stripped
    return text


class TlonPlatformAdapter(BasePlatformAdapter):
    """Async Tlon/Urbit adapter implementing the BasePlatformAdapter interface.

    Supports 1-on-1 DMs, group DMs (clubs), and group channels via Urbit HTTP
    channel API + SSE.
    """

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform("tlon"))

        extra = getattr(config, "extra", {}) or {}

        # Connection settings — env vars take precedence over config.yaml
        self.ship_url: str = (
            os.getenv("TLON_SHIP_URL") or extra.get("ship_url", "")
        ).rstrip("/")
        self.ship: str = (
            os.getenv("TLON_SHIP") or extra.get("ship", "")
        ).strip()
        self.login_code: str = (
            os.getenv("TLON_LOGIN_CODE") or extra.get("login_code", "")
        ).strip()

        # Access control — env vars override config.yaml extra keys
        raw_allowed = os.getenv("TLON_ALLOWED_USERS") or extra.get("allowed_users", "")
        self._allowed_ships: set[str] = {
            s.strip() for s in raw_allowed.split(",") if s.strip()
        }
        allow_all_raw = os.getenv("TLON_ALLOW_ALL_USERS") or str(extra.get("allow_all_users", ""))
        self._allow_all: bool = allow_all_raw.strip() in ("1", "true", "yes")
        self._owner_ship: str = (
            os.getenv("TLON_OWNER_SHIP") or extra.get("owner_ship", "")
        ).strip()

        # Group channel filter — set of nest IDs to accept; empty = off; {"*"} = all
        raw_channels = (
            os.getenv("TLON_CHANNELS") or extra.get("channels", "")
        )
        if isinstance(raw_channels, list):
            self._channel_filter: set[str] = {c.strip() for c in raw_channels if c.strip()}
        else:
            self._channel_filter = {c.strip() for c in str(raw_channels).split(",") if c.strip()}

        # Group channel mention gating
        # When True, only respond to channel messages that @mention the bot.
        self._mention_gate: bool = (
            os.getenv("TLON_MENTION_GATE", "1").strip() not in ("0", "false", "no")
        )
        # Bot's display nickname (fetched on connect via contacts scry)
        self._bot_nickname: Optional[str] = None

        # Auto-discovery of group channels via /groups scry
        auto_disc_raw = os.getenv("TLON_AUTO_DISCOVER") or str(extra.get("auto_discover", ""))
        self._auto_discover: bool = auto_disc_raw.strip() in ("1", "true", "yes")

        self.client = UrbitClient(
            ship_url=self.ship_url,
            ship=self.ship,
            login_code=self.login_code,
        )
        self._listen_task: Optional[asyncio.Task] = None

    @property
    def name(self) -> str:
        return "Tlon"

    # ── Connection lifecycle ──────────────────────────────────────────────

    async def connect(self) -> bool:
        """Authenticate, open an Urbit SSE channel, and subscribe to DMs."""
        if not self.ship_url or not self.login_code:
            logger.error(
                "Tlon: TLON_SHIP_URL and TLON_LOGIN_CODE must be set. "
                "Run `hermes config set` or export the env vars."
            )
            return False

        logger.info("Connecting to Tlon ship at %s ...", self.ship_url)
        if not await self.client.authenticate():
            logger.error("Tlon: authentication failed.")
            return False

        logger.info("Tlon: authenticated as %s.", self.ship or "(unknown)")

        # Fetch the bot's own contact profile for mention detection
        try:
            profile = await self.client.scry("/contacts/v1/self.json")
            if isinstance(profile, dict):
                nick = profile.get("nickname", {})
                if isinstance(nick, dict):
                    nick = nick.get("value", "")
                self._bot_nickname = str(nick).strip() if nick else None
                if self._bot_nickname:
                    logger.info("Tlon: bot nickname: %s", self._bot_nickname)
        except Exception as exc:
            logger.debug("Tlon: could not fetch self profile (non-fatal): %s", exc)

        # Auto-discover group channels from /groups scry
        if self._auto_discover:
            discovered = await self._discover_channels()
            self._channel_filter.update(discovered)

        try:
            await self.client.open_channel()
        except Exception as exc:
            logger.error("Tlon: failed to open channel: %s", exc)
            return False

        # Subscribe to %chat /v4 — receives writ-response-4 events for DMs + clubs
        try:
            await self.client.subscribe(
                app="chat",
                path="/v4",
                on_event=self._on_dm_event,
            )
        except Exception as exc:
            logger.error("Tlon: failed to subscribe to chat /v4: %s", exc)
            return False

        # Subscribe to %channels /v4 — receives channel post events
        try:
            await self.client.subscribe(
                app="channels",
                path="/v4",
                on_event=self._on_channel_event,
            )
        except Exception as exc:
            logger.error("Tlon: failed to subscribe to channels /v4: %s", exc)
            return False

        # Start the background SSE reader
        await self.client.open_sse_stream()
        self._mark_connected()
        if self._channel_filter:
            mention_note = " (mention-gated)" if self._mention_gate else " (all messages)"
            logger.info(
                "Tlon: connected — DMs + %d channel(s)%s: %s",
                len(self._channel_filter),
                mention_note,
                ", ".join(sorted(self._channel_filter)),
            )
        else:
            logger.info(
                "Tlon: connected — listening for DMs only. "
                "Set TLON_CHANNELS (or TLON_AUTO_DISCOVER=1) to also receive channel messages."
            )
        return True

    async def disconnect(self) -> None:
        """Close the SSE stream and HTTP session."""
        self._mark_disconnected()
        await self.client.close()
        logger.info("Tlon: disconnected.")

    # ── Sending ───────────────────────────────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a message to a Tlon DM, club (group DM), or group channel.

        chat_id formats accepted:
          - "~sampel-palnet"            (1-on-1 DM: bare ship name)
          - "dm/~sampel-palnet"         (1-on-1 DM: openclaw-style prefix)
          - "club/0v3.abc12"            (group DM: club/ prefix)
          - "0v3.abc12"                 (group DM: bare club ID)
          - "channel/chat/~host/name"   (group channel: channel/ + nest)
        """
        # Tlon clients can't send "/command" — rewrite approval hints in text
        content = _strip_slashes_for_tlon(content)

        # Use thread_id from metadata as reply_to when the gateway provides it
        if reply_to is None and isinstance(metadata, dict):
            reply_to = metadata.get("thread_id") or None

        # --- Group channel ---
        if chat_id.startswith("channel/"):
            nest = chat_id.removeprefix("channel/")
            logger.info("Tlon: sending channel post to %s%s", nest,
                        f" (thread {reply_to})" if reply_to else "")
            try:
                message_id = await self.client.send_channel_post(
                    nest=nest,
                    text=content,
                    reply_to=reply_to,
                )
                return SendResult(success=True, message_id=message_id)
            except Exception as exc:
                logger.error("Tlon: channel send failed to %s: %s", nest, exc)
                return SendResult(success=False, error=str(exc))

        # Route to club or DM send based on chat_id format
        bare = chat_id.removeprefix("club/").removeprefix("dm/").strip()
        is_club = bare.startswith("0v")

        if is_club:
            logger.info("Tlon: sending club message to %s%s", bare,
                        f" (thread {reply_to})" if reply_to else "")
            try:
                message_id = await self.client.send_club_msg(
                    club_id=bare,
                    text=content,
                    reply_to=reply_to,
                )
                return SendResult(success=True, message_id=message_id)
            except Exception as exc:
                logger.error("Tlon: club send failed to %s: %s", bare, exc)
                return SendResult(success=False, error=str(exc))
        else:
            # 1-on-1 DM — ensure ship has "~" prefix
            target = bare if bare.startswith("~") else f"~{bare}"
            logger.info("Tlon: sending DM to %s%s", target,
                        f" (thread {reply_to})" if reply_to else "")
            try:
                message_id = await self.client.send_dm(
                    to_ship=target,
                    text=content,
                    reply_to=reply_to,
                )
                return SendResult(success=True, message_id=message_id)
            except Exception as exc:
                logger.error("Tlon: send failed to %s: %s", target, exc)
                return SendResult(success=False, error=str(exc))

    # ── Chat info ─────────────────────────────────────────────────────────

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        if chat_id.startswith("channel/"):
            nest = chat_id.removeprefix("channel/")
            return {"name": nest, "type": "channel", "platform": "tlon"}
        bare = chat_id.removeprefix("club/").removeprefix("dm/").strip()
        chat_type = "club" if bare.startswith("0v") else "dm"
        return {"name": bare, "type": chat_type, "platform": "tlon"}

    # ── Inbound DM handler ────────────────────────────────────────────────

    def _on_dm_event(self, payload: Any) -> None:
        """Called by UrbitClient for each dm-inbox /updates SSE event.

        Expected payload shape (WritResponse):
          {
            "whom": "~sender-ship",
            "id":   "~sender-ship/timestamp",
            "response": {
              "add": {
                "essay": {
                  "content": [...story...],
                  "author":  "~sender-ship",
                  "sent":    1715134800000
                }
              }
            }
          }

        We ignore reactions, deletions, and our own messages.
        """
        # DM invite events arrive as a JSON array of {ship: "...", ...} dicts
        if isinstance(payload, list):
            asyncio.create_task(self._accept_dm_invites(payload))
            return

        if not isinstance(payload, dict):
            return

        whom = payload.get("whom", "")
        response = payload.get("response", {})
        if not isinstance(response, dict):
            return

        # --- top-level message ("add") ---
        add = response.get("add")
        if isinstance(add, dict):
            essay = add.get("essay")
            if not isinstance(essay, dict):
                return
            author = essay.get("author", "")
            content = essay.get("content", [])
            sent = essay.get("sent")
            msg_id = payload.get("id", f"{author}/{int(time.time()*1000)}")

        # --- thread reply ("reply") ---
        elif isinstance(response.get("reply"), dict):
            reply_wrapper = response["reply"]
            delta = reply_wrapper.get("delta", {})
            reply_add = delta.get("add", {}) if isinstance(delta, dict) else {}
            essay = reply_add.get("reply-essay") if isinstance(reply_add, dict) else None
            if not isinstance(essay, dict):
                return
            author = essay.get("author", "")
            content = essay.get("content", [])
            sent = essay.get("sent")
            # Use the reply's own id if available, else fall back to thread root id
            msg_id = reply_wrapper.get("id") or payload.get("id", f"{author}/{int(time.time()*1000)}")
            # thread_root_id: the message this reply is anchored to.
            # Stored as thread_id on the source so the gateway passes it back
            # to send() as metadata["thread_id"], keeping the reply in-thread.
            _thread_root_id = payload.get("id") or None

        # --- other deltas (reactions, deletions, etc.) — ignore ---
        else:
            return

        # Normalise author ship
        if isinstance(author, dict):
            author = author.get("ship", "")
        author = str(author).strip()
        if not author.startswith("~"):
            author = f"~{author}"

        # Ignore messages sent by the bot itself
        if author == self.ship:
            logger.debug("Tlon: ignoring our own message (author=%s)", author)
            return

        # Extract plain text
        text = story_to_text(content) if isinstance(content, list) else str(content)
        if not text.strip():
            logger.debug("Tlon: ignoring empty/non-text DM from %s", author)
            return

        # Normalize bare approval replies — Tlon can't send "/approve" because
        # the client intercepts slash-commands, so users type "approve always"
        # etc. and we restore the slash before the gateway sees the message.
        text = _restore_slash_for_gateway(text)

        # Access control
        if not self._is_ship_allowed(author):
            logger.info("Tlon: DM from %s blocked (not in allowlist)", author)
            return

        # Use the sender ship as chat_id for 1-on-1 DMs;
        # use the club ID for group DMs (clubs start with "0v")
        if isinstance(whom, str) and whom.startswith("0v"):
            chat_id = f"club/{whom}"
            chat_type = "club"
            chat_name = whom
        else:
            chat_id = author
            chat_type = "dm"
            chat_name = author

        logger.info("Tlon: inbound %s from %s: %r", chat_type, author, text[:80])

        # thread_root_id is set for thread replies (None for top-level messages)
        _thread_root_id = locals().get("_thread_root_id")

        source = self.build_source(
            chat_id=chat_id,
            chat_name=chat_name,
            chat_type=chat_type,
            user_id=author,
            user_name=author,
            thread_id=_thread_root_id,
        )

        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=str(msg_id),
            timestamp=datetime.datetime.now(),
        )

        # handle_message is synchronous in BasePlatformAdapter
        asyncio.create_task(self._dispatch(event))

    # ── Inbound channel handler ───────────────────────────────────────────

    def _on_channel_event(self, payload: Any) -> None:
        """Called by UrbitClient for each channels /v4 SSE event.

        Expected payload shape (ChannelsSubscribeResponse):
          {
            "nest": "chat/~host/channel-name",
            "response": {
              "post": {
                "id": "170...",
                "r-post": {
                  "set": {
                    "seal": {...},
                    "essay": {
                      "content": [...story...],
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

        For thread replies "r-post" contains a "reply" key instead of "set".
        Deletions have {"set": null}.
        """
        if not isinstance(payload, dict):
            return

        nest = payload.get("nest", "")
        if not nest:
            return

        # Apply channel filter
        if not self._channel_filter:
            logger.debug(
                "Tlon: channel event from %s ignored (TLON_CHANNELS not set)", nest
            )
            return
        if "*" not in self._channel_filter and nest not in self._channel_filter:
            logger.debug("Tlon: channel event from %s ignored (not in TLON_CHANNELS)", nest)
            return

        response = payload.get("response", {})
        if not isinstance(response, dict):
            return

        post_resp = response.get("post")
        if not isinstance(post_resp, dict):
            return

        post_id = post_resp.get("id", "")
        r_post = post_resp.get("r-post", {})
        if not isinstance(r_post, dict):
            return

        # --- thread reply ---
        reply_block = r_post.get("reply")
        if isinstance(reply_block, dict):
            r_reply = reply_block.get("r-reply", {})
            if not isinstance(r_reply, dict):
                return
            reply_set = r_reply.get("set")
            if not isinstance(reply_set, dict):
                return  # deletion or null
            reply_essay = reply_set.get("essay") or reply_set  # reply-essay may be top-level
            author = reply_essay.get("author", "")
            content = reply_essay.get("content", [])
            sent = reply_essay.get("sent")
            reply_id = reply_block.get("id", "")
            msg_id = reply_id or f"{author}/{int(time.time()*1000)}"
            thread_root_id = post_id or None

        # --- top-level post ---
        elif "set" in r_post:
            post_set = r_post.get("set")
            if not isinstance(post_set, dict):
                return  # deletion (set=null)
            essay = post_set.get("essay", {})
            if not isinstance(essay, dict):
                return
            author = essay.get("author", "")
            content = essay.get("content", [])
            sent = essay.get("sent")
            msg_id = post_id or f"{author}/{int(time.time()*1000)}"
            thread_root_id = None

        else:
            # Unrecognised r-post shape — skip
            return

        # Normalise author (may be plain string or {ship: ...} object)
        if isinstance(author, dict):
            author = author.get("ship", "")
        author = str(author).strip()
        if not author.startswith("~"):
            author = f"~{author}"

        # Ignore messages sent by the bot itself
        if author == self.ship:
            logger.debug("Tlon: ignoring our own channel post (author=%s)", author)
            return

        text = story_to_text(content) if isinstance(content, list) else str(content)
        if not text.strip():
            logger.debug("Tlon: ignoring empty channel post from %s in %s", author, nest)
            return

        text = _restore_slash_for_gateway(text)

        if not self._is_ship_allowed(author):
            logger.info("Tlon: channel post from %s blocked (not in allowlist)", author)
            return

        # Mention gate — only respond when the bot is addressed
        if self._mention_gate and not self._is_bot_mentioned(text):
            logger.debug(
                "Tlon: channel post from %s in %s ignored (bot not mentioned)", author, nest
            )
            return
        text = self._strip_bot_mention(text)
        if not text.strip():
            logger.debug("Tlon: ignoring channel post that was only a mention from %s", author)
            return

        chat_id = f"channel/{nest}"
        logger.info("Tlon: inbound channel post from %s in %s: %r", author, nest, text[:80])

        source = self.build_source(
            chat_id=chat_id,
            chat_name=nest,
            chat_type="channel",
            user_id=author,
            user_name=author,
            thread_id=thread_root_id,
        )

        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=str(msg_id),
            timestamp=datetime.datetime.now(),
        )

        asyncio.create_task(self._dispatch(event))

    async def _dispatch(self, event: MessageEvent) -> None:
        """Hand the event to the base-class handler (runs in event loop)."""
        try:
            await self.handle_message(event)
        except Exception as exc:
            logger.exception("Tlon: error dispatching message event: %s", exc)

    # ── Mention detection ─────────────────────────────────────────────────

    def _is_bot_mentioned(self, text: str) -> bool:
        """Return True if the bot's ship name or display nickname appears in text."""
        lower = text.lower()
        ship_bare = self.ship.lstrip("~").lower()
        if ship_bare and (ship_bare in lower or f"~{ship_bare}" in lower):
            return True
        if self._bot_nickname and self._bot_nickname.lower() in lower:
            return True
        return False

    def _strip_bot_mention(self, text: str) -> str:
        """Remove leading/inline bot @-mentions from text before passing to AI."""
        ship_bare = re.escape(self.ship.lstrip("~"))
        # Strip ~ship or @~ship mentions (with optional trailing space/comma)
        text = re.sub(
            rf"(?:@~{ship_bare}|~{ship_bare})[,\s]*",
            " ", text, flags=re.IGNORECASE,
        ).strip()
        if self._bot_nickname:
            nick = re.escape(self._bot_nickname)
            text = re.sub(
                rf"(?:@{nick}|{nick})[,\s]*",
                " ", text, flags=re.IGNORECASE,
            ).strip()
        # Collapse any extra whitespace introduced by substitution
        return re.sub(r" {2,}", " ", text).strip()

    # ── DM invite handling ────────────────────────────────────────────────

    async def _accept_dm_invites(self, invites: list) -> None:
        """Auto-accept DM invites from allowed ships via chat-dm-rsvp poke."""
        for invite in invites:
            if not isinstance(invite, dict):
                continue
            ship = invite.get("ship", "").strip()
            if not ship:
                continue
            if not ship.startswith("~"):
                ship = f"~{ship}"
            if not self._is_ship_allowed(ship):
                logger.debug("Tlon: ignoring DM invite from %s (not in allowlist)", ship)
                continue
            try:
                await self.client.poke(
                    app="chat",
                    mark="chat-dm-rsvp",
                    poke_json={"ship": ship.lstrip("~"), "ok": True},
                )
                logger.info("Tlon: accepted DM invite from %s", ship)
            except Exception as exc:
                logger.warning("Tlon: failed to accept DM invite from %s: %s", ship, exc)

    # ── Channel auto-discovery ────────────────────────────────────────────

    async def _discover_channels(self) -> set:
        """Scry /groups/v1/groups.json and return the set of chat/* nests."""
        try:
            groups = await self.client.scry("/groups/v1/groups.json")
        except Exception as exc:
            logger.warning("Tlon: channel auto-discovery failed: %s", exc)
            return set()
        if not isinstance(groups, dict):
            return set()
        nests: set = set()
        for group_data in groups.values():
            if not isinstance(group_data, dict):
                continue
            channels = group_data.get("channels", {})
            if not isinstance(channels, dict):
                continue
            for nest in channels:
                if nest.startswith("chat/") or nest.startswith("heap/"):
                    nests.add(nest)
        logger.info(
            "Tlon: auto-discovered %d channel(s): %s",
            len(nests), ", ".join(sorted(nests)) or "(none)",
        )
        return nests

    # ── Image sending ─────────────────────────────────────────────────────

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "SendResult":
        """Send an image as a Tlon story block.image verse.

        image_url must be an http/https URL — Tlon has no file-upload API via
        Eyre.  For local file paths the base-class fallback (URL as text) is
        used instead.  Caption text (if any) is prepended as inline blocks.
        """
        from .urbit import text_to_story as _t2s

        if not image_url.startswith(("http://", "https://")):
            logger.warning(
                "Tlon: send_image() only supports http/https URLs, got %r — "
                "falling back to text", image_url,
            )
            return await super().send_image(chat_id, image_url, caption, reply_to, metadata)

        story: list = []
        if caption:
            story.extend(_t2s(caption))
        story.append({
            "block": {
                "image": {
                    "src": image_url,
                    "alt": caption or "",
                    "width": 0,
                    "height": 0,
                }
            }
        })

        if reply_to is None and isinstance(metadata, dict):
            reply_to = metadata.get("thread_id") or None

        try:
            if chat_id.startswith("channel/"):
                nest = chat_id.removeprefix("channel/")
                msg_id = await self.client.send_channel_post(
                    nest=nest, text="", story=story, reply_to=reply_to,
                )
            else:
                bare = chat_id.removeprefix("club/").removeprefix("dm/").strip()
                if bare.startswith("0v"):
                    msg_id = await self.client.send_club_msg(
                        club_id=bare, text="", story=story, reply_to=reply_to,
                    )
                else:
                    target = bare if bare.startswith("~") else f"~{bare}"
                    msg_id = await self.client.send_dm(
                        to_ship=target, text="", story=story, reply_to=reply_to,
                    )
            return SendResult(success=True, message_id=msg_id)
        except Exception as exc:
            logger.error("Tlon: send_image failed to %s: %s", chat_id, exc)
            return SendResult(success=False, error=str(exc))

    # ── Access control ────────────────────────────────────────────────────

    def _is_ship_allowed(self, ship: str) -> bool:
        """Return True if this ship is allowed to send messages to the bot."""
        if self._allow_all:
            return True
        if self._owner_ship and ship == self._owner_ship:
            return True
        if self._allowed_ships and ship in self._allowed_ships:
            return True
        # If no allowlist configured at all, allow everyone
        if not self._allowed_ships and not self._owner_ship:
            return True
        return False


# ── Plugin entry points ───────────────────────────────────────────────────────

def check_requirements() -> bool:
    """Return True when the minimum required env vars are set."""
    return bool(
        os.getenv("TLON_SHIP_URL") and os.getenv("TLON_LOGIN_CODE")
    )


def validate_config(config) -> bool:
    """Return True when the PlatformConfig has enough data to connect."""
    extra = getattr(config, "extra", {}) or {}
    ship_url = os.getenv("TLON_SHIP_URL") or extra.get("ship_url", "")
    login_code = os.getenv("TLON_LOGIN_CODE") or extra.get("login_code", "")
    return bool(ship_url and login_code)


def _env_enablement() -> Optional[dict]:
    """Seed PlatformConfig.extra from env vars for env-only setups."""
    ship_url = os.getenv("TLON_SHIP_URL", "").strip()
    login_code = os.getenv("TLON_LOGIN_CODE", "").strip()
    if not (ship_url and login_code):
        return None

    seed: dict = {"ship_url": ship_url, "login_code": login_code}

    ship = os.getenv("TLON_SHIP", "").strip()
    if ship:
        seed["ship"] = ship

    home_channel = os.getenv("TLON_HOME_CHANNEL", "").strip()
    if home_channel:
        seed["home_channel"] = {"chat_id": home_channel, "name": "Home"}

    return seed


def register(ctx):
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name="tlon",
        label="Tlon",
        adapter_factory=lambda cfg: TlonPlatformAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        env_enablement_fn=_env_enablement,
        required_env=["TLON_SHIP_URL", "TLON_LOGIN_CODE"],
        cron_deliver_env_var="TLON_HOME_CHANNEL",
        allowed_users_env="TLON_ALLOWED_USERS",
        allow_all_env="TLON_ALLOW_ALL_USERS",
        max_message_length=0,
        platform_hint=(
            "You are chatting via Tlon (Urbit decentralized messenger). "
            "Keep responses concise. Markdown is supported."
        ),
        emoji="🪐",
    )
