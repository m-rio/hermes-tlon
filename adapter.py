"""
Tlon/Urbit Platform Adapter for Hermes Agent.

Connects to an Urbit ship and relays DMs to/from the Hermes agent via
the gateway.  Group channel support is Phase 3.

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
    TLON_ALLOWED_USERS, TLON_ALLOW_ALL_USERS
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

    Phase 2: DM send + receive via Urbit HTTP channel API + SSE.
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

        # Access control
        raw_allowed = os.getenv("TLON_ALLOWED_USERS", "")
        self._allowed_ships: set[str] = {
            s.strip() for s in raw_allowed.split(",") if s.strip()
        }
        self._allow_all: bool = os.getenv("TLON_ALLOW_ALL_USERS", "").strip() in ("1", "true", "yes")
        self._owner_ship: str = os.getenv("TLON_OWNER_SHIP", "").strip()

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

        # Start the background SSE reader
        await self.client.open_sse_stream()
        self._mark_connected()
        logger.info("Tlon: connected and listening for DMs.")
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
        """Send a message to a Tlon DM.

        chat_id formats accepted:
          - "~sampel-palnet"       (bare ship name)
          - "dm/~sampel-palnet"    (openclaw-style DM prefix)
        """
        # Normalise: strip the "dm/" prefix if present
        target = chat_id.removeprefix("dm/").strip()
        if not target.startswith("~"):
            target = f"~{target}"

        # Tlon clients can't send "/command" — rewrite approval hints in text
        content = _strip_slashes_for_tlon(content)

        # Use thread_id from metadata as reply_to when the gateway provides it
        # (set when the inbound message was itself a thread reply)
        if reply_to is None and isinstance(metadata, dict):
            reply_to = metadata.get("thread_id") or None

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
        target = chat_id.removeprefix("dm/").strip()
        return {"name": target, "type": "dm", "platform": "tlon"}

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
        if not isinstance(payload, dict):
            return

        # Some events are arrays (DM invite lists) — skip
        if isinstance(payload, list):
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

        # Use the sender ship as chat_id for DMs
        chat_id = author

        logger.info("Tlon: inbound DM from %s: %r", author, text[:80])

        # thread_root_id is set for thread replies (None for top-level messages)
        _thread_root_id = locals().get("_thread_root_id")

        source = self.build_source(
            chat_id=chat_id,
            chat_name=author,
            chat_type="dm",
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

    async def _dispatch(self, event: MessageEvent) -> None:
        """Hand the event to the base-class handler (runs in event loop)."""
        try:
            await self.handle_message(event)
        except Exception as exc:
            logger.exception("Tlon: error dispatching message event: %s", exc)

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
