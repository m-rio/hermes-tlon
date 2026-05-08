"""
Tlon/Urbit Platform Adapter for Hermes Agent.

Connects to an Urbit ship and relays DMs and group channel messages
to/from the Hermes agent via the gateway.

Configuration in config.yaml::

    gateway:
      platforms:
        tlon:
          enabled: true
          extra:
            ship_url: "https://your-ship.tlon.network"
            ship: "~sampel-palnet"
            login_code: "lidlut-tabwed-pillex-ridrup"

Or via environment variables (overrides config.yaml):
    TLON_SHIP_URL, TLON_SHIP, TLON_LOGIN_CODE,
    TLON_HOME_CHANNEL, TLON_OWNER_SHIP,
    TLON_ALLOWED_USERS, TLON_ALLOW_ALL_USERS
"""

import asyncio
import logging
import os
from typing import Any, Dict, Optional

logger = logging.getLogger("hermes.tlon.adapter")

# Lazy imports — imported at function/class level to avoid errors when the
# plugin is discovered but the gateway hasn't been fully initialised yet.
from gateway.platforms.base import (
    BasePlatformAdapter,
    SendResult,
    MessageEvent,
    MessageType,
)
from gateway.config import Platform, PlatformConfig

from .urbit import UrbitClient


class TlonPlatformAdapter(BasePlatformAdapter):
    """Async Tlon/Urbit adapter implementing the BasePlatformAdapter interface."""

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
        """Authenticate against the Urbit ship and start the SSE listener."""
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

        logger.info("Tlon: authenticated successfully as %s.", self.ship or "(unknown)")

        # Phase 2: start SSE listener here
        # self._listen_task = asyncio.create_task(self._listen_loop())

        self._mark_connected()
        return True

    async def disconnect(self) -> None:
        """Cancel the listener and close the HTTP session."""
        self._mark_disconnected()
        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
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
        """Send a message to a Tlon DM or channel.

        chat_id formats:
          - DM:      "~sampel-palnet"  or  "dm/~sampel-palnet"
          - Channel: "chat/~host/channel-name"
        """
        logger.info("Tlon: sending to %s", chat_id)
        try:
            message_id = await self.client.send_message(
                target=chat_id,
                text=content,
                reply_to=reply_to,
            )
            return SendResult(success=True, message_id=message_id)
        except Exception as exc:
            logger.error("Tlon: send failed to %s: %s", chat_id, exc)
            return SendResult(success=False, error=str(exc))

    # ── Chat info ─────────────────────────────────────────────────────────

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return minimal metadata about a DM partner or channel."""
        if chat_id.startswith("~") or chat_id.startswith("dm/"):
            ship = chat_id.removeprefix("dm/")
            return {"name": ship, "type": "dm", "platform": "tlon"}
        return {"name": chat_id, "type": "channel", "platform": "tlon"}

    # ── Inbound listener (Phase 2 stub) ──────────────────────────────────

    async def _listen_loop(self) -> None:
        """SSE listener — to be implemented in Phase 2."""
        # Will subscribe to /~/channel via Urbit SSE and call
        # self.handle_message(MessageEvent(...)) for each inbound message.
        logger.warning("Tlon: inbound listener not yet implemented (Phase 2).")
        while True:
            await asyncio.sleep(60)


# ── Plugin entry points ───────────────────────────────────────────────────────

def check_requirements() -> bool:
    """Return True when the minimum required env vars are set."""
    return bool(
        os.getenv("TLON_SHIP_URL") and os.getenv("TLON_LOGIN_CODE")
    )


def validate_config(config) -> bool:
    """Return True when the PlatformConfig has enough data to attempt a connection."""
    extra = getattr(config, "extra", {}) or {}
    ship_url = os.getenv("TLON_SHIP_URL") or extra.get("ship_url", "")
    login_code = os.getenv("TLON_LOGIN_CODE") or extra.get("login_code", "")
    return bool(ship_url and login_code)


def _env_enablement() -> Optional[dict]:
    """Seed PlatformConfig.extra from env vars for env-only setups.

    Returns a dict that becomes PlatformConfig.extra when all required vars
    are present, or None to skip auto-enablement.
    """
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
        max_message_length=0,  # Tlon has no hard limit we need to chunk at
        platform_hint=(
            "You are chatting via Tlon (Urbit decentralized messenger). "
            "Keep responses concise. Markdown is supported."
        ),
        emoji="🪐",
    )
