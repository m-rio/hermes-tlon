"""
Urbit HTTP API client — Phase 1 skeleton.

Auth is correct (POST /~/login, cookie-based).
Send is a stub that will be replaced in Phase 2 with real Urbit poke calls.
SSE subscription is not yet implemented (Phase 2).

Urbit HTTP API reference:
  - Auth:      POST /~/login  body: password=<code> (form-encoded)
               response: Set-Cookie header (urbitauth=...)
  - Poke:      PUT  /~/channel/<uid>
               body: JSON array of poke/subscribe/ack actions
  - SSE:       GET  /~/channel/<uid>  (EventSource)
"""

import aiohttp
import logging
import os
from typing import Optional

logger = logging.getLogger("hermes.tlon.urbit")


class UrbitClient:
    def __init__(self, ship_url: str, ship: str, login_code: str):
        self.ship_url = ship_url.rstrip("/")
        self.ship = ship.strip()
        self.login_code = login_code.strip()

        # aiohttp session — created lazily, carries the auth cookie
        self._session: Optional[aiohttp.ClientSession] = None
        self._authenticated: bool = False

    # ── Session management ────────────────────────────────────────────────

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            jar = aiohttp.CookieJar(unsafe=True)
            self._session = aiohttp.ClientSession(cookie_jar=jar)
        return self._session

    # ── Authentication ────────────────────────────────────────────────────

    async def authenticate(self) -> bool:
        """POST /~/login with the access code; stores the auth cookie.

        Urbit ships use a cookie-based session — there is no bearer token.
        The login endpoint accepts the code as a form-encoded `password` field
        and responds with a `set-cookie` header on success.
        """
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
                # Drain body — some Urbit setups finalise Set-Cookie after body read
                await resp.text()

                if resp.status not in (200, 204, 302):
                    logger.error(
                        "Tlon auth failed: HTTP %s at %s", resp.status, url
                    )
                    return False

                # Verify the cookie jar got populated
                cookies = session.cookie_jar.filter_cookies(self.ship_url)
                if not cookies:
                    logger.error(
                        "Tlon auth: no cookie returned — check ship URL and login code"
                    )
                    return False

                self._authenticated = True
                logger.debug("Tlon: auth cookie received for %s", self.ship_url)
                return True

        except Exception as exc:
            logger.exception("Tlon: authentication error: %s", exc)
            return False

    # ── Sending ───────────────────────────────────────────────────────────

    async def send_message(
        self,
        target: str,
        text: str,
        reply_to: Optional[str] = None,
    ) -> str:
        """Send a message to a DM partner or channel.

        NOTE: This is a Phase 1 stub — it authenticates correctly but the
        actual Urbit poke payload is not yet implemented. Phase 2 will add
        real %dm-inbox / %chat poke calls using the Urbit HTTP channel API.

        Returns a message_id string (placeholder for now).
        Raises on unrecoverable errors.
        """
        if not self._authenticated:
            if not await self.authenticate():
                raise RuntimeError("Tlon: authentication failed — cannot send message")

        # Phase 2: replace this stub with a real Urbit HTTP poke.
        # The poke goes to PUT /~/channel/<uid> with a JSON payload like:
        #   [{"id": 1, "action": "poke", "ship": self.ship,
        #     "app": "chat", "mark": "chat-action", "json": {...}}]
        logger.warning(
            "Tlon send_message: Phase 2 not yet implemented — "
            "message to %s was not actually delivered.", target
        )
        return f"tlon-stub/{target}"

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        self._authenticated = False
