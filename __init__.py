from .adapter import TlonPlatformAdapter, check_requirements, _env_enablement

def register(ctx):
    ctx.register_platform(
        name="tlon",
        label="Tlon",
        adapter_factory=lambda cfg: TlonPlatformAdapter(cfg),
        check_fn=check_requirements,
        env_enablement_fn=_env_enablement,
        emoji="🪐",
        platform_hint="You are chatting via Tlon. Keep responses concise.",
    )
