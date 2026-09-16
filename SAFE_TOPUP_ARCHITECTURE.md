# Safe shared-account top-up architecture

Telegram requests can arrive concurrently, but redemption against the single shared
Smile.one account/session is serialized through one worker and one asyncio lock.

Included:
- Async queue for incoming BR/PH top-up requests
- One shared-account redemption lane
- MongoDB status records and unique activation-code idempotency
- Existing game-purchase concurrency path unchanged
- No new CAPTCHA/Cloudflare/anti-bot bypass logic
- Proxy credentials removed from source; configure WEBSHARE_PROXIES through the environment

This intentionally does not run 50 redemptions simultaneously on one shared account.
Use only with an authorized account and within Smile.one terms and rate limits.
