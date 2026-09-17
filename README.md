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


## User wallet flow

Each authorized Telegram user has an independent `balance` field in `resellers`.
The `.bal` command is role-aware: the Owner sees the shared official Smile.one
BR/PH balance; an authorized non-owner sees only their own user wallet balance.

For game purchases the flow is:
1. Check the user's wallet balance.
2. Check the relevant official Smile.one balance.
3. Start the purchase only if both checks pass.
4. After the purchase result is known, charge only the amount for successfully
   completed items. Failed items are not charged, so their amount remains in the
   user's wallet automatically.
5. Wallet debits use an atomic MongoDB update and per-user locking to avoid
   concurrent overspending.

Owner commands:
- `.addbal USER_ID AMOUNT` adds wallet credit.
- `.rmbal USER_ID AMOUNT` removes wallet credit without allowing a negative balance.
