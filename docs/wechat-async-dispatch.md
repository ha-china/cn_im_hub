# WeChat asynchronous dispatch (development)

- Keep upstream cursor + pending storage, with atomic Store writes and disk read-back before dispatch or cursor advancement. Persisted running entries are not replayed after a restart; this is intentionally not exactly-once delivery. A crash during the commit-to-execution boundary can still drop an unexecuted message.
- Poll independently of conversations. At most two normal tasks and two fast tasks per WeChat account; one normal turn per sender, preserving normal turn order. Fast replies may arrive out of order, and do not alter the LLM chat history.
- Only strictly recognized speech-only YAML intent scripts bypass an occupied lane. Uncertain matches, trigger configuration, unsupported APIs, media and control messages fall back to normal dispatch. Do not bypass a configured sentence-trigger registry.
- Conversation deadline 120s, outer normal message deadline 180s, fast recognition/render deadlines 5s each, fast tracker/send deadlines 15s each. Cancellation is cooperative; these are not hard wall-clock limits. Never retry a potentially executed conversation on error.
- Explicit protocol message_id/client_id deduplication, bounded to 2048 keys and 24h, no deduplication by text. Other ID aliases require protocol evidence before adoption. Backpressure at 100 pending entries (one received batch may exceed the threshold).
- Stop joins all tracked workers and in-flight storage writes. The HA Store may defer writes during Core shutdown: verification fails closed. An interrupted running message is never automatically replayed.
- DEBUG route/count/timing logs do not include message bodies, context tokens or account IDs. Other pre-existing integration/provider logs may still contain identifying data.
- Config entries, storage key/version, notification services and provider protocols unchanged. Old versions can read queued/running fields; rollback to a release predating pending storage can lose queued messages. Roll back only while idle.
- HA recognition internals reviewed for Core 2026.11.3; compatibility outside that runtime is not certified. No allowlist, role system or Claw integration.
