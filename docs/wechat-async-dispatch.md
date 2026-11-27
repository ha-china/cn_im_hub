# WeChat asynchronous dispatch (development)

- Keep upstream cursor + pending storage, add bounded 24-hour protocol-ID deduplication.
- Poll independently of conversations. At most two normal tasks per WeChat account and one per sender; normal turns preserve sender order.
- While a normal lane is busy, strict read-only YAML intent scripts may reply independently. No speculative execution, no LLM, no chat-history changes, no media parsing on this path. Action scripts, triggers, unmatched messages, attachments and unsupported recognition APIs remain in normal dispatch.
- Conversation deadline: 120 seconds; fast recognition/render deadlines: 5 seconds each. No automatic replay after an attempted conversation API call. A timeout does not prove an action was undone.
- Normal and fast replies can arrive out of order. Fast queries do not alter a slow conversation's typing indicator.
- Queue backpressure at 100 pending entries (a received batch may exceed the threshold); explicit message ID cache is bounded to 2048. Messages without IDs are not deduplicated by text.
- Preserve existing config entry IDs, service schemas, provider protocols and storage keys. No migration, no role/allowlist feature.
- A running message is not retried after a restart; queued messages are recovered, as in upstream. This is not exactly-once delivery.
- The HA recognition path was reviewed against Core 2026.11.3; earlier versions need compatibility testing. Media and device controls retain their existing semantics.
- Before release: validate with real HA, concurrent queries, multi-turn dialogue, duplicate IDs, restart/reload, and existing proactive notifications. The current branch is experimental, not an official release.
