# Planning V2 exact provider-delivery matrix

Status: implementation coverage contract
Scope: every registered Hermes outbound adapter that can carry a Planning turn

## Contract

A provider is Planning-capable only when its concrete bound adapter owns a
`send_semantic_exact_attempt` implementation and a tested, versioned logical
content budget. A registry boolean, inherited ordinary `send`, synthetic
success, or local proxy acceptance is not delivery proof.

Every eligible row must prove:

1. success performs exactly one true provider message write;
2. validation and oversize rejection perform zero provider writes;
3. no probe, typing, reaction, chunk, redirect, retry, format fallback, or
   alternative-target fallback occurs inside the exact attempt;
4. explicit pre-acceptance throttling/rejection may be retryable only when the
   provider proves no message write began;
5. HTTP 5xx, timeout, connection reset, malformed receipt, or response loss
   after a write may have started is ambiguous unless a durable native
   idempotency primitive makes replay safe;
6. the semantic digest binds either exact provider bytes or canonical content
   plus a versioned deterministic encoder;
7. scheduler retry uses the same delivery ID, payload, encoder, route, and
   concrete exact method;
8. every primary and profile-bound adapter is enumerated at runtime. A
   nonconformant binding fails Planning preflight before manifest, ledger, or
   provider mutation;
9. a standalone exact sender is eligible only when the registry declaration
   owns a concrete callable. A boolean claim cannot make the ordinary
   standalone sender exact. The structurally eligible standalone set is
   Signal, Discord, Slack, and Matrix.

## Direct outbound adapters

| Provider | Safe logical budget | Exact primitive | Required exclusions / proof | Implementation class |
|---|---:|---|---|---|
| Signal | 8,000 | One direct signal-cli JSON-RPC HTTP POST through `_semantic_exact_rpc_send` | No generic `_rpc`, recipient lookup, or chunking; success requires a real positive non-boolean timestamp/ID | Existing exact path and standalone exact owner |
| Discord | 2,000 | One Create Message HTTP POST with deterministic nonce and `enforce_nonce` | Reject forum creation; no channel fetch; bounded nonce replay; 5xx ambiguous | Existing exact path |
| Matrix | 500–65,535, frozen per manifest | One `room_send` / `send_message_event` with stable transaction ID | Same transaction ID across process restart; wire-level replay proof | Existing exact path |
| Slack | 39,000 encoded, current conservative 16,000 | One direct `chat.postMessage` POST | No redirects; encoded mrkdwn budget; 5xx/transport/malformed receipt ambiguous | Existing exact path |
| Telegram | 4,096 UTF-16, current conservative 1,800 | One direct Bot API `sendMessage` POST | No redirects; encoded MarkdownV2 budget; 5xx/transport ambiguous | Existing exact path |
| WhatsApp Cloud | 4,000 Unicode code points | One Graph `/messages` POST using `whatsapp-cloud-text-json-v1` | No chunks/format rewrite/redirect; require real `wamid`; 5xx/transport/malformed receipt ambiguous | Implemented and provider-boundary tested |
| Home Assistant | 4,000 Unicode code points | One persistent-notification REST POST using `homeassistant-persistent-notification-json-v1` | No truncation/redirect; stable gateway receipt only after definite 2xx | Implemented and provider-boundary tested |
| IRC | 80 Unicode code points plus a 512-byte final frame cap | One single-line `PRIVMSG` write/drain using `irc-lf-to-u2028-utf8-v1` | Deterministic LF→U+2028 encoding; no line splitting/sleeps; socket loss ambiguous | Implemented and provider-boundary tested |
| Mattermost | 4,000 Unicode code points | One direct `/api/v4/posts` POST using `mattermost-post-json-v1` | Pre-resolved channel/root; no GET probe or broken-root flat fallback; require real post ID | Implemented and provider-boundary tested |
| ntfy | 4,000 Unicode code points | One publish POST using `ntfy-text-utf8-v1` | Exact UTF-8 body; require real returned ID; no synthetic UUID proof | Implemented and provider-boundary tested |
| SMS / Twilio | 1,400 Unicode code points | One `Messages.json` POST using `twilio-message-form-v1` | One exact form body; require SID; 5xx/transport/malformed receipt ambiguous | Implemented and provider-boundary tested |
| Email / SMTP | 50,000 Unicode code points | One `smtp.send_message` DATA transaction using `smtp-mime-text-utf8-v1` | Deterministic Message-ID and MIME body; no connection fallback or second DATA; success only after SMTP acceptance | Implemented and provider-boundary tested |
| WeCom WebSocket | 4,000 Unicode code points | One proactive `aibot_send_msg` frame using `wecom-aibot-send-msg-markdown-json-v1` | Deterministic correlated request ID; no reply/proactive fallback or truncation; ACK required | Implemented and provider-boundary tested |
| BlueBubbles | 4,000 Unicode code points | One `/api/v1/message/text` POST using `bluebubbles-text-json-v1` | Stable pre-resolved chat GUID and temp GUID; no lookup/create fallback; require real message GUID | Implemented and provider-boundary tested |
| LINE | 4,500 Unicode code points | One Push API text POST using `line-push-text-json-v1` | No reply-token consumption, reply→push fallback, batching, button cache, formatting, or chunking; require real sent-message ID | Implemented and provider-boundary tested |
| QQ Bot | 4,000 Unicode code points | One direct preselected c2c/group/guild REST POST using `qqbot-routed-message-json-v1` | Frozen chat type + text/markdown mode; cached token only; no route guessing, reconnect wait, token refresh, redirect, retry, or synthetic receipt | Implemented and provider-boundary tested |
| Weixin | 2,000 Unicode code points | One iLink final-text POST using `weixin-ilink-final-text-json-v1` | Stable client ID; current durable context token only; no media extraction, chunks, stale-token fallback, redirects, or internal rate-limit retry | Implemented and provider-boundary tested |
| Yuanbao | 4,000 Unicode code points | One TIM text protobuf c2c/group WebSocket request using `yuanbao-tim-text-protobuf-v1` | Stable correlated request ID; bypass OutboundManager chunks/retries, typing, and FINISH heartbeat; exact response ACK required | Implemented and provider-boundary tested |
| DingTalk | 20,000 Unicode code points | One session-webhook markdown POST using `dingtalk-session-webhook-markdown-json-v1` | Durable route stores only transport kind; expiring webhook stays live-memory only; no cards, sibling close, stream, reaction, redirects, fallback, or synthetic acceptance | Implemented, provider-boundary and fresh-process rebind tested |
| Feishu | 8,000 Unicode code points | One direct no-redirect REST message POST after pre-write tenant-token preparation using `feishu-text-json-v1` | Deterministic UUID; raw text JSON; no SDK retry, reply/create fallback, post→plain fallback, redirects, or chunks; require real `message_id`; persist thread or reply ID when used | Implemented and provider-boundary tested |
| Google Chat | 4,000 Unicode code points | One direct `messages.create.execute` using `google-chat-text-json-v1` | Deterministic `client-*` message ID; auth replay and redirects disabled; `num_retries=0`; exact thread uses `REPLY_MESSAGE_OR_FAIL`; require real message resource name | Implemented and provider-boundary tested |
| SimpleX | 8,000 Unicode code points | One correlated `APISendMessages` WebSocket command using `simplex-api-send-messages-json-v1` | Numeric pre-resolved contact/group ID; deterministic correlation ID; require exact echoed content, route, send direction, success status, and real `CIMeta.itemId`; timeout/reset/malformed ACK ambiguous; no resend | Implemented against the current official Bot API and provider-boundary tested |
| WeCom Callback | 2,048 Unicode code points | One no-redirect `message/send` POST after pre-write token preparation using `wecom-callback-text-json-v1` | Persist exact non-secret app+corp route; never rediscover/fall back to app zero; token rejection evicts cache for a later attempt but never reposts inside the same attempt; require real `msgid` | Implemented and provider-boundary tested |
| Relay | Not eligible today | Current connector returns only a generic `outbound_result` | Planning fails before mutation with `connector_true_provider_receipt_not_negotiated`; a local relay/WebSocket ACK is never provider delivery proof | Explicitly Planning-ineligible |
| Photon | 8,000 Unicode code points | One `/send-exact` sidecar call wrapping exactly one Spectrum `space.send(markdown(...))` using `photon-spectrum-markdown-v1` | Frozen DM/space resolution branch; no route fallback, retry, truncation, or plain-text fallback; require correlated digest/encoder/delivery identity and a real Photon message ID | Implemented at Python, sidecar, and provider-SDK boundary |
| WhatsApp bridge / Baileys | 4,096 UTF-16 code units | One local `/send-exact` call wrapping one `sock.sendMessage` using `whatsapp-baileys-text-json-v1` | Stable `3EB0…` key; no prefix, formatting, quote, split, sleep, link-preview probe, redirect, or fallback; require the same returned `key.id`; response loss is ambiguous | Implemented and pinned-bridge boundary tested |
| Teams | 28,000 Unicode code points | One `microsoft-teams-apps` ActivitySender client POST using `teams-botframework-markdown-json-v1` | Frozen conversation/reply route; no chunks or reply→flat fallback; require a real ResourceResponse ID and reject the SDK placeholder | Implemented and SDK-boundary tested |

## Indirect or response-plane surfaces

These rows must not claim ordinary provider-message conformance. They need a
separate durable response-plane receipt or must fail closed before Planning
mutation.

| Surface | Current behavior | Required classification |
|---|---|---|
| API Server | HTTP/SSE request-response plane has no persistent push channel after the request closes | Planning-ineligible: `response_plane_without_push_receipt`; rejected before manifest, ledger, or `send()` |
| MSGraph Webhook | Change-notification ingress only; `send()` merely logs | Planning-ineligible: `ingress_only_without_outbound_receipt`; rejected before manifest, ledger, or `send()` |
| Generic Webhook | Runtime route may log, create a GitHub comment, or delegate to a different adapter | Planning-ineligible: `delegated_destination_not_frozen`; exact delivery can be enabled only after the concrete destination/account/route is frozen before Planning staging |
| Raft | Raft CLI/UI owns the response plane while the adapter `send()` is a no-op success | Planning-ineligible: `external_cli_without_response_receipt`; rejected before manifest, ledger, or `send()` |
| Relay | The current connector handshake describes presentation limits, while `outbound_result` proves only that the connector answered | Planning-ineligible: `connector_true_provider_receipt_not_negotiated`; rejected before manifest, ledger, or relay `send()` |

## Implemented-row evidence

`tests/gateway/test_semantic_exact_additional_providers.py` exercises Home
Assistant, IRC, Mattermost, ntfy, and SMS/Twilio at their final HTTP, form, or
socket write boundary. `tests/gateway/test_semantic_exact_additional_providers_wave2.py`
does the same for WhatsApp Cloud, Email/SMTP, WeCom WebSocket, BlueBubbles, and
LINE. `tests/gateway/test_semantic_exact_wave3.py` covers QQ Bot, Weixin,
Yuanbao, and DingTalk, including DingTalk's real fresh-process loss/rebind
boundary and proof that its secret session webhook never enters the SQLite
route. `tests/gateway/test_semantic_exact_additional_providers_wave4.py`
covers Feishu, Google Chat, SimpleX, and WeCom Callback. SimpleX uses the
current official `APISendMessages` correlated `newChatItems` response and
accepts only its real `CIMeta.itemId`; its ordinary fire-and-forget path is
never used as Planning proof. Together they freeze the advertised encoder and conservative budget,
assert raw logical content against the actual wire JSON/body/form/MIME/frame,
and prove one-write success, zero-write validation/oversize/historical-encoder
rejection, no redirect follow, explicit pre-write throttling only, and
ambiguous 5xx/timeout/reset/malformed-receipt outcomes. Provider legacy suites
run alongside these focused tests to catch changes to ordinary delivery.

`tests/gateway/test_semantic_exact_photon.py` and
`plugins/platforms/photon/sidecar/semantic_exact.test.mjs` cover Photon on both
sides of the loopback boundary. The Python adapter freezes a flat DM/space
route, exact encoder, content digest, delivery ID, and unit in one no-redirect
`/send-exact` request. The sidecar validates those values before resolving one
deterministic space branch and performs exactly one Spectrum `space.send`.
Success requires the real Photon message ID plus the echoed encoder, digest,
route, delivery ID, and unit. Route-resolution failure is the only retryable
pre-write case; SDK failure, response loss, and malformed receipts are
ambiguous and never cause an in-attempt resend.

`tests/gateway/test_semantic_exact_whatsapp_teams.py` covers both
cross-boundary transports. Its Node child suite exercises the repo-owned
`/send-exact` helper, one Baileys call, raw content, explicit
`linkPreview: null`, stable key, UTF-16 rejection, invalid-receipt ambiguity,
and the package-lock pin to Baileys `7.0.0-rc13`. That pinned implementation
reuses the supplied `messageId` when handling protocol decryption retry
receipts, so retransmitted frames remain one logical WhatsApp message rather
than a second Hermes delivery attempt. The same Python suite proves Teams
uses one SDK ActivitySender call, never falls back from reply to flat send,
treats 429 as explicit rejection, and rejects the SDK's
`DO_NOT_USE_PLACEHOLDER_ID` receipt as post-write ambiguous.

Exact test receipts from 2026-07-28:

- `pytest` over every exact-provider, registry, and Planning-ineligible suite
  — 258 passed, 0 failed.
- `pytest tests/gateway/test_semantic_exact_photon.py`
  — 11 passed, 0 failed, including the spawned Node provider-boundary suite.
- `pytest tests/plugins/platforms/photon`
  — 111 passed, 0 failed.
- `pytest tests/gateway/relay tests/gateway/test_relay_capability_surface.py tests/gateway/test_relay_upstream_authz.py`
  — 194 passed, 0 failed.
- `scripts/run_tests.sh tests/gateway/test_semantic_exact_whatsapp_teams.py`
  — 13 passed, 0 failed. This includes the Node `/send-exact` child suite.
- `scripts/run_tests.sh tests/gateway/test_teams.py tests/gateway/test_whatsapp_formatting.py tests/gateway/test_whatsapp_connect.py tests/gateway/test_platform_registry.py tests/hermes_cli/test_semantic_delivery_retry.py`
  — 179 passed, 0 failed.
- `pytest tests/gateway/test_platform_registry.py`
  — 63 passed, proving callable-owned standalone eligibility and rejecting a
  boolean-only declaration.
- `pytest tests/tools/test_send_message_tool.py`
  — 182 passed, including the public Matrix/Slack semantic-media shape guard:
  an exact delivery contract plus local media fails before provider I/O.
- `pytest tests/gateway/test_planning_preview_delivery.py`
  — 42 passed, including fresh readback of bounded HTTP/native provider
  rejection evidence without losing the exact provider receipt identity.
- `pytest tests/hermes_cli/test_semantic_delivery.py tests/hermes_cli/test_semantic_delivery_process_convergence.py tests/hermes_cli/test_planning_preview_ack_outbox.py`
  — 45 passed. `test_semantic_delivery_retry.py` adds 7 passing automatic
  retry/evidence tests.

`tests/gateway/test_planning_ineligible_response_planes.py` binds the real API
Server, MSGraph Webhook, Generic Webhook, Raft, and Relay adapter classes into
runtime conformance enumeration. Each is conformant only through its explicit
provider-owned Planning-ineligible reason, and each has a mutation trap proving
preflight returns before segmentation manifest creation, semantic ledger
creation, retry staging, or provider `send()`. The Base integration assertion
also proves this action cannot be converted into a generic fallback message.

Relay remains deliberately closed. To become eligible, the connector must add
an additive handshake capability for every advertised `(platform, botId)`
identity containing the exact provider name, capability-contract version,
logical budget and length semantics, versioned wire encoder, and route-contract
version. The gateway exact action must carry the unchanged delivery ID,
delivery unit, canonical content digest, full encoding contract, and frozen
non-secret provider route. Its correlated result must distinguish a proven
zero-write rejection from an ambiguous attempted write and, on success, echo
the delivery ID, digest, and encoder while returning the true provider message
ID from the connector's concrete provider adapter. Generic `outbound_result`
success, relay-bus publication, WebSocket receipt, or a connector-generated ID
must never satisfy that contract. Until both repositories implement and test
that provider boundary, runtime preflight rejects Relay before manifest,
ledger, retry staging, or relay mutation.

## Bounded-field constraint receipts

No Planning field is silently sliced. A provider identifier outside its
accepted envelope is rejected before a write; an oversized free-form
diagnostic becomes structured evidence with its full SHA-256 digest, exact
byte/character counts, a forced-redacted preview, and explicit `truncated` /
`redacted` flags.

| Boundary | Receipt | Scope | Continuation | Retirement |
|---|---|---|---|---|
| Provider rejection preview: 200 characters; forced-redaction scan window: 8,192 characters | Provider bodies are untrusted, can contain credentials, and can be arbitrarily large. | One provider HTTP/sidecar rejection attached to a semantic attempt. The original body is never persisted. | `hermes.provider-rejection-evidence/1` retains provider, status, a required representation discriminator (`raw_bytes`, `utf8_text`, or `canonical_json`), SHA-256 and exact byte/character counts for that representation, a bounded redacted preview, scan-window count, and explicit truncation/redaction. `canonical_json` is used when an SDK exposes only a decoded value and is never described as original wire evidence. Retry/reject state and ambiguity classification remain unchanged. | Replace with a streaming forced-redactor plus encrypted, expiry-bound diagnostic artifact if operators need full-body retrieval. |
| Generic semantic/delivery error preview: 200 characters; redaction window: 8,192 | Exceptions and provider errors are untrusted and may be huge or secret-bearing. | Durable semantic `provider_error`, retry/group `last_error`, and legacy final-delivery failure evidence. | Short safe machine codes remain exact. Otherwise `hermes.bounded-diagnostic/1` or `hermes.delivery-error-evidence/1` retains the full digest and counts with explicit truncation/redaction; retry and group state continue automatically without operator repair. | Retire when all producers emit typed bounded error objects and no free-form exception crosses a durable boundary. |
| Exact provider route: at most 12 fields; key at most 64 protocol-safe characters; value at most 240 characters | This is a secret-free routing discriminator, not an arbitrary metadata/document channel. Bounding it prevents secrets, URLs, and unbounded payloads from entering the durable ledger. | One frozen `(provider, account, chat/thread/reply)` exact-attempt route. | Invalid routes fail before manifest/provider mutation; the unchanged payload remains in the local/R2-capable retry backend and can continue after the live adapter rebinds a valid route. | Replace field values with opaque route-reference records if a conformant provider needs a larger non-secret route. |
| Semantic identity envelopes: delivery ID 240; account ID 500; route identifier 4,096; completion reference 2,048 | Delivery IDs are idempotency keys; account/route/completion fields select authority. Controls, secrets, ambiguous whitespace, and unbounded values cannot be durable selectors. | One semantic delivery group and its frozen live route/completion callback. | Validation is typed and pre-write. Artifacts/payloads stay recoverable under the same profile; callers correct the selector and start a new immutable delivery identity rather than receiving a mutated/truncated one. | Replace large route/completion identifiers with content-addressed opaque records owned by their backend. |
| Provider message receipt ID: exact, with no generic length cap | A receipt ID is provider identity, so truncating or hashing it would fabricate a different message. Provider-native IDs can exceed another provider's documented limit. | One confirmed provider receipt from adapter through Hermes and Dev Hub readback. | The exact Unicode value is retained end to end. Empty IDs and control-bearing/NUL values are invalid receipts and remain ambiguous; length alone never mutates or rejects identity. | A provider-specific structural constraint may be added only from a published provider contract and a boundary test; there is no cross-provider retirement cap. |
| Protocol/provider route envelopes | IRC target 64 bytes and full wire frame 512 bytes; SMTP mailbox 320 and reply ID 998; LINE target 64; ntfy topic 256; Twilio E.164 7–15 digits; WhatsApp JID 240; Photon space 240; Teams reply 128 and conversation/message 1,024; WeCom route 256; BlueBubbles chat/reply 512. | Only the final provider-defined address/receipt field for one exact write. Content budgets are separately versioned in the provider matrix. | Every violation is a zero-write typed rejection. The durable semantic payload is preserved; the next attempt requires a valid provider address and never receives a sliced or guessed target. | Each envelope retires only when the provider publishes a larger bound and its provider-boundary tests prove the new exact bytes/identity. Provider-independent arbitrary metadata must instead use an opaque route reference. |
| Artifact snapshot filename: 240 UTF-8 bytes; completion text fields: 2,000 characters | Filesystem component limits and recovery-receipt parsing are safety boundaries, not content-loss limits. | Private local ingress-spool filename and small typed Hub completion identifiers only; artifact bytes are unaffected. | Unsafe/oversized names become `artifact-<content-digest>.bin`, preserving immutable bytes/checksum. Oversized completion fields reject the completion receipt and retain the live spool for replay. | Keep the filename fallback while local filesystems require bounded components; replace completion text with opaque receipt references if Hub identifiers exceed the envelope. |
| Planning-ineligible reason: 160 protocol-safe characters | This is a registry machine code, not operator prose. | One provider declaration, lower-case `[a-z0-9_.-]`. | Invalid declarations fail registration and therefore Planning preflight before mutation. Full operator explanation belongs in this matrix while the stable code remains actionable. | Version the declaration schema if machine codes require namespacing beyond 160 characters. |

Coverage anchors:

- `tests/gateway/test_semantic_exact_rejection_evidence.py` proves full digest,
  exact counts, forced redaction, and explicit truncation.
- The HTTP adapter suites prove structured evidence survives the real provider
  boundary without changing zero-write/ambiguous semantics.
- `tests/hermes_cli/test_semantic_delivery.py` proves permanent and retryable
  rejection evidence survives a fresh Python-process readback, while an
  long provider message IDs survive a fresh durable receipt without truncation.
- `tests/gateway/test_delivery_ledger.py` proves the legacy final-response
  ledger stores bounded structured error evidence instead of `error[:N]`.
- Dev Hub accepts only the exact versioned bounded-diagnostic,
  HTTP-rejection, or native-protocol-rejection schemas. Raw exception strings,
  impossible counts, extra-key smuggling, control/surrogate text, and
  credential-looking previews fail closed before durable projection.

Focused bounded-field receipt from 2026-07-28: the rejection-evidence,
HTTP-provider, DingTalk, Photon, semantic delivery/retry, and legacy delivery
ledger suites passed 191 tests with 0 failures; Ruff, compileall, and
`git diff --check` were also green.

## Release gate

The release conformance suite must enumerate all rows above and every active
primary/profile binding. A direct outbound binding is green only after its
provider-boundary tests cover success, zero-write validation, permanent
rejection, proven pre-write throttle, ambiguous post-write failures, redirect,
thread/target integrity, encoded-length segmentation, restart boundaries, and
scheduler identity replay. No unsupported bound adapter is silently downgraded
to a generic recovery message.
