# Dev Hub Planning V2 in Hermes

Hermes exposes Dev Hub Planning V2 as one explicit, opt-in action. It does not
replace Hermes conversation, intercept ordinary messages, shadow existing
traffic, or replace the current Kanban/Agent Ops tasking tools.

## Enablement

The active Hermes profile must opt in for every surface that should expose the
action. A cross-provider thread therefore enables both providers explicitly:

```yaml
platform_toolsets:
  discord:
    - hermes-discord
    - planning_v2
  slack:
    - hermes-slack
    - planning_v2
```

Use the same shape with `cli` and `hermes-cli` for the CLI surface. Planning V2
is a configurable, default-off toolset; adding it for one provider does not
enable it for another provider.

The runner machine must provide the exact Dev Hub credentials:

```text
AGENT_OPS_API_URL=http://127.0.0.1:4570
AGENT_OPS_RUNNER_ID=<token-bound runner id>
AGENT_OPS_RUNNER_TOKEN=<runner bearer token>
```

`AGENT_OPS_RUNNER_ID` is not the gateway relay instance id. Dev Hub attests
that every planning origin's `gatewayInstanceId` equals the runner identity
bound to the bearer token. The tool remains absent when either the profile
opt-in or credentials are missing.

Each direct provider adapter also needs a stable, opaque account identifier.
It identifies the bot/account namespace across restarts and token rotations;
it is not a token and must not be derived from one:

```yaml
platforms:
  discord:
    extra:
      gateway_account_id: discord-production-bot
  slack:
    extra:
      gateway_account_id: slack-production-app
```

Without it, write actions fail closed with
`planning.origin_incomplete` before contacting Dev Hub.

Dev Hub must already have an active external-identity link for every provider
sender that can write to a planning thread. Discord and Slack sender identities
must resolve to the same Hub owner for a cross-provider continuation. The Hub
returns the typed 403 `planning.external_identity_not_linked` when a link is
missing, or `planning.external_identity_owner_mismatch` when it belongs to a
different user. An explicit thread id never bypasses either ownership check.

## User flow

The model-facing tool is `agent_ops_planning_v2`:

| Action | Effect |
|---|---|
| `create` | Persist the current scoped gateway input as a new planning thread and, by default, start a run. |
| `continue` | Append the current scoped gateway input to an explicit `thread_id`, bind that endpoint, invalidate a stale preview, and, by default, start a new run. |
| `status` | Read concise thread, run, work-progress, needs-decision, and preview facts. |
| `events` | Read semantic events after an explicit sequence cursor. |
| `start_run` | Start or replay a run for an explicit thread, including recovery after a lost response. |
| `approve_apply` | Approve one exact accepted preview from a later explicit user turn and request the existing canonical apply operation. |

Cross-provider continuation is intentional and explicit. For example, a
thread created from Discord can be continued from Slack by passing the same
Dev Hub `thread_id`. Hermes never guesses this relationship from matching
message text, usernames, channel names, or provider-local thread ids.

The write origin comes only from the scoped `TurnOriginV1` attached by the
gateway for that conversation turn. It contains provider, account, chat,
message, sender, timestamp, and stable provider-event identity. The scope is
reset after the turn, including exception paths, so a later turn cannot inherit
another user's origin.

Input payloads may contain arbitrary structured planning content and artifact
references. The Hermes client does not impose an input-history, artifact, or
work-item count ceiling. Paginated reads pin the first page's input basis, so a
concurrent append is picked up by the next read instead of mixing two planning
revisions. Page size is merely a transport control, not a semantic limit.
Artifact bytes and durability remain the Dev Hub storage plane's responsibility
rather than being hidden in a provider chat or an ephemeral Hermes process.

## Exact preview approval

`approve_apply` is deliberately narrower than the other actions. Hermes calls
it only after Dev Hub has returned all three immutable values:

- `preview_result_id`;
- `expected_preview_hash`;
- `expected_plan_hash`.

The user must explicitly approve that exact preview in a later conversation
turn. A request to plan, approval-like wording from an earlier turn, silence,
or the model's own judgment is never approval. During a real gateway turn,
Hermes sends the exact current `user_task` as `approvalMessage`; a
model-supplied `approval_message` cannot replace or rewrite it. The explicit
argument exists only for non-gateway callers and replay-focused tests.

The current scoped `TurnOriginV1` is mandatory. Dev Hub verifies that its
machine identity is attested by the runner token, its provider sender maps to
the thread owner, and its exact provider endpoint is actively bound to the
thread. This same rule supports intentional cross-provider approval: a Slack
turn can approve a preview created from Discord only after that Slack endpoint
was explicitly bound to the same planning thread.

Hermes derives the approval replay key from runner, planning thread, preview,
provider, provider account, and provider-event identity. It uses the distinct
`hermes-planning-approval-v1` namespace and never includes approval or planning
message content. Dev Hub checks the supplied preview and plan hashes again
inside canonical apply admission, so a concurrent edit, replacement preview,
changed input head, or registry drift requires a new preview instead of
applying stale work.

## Retry and ambiguity contract

Hermes retries only transport failures and only when the Hub operation has a
stable replay identity. It never converts an HTTP error into a retry.

| Operation | Automatic transport retry | Replay identity |
|---|---:|---|
| Thread/read/input/event reads | Yes, bounded | Read-only |
| Thread create | Yes, bounded | Scoped provider event |
| Input append / endpoint bind | Yes, bounded | Scoped provider event |
| Run create | Yes, bounded | Exact `Idempotency-Key` header |
| Exact preview approve/apply | Yes, bounded | Exact approval `Idempotency-Key` + bound provider event |
| Work heartbeat | Yes, bounded | Worker + attempt + lease epoch fence |
| Work complete | Yes, bounded | Deterministic attempt/result identity |
| Work claim | No | A lost response may already hold a lease |
| Work fail | No | Failure policy may have advanced the graph |
| Work resume | No | Resume may have already changed work state |

The default budget is one retry after the first failed transport attempt. The
canonical JSON bytes and idempotency key are reused exactly. HTTP status and
typed Hub codes are returned without being flattened into a generic failure.

Every non-GET timeout is reported as outcome-ambiguous. If the planning input
was stored but the run response remains unavailable, the tool returns a
machine-readable recovery action containing the same thread id and exact run
idempotency key. Replaying that action reads as the same run rather than
creating another one.

If an approval response is lost after Dev Hub commits it, the tool returns an
`approve_apply` recovery action with the same thread, preview id, both hashes,
approval evidence, exact approval message, and idempotency key. Replaying it
from the same scoped provider event returns the existing apply binding and
cannot enqueue a second Jira operation. Typed Hub rejections—including
same-turn approval, ambiguous intent, an unbound provider, stale hashes, and
registry drift—pass through unchanged and are not converted into retry loops.

## Progress surface

The status result avoids dumping the complete internal work graph. It returns:

- total, completed, active, and waiting work counts by status;
- progress facts for active or waiting work;
- structured `needsDecision` facts and reasons;
- active preview version/result facts;
- bound provider names and stable thread/run ids.

This keeps chat updates short while preserving enough structured state for
Hermes to explain what is happening, ask for a real decision, or continue the
same planning thread from another provider.
