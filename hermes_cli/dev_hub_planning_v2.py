"""Typed HTTP client for the Dev Hub Planning V2 runner boundary.

The client deliberately mirrors the Hub's idempotency model instead of adding
another one:

* thread create/input/bind requests are replay-safe by provider event origin;
* run creation is replay-safe only with the exact ``Idempotency-Key`` header;
* exact preview approval/apply is replay-safe by its bound turn + header;
* work completion is replay-safe by attempt/result identity;
* claim, failure, and resume are never retried after an ambiguous transport
  failure.

This module owns transport and wire typing only.  The model-facing, explicitly
opted-in planning action lives in :mod:`tools.dev_hub_planning_tool`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import os
import time
from typing import (
    Any,
    Callable,
    Generic,
    Iterator,
    Literal,
    Mapping,
    Optional,
    TypeVar,
    TypedDict,
    cast,
)
from urllib import error, parse, request

from hermes_cli.turn_origin import (
    TurnOriginV1,
    coerce_turn_origin,
    get_current_turn_origin,
)


PLANNING_V2_PREFIX = "/api/runner/planning/v2"
PLANNING_V2_ORIGIN_SCHEMA_VERSION = "1.0"
PLANNING_V2_CLIENT_VERSION = "1.0"
DEFAULT_DEV_HUB_URL = "http://127.0.0.1:4570"
DEFAULT_TIMEOUT_SECONDS = 20.0
DEFAULT_TRANSPORT_RETRIES = 1
DEFAULT_LEASE_SECONDS = 300
MAX_PREVIEW_PAGE_SIZE = 200


class PlanningOriginPayload(TypedDict):
    schemaVersion: Literal["1.0"]
    provider: str
    gatewayInstanceId: str
    gatewayAccountId: str
    chatId: str
    threadId: Optional[str]
    messageId: str
    senderId: str
    chatType: Optional[str]
    sourceTimestamp: Optional[str]
    providerEventId: str


class PlanningThreadDTO(TypedDict, total=False):
    schemaVersion: str
    kind: str
    threadId: str
    tenantId: str
    projectId: Optional[str]
    ownerUserId: str
    title: Optional[str]
    status: str
    policyVersion: str
    policy: dict[str, Any]
    headInputSequence: int
    activeRunId: Optional[str]
    activePreviewVersionId: Optional[str]
    stateVersion: int
    createdBy: str
    createdAt: str
    updatedAt: str
    terminalAt: Optional[str]


class PlanningWorkItemDTO(TypedDict, total=False):
    schemaVersion: str
    kind: str
    workItemId: str
    idempotencyKey: str
    threadId: str
    runId: str
    workKind: str
    scopeKey: str
    basisInputSequence: int
    policyVersion: str
    input: dict[str, Any]
    inputHash: str
    requiredCapabilities: list[str]
    routePolicy: dict[str, Any]
    priority: int
    status: str
    retryEpoch: int
    retryNotBefore: Optional[str]
    leaseHolder: Optional[str]
    leaseExpiresAt: Optional[str]
    leaseEpoch: int
    activeAttemptId: Optional[str]
    acceptedResultId: Optional[str]
    progress: dict[str, Any]
    progressFingerprint: Optional[str]
    lastProgressAt: Optional[str]
    terminalReason: Optional[str]
    supersededByWorkItemId: Optional[str]
    createdAt: str
    updatedAt: str
    terminalAt: Optional[str]


class PlanningWorkAttemptDTO(TypedDict, total=False):
    schemaVersion: str
    kind: str
    attemptId: str
    workItemId: str
    runId: str
    attemptNo: int
    retryEpoch: int
    leaseEpoch: int
    workerId: str
    inputHash: str
    modelProvider: Optional[str]
    modelName: Optional[str]
    modelRoute: Optional[str]
    routeMetadata: dict[str, Any]
    status: str
    progress: dict[str, Any]
    progressFingerprint: Optional[str]
    tokensInput: int
    tokensOutput: int
    startedAt: Optional[str]
    heartbeatAt: Optional[str]
    completedAt: Optional[str]
    createdAt: str
    updatedAt: str


class PlanningRunDTO(TypedDict, total=False):
    schemaVersion: str
    kind: str
    runId: str
    idempotencyKey: str
    threadId: str
    runSequence: int
    basisInputSequence: int
    inputDigest: str
    supersedesRunId: Optional[str]
    status: str
    policyVersion: str
    policy: dict[str, Any]
    routePolicy: dict[str, Any]
    stateVersion: int
    retryEpoch: int
    requestedBy: str
    correlationId: str
    traceId: Optional[str]
    workItems: list[PlanningWorkItemDTO]
    progress: dict[str, Any]
    createdAt: str
    updatedAt: str
    startedAt: Optional[str]
    completedAt: Optional[str]
    cancelledAt: Optional[str]


class PlanningWorkResultDTO(TypedDict, total=False):
    schemaVersion: str
    kind: str
    resultId: str
    idempotencyKey: str
    workItemId: str
    attemptId: str
    basisInputSequence: int
    leaseEpoch: int
    resultKind: str
    resultSchemaVersion: str
    result: dict[str, Any]
    resultHash: str
    provenance: dict[str, Any]
    disposition: str
    derivedFromResultId: Optional[str]
    createdAt: str
    updatedAt: str
    acceptedAt: Optional[str]


class PlanningWorkFailureDTO(TypedDict, total=False):
    schemaVersion: str
    kind: str
    failureId: str
    idempotencyKey: str
    workItemId: str
    attemptId: str
    errorCode: str
    failureClass: str
    failureFingerprint: str
    detail: dict[str, Any]
    retryable: bool
    consumesAttempt: bool
    retryDisposition: str
    retryEpoch: int
    occurredAt: str
    resolvedAt: Optional[str]
    resolution: Optional[str]
    createdAt: str
    updatedAt: str


class PlanningApplyDTO(TypedDict):
    ok: bool
    replayed: bool
    applyBindingId: str
    threadId: str
    runId: str
    previewResultId: str
    previewResultHash: str
    planHash: str
    basisInputSequence: int
    planId: str
    approvalId: str
    operationId: str
    status: str
    operation: Optional[dict[str, Any]]
    error: Optional[dict[str, Any]]
    createdAt: str
    updatedAt: str


class PlanningClaimResultContext(TypedDict):
    workItem: PlanningWorkItemDTO
    result: Optional[PlanningWorkResultDTO]


class PlanningClaimContext(TypedDict):
    run: PlanningRunDTO
    predecessors: list[PlanningClaimResultContext]
    acceptedRunResults: list[PlanningClaimResultContext]
    failures: list[PlanningWorkFailureDTO]


class PlanningThreadProjection(TypedDict):
    thread: PlanningThreadDTO
    bindings: list[dict[str, Any]]
    inputEventCount: int
    semanticEventHead: int


class PlanningThreadMutation(PlanningThreadProjection, total=False):
    inputEvent: dict[str, Any]
    binding: dict[str, Any]
    endpoint: dict[str, Any]
    duplicate: bool
    previewInvalidated: bool


class PlanningEventsProjection(TypedDict):
    threadId: str
    afterSequence: int
    events: list[dict[str, Any]]
    nextSequence: int


class PlanningInputsPage(TypedDict):
    threadId: str
    basisInputSequence: int
    inputs: list[dict[str, Any]]
    hasMore: bool
    nextAfterSequence: int


class PlanningPreviewPage(TypedDict):
    threadId: str
    runId: str
    previewResultId: str
    previewResultHash: str
    planHash: str
    basisInputSequence: int
    taskCount: int
    offset: int
    limit: int
    returned: int
    tasks: list[dict[str, Any]]
    hasMore: bool
    nextOffset: Optional[int]
    title: str
    objective: str
    summary: str
    decisions: list[dict[str, Any]]
    coverage: dict[str, Any]
    acceptedAt: str
    approvalEligible: bool


class PlanningClaimProjection(TypedDict):
    workItem: PlanningWorkItemDTO
    attempt: PlanningWorkAttemptDTO
    context: PlanningClaimContext


class PlanningWorkMutation(TypedDict, total=False):
    ok: bool
    replayed: bool
    progressAdvanced: bool
    leaseExpiresAt: str
    workItem: PlanningWorkItemDTO
    attempt: PlanningWorkAttemptDTO
    result: PlanningWorkResultDTO
    expandedWorkItems: list[PlanningWorkItemDTO]
    decision: dict[str, Any]
    run: PlanningRunDTO
    runState: Optional[str]


ResponseT = TypeVar("ResponseT")


@dataclass(frozen=True, slots=True)
class PlanningV2Response(Generic[ResponseT]):
    """One accepted Planning V2 HTTP response."""

    status: int
    payload: ResponseT
    transport_attempts: int = 1

    @property
    def replayed(self) -> bool:
        if self.status == 200 and isinstance(self.payload, Mapping):
            return any(
                self.payload.get(field) is True
                for field in ("duplicate", "replayed")
            )
        return False


class PlanningV2ClientError(RuntimeError):
    """Base typed failure for config, origin, transport, protocol, and Hub errors."""

    def __init__(
        self,
        code: str,
        *,
        status: Optional[int] = None,
        detail: Any = None,
        payload: Optional[dict[str, Any]] = None,
        retryable: bool = False,
        ambiguous: bool = False,
        attempts: int = 0,
    ) -> None:
        self.code = code
        self.status = status
        self.detail = detail if detail is not None else code
        self.payload = payload or {}
        self.retryable = retryable
        self.ambiguous = ambiguous
        self.attempts = attempts
        super().__init__(str(self.detail))

    def compact(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "ok": False,
            "code": self.code,
        }
        if self.status is not None:
            result["httpStatus"] = self.status
        if self.detail not in (None, "", self.code):
            result["detail"] = self.detail
        if self.retryable:
            result["retryable"] = True
        if self.ambiguous:
            result["outcomeAmbiguous"] = True
        if self.attempts:
            result["transportAttempts"] = self.attempts
        return result


class PlanningV2ConfigError(PlanningV2ClientError):
    """The local machine is not configured for the exact runner contract."""


class PlanningV2OriginError(PlanningV2ClientError):
    """The active turn cannot produce the Hub's attested TurnOriginV1."""


class PlanningV2TransportError(PlanningV2ClientError):
    """No HTTP response was received."""


class PlanningV2ProtocolError(PlanningV2ClientError):
    """The Hub returned a response outside the documented JSON contract."""


class PlanningV2HTTPError(PlanningV2ClientError):
    """The Hub returned a typed non-success response."""


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _text(value: Any) -> str:
    return str(value or "").strip()


def _runtime_env(name: str, *, prefer_dotenv: bool = False) -> str:
    """Resolve Agent Ops values without bypassing profile-scoped secrets."""

    try:
        if prefer_dotenv:
            from hermes_cli.config import get_env_value_prefer_dotenv

            return _text(get_env_value_prefer_dotenv(name))
        from hermes_cli.config import get_env_value

        return _text(get_env_value(name))
    except Exception:
        return _text(os.environ.get(name))


def _clean_base_url(raw: str) -> str:
    value = _text(raw).rstrip("/")
    parsed = parse.urlparse(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise PlanningV2ConfigError(
            "planning.hub_url_invalid",
            detail="AGENT_OPS_API_URL must be an http(s) origin without credentials, query, or fragment.",
        )
    return value


def _validate_source_timestamp(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = _text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PlanningV2OriginError(
            "planning.origin_invalid",
            detail="Turn origin source_timestamp must be ISO-8601.",
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PlanningV2OriginError(
            "planning.origin_invalid",
            detail="Turn origin source_timestamp must include a timezone.",
        )
    return text


def planning_origin_from_turn(
    origin: TurnOriginV1 | Mapping[str, Any] | None,
    *,
    runner_id: str,
) -> PlanningOriginPayload:
    """Map Hermes' scoped origin to the Hub's exact attested wire contract.

    Message content is never consulted.  Missing identity is rejected instead
    of guessed, because ``gatewayInstanceId`` is checked against the bearer
    token's runner and provider event identity is the write deduplication key.
    """

    normalized = coerce_turn_origin(origin)
    if normalized is None:
        raise PlanningV2OriginError(
            "planning.origin_missing",
            detail=(
                "This planning action needs an active gateway turn origin. "
                "Normal Hermes discussion remains available."
            ),
        )
    machine_id = _text(runner_id)
    if not machine_id:
        raise PlanningV2ConfigError(
            "planning.runner_id_missing",
            detail="AGENT_OPS_RUNNER_ID is required.",
        )
    provider = _text(normalized.provider)
    required = {
        "provider": provider,
        "gateway_account_id": _text(normalized.gateway_account_id),
        "chat_id": _text(normalized.chat_id),
        "message_id": _text(normalized.message_id),
        "sender_id": _text(normalized.sender_id),
        "event_id": _text(normalized.event_id),
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise PlanningV2OriginError(
            "planning.origin_incomplete",
            detail={"missing": missing},
        )
    if provider != provider.lower():
        raise PlanningV2OriginError(
            "planning.origin_invalid",
            detail="Turn origin provider must be lowercase.",
        )
    return {
        "schemaVersion": PLANNING_V2_ORIGIN_SCHEMA_VERSION,
        "provider": provider,
        "gatewayInstanceId": machine_id,
        "gatewayAccountId": required["gateway_account_id"],
        "chatId": required["chat_id"],
        "threadId": _text(normalized.thread_id) or None,
        "messageId": required["message_id"],
        "senderId": required["sender_id"],
        "chatType": _text(normalized.chat_type) or None,
        "sourceTimestamp": _validate_source_timestamp(
            normalized.source_timestamp
        ),
        "providerEventId": required["event_id"],
    }


def planning_origin_from_current_turn(*, runner_id: str) -> PlanningOriginPayload:
    return planning_origin_from_turn(
        get_current_turn_origin(),
        runner_id=runner_id,
    )


def derive_run_idempotency_key(
    *,
    runner_id: str,
    thread_id: str,
    provider: str,
    gateway_account_id: str,
    provider_event_id: str,
) -> str:
    """Derive one retry key from the Hub's complete source-event namespace."""

    identity = {
        "schemaVersion": PLANNING_V2_ORIGIN_SCHEMA_VERSION,
        "runnerId": _text(runner_id),
        "threadId": _text(thread_id),
        "provider": _text(provider),
        "gatewayAccountId": _text(gateway_account_id),
        "providerEventId": _text(provider_event_id),
    }
    if not all(identity.values()):
        raise PlanningV2OriginError(
            "planning.run_identity_incomplete",
            detail=(
                "runner, planning thread, provider account, and provider event "
                "identities are required."
            ),
        )
    digest = hashlib.sha256(_canonical_json(identity).encode("utf-8")).hexdigest()
    return f"hermes-planning-run-v1:{digest}"


def derive_approval_idempotency_key(
    *,
    runner_id: str,
    thread_id: str,
    preview_result_id: str,
    provider: str,
    gateway_account_id: str,
    provider_event_id: str,
) -> str:
    """Derive one approval replay key from the complete source-event scope.

    Approval and run admission intentionally use distinct namespaces.  The
    key includes the exact preview identity but never approval or planning
    message content.
    """

    identity = {
        "schemaVersion": PLANNING_V2_ORIGIN_SCHEMA_VERSION,
        "runnerId": _text(runner_id),
        "threadId": _text(thread_id),
        "previewResultId": _text(preview_result_id),
        "provider": _text(provider),
        "gatewayAccountId": _text(gateway_account_id),
        "providerEventId": _text(provider_event_id),
    }
    if not all(identity.values()):
        raise PlanningV2OriginError(
            "planning.approval_identity_incomplete",
            detail=(
                "runner, planning thread, preview, provider account, and "
                "provider event identities are required."
            ),
        )
    digest = hashlib.sha256(_canonical_json(identity).encode("utf-8")).hexdigest()
    return f"hermes-planning-approval-v1:{digest}"


def _is_timeout(exc: BaseException) -> bool:
    reason = getattr(exc, "reason", None)
    return (
        isinstance(exc, TimeoutError)
        or isinstance(reason, TimeoutError)
        or "timed out" in str(exc).lower()
        or "timed out" in str(reason).lower()
    )


class PlanningV2Client:
    """Exact-status synchronous client for ``/api/runner/planning/v2``."""

    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        runner_id: Optional[str] = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        max_transport_retries: int = DEFAULT_TRANSPORT_RETRIES,
        transport: Optional[Callable[..., Any]] = None,
        sleep: Optional[Callable[[float], None]] = None,
    ) -> None:
        self.base_url = _clean_base_url(
            base_url
            or _runtime_env("AGENT_OPS_API_URL")
            or DEFAULT_DEV_HUB_URL
        )
        self.token = _text(
            token
            if token is not None
            else _runtime_env("AGENT_OPS_RUNNER_TOKEN", prefer_dotenv=True)
        )
        self.runner_id = _text(
            runner_id
            if runner_id is not None
            else _runtime_env("AGENT_OPS_RUNNER_ID")
        )
        if not self.token:
            raise PlanningV2ConfigError(
                "planning.runner_token_missing",
                detail="AGENT_OPS_RUNNER_TOKEN is required.",
            )
        if "\r" in self.token or "\n" in self.token:
            raise PlanningV2ConfigError("planning.runner_token_invalid")
        if not self.runner_id:
            raise PlanningV2ConfigError(
                "planning.runner_id_missing",
                detail="AGENT_OPS_RUNNER_ID is required.",
            )
        self.timeout = max(0.1, float(timeout))
        self.max_transport_retries = max(0, int(max_transport_retries))
        self._transport = transport or request.urlopen
        self._sleep = sleep or time.sleep

    def current_origin(self) -> PlanningOriginPayload:
        return planning_origin_from_current_turn(runner_id=self.runner_id)

    def default_worker_id(self, suffix: str = "planning-1") -> str:
        local = _text(suffix)
        return self.runner_id if not local else f"{self.runner_id}:{local}"

    def _worker_id(self, value: Optional[str]) -> str:
        worker_id = _text(value) or self.default_worker_id()
        if (
            worker_id != self.runner_id
            and not worker_id.startswith(f"{self.runner_id}:")
        ):
            raise PlanningV2ConfigError(
                "planning.worker_id_invalid",
                detail=(
                    "workerId must equal AGENT_OPS_RUNNER_ID or be namespaced "
                    "under '<runnerId>:'."
                ),
            )
        return worker_id

    @staticmethod
    def _decode_payload(
        raw: bytes,
        *,
        status: int,
    ) -> Optional[dict[str, Any]]:
        if not raw:
            return None if status == 204 else {}
        try:
            decoded = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PlanningV2ProtocolError(
                "planning.hub_response_invalid",
                status=status,
                detail="Dev Hub returned invalid JSON.",
            ) from exc
        if not isinstance(decoded, dict):
            raise PlanningV2ProtocolError(
                "planning.hub_response_invalid",
                status=status,
                detail="Dev Hub response must be a JSON object.",
            )
        return decoded

    @staticmethod
    def _raise_http_error(
        *,
        status: int,
        payload: Optional[dict[str, Any]],
    ) -> None:
        body = payload or {}
        server_code = body.get("code")
        code = (
            str(server_code)
            if isinstance(server_code, str) and server_code
            else f"planning.http_{status}"
        )
        detail = body.get("detail", code)
        raise PlanningV2HTTPError(
            code,
            status=status,
            detail=detail,
            payload=body,
            retryable=status in {429, 502, 503, 504},
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: Optional[dict[str, Any]] = None,
        expected_statuses: frozenset[int] = frozenset({200}),
        idempotency_key: Optional[str] = None,
        retry_safe: bool = False,
    ) -> PlanningV2Response[Any]:
        encoded = (
            _canonical_json(body).encode("utf-8")
            if body is not None
            else None
        )
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
            "User-Agent": (
                f"hermes-agent/dev-hub-planning-v2-"
                f"{PLANNING_V2_CLIENT_VERSION}"
            ),
        }
        if encoded is not None:
            headers["Content-Type"] = "application/json"
        if idempotency_key is not None:
            key = _text(idempotency_key)
            if not key:
                raise PlanningV2ConfigError(
                    "planning.idempotency_key_required"
                )
            if "\r" in key or "\n" in key:
                raise PlanningV2ConfigError(
                    "planning.idempotency_key_invalid",
                    detail="Idempotency-Key cannot contain a newline.",
                )
            headers["Idempotency-Key"] = key

        attempts = 0
        while True:
            attempts += 1
            req = request.Request(
                f"{self.base_url}{path}",
                data=encoded,
                headers=headers,
                method=method,
            )
            response: Any = None
            try:
                response = self._transport(req, timeout=self.timeout)
                raw = response.read()
                status = int(response.status)
            except error.HTTPError as exc:
                response = exc
                raw = exc.read()
                status = int(exc.code)
            except (error.URLError, TimeoutError, OSError) as exc:
                can_retry = (
                    retry_safe
                    and attempts <= self.max_transport_retries
                )
                if can_retry:
                    self._sleep(min(0.5, 0.05 * (2 ** (attempts - 1))))
                    continue
                code = (
                    "planning.hub_timeout"
                    if _is_timeout(exc)
                    else "planning.hub_unreachable"
                )
                raise PlanningV2TransportError(
                    code,
                    detail=(
                        "Dev Hub request timed out."
                        if code.endswith("timeout")
                        else "Dev Hub could not be reached."
                    ),
                    retryable=retry_safe,
                    ambiguous=method != "GET",
                    attempts=attempts,
                ) from exc
            finally:
                if response is not None:
                    close = getattr(response, "close", None)
                    if callable(close):
                        close()

            payload = self._decode_payload(raw, status=status)
            if status not in expected_statuses:
                self._raise_http_error(status=status, payload=payload)
            return PlanningV2Response(
                status=status,
                payload=payload,
                transport_attempts=attempts,
            )

    @staticmethod
    def _quoted(value: str) -> str:
        text = _text(value)
        if not text:
            raise PlanningV2ConfigError(
                "planning.identifier_required"
            )
        return parse.quote(text, safe="")

    @staticmethod
    def _shape_error(
        response: PlanningV2Response[Any],
        *,
        context: str,
        detail: str,
    ) -> None:
        raise PlanningV2ProtocolError(
            "planning.hub_response_invalid",
            status=response.status,
            detail=f"{context}: {detail}",
        )

    @classmethod
    def _require_fields(
        cls,
        response: PlanningV2Response[Any],
        *,
        context: str,
        fields: Mapping[str, type],
    ) -> Mapping[str, Any]:
        payload = response.payload
        if not isinstance(payload, Mapping):
            cls._shape_error(
                response,
                context=context,
                detail="response must be an object",
            )
        for name, expected in fields.items():
            value = payload.get(name)
            valid = isinstance(value, expected)
            if expected is int and isinstance(value, bool):
                valid = False
            if not valid:
                cls._shape_error(
                    response,
                    context=context,
                    detail=f"{name} has an invalid type",
                )
        return payload

    @classmethod
    def _validate_thread_projection(
        cls,
        response: PlanningV2Response[Any],
        *,
        mutation: bool = False,
    ) -> PlanningV2Response[Any]:
        fields: dict[str, type] = {
            "thread": dict,
            "bindings": list,
            "inputEventCount": int,
            "semanticEventHead": int,
        }
        if mutation:
            fields["duplicate"] = bool
        payload = cls._require_fields(
            response,
            context="planning thread response",
            fields=fields,
        )
        thread = payload["thread"]
        if not isinstance(thread, Mapping) or not _text(thread.get("threadId")):
            cls._shape_error(
                response,
                context="planning thread response",
                detail="thread.threadId is required",
            )
        return response

    @classmethod
    def _validate_run_projection(
        cls,
        response: PlanningV2Response[Any],
    ) -> PlanningV2Response[Any]:
        cls._require_fields(
            response,
            context="planning run response",
            fields={
                "runId": str,
                "threadId": str,
                "status": str,
                "basisInputSequence": int,
            },
        )
        return response

    @classmethod
    def _validate_inputs_page(
        cls,
        response: PlanningV2Response[Any],
    ) -> PlanningV2Response[Any]:
        cls._require_fields(
            response,
            context="planning input page",
            fields={
                "threadId": str,
                "basisInputSequence": int,
                "inputs": list,
                "hasMore": bool,
                "nextAfterSequence": int,
            },
        )
        return response

    @classmethod
    def _validate_events_projection(
        cls,
        response: PlanningV2Response[Any],
    ) -> PlanningV2Response[Any]:
        cls._require_fields(
            response,
            context="planning events response",
            fields={
                "threadId": str,
                "afterSequence": int,
                "events": list,
                "nextSequence": int,
            },
        )
        return response

    @classmethod
    def _validate_preview_page(
        cls,
        response: PlanningV2Response[Any],
        *,
        thread_id: str,
        preview_result_id: str,
        offset: int,
    ) -> PlanningV2Response[Any]:
        payload = cls._require_fields(
            response,
            context="planning preview page",
            fields={
                "threadId": str,
                "runId": str,
                "previewResultId": str,
                "previewResultHash": str,
                "planHash": str,
                "basisInputSequence": int,
                "taskCount": int,
                "offset": int,
                "limit": int,
                "returned": int,
                "tasks": list,
                "hasMore": bool,
                "title": str,
                "objective": str,
                "summary": str,
                "decisions": list,
                "coverage": dict,
                "acceptedAt": str,
                "approvalEligible": bool,
            },
        )
        expected = {
            "threadId": _text(thread_id),
            "previewResultId": _text(preview_result_id),
            "offset": int(offset),
        }
        mismatches = {
            name: {"expected": value, "actual": payload.get(name)}
            for name, value in expected.items()
            if payload.get(name) != value
        }
        if mismatches:
            cls._shape_error(
                response,
                context="planning preview page",
                detail=f"identity mismatch: {mismatches}",
            )
        for name in (
            "threadId",
            "runId",
            "previewResultId",
            "previewResultHash",
            "planHash",
            "title",
            "acceptedAt",
        ):
            if not _text(payload[name]):
                cls._shape_error(
                    response,
                    context="planning preview page",
                    detail=f"{name} must be non-empty",
                )
        if (
            payload["basisInputSequence"] < 0
            or payload["taskCount"] < 0
            or payload["returned"] < 0
        ):
            cls._shape_error(
                response,
                context="planning preview page",
                detail=(
                    "basisInputSequence, taskCount, and returned must be "
                    "non-negative"
                ),
            )
        if payload["offset"] < 0 or payload["limit"] < 1:
            cls._shape_error(
                response,
                context="planning preview page",
                detail="offset must be non-negative and limit must be positive",
            )
        if any(not isinstance(task, Mapping) for task in payload["tasks"]):
            cls._shape_error(
                response,
                context="planning preview page",
                detail="every tasks entry must be an object",
            )
        if any(
            not isinstance(decision, Mapping)
            for decision in payload["decisions"]
        ):
            cls._shape_error(
                response,
                context="planning preview page",
                detail="every decisions entry must be an object",
            )
        if payload["returned"] != len(payload["tasks"]):
            cls._shape_error(
                response,
                context="planning preview page",
                detail="returned must equal the number of tasks",
            )
        next_offset = payload.get("nextOffset")
        if "nextOffset" not in payload or (
            next_offset is not None
            and (
                not isinstance(next_offset, int)
                or isinstance(next_offset, bool)
            )
        ):
            cls._shape_error(
                response,
                context="planning preview page",
                detail="nextOffset must be an integer or null",
            )
        if payload["hasMore"]:
            if next_offset is None or next_offset <= payload["offset"]:
                cls._shape_error(
                    response,
                    context="planning preview page",
                    detail="nextOffset must advance while hasMore is true",
                )
        elif next_offset is not None:
            cls._shape_error(
                response,
                context="planning preview page",
                detail="nextOffset must be null on the final page",
            )
        return response

    @classmethod
    def _validate_claim_projection(
        cls,
        response: PlanningV2Response[Any],
    ) -> PlanningV2Response[Any]:
        if response.status == 204:
            return response
        cls._require_fields(
            response,
            context="planning claim response",
            fields={
                "workItem": dict,
                "attempt": dict,
                "context": dict,
            },
        )
        return response

    @classmethod
    def _validate_work_mutation(
        cls,
        response: PlanningV2Response[Any],
    ) -> PlanningV2Response[Any]:
        cls._require_fields(
            response,
            context="planning work response",
            fields={"ok": bool},
        )
        return response

    @classmethod
    def _validate_apply_mutation(
        cls,
        response: PlanningV2Response[Any],
        *,
        thread_id: str,
        preview_result_id: str,
        preview_hash: str,
        plan_hash: str,
    ) -> PlanningV2Response[Any]:
        payload = cls._require_fields(
            response,
            context="planning preview approval response",
            fields={
                "ok": bool,
                "replayed": bool,
                "applyBindingId": str,
                "threadId": str,
                "runId": str,
                "previewResultId": str,
                "previewResultHash": str,
                "planHash": str,
                "basisInputSequence": int,
                "planId": str,
                "approvalId": str,
                "operationId": str,
                "status": str,
                "createdAt": str,
                "updatedAt": str,
            },
        )
        for name in (
            "applyBindingId",
            "threadId",
            "runId",
            "previewResultId",
            "previewResultHash",
            "planHash",
            "planId",
            "approvalId",
            "operationId",
            "status",
            "createdAt",
            "updatedAt",
        ):
            if not _text(payload[name]):
                cls._shape_error(
                    response,
                    context="planning preview approval response",
                    detail=f"{name} must be non-empty",
                )
        expected = {
            "threadId": _text(thread_id),
            "previewResultId": _text(preview_result_id),
            "previewResultHash": _text(preview_hash),
            "planHash": _text(plan_hash),
        }
        mismatches = {
            name: {"expected": value, "actual": payload.get(name)}
            for name, value in expected.items()
            if payload.get(name) != value
        }
        if mismatches:
            cls._shape_error(
                response,
                context="planning preview approval response",
                detail=f"identity mismatch: {mismatches}",
            )
        if payload["ok"] is not True:
            cls._shape_error(
                response,
                context="planning preview approval response",
                detail="ok must be true for a successful HTTP response",
            )
        for nullable_object in ("operation", "error"):
            if nullable_object not in payload:
                cls._shape_error(
                    response,
                    context="planning preview approval response",
                    detail=f"{nullable_object} is required",
                )
            value = payload.get(nullable_object)
            if value is not None and not isinstance(value, Mapping):
                cls._shape_error(
                    response,
                    context="planning preview approval response",
                    detail=f"{nullable_object} must be an object or null",
                )
        return response

    def create_thread(
        self,
        *,
        origin: PlanningOriginPayload,
        payload: dict[str, Any],
        input_kind: str = "message",
        title: Optional[str] = None,
        project_id: Optional[str] = None,
        policy_version: Optional[str] = None,
        policy: Optional[dict[str, Any]] = None,
    ) -> PlanningV2Response[PlanningThreadMutation]:
        body: dict[str, Any] = {
            "origin": dict(origin),
            "inputKind": _text(input_kind) or "message",
            "payload": payload,
            "policy": policy or {},
        }
        if title is not None:
            body["title"] = title
        if project_id is not None:
            body["projectId"] = project_id
        if policy_version is not None:
            body["policyVersion"] = policy_version
        response = self._request(
            "POST",
            f"{PLANNING_V2_PREFIX}/threads",
            body=body,
            expected_statuses=frozenset({200, 201}),
            retry_safe=True,
        )
        return cast(
            PlanningV2Response[PlanningThreadMutation],
            self._validate_thread_projection(response, mutation=True),
        )

    def get_thread(
        self,
        thread_id: str,
    ) -> PlanningV2Response[PlanningThreadProjection]:
        response = self._request(
            "GET",
            (
                f"{PLANNING_V2_PREFIX}/threads/"
                f"{self._quoted(thread_id)}"
            ),
            retry_safe=True,
        )
        return cast(
            PlanningV2Response[PlanningThreadProjection],
            self._validate_thread_projection(response),
        )

    def append_thread_input(
        self,
        thread_id: str,
        *,
        origin: PlanningOriginPayload,
        payload: dict[str, Any],
        input_kind: str = "message",
    ) -> PlanningV2Response[PlanningThreadMutation]:
        response = self._request(
            "POST",
            (
                f"{PLANNING_V2_PREFIX}/threads/"
                f"{self._quoted(thread_id)}/inputs"
            ),
            body={
                "origin": dict(origin),
                "inputKind": _text(input_kind) or "message",
                "payload": payload,
            },
            expected_statuses=frozenset({200, 201}),
            retry_safe=True,
        )
        return cast(
            PlanningV2Response[PlanningThreadMutation],
            self._validate_thread_projection(response, mutation=True),
        )

    def get_thread_inputs(
        self,
        thread_id: str,
        *,
        after_sequence: int = 0,
        limit: int = 100,
    ) -> PlanningV2Response[PlanningInputsPage]:
        sequence = int(after_sequence)
        page_size = int(limit)
        if sequence < 0:
            raise PlanningV2ConfigError(
                "planning.input_cursor_invalid"
            )
        if page_size < 1 or page_size > 500:
            raise PlanningV2ConfigError(
                "planning.input_page_size_invalid",
                detail="Dev Hub input page size must be between 1 and 500.",
            )
        query = parse.urlencode(
            {
                "afterSequence": sequence,
                "limit": page_size,
            }
        )
        response = self._request(
            "GET",
            (
                f"{PLANNING_V2_PREFIX}/threads/"
                f"{self._quoted(thread_id)}/inputs?{query}"
            ),
            retry_safe=True,
        )
        return cast(
            PlanningV2Response[PlanningInputsPage],
            self._validate_inputs_page(response),
        )

    def iter_thread_inputs(
        self,
        thread_id: str,
        *,
        after_sequence: int = 0,
        page_size: int = 500,
    ) -> Iterator[dict[str, Any]]:
        """Yield one immutable input revision without a client count cap."""

        cursor = int(after_sequence)
        basis_sequence: Optional[int] = None
        while True:
            page = self.get_thread_inputs(
                thread_id,
                after_sequence=cursor,
                limit=page_size,
            ).payload
            page_basis = int(page["basisInputSequence"])
            if basis_sequence is None:
                basis_sequence = page_basis
            for item in page["inputs"]:
                sequence = int(item.get("sequence", 0))
                if sequence <= basis_sequence:
                    yield item
            if cursor >= basis_sequence or any(
                int(item.get("sequence", 0)) >= basis_sequence
                for item in page["inputs"]
            ):
                return
            if not page["hasMore"]:
                return
            next_cursor = int(page["nextAfterSequence"])
            if next_cursor <= cursor:
                raise PlanningV2ProtocolError(
                    "planning.input_cursor_stalled",
                    detail="Dev Hub input pagination did not advance.",
                )
            cursor = next_cursor

    def get_thread_events(
        self,
        thread_id: str,
        *,
        after_sequence: int = 0,
    ) -> PlanningV2Response[PlanningEventsProjection]:
        sequence = int(after_sequence)
        if sequence < 0:
            raise PlanningV2ConfigError(
                "planning.event_cursor_invalid"
            )
        query = parse.urlencode({"afterSequence": sequence})
        response = self._request(
            "GET",
            (
                f"{PLANNING_V2_PREFIX}/threads/"
                f"{self._quoted(thread_id)}/events?{query}"
            ),
            retry_safe=True,
        )
        return cast(
            PlanningV2Response[PlanningEventsProjection],
            self._validate_events_projection(response),
        )

    def get_preview_page(
        self,
        thread_id: str,
        preview_result_id: str,
        *,
        offset: int = 0,
        limit: int = 50,
    ) -> PlanningV2Response[PlanningPreviewPage]:
        page_offset = int(offset)
        page_size = int(limit)
        if page_offset < 0:
            raise PlanningV2ConfigError(
                "planning.preview_offset_invalid"
            )
        if page_size < 1 or page_size > MAX_PREVIEW_PAGE_SIZE:
            raise PlanningV2ConfigError(
                "planning.preview_page_size_invalid",
                detail=(
                    "Dev Hub preview page size must be between 1 and "
                    f"{MAX_PREVIEW_PAGE_SIZE}."
                ),
            )
        query = parse.urlencode(
            {
                "offset": page_offset,
                "limit": page_size,
            }
        )
        response = self._request(
            "GET",
            (
                f"{PLANNING_V2_PREFIX}/threads/"
                f"{self._quoted(thread_id)}/previews/"
                f"{self._quoted(preview_result_id)}?{query}"
            ),
            retry_safe=True,
        )
        return cast(
            PlanningV2Response[PlanningPreviewPage],
            self._validate_preview_page(
                response,
                thread_id=thread_id,
                preview_result_id=preview_result_id,
                offset=page_offset,
            ),
        )

    def iter_preview_tasks(
        self,
        thread_id: str,
        preview_result_id: str,
        *,
        offset: int = 0,
        page_size: int = 100,
    ) -> Iterator[dict[str, Any]]:
        """Yield every preview task without a client-side task-count cap."""

        cursor = int(offset)
        identity: Optional[tuple[Any, ...]] = None
        while True:
            page = self.get_preview_page(
                thread_id,
                preview_result_id,
                offset=cursor,
                limit=page_size,
            ).payload
            page_identity = (
                page["threadId"],
                page["runId"],
                page["previewResultId"],
                page["previewResultHash"],
                page["planHash"],
                page["basisInputSequence"],
                page["taskCount"],
                page["acceptedAt"],
            )
            if identity is None:
                identity = page_identity
            elif page_identity != identity:
                raise PlanningV2ProtocolError(
                    "planning.preview_revision_changed",
                    detail=(
                        "Dev Hub preview identity changed during pagination."
                    ),
                )
            yield from page["tasks"]
            if not page["hasMore"]:
                return
            next_offset = page["nextOffset"]
            if next_offset is None:
                raise PlanningV2ProtocolError(
                    "planning.preview_cursor_missing",
                    detail=(
                        "Dev Hub omitted the next preview offset while more "
                        "tasks remain."
                    ),
                )
            if next_offset <= cursor:
                raise PlanningV2ProtocolError(
                    "planning.preview_cursor_stalled",
                    detail="Dev Hub preview pagination did not advance.",
                )
            cursor = next_offset

    def bind_thread(
        self,
        thread_id: str,
        *,
        origin: PlanningOriginPayload,
        delivery_mode: Literal[
            "primary", "subscribed", "muted"
        ] = "subscribed",
    ) -> PlanningV2Response[PlanningThreadMutation]:
        if delivery_mode not in {"primary", "subscribed", "muted"}:
            raise PlanningV2ConfigError(
                "planning.delivery_mode_invalid"
            )
        response = self._request(
            "POST",
            (
                f"{PLANNING_V2_PREFIX}/threads/"
                f"{self._quoted(thread_id)}/bindings"
            ),
            body={
                "origin": dict(origin),
                "deliveryMode": delivery_mode,
            },
            expected_statuses=frozenset({200, 201}),
            retry_safe=True,
        )
        return cast(
            PlanningV2Response[PlanningThreadMutation],
            self._validate_thread_projection(response, mutation=True),
        )

    def create_run(
        self,
        thread_id: str,
        *,
        idempotency_key: str,
        policy: Optional[dict[str, Any]] = None,
        route_policy: Optional[dict[str, Any]] = None,
        correlation_id: Optional[str] = None,
    ) -> PlanningV2Response[PlanningRunDTO]:
        response = self._request(
            "POST",
            (
                f"{PLANNING_V2_PREFIX}/threads/"
                f"{self._quoted(thread_id)}/runs"
            ),
            body={
                "policy": policy or {},
                "routePolicy": route_policy or {},
                "correlationId": correlation_id,
            },
            expected_statuses=frozenset({200, 201}),
            idempotency_key=idempotency_key,
            retry_safe=True,
        )
        return cast(
            PlanningV2Response[PlanningRunDTO],
            self._validate_run_projection(response),
        )

    def get_run(
        self,
        run_id: str,
    ) -> PlanningV2Response[PlanningRunDTO]:
        response = self._request(
            "GET",
            f"{PLANNING_V2_PREFIX}/runs/{self._quoted(run_id)}",
            retry_safe=True,
        )
        return cast(
            PlanningV2Response[PlanningRunDTO],
            self._validate_run_projection(response),
        )

    def approve_and_apply_preview(
        self,
        thread_id: str,
        preview_result_id: str,
        *,
        idempotency_key: str,
        origin: PlanningOriginPayload,
        expected_preview_hash: str,
        expected_plan_hash: str,
        approval_message: str,
        approval_evidence: Optional[dict[str, Any]] = None,
    ) -> PlanningV2Response[PlanningApplyDTO]:
        preview_hash = _text(expected_preview_hash)
        plan_hash = _text(expected_plan_hash)
        if not preview_hash or not plan_hash:
            raise PlanningV2ConfigError(
                "planning.approval_hash_required",
                detail=(
                    "expected_preview_hash and expected_plan_hash are required."
                ),
            )
        message = str(approval_message)
        if not message.strip():
            raise PlanningV2ConfigError(
                "planning.approval_message_required"
            )
        response = self._request(
            "POST",
            (
                f"{PLANNING_V2_PREFIX}/threads/"
                f"{self._quoted(thread_id)}/previews/"
                f"{self._quoted(preview_result_id)}/approve-apply"
            ),
            body={
                "origin": dict(origin),
                "expectedPreviewHash": preview_hash,
                "expectedPlanHash": plan_hash,
                "approvalMessage": message,
                "approvalEvidence": approval_evidence,
            },
            idempotency_key=idempotency_key,
            retry_safe=True,
        )
        return cast(
            PlanningV2Response[PlanningApplyDTO],
            self._validate_apply_mutation(
                response,
                thread_id=thread_id,
                preview_result_id=preview_result_id,
                preview_hash=preview_hash,
                plan_hash=plan_hash,
            ),
        )

    def claim_work(
        self,
        *,
        worker_id: Optional[str] = None,
        capabilities: Optional[list[str]] = None,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
        model_provider: Optional[str] = None,
        model_name: Optional[str] = None,
        model_route: Optional[str] = None,
        route_metadata: Optional[dict[str, Any]] = None,
    ) -> PlanningV2Response[Optional[PlanningClaimProjection]]:
        response = self._request(
            "POST",
            f"{PLANNING_V2_PREFIX}/work/claim",
            body={
                "workerId": self._worker_id(worker_id),
                "capabilities": list(capabilities or []),
                "leaseSeconds": int(lease_seconds),
                "modelProvider": model_provider,
                "modelName": model_name,
                "modelRoute": model_route,
                "routeMetadata": route_metadata or {},
            },
            expected_statuses=frozenset({200, 204}),
            retry_safe=False,
        )
        return cast(
            PlanningV2Response[Optional[PlanningClaimProjection]],
            self._validate_claim_projection(response),
        )

    def heartbeat_work(
        self,
        work_item_id: str,
        *,
        worker_id: str,
        attempt_id: str,
        lease_epoch: int,
        progress: dict[str, Any],
        tokens_input: int = 0,
        tokens_output: int = 0,
        lease_seconds: int = DEFAULT_LEASE_SECONDS,
    ) -> PlanningV2Response[PlanningWorkMutation]:
        response = self._request(
            "POST",
            (
                f"{PLANNING_V2_PREFIX}/work/"
                f"{self._quoted(work_item_id)}/heartbeat"
            ),
            body={
                "workerId": self._worker_id(worker_id),
                "attemptId": _text(attempt_id),
                "leaseEpoch": int(lease_epoch),
                "progress": progress,
                "tokensInput": int(tokens_input),
                "tokensOutput": int(tokens_output),
                "leaseSeconds": int(lease_seconds),
            },
            retry_safe=True,
        )
        return cast(
            PlanningV2Response[PlanningWorkMutation],
            self._validate_work_mutation(response),
        )

    def complete_work(
        self,
        work_item_id: str,
        *,
        worker_id: str,
        attempt_id: str,
        lease_epoch: int,
        result_kind: str,
        schema_version: str,
        result: dict[str, Any],
        provenance: Optional[dict[str, Any]] = None,
        degraded: bool = False,
    ) -> PlanningV2Response[PlanningWorkMutation]:
        response = self._request(
            "POST",
            (
                f"{PLANNING_V2_PREFIX}/work/"
                f"{self._quoted(work_item_id)}/complete"
            ),
            body={
                "workerId": self._worker_id(worker_id),
                "attemptId": _text(attempt_id),
                "leaseEpoch": int(lease_epoch),
                "resultKind": _text(result_kind),
                "schemaVersion": _text(schema_version),
                "result": result,
                "provenance": provenance or {},
                "degraded": bool(degraded),
            },
            retry_safe=True,
        )
        return cast(
            PlanningV2Response[PlanningWorkMutation],
            self._validate_work_mutation(response),
        )

    def fail_work(
        self,
        work_item_id: str,
        *,
        worker_id: str,
        attempt_id: str,
        lease_epoch: int,
        error_code: str,
        failure_class: str,
        detail: Optional[dict[str, Any]] = None,
        required_input: bool = True,
        fallback_routes_remaining: int = 0,
        fallback_allowed: bool = False,
        source_reacquire_available: bool = False,
    ) -> PlanningV2Response[PlanningWorkMutation]:
        response = self._request(
            "POST",
            (
                f"{PLANNING_V2_PREFIX}/work/"
                f"{self._quoted(work_item_id)}/fail"
            ),
            body={
                "workerId": self._worker_id(worker_id),
                "attemptId": _text(attempt_id),
                "leaseEpoch": int(lease_epoch),
                "errorCode": _text(error_code),
                "failureClass": _text(failure_class),
                "detail": detail or {},
                "requiredInput": bool(required_input),
                "fallbackRoutesRemaining": int(
                    fallback_routes_remaining
                ),
                "fallbackAllowed": bool(fallback_allowed),
                "sourceReacquireAvailable": bool(
                    source_reacquire_available
                ),
            },
            retry_safe=False,
        )
        return cast(
            PlanningV2Response[PlanningWorkMutation],
            self._validate_work_mutation(response),
        )

    def resume_work(
        self,
        work_item_id: str,
        *,
        reason: str,
    ) -> PlanningV2Response[PlanningWorkMutation]:
        response = self._request(
            "POST",
            (
                f"{PLANNING_V2_PREFIX}/work/"
                f"{self._quoted(work_item_id)}/resume"
            ),
            body={"reason": _text(reason)},
            retry_safe=False,
        )
        return cast(
            PlanningV2Response[PlanningWorkMutation],
            self._validate_work_mutation(response),
        )


__all__ = [
    "DEFAULT_LEASE_SECONDS",
    "MAX_PREVIEW_PAGE_SIZE",
    "PLANNING_V2_PREFIX",
    "PlanningClaimProjection",
    "PlanningClaimContext",
    "PlanningClaimResultContext",
    "PlanningEventsProjection",
    "PlanningInputsPage",
    "PlanningOriginPayload",
    "PlanningApplyDTO",
    "PlanningPreviewPage",
    "PlanningRunDTO",
    "PlanningThreadMutation",
    "PlanningThreadProjection",
    "PlanningV2Client",
    "PlanningV2ClientError",
    "PlanningV2ConfigError",
    "PlanningV2HTTPError",
    "PlanningV2OriginError",
    "PlanningV2ProtocolError",
    "PlanningV2Response",
    "PlanningV2TransportError",
    "PlanningWorkAttemptDTO",
    "PlanningWorkFailureDTO",
    "PlanningWorkItemDTO",
    "PlanningWorkMutation",
    "PlanningWorkResultDTO",
    "derive_approval_idempotency_key",
    "derive_run_idempotency_key",
    "planning_origin_from_current_turn",
    "planning_origin_from_turn",
]
