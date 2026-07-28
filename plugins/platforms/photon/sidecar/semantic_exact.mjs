import crypto from "node:crypto";

export const PHOTON_EXACT_WIRE_ENCODING =
  "photon-spectrum-markdown-v1";
export const PHOTON_EXACT_MAX_CODEPOINTS = 8000;

const DM_CHAT_GUID_RE = /^any;-;(\+\d{6,})$/;
const E164_RE = /^\+\d{6,}$/;

function safeIdentifier(value, maxLength = 240) {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= maxLength &&
    !/[\u0000-\u001f\u007f]/.test(value)
  );
}

export function photonExactRouteKind(spaceId) {
  if (!safeIdentifier(spaceId)) return null;
  return E164_RE.test(spaceId) || DM_CHAT_GUID_RE.test(spaceId)
    ? "dm_phone"
    : "space_id";
}

export function photonExactDigest({
  spaceId,
  text,
  deliveryId,
  deliveryUnit,
}) {
  return (
    "sha256:" +
    crypto
      .createHash("sha256")
      .update(
        [
          PHOTON_EXACT_WIRE_ENCODING,
          spaceId,
          deliveryId,
          String(deliveryUnit),
          text,
        ].join("\u0000"),
        "utf8"
      )
      .digest("hex")
  );
}

export class PhotonSemanticExactError extends Error {
  constructor(
    code,
    {
      providerWriteAttempted = false,
      retryable = false,
    } = {}
  ) {
    super(code);
    this.name = "PhotonSemanticExactError";
    this.code = code;
    this.providerWriteAttempted = providerWriteAttempted;
    this.retryable = retryable;
  }
}

/**
 * Resolve one frozen Photon space and perform exactly one true message write.
 *
 * The caller owns the HTTP response. This helper owns the provider boundary so
 * it can truthfully distinguish route-resolution failures (zero writes) from
 * an SDK error or malformed receipt after `space.send()` began (ambiguous).
 */
export async function sendPhotonSemanticExact({
  body,
  resolveExactSpace,
  markdown,
}) {
  const {
    spaceId,
    text,
    deliveryId,
    deliveryUnit,
    routeKind,
    wireEncoding,
    contentDigest,
  } = body || {};
  const expectedRouteKind = photonExactRouteKind(spaceId);
  const codepoints =
    typeof text === "string" ? Array.from(text).length : -1;
  if (
    !safeIdentifier(spaceId) ||
    typeof text !== "string" ||
    text.trim().length === 0 ||
    codepoints < 1 ||
    codepoints > PHOTON_EXACT_MAX_CODEPOINTS ||
    !safeIdentifier(deliveryId, 500) ||
    !Number.isSafeInteger(deliveryUnit) ||
    deliveryUnit < 0 ||
    routeKind !== expectedRouteKind ||
    wireEncoding !== PHOTON_EXACT_WIRE_ENCODING ||
    contentDigest !==
      photonExactDigest({ spaceId, text, deliveryId, deliveryUnit })
  ) {
    throw new PhotonSemanticExactError(
      "photon_semantic_exact_request_invalid"
    );
  }
  if (
    typeof resolveExactSpace !== "function" ||
    typeof markdown !== "function"
  ) {
    throw new PhotonSemanticExactError(
      "photon_semantic_exact_runtime_unavailable",
      { retryable: true }
    );
  }

  let space;
  try {
    space = await resolveExactSpace(spaceId, routeKind);
  } catch {
    throw new PhotonSemanticExactError(
      "photon_semantic_exact_route_unavailable",
      { retryable: true }
    );
  }
  if (!space || typeof space.send !== "function") {
    throw new PhotonSemanticExactError(
      "photon_semantic_exact_route_unavailable",
      { retryable: true }
    );
  }

  let builder;
  try {
    builder = markdown(text);
  } catch {
    throw new PhotonSemanticExactError(
      "photon_semantic_exact_encoding_failed"
    );
  }

  let result;
  try {
    // This is the sole true provider message write in the exact attempt.
    result = await space.send(builder);
  } catch {
    throw new PhotonSemanticExactError(
      "photon_semantic_exact_provider_ambiguous",
      { providerWriteAttempted: true }
    );
  }
  const messageId =
    typeof result?.id === "string" ? result.id.trim() : "";
  if (!safeIdentifier(messageId, 500)) {
    throw new PhotonSemanticExactError(
      "photon_semantic_exact_receipt_invalid",
      { providerWriteAttempted: true }
    );
  }
  return {
    messageId,
    deliveryId,
    deliveryUnit,
    routeKind,
    wireEncoding,
    contentDigest,
  };
}
