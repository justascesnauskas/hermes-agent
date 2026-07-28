import assert from "node:assert/strict";
import test from "node:test";

import {
  PHOTON_EXACT_WIRE_ENCODING,
  PhotonSemanticExactError,
  photonExactDigest,
  sendPhotonSemanticExact,
} from "./semantic_exact.mjs";

function exactBody(overrides = {}) {
  const base = {
    spaceId: "any;-;+37060000000",
    text: "**Planning preview**\nApprove or revise.",
    deliveryId: "delivery-photon-1",
    deliveryUnit: 0,
    routeKind: "dm_phone",
    wireEncoding: PHOTON_EXACT_WIRE_ENCODING,
  };
  const body = { ...base, ...overrides };
  body.contentDigest =
    overrides.contentDigest ??
    photonExactDigest(body);
  return body;
}

test("exact Photon helper preserves content and performs one provider write", async () => {
  const sent = [];
  const space = {
    async send(builder) {
      sent.push(builder);
      return { id: "photon-message-123" };
    },
  };
  const result = await sendPhotonSemanticExact({
    body: exactBody(),
    resolveExactSpace: async () => space,
    markdown: (text) => ({ type: "markdown", text }),
  });

  assert.deepEqual(sent, [
    {
      type: "markdown",
      text: "**Planning preview**\nApprove or revise.",
    },
  ]);
  assert.equal(result.messageId, "photon-message-123");
  assert.equal(result.deliveryId, "delivery-photon-1");
  assert.equal(result.wireEncoding, PHOTON_EXACT_WIRE_ENCODING);
  assert.equal(result.contentDigest, exactBody().contentDigest);
});

test("invalid and oversize requests perform zero provider writes", async () => {
  let resolves = 0;
  let writes = 0;
  const invoke = (body) =>
    sendPhotonSemanticExact({
      body,
      resolveExactSpace: async () => {
        resolves += 1;
        return { send: async () => { writes += 1; } };
      },
      markdown: (text) => text,
    });

  await assert.rejects(
    invoke(exactBody({ text: "x".repeat(8001) })),
    (error) =>
      error instanceof PhotonSemanticExactError &&
      error.providerWriteAttempted === false
  );
  await assert.rejects(
    invoke(exactBody({ routeKind: "space_id" })),
    (error) =>
      error instanceof PhotonSemanticExactError &&
      error.providerWriteAttempted === false
  );
  assert.equal(resolves, 0);
  assert.equal(writes, 0);
});

test("route failure is safely retryable before the provider write", async () => {
  await assert.rejects(
    sendPhotonSemanticExact({
      body: exactBody(),
      resolveExactSpace: async () => {
        throw new Error("upstream unavailable");
      },
      markdown: (text) => text,
    }),
    (error) =>
      error instanceof PhotonSemanticExactError &&
      error.providerWriteAttempted === false &&
      error.retryable === true
  );
});

test("provider failure and malformed receipt are ambiguous without a resend", async () => {
  let writes = 0;
  const invoke = (result, error) =>
    sendPhotonSemanticExact({
      body: exactBody(),
      resolveExactSpace: async () => ({
        async send() {
          writes += 1;
          if (error) throw error;
          return result;
        },
      }),
      markdown: (text) => text,
    });

  await assert.rejects(
    invoke(null, new Error("response lost")),
    (error) =>
      error instanceof PhotonSemanticExactError &&
      error.providerWriteAttempted === true &&
      error.retryable === false
  );
  await assert.rejects(
    invoke({ id: null }),
    (error) =>
      error instanceof PhotonSemanticExactError &&
      error.providerWriteAttempted === true &&
      error.retryable === false
  );
  assert.equal(writes, 2);
});
