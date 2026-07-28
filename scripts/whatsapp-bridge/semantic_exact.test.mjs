import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

import {
  SemanticExactBridgeError,
  WHATSAPP_SEMANTIC_EXACT_CONTRACT,
  semanticExactMessageId,
  sendSemanticExactText,
} from './semantic_exact.js';

const basePayload = {
  chatId: '15551234567@s.whatsapp.net',
  deliveryId: 'preview_delivery_01',
  encodingContract: WHATSAPP_SEMANTIC_EXACT_CONTRACT,
  message: 'Frozen planning preview\nhttps://example.test/review',
};

test('exact bridge makes one raw Baileys call with stable key and no link probe', async () => {
  const calls = [];
  const result = await sendSemanticExactText({
    payload: basePayload,
    sendMessage: async (...args) => {
      calls.push(args);
      return { key: { id: args[2].messageId } };
    },
  });

  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0][0], basePayload.chatId);
  assert.deepEqual(calls[0][1], {
    text: basePayload.message,
    linkPreview: null,
  });
  assert.deepEqual(calls[0][2], { messageId: result.messageId });
  assert.equal(result.messageId, semanticExactMessageId(basePayload));
  assert.equal(result.messageId, '3EB0511011225520AE1D6A');
  assert.match(result.messageId, /^3EB0[A-F0-9]{18}$/);
});

test('same delivery identity remains one logical key across process retries', () => {
  const first = semanticExactMessageId(basePayload);
  const replay = semanticExactMessageId({ ...basePayload });
  const changedContent = semanticExactMessageId({
    ...basePayload,
    message: `${basePayload.message}.`,
  });
  const changedTarget = semanticExactMessageId({
    ...basePayload,
    chatId: '120363000000000000@g.us',
  });

  assert.equal(replay, first);
  assert.notEqual(changedContent, first);
  assert.notEqual(changedTarget, first);
});

test('UTF-16 budget and payload validation reject before Baileys', async () => {
  let calls = 0;
  const sendMessage = async (_chatId, _content, options) => {
    calls += 1;
    return { key: { id: options.messageId } };
  };
  const exactLimit = {
    ...basePayload,
    message: '🚀'.repeat(2048),
  };
  await sendSemanticExactText({ payload: exactLimit, sendMessage });
  assert.equal(calls, 1);

  await assert.rejects(
    sendSemanticExactText({
      payload: { ...exactLimit, message: `${exactLimit.message}x` },
      sendMessage,
    }),
    (error) =>
      error instanceof SemanticExactBridgeError &&
      error.providerWriteAttempted === false,
  );
  assert.equal(calls, 1);
});

test('missing or changed Baileys key is post-write ambiguous', async () => {
  let calls = 0;
  await assert.rejects(
    sendSemanticExactText({
      payload: basePayload,
      sendMessage: async () => {
        calls += 1;
        return { key: { id: 'different-provider-key' } };
      },
    }),
    (error) =>
      error instanceof SemanticExactBridgeError &&
      error.providerWriteAttempted === true &&
      error.statusCode === 502,
  );
  assert.equal(calls, 1);
});

test('bridge pins the audited Baileys rc13 protocol implementation', async () => {
  const packageJson = JSON.parse(
    await readFile(new URL('./package.json', import.meta.url), 'utf8'),
  );
  const packageLock = JSON.parse(
    await readFile(new URL('./package-lock.json', import.meta.url), 'utf8'),
  );
  const locked =
    packageLock.packages?.['node_modules/@whiskeysockets/baileys'];

  assert.equal(
    packageJson.dependencies['@whiskeysockets/baileys'],
    '7.0.0-rc13',
  );
  assert.equal(locked?.version, '7.0.0-rc13');
  assert.match(locked?.integrity || '', /^sha512-/);
});
