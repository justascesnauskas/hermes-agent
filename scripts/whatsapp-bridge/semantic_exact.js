import { createHash } from 'crypto';

export const WHATSAPP_SEMANTIC_EXACT_CONTRACT =
  'whatsapp-baileys-text-json-v1';
export const WHATSAPP_SEMANTIC_EXACT_MAX_UTF16 = 4096;

export class SemanticExactBridgeError extends Error {
  constructor(message, { providerWriteAttempted, statusCode }) {
    super(message);
    this.name = 'SemanticExactBridgeError';
    this.providerWriteAttempted = providerWriteAttempted;
    this.statusCode = statusCode;
  }
}

function hasUnpairedSurrogate(value) {
  for (let index = 0; index < value.length; index += 1) {
    const unit = value.charCodeAt(index);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const following = value.charCodeAt(index + 1);
      if (!(following >= 0xdc00 && following <= 0xdfff)) return true;
      index += 1;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      return true;
    }
  }
  return false;
}

function invalidControl(value) {
  for (const character of value) {
    const codepoint = character.codePointAt(0);
    if (codepoint < 32 || codepoint === 127) return true;
  }
  return false;
}

function failValidation(message) {
  throw new SemanticExactBridgeError(message, {
    providerWriteAttempted: false,
    statusCode: 400,
  });
}

export function validateSemanticExactPayload(payload) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    failValidation('semantic exact payload must be an object');
  }
  const expectedKeys = [
    'chatId',
    'deliveryId',
    'encodingContract',
    'message',
  ];
  const actualKeys = Object.keys(payload).sort();
  if (
    actualKeys.length !== expectedKeys.length ||
    actualKeys.some((key, index) => key !== expectedKeys[index])
  ) {
    failValidation('semantic exact payload fields are invalid');
  }
  const { chatId, deliveryId, encodingContract, message } = payload;
  if (
    typeof chatId !== 'string' ||
    chatId.length < 3 ||
    chatId.length > 240 ||
    invalidControl(chatId) ||
    !/^[A-Za-z0-9_.:-]+@(s\.whatsapp\.net|g\.us|lid)$/.test(chatId)
  ) {
    failValidation('semantic exact chatId is invalid');
  }
  if (
    typeof deliveryId !== 'string' ||
    !deliveryId ||
    deliveryId.length > 512 ||
    invalidControl(deliveryId)
  ) {
    failValidation('semantic exact deliveryId is invalid');
  }
  if (encodingContract !== WHATSAPP_SEMANTIC_EXACT_CONTRACT) {
    failValidation('semantic exact encoding contract is invalid');
  }
  if (
    typeof message !== 'string' ||
    !message.trim() ||
    message.length > WHATSAPP_SEMANTIC_EXACT_MAX_UTF16 ||
    hasUnpairedSurrogate(message)
  ) {
    failValidation('semantic exact message is invalid');
  }
  return { chatId, deliveryId, encodingContract, message };
}

export function semanticExactMessageId(payload) {
  const validated = validateSemanticExactPayload(payload);
  const canonical = JSON.stringify({
    chatId: validated.chatId,
    deliveryId: validated.deliveryId,
    encodingContract: validated.encodingContract,
    message: validated.message,
  });
  return (
    '3EB0' +
    createHash('sha256').update(canonical, 'utf8').digest('hex')
      .toUpperCase().slice(0, 18)
  );
}

export async function sendSemanticExactText({ payload, sendMessage }) {
  const validated = validateSemanticExactPayload(payload);
  if (typeof sendMessage !== 'function') {
    failValidation('semantic exact sendMessage binding is invalid');
  }
  const expectedMessageId = semanticExactMessageId(validated);
  const sent = await sendMessage(
    validated.chatId,
    {
      text: validated.message,
      // An explicit null bypasses Baileys URL metadata lookup.
      linkPreview: null,
    },
    {
      // Baileys preserves this key for protocol-level retry receipts, so every
      // retransmission remains the same logical WhatsApp message.
      messageId: expectedMessageId,
    },
  );
  const providerMessageId =
    typeof sent?.key?.id === 'string' ? sent.key.id.trim() : '';
  if (providerMessageId !== expectedMessageId) {
    throw new SemanticExactBridgeError(
      'semantic exact provider receipt is invalid',
      {
        providerWriteAttempted: true,
        statusCode: 502,
      },
    );
  }
  return {
    messageId: providerMessageId,
    sent,
  };
}
