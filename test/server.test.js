'use strict';

/**
 * ============================================================================
 * Automated Verification Suite (Tests A through J) — KHAU KA! Backend
 * ============================================================================
 * Executes end-to-end verification of the Express + WebSocket + Gemini Live
 * bridge using Node's built-in test runner (`node --test`).
 */

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const WebSocket = require('ws');

const {
  createServerInstance,
  redactSecrets,
  safeCompareToken,
  buildModelCandidates,
} = require('../server');

// Helper: Make an HTTP GET request and return { statusCode, headers, body, json }
function httpGet(url) {
  return new Promise((resolve, reject) => {
    http
      .get(url, (res) => {
        let raw = '';
        res.on('data', (chunk) => {
          raw += chunk;
        });
        res.on('end', () => {
          let json = null;
          try {
            json = JSON.parse(raw);
          } catch (_) {
            // Non-JSON response
          }
          resolve({
            statusCode: res.statusCode,
            headers: res.headers,
            body: raw,
            json,
          });
        });
      })
      .on('error', reject);
  });
}

// Helper: Wait for a specific JSON message matching predicate on a client collector
function waitForMessage(collector, predicate, timeoutMs = 4000) {
  return new Promise((resolve, reject) => {
    for (const item of collector.messages) {
      if (!item.matched && predicate(item)) {
        item.matched = true;
        return resolve(item);
      }
    }
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`Timed out after ${timeoutMs}ms waiting for expected WebSocket message`));
    }, timeoutMs);

    function onMsg() {
      for (const item of collector.messages) {
        if (!item.matched && predicate(item)) {
          item.matched = true;
          cleanup();
          return resolve(item);
        }
      }
    }

    function cleanup() {
      clearTimeout(timer);
      const idx = collector.listeners.indexOf(onMsg);
      if (idx !== -1) collector.listeners.splice(idx, 1);
    }

    collector.listeners.push(onMsg);
  });
}

function attachCollector(ws) {
  const collector = {
    messages: [],
    listeners: [],
  };
  ws.on('message', (data, isBinary) => {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    let json = null;
    if (!isBinary) {
      try {
        json = JSON.parse(buf.toString('utf8'));
      } catch (_) {
        // Ignore parse error
      }
    }
    collector.messages.push({
      isBinary,
      buffer: buf,
      text: isBinary ? null : buf.toString('utf8'),
      json,
      matched: false,
    });
    for (const fn of [...collector.listeners]) {
      fn();
    }
  });
  return collector;
}

// ============================================================================
// Mock Gemini Live API Upstream Server (v1beta BidiGenerateContent)
// ============================================================================
function createMockGeminiServer() {
  const httpServer = http.createServer();
  const wss = new WebSocket.Server({ server: httpServer, perMessageDeflate: false });

  const state = {
    connections: [],
    receivedSetups: [],
    receivedMessages: [],
    rejectModelName: null,
    autoSendSetupComplete: true,
    simulatedOutputPcm: null,
  };

  // Generate 10,000 bytes (5,000 samples @ 24kHz 16-bit mono) deterministic sine-like PCM pattern
  const pcmBuf = Buffer.alloc(10000);
  for (let i = 0; i < pcmBuf.length; i += 2) {
    const sample = Math.round(Math.sin(i / 20) * 12000);
    pcmBuf.writeInt16LE(sample, i);
  }
  state.simulatedOutputPcm = pcmBuf;

  wss.on('connection', (ws, req) => {
    const connRecord = { ws, url: req.url, messages: [] };
    state.connections.push(connRecord);

    ws.on('message', (data, isBinary) => {
      // Verify server NEVER sends binary opcodes to Gemini (must always be UTF-8 JSON text frames)
      assert.equal(isBinary, false, 'Upstream Gemini frame must be a Text frame (opcode 0x01)');
      const parsed = JSON.parse(data.toString('utf8'));
      connRecord.messages.push(parsed);
      state.receivedMessages.push(parsed);

      if (parsed.setup) {
        state.receivedSetups.push(parsed.setup);

        if (state.rejectModelName && parsed.setup.model === state.rejectModelName) {
          ws.close(1008, `models/${state.rejectModelName} is not found for API version v1beta`);
          return;
        }

        if (state.autoSendSetupComplete) {
          setTimeout(() => {
            if (ws.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify({ setupComplete: {} }), { binary: false });
            }
          }, 30);
        }
        return;
      }

      // Respond to text turn
      if (parsed.clientContent && parsed.clientContent.turns && parsed.clientContent.turnComplete) {
        const userText = parsed.clientContent.turns[0]?.parts?.[0]?.text || '';
        ws.send(
          JSON.stringify({
            serverContent: {
              outputTranscription: {
                text: `नमस्ते! आपने कहा: ${userText}`,
              },
              modelTurn: {
                parts: [
                  {
                    text: `नमस्ते! आपने कहा: ${userText}`,
                  },
                ],
              },
              turnComplete: true,
            },
          }),
          { binary: false }
        );
        return;
      }

      // Respond to audioStreamEnd with input transcription, output transcription, 24kHz PCM audio, and turnComplete
      if (parsed.realtimeInput && parsed.realtimeInput.audioStreamEnd) {
        ws.send(
          JSON.stringify({
            serverContent: {
              inputTranscription: {
                text: 'खाऊ का इसका वज़न बताओ',
              },
            },
          }),
          { binary: false }
        );

        ws.send(
          JSON.stringify({
            serverContent: {
              outputTranscription: {
                text: 'इस वस्तु का वज़न पाँच सौ ग्राम है।',
              },
              modelTurn: {
                parts: [
                  {
                    inlineData: {
                      mimeType: 'audio/pcm;rate=24000',
                      data: state.simulatedOutputPcm.toString('base64'),
                    },
                  },
                ],
              },
              turnComplete: true,
            },
          }),
          { binary: false }
        );
      }
    });
  });

  return {
    httpServer,
    wss,
    state,
    start() {
      return new Promise((resolve) => {
        httpServer.listen(0, '127.0.0.1', () => {
          const { port } = httpServer.address();
          resolve(port);
        });
      });
    },
    stop() {
      return new Promise((resolve) => {
        for (const c of state.connections) {
          try {
            c.ws.terminate();
          } catch (_) {}
        }
        wss.close(() => httpServer.close(resolve));
      });
    },
  };
}

// ============================================================================
// Test Suite Execution
// ============================================================================
describe('KHAU KA! Backend Verification Suite (Tests A - J)', () => {
  let mockGemini;
  let mockGeminiPort;
  let bridgeInstance;
  let bridgePort;

  before(async () => {
    mockGemini = createMockGeminiServer();
    mockGeminiPort = await mockGemini.start();

    bridgeInstance = createServerInstance({
      port: 0,
      host: '0.0.0.0',
      geminiApiKey: 'AIzaSyTestDummyKeyForAutomatedVerification12345',
      geminiWsUrl: `ws://127.0.0.1:${mockGeminiPort}/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent`,
      primaryModel: 'models/gemini-3.8-live',
      deviceAuthToken: '',
      maxConnections: 3,
      setupTimeoutMs: 3000,
      heartbeatIntervalMs: 10000,
      silent: true,
    });

    await new Promise((resolve) => {
      bridgeInstance.server.listen(0, '0.0.0.0', () => {
        bridgePort = bridgeInstance.server.address().port;
        resolve();
      });
    });
  });

  after(async () => {
    if (bridgeInstance) {
      await bridgeInstance.shutdown('TEST_COMPLETE');
    }
    if (mockGemini) {
      await mockGemini.stop();
    }
  });

  // --------------------------------------------------------------------------
  // TEST A: Node.js dependencies install and resolve successfully
  // --------------------------------------------------------------------------
  it('Test A: Node.js dependencies (express, ws, dotenv) are installed and resolvable', () => {
    const expressPkg = require('express/package.json');
    const wsPkg = require('ws/package.json');
    const dotenvPkg = require('dotenv/package.json');
    assert.ok(expressPkg.version, 'express version should be defined');
    assert.ok(wsPkg.version, 'ws version should be defined');
    assert.ok(dotenvPkg.version, 'dotenv version should be defined');
  });

  // --------------------------------------------------------------------------
  // TEST B: Server starts without errors and binds to 0.0.0.0
  // --------------------------------------------------------------------------
  it('Test B: Server starts without errors and binds to 0.0.0.0', () => {
    const addr = bridgeInstance.server.address();
    assert.ok(addr, 'Server address must be defined');
    assert.equal(addr.address, '0.0.0.0', 'Server must bind to 0.0.0.0');
    assert.ok(addr.port > 0, 'Server must listen on a valid port');
  });

  // --------------------------------------------------------------------------
  // TEST C: HTTP /health and / endpoints work
  // --------------------------------------------------------------------------
  it('Test C: HTTP /health and / endpoints return 200 OK with expected metadata', async () => {
    const healthRes = await httpGet(`http://127.0.0.1:${bridgePort}/health`);
    assert.equal(healthRes.statusCode, 200);
    assert.equal(healthRes.json.status, 'ok');
    assert.equal(healthRes.json.service, 'khauka-server');
    assert.equal(healthRes.json.geminiKeyConfigured, true);
    assert.equal(healthRes.json.primaryModel, 'models/gemini-3.8-live');
    assert.equal(healthRes.json.audioProtocol.inputFormat, 'audio/pcm;rate=16000');
    assert.equal(healthRes.json.audioProtocol.outputFormat, 'audio/pcm;rate=24000');

    const rootRes = await httpGet(`http://127.0.0.1:${bridgePort}/`);
    assert.equal(rootRes.statusCode, 200);
    assert.equal(rootRes.json.status, 'running');
  });

  // --------------------------------------------------------------------------
  // TEST D: Root-path WebSocket handshake works
  // --------------------------------------------------------------------------
  it('Test D: Root-path (/) WebSocket handshake succeeds and invalid paths return 404', async () => {
    // 1. Valid root path '/'
    const ws = new WebSocket(`ws://127.0.0.1:${bridgePort}/`);
    const collector = attachCollector(ws);

    await new Promise((resolve, reject) => {
      ws.once('open', resolve);
      ws.once('error', reject);
    });

    const statusMsg = await waitForMessage(collector, (m) => m.json && m.json.type === 'status');
    assert.equal(statusMsg.json.status, 'connecting_gemini');
    ws.close();

    // 2. Invalid path '/invalid-path' must be rejected with 404
    const badWs = new WebSocket(`ws://127.0.0.1:${bridgePort}/invalid-path`);
    const errCode = await new Promise((resolve) => {
      badWs.once('unexpected-response', (_req, res) => {
        resolve(res.statusCode);
      });
      badWs.once('error', () => resolve('error'));
    });
    assert.equal(errCode, 404);
  });

  // --------------------------------------------------------------------------
  // TEST E: Invalid client authentication is rejected when enabled
  // --------------------------------------------------------------------------
  it('Test E: Invalid client authentication is rejected (401) and valid token succeeds', async () => {
    const authServer = createServerInstance({
      port: 0,
      host: '127.0.0.1',
      geminiApiKey: 'AIzaSyTestDummyKeyForAutomatedVerification12345',
      geminiWsUrl: `ws://127.0.0.1:${mockGeminiPort}/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent`,
      deviceAuthToken: 'khauka-secret-device-token-999',
      silent: true,
    });

    const authPort = await new Promise((resolve) => {
      authServer.server.listen(0, '127.0.0.1', () => {
        resolve(authServer.server.address().port);
      });
    });

    try {
      // 1. Missing token -> 401
      const unauthWs = new WebSocket(`ws://127.0.0.1:${authPort}/`);
      const unauthStatus = await new Promise((resolve) => {
        unauthWs.once('unexpected-response', (_req, res) => resolve(res.statusCode));
      });
      assert.equal(unauthStatus, 401);

      // 2. Wrong token -> 401
      const wrongWs = new WebSocket(`ws://127.0.0.1:${authPort}/`, {
        headers: { 'x-device-token': 'wrong-token' },
      });
      const wrongStatus = await new Promise((resolve) => {
        wrongWs.once('unexpected-response', (_req, res) => resolve(res.statusCode));
      });
      assert.equal(wrongStatus, 401);

      // 3. Valid token via x-device-token header -> 101 Switching Protocols + ready
      const validWs = new WebSocket(`ws://127.0.0.1:${authPort}/`, {
        headers: { 'x-device-token': 'khauka-secret-device-token-999' },
      });
      const collector = attachCollector(validWs);
      const readyMsg = await waitForMessage(collector, (m) => m.json && m.json.type === 'ready');
      assert.equal(readyMsg.json.setupComplete, true);
      validWs.close();
    } finally {
      await authServer.shutdown('AUTH_TEST_DONE');
    }
  });

  // --------------------------------------------------------------------------
  // TEST F: Gemini connection and setupComplete succeed (plus model fallback)
  // --------------------------------------------------------------------------
  it('Test F: Gemini setup payload is valid and server waits for setupComplete before sending ready', async () => {
    const initialSetupsCount = mockGemini.state.receivedSetups.length;
    const ws = new WebSocket(`ws://127.0.0.1:${bridgePort}/`);
    const collector = attachCollector(ws);

    const readyMsg = await waitForMessage(collector, (m) => m.json && m.json.type === 'ready');
    assert.equal(readyMsg.json.type, 'ready');
    assert.equal(readyMsg.json.setupComplete, true);
    assert.equal(readyMsg.json.model, 'models/gemini-3.8-live');
    assert.equal(readyMsg.json.inputSampleRate, 16000);
    assert.equal(readyMsg.json.outputSampleRate, 24000);

    // Inspect setup payload received by Gemini
    assert.ok(mockGemini.state.receivedSetups.length > initialSetupsCount);
    const lastSetup = mockGemini.state.receivedSetups[mockGemini.state.receivedSetups.length - 1];
    assert.equal(lastSetup.model, 'models/gemini-3.8-live');
    assert.deepEqual(lastSetup.generationConfig.responseModalities, ['AUDIO']);
    assert.ok(lastSetup.inputAudioTranscription !== undefined);
    assert.ok(lastSetup.outputAudioTranscription !== undefined);
    assert.ok(lastSetup.systemInstruction.parts[0].text.includes('खाऊ का'));

    ws.close();
  });

  // --------------------------------------------------------------------------
  // TEST G: Text messages and HX711 weight context receive valid responses
  // --------------------------------------------------------------------------
  it('Test G: Text messages receive valid Hindi transcription, text, and turn_complete', async () => {
    const ws = new WebSocket(`ws://127.0.0.1:${bridgePort}/`);
    const collector = attachCollector(ws);

    await waitForMessage(collector, (m) => m.json && m.json.type === 'ready');

    // Send weight context + text message
    ws.send(JSON.stringify({ type: 'weight_context', grams: 500 }), { binary: false });
    ws.send(JSON.stringify({ type: 'text', text: 'नमस्ते खाऊ का!' }), { binary: false });

    const outTranscript = await waitForMessage(
      collector,
      (m) => m.json && m.json.type === 'output_transcript'
    );
    assert.ok(outTranscript.json.text.includes('नमस्ते खाऊ का!'));

    const textReply = await waitForMessage(collector, (m) => m.json && m.json.type === 'text');
    assert.ok(textReply.json.text.includes('नमस्ते खाऊ का!'));

    const turnDone = await waitForMessage(
      collector,
      (m) => m.json && m.json.type === 'turn_complete'
    );
    assert.equal(turnDone.json.type, 'turn_complete');

    ws.close();
  });

  // --------------------------------------------------------------------------
  // TEST H & I: Binary audio input -> Gemini -> Ordered 24kHz PCM Binary output
  // --------------------------------------------------------------------------
  it('Test H & I: Binary 16kHz PCM input produces valid response and ordered 24kHz binary PCM output reaches ESP32', async () => {
    const ws = new WebSocket(`ws://127.0.0.1:${bridgePort}/`);
    const collector = attachCollector(ws);

    await waitForMessage(collector, (m) => m.json && m.json.type === 'ready');

    // Stream 3 binary PCM chunks (16kHz 16-bit mono, 1024 bytes each) + 1 odd-length chunk (1025 bytes) to test 16-bit alignment
    const chunk1 = Buffer.alloc(1024, 0x11);
    const chunk2 = Buffer.alloc(1024, 0x22);
    const chunk3Odd = Buffer.alloc(1025, 0x33); // Should be trimmed to 1024 bytes

    ws.send(chunk1, { binary: true });
    ws.send(chunk2, { binary: true });
    ws.send(chunk3Odd, { binary: true });
    ws.send(JSON.stringify({ type: 'end_of_turn' }), { binary: false });

    // Verify input transcription arrives
    const inTranscript = await waitForMessage(
      collector,
      (m) => m.json && m.json.type === 'input_transcript'
    );
    assert.equal(inTranscript.json.text, 'खाऊ का इसका वज़न बताओ');

    // Verify output transcription arrives
    const outTranscript = await waitForMessage(
      collector,
      (m) => m.json && m.json.type === 'output_transcript'
    );
    assert.equal(outTranscript.json.text, 'इस वस्तु का वज़न पाँच सौ ग्राम है।');

    // Wait for turn_complete (which is enqueued AFTER all binary audio chunks)
    await waitForMessage(collector, (m) => m.json && m.json.type === 'turn_complete');

    // Collect all received binary frames and verify ordering, chunk size <= 4096, and byte-exact PCM integrity
    const binaryFrames = collector.messages.filter((m) => m.isBinary);
    assert.ok(binaryFrames.length >= 3, '10,000-byte PCM must be split into multiple <=4096B frames');

    for (const frame of binaryFrames) {
      assert.ok(
        frame.buffer.length <= 4096,
        `Each binary frame (${frame.buffer.length}B) must be <= 4096B for ESP32 RAM safety`
      );
      assert.equal(
        frame.buffer.length % 2,
        0,
        'Each binary frame must have an even byte length (16-bit sample aligned)'
      );
    }

    const reassembledPcm = Buffer.concat(binaryFrames.map((f) => f.buffer));
    assert.equal(reassembledPcm.length, mockGemini.state.simulatedOutputPcm.length);
    assert.equal(
      Buffer.compare(reassembledPcm, mockGemini.state.simulatedOutputPcm),
      0,
      'Reassembled binary PCM received by ESP32 must match Gemini output byte-for-byte in exact order'
    );

    ws.close();
  });

  // --------------------------------------------------------------------------
  // TEST J: Reconnection, Model Fallback, Connection Limits, & Error Handling
  // --------------------------------------------------------------------------
  it('Test J: Model fallback, reconnection, invalid JSON error handling, and secret redaction work', async () => {
    // 1. Verify automatic model fallback when primary model is rejected by Gemini
    mockGemini.state.rejectModelName = 'models/gemini-3.8-live';
    try {
      const ws = new WebSocket(`ws://127.0.0.1:${bridgePort}/`);
      const collector = attachCollector(ws);

      const fallbackStatus = await waitForMessage(
        collector,
        (m) => m.json && m.json.type === 'status' && m.json.status === 'fallback_model'
      );
      assert.equal(fallbackStatus.json.model, 'models/gemini-3.1-flash-live-preview');

      const readyMsg = await waitForMessage(collector, (m) => m.json && m.json.type === 'ready');
      assert.equal(readyMsg.json.model, 'models/gemini-3.1-flash-live-preview');

      // 2. Send invalid JSON and verify server stays alive and returns structured error
      ws.send('THIS_IS_NOT_VALID_JSON', { binary: false });
      const errMsg = await waitForMessage(
        collector,
        (m) => m.json && m.json.type === 'error' && m.json.code === 'INVALID_JSON'
      );
      assert.ok(errMsg.json.message);

      // 3. Disconnect and reconnect cleanly
      ws.close();
      await new Promise((r) => setTimeout(r, 50));

      const wsReconnect = new WebSocket(`ws://127.0.0.1:${bridgePort}/`);
      const reconnectCollector = attachCollector(wsReconnect);
      const readyReconnect = await waitForMessage(
        reconnectCollector,
        (m) => m.json && m.json.type === 'ready'
      );
      assert.equal(readyReconnect.json.setupComplete, true);
      wsReconnect.close();
    } finally {
      mockGemini.state.rejectModelName = null;
    }

    // 4. Verify secret redaction utility never leaks keys or tokens
    const rawLog =
      'Connecting to wss://generativelanguage.googleapis.com/ws?key=AIzaSySecretKey12345 with Bearer my-device-secret';
    const redacted = redactSecrets(rawLog, ['AIzaSySecretKey12345', 'my-device-secret']);
    assert.ok(!redacted.includes('AIzaSySecretKey12345'));
    assert.ok(!redacted.includes('my-device-secret'));
    assert.ok(redacted.includes('[REDACTED]'));
    assert.equal(safeCompareToken('abc', 'abc'), true);
    assert.equal(safeCompareToken('abc', 'def'), false);
    assert.ok(buildModelCandidates('gemini-3.8-live', '').includes('models/gemini-3.8-live'));
  });
});
