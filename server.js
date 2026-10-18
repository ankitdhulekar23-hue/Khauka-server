'use strict';

/**
 * ============================================================================
 * KHAU KA! Hindi Voice Assistant — Production WebSocket & Gemini Live Backend
 * ============================================================================
 * Repository : Khauka-server
 * Architecture: ESP32-S3 <--(WSS: Binary PCM + JSON Control)--> Render Node.js
 *               <--(WSS: Gemini Live API v1beta BidiGenerateContent)--> Google
 *
 * Audio Protocol:
 * - ESP32 -> Server : Binary WebSocket frames (16 kHz, 16-bit signed LE mono PCM)
 *                     + JSON control frames (end_of_turn, text, weight_context, ping)
 * - Server -> Gemini: JSON text frames (setup, realtimeInput.audio, audioStreamEnd, clientContent)
 * - Gemini -> Server: JSON frames (setupComplete, serverContent, transcriptions, inlineData)
 * - Server -> ESP32 : Ordered Binary WebSocket frames (24 kHz, 16-bit signed LE mono PCM,
 *                     chunked <= 4096 bytes for ESP32 RAM safety)
 *                     + JSON control frames (ready, input_transcript, output_transcript,
 *                       text, turn_complete, interrupted, error)
 * ============================================================================
 */

require('dotenv').config();

const http = require('http');
const crypto = require('crypto');
const express = require('express');
const WebSocket = require('ws');

// ----------------------------------------------------------------------------
// Configuration Helpers
// ----------------------------------------------------------------------------

const DEFAULT_SYSTEM_INSTRUCTION =
  'आप "खाऊ का!" (KHAU KA!) नाम के एक स्मार्ट, विनम्र और मददगार हिंदी वॉइस असिस्टेंट और स्मार्ट वज़न कांटा (weighing scale) सहायक हैं। ' +
  'हमेशा स्पष्ट, संक्षिप्त और स्वाभाविक हिंदी में उत्तर दें। ' +
  'यदि उपयोगकर्ता वज़न, खाद्य पदार्थ, कीमत या पोषण के बारे में पूछे, तो तुरंत सटीक और उपयोगी जानकारी दें।';

const DEFAULT_MODEL_FALLBACKS = [
  'models/gemini-3.8-live',
  'models/gemini-3.1-flash-live-preview',
  'models/gemini-2.5-flash-native-audio-preview-12-2025',
  'models/gemini-2.0-flash-exp',
];

const ESP32_AUDIO_CHUNK_MAX_BYTES = 4096; // 4 KB (~85ms @ 24kHz 16-bit mono) for ESP32 heap safety

/**
 * Redacts API keys and tokens from any log string or error message.
 */
function redactSecrets(input, secrets = []) {
  if (input === null || input === undefined) return '';
  let str = typeof input === 'string' ? input : String(input);
  str = str.replace(/([?&](?:key|token)=)[^&\s"']+/gi, '$1[REDACTED]');
  str = str.replace(/(Bearer\s+)[^\s"']+/gi, '$1[REDACTED]');
  for (const secret of secrets) {
    if (secret && typeof secret === 'string' && secret.length >= 4) {
      str = str.split(secret).join('[REDACTED]');
    }
  }
  return str;
}

/**
 * Constant-time token comparison to prevent timing side-channel attacks.
 */
function safeCompareToken(provided, expected) {
  if (typeof provided !== 'string' || typeof expected !== 'string' || !expected) {
    return false;
  }
  const bufA = Buffer.from(provided, 'utf8');
  const bufB = Buffer.from(expected, 'utf8');
  if (bufA.length !== bufB.length) {
    return false;
  }
  return crypto.timingSafeEqual(bufA, bufB);
}

/**
 * Extracts client authentication token from upgrade request headers or query string.
 */
function extractClientToken(req) {
  const authHeader = req.headers['authorization'];
  if (typeof authHeader === 'string' && authHeader.toLowerCase().startsWith('bearer ')) {
    return authHeader.slice(7).trim();
  }
  const deviceHeader = req.headers['x-device-token'];
  if (typeof deviceHeader === 'string' && deviceHeader.trim()) {
    return deviceHeader.trim();
  }
  try {
    const parsedUrl = new URL(req.url || '/', 'http://localhost');
    const queryToken = parsedUrl.searchParams.get('token');
    if (queryToken) return queryToken.trim();
  } catch (_) {
    // Ignore malformed URL
  }
  return null;
}

/**
 * Builds the deduplicated list of Gemini Live API model candidates to try.
 */
function buildModelCandidates(primaryModel, fallbacksEnv) {
  const list = [];
  const addModel = (m) => {
    if (!m || typeof m !== 'string') return;
    const trimmed = m.trim();
    if (!trimmed) return;
    const normalized = trimmed.startsWith('models/') ? trimmed : `models/${trimmed}`;
    if (!list.includes(normalized)) list.push(normalized);
  };

  addModel(primaryModel || 'models/gemini-3.8-live');

  const rawFallbacks = fallbacksEnv
    ? fallbacksEnv.split(',')
    : DEFAULT_MODEL_FALLBACKS;
  for (const fb of rawFallbacks) {
    addModel(fb);
  }
  return list;
}

// ----------------------------------------------------------------------------
// Server Factory (Supports both production runtime and isolated unit tests)
// ----------------------------------------------------------------------------

function createServerInstance(overrides = {}) {
  const config = {
    port: Number(overrides.port ?? process.env.PORT ?? 10000),
    host: overrides.host ?? process.env.HOST ?? '0.0.0.0',
    geminiApiKey: overrides.geminiApiKey ?? process.env.GEMINI_API_KEY ?? '',
    geminiWsUrl:
      overrides.geminiWsUrl ??
      process.env.GEMINI_WS_URL ??
      'wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent',
    primaryModel: overrides.primaryModel ?? process.env.GEMINI_MODEL ?? 'models/gemini-3.8-live',
    modelFallbacks: overrides.modelFallbacks ?? process.env.GEMINI_MODEL_FALLBACKS ?? '',
    voiceName: overrides.voiceName ?? process.env.GEMINI_VOICE ?? 'Aoede',
    systemInstruction:
      overrides.systemInstruction ?? process.env.SYSTEM_INSTRUCTION ?? DEFAULT_SYSTEM_INSTRUCTION,
    deviceAuthToken: overrides.deviceAuthToken ?? process.env.DEVICE_AUTH_TOKEN ?? '',
    maxConnections: Number(overrides.maxConnections ?? process.env.MAX_CONNECTIONS ?? 10),
    setupTimeoutMs: Number(overrides.setupTimeoutMs ?? process.env.SETUP_TIMEOUT_MS ?? 15000),
    heartbeatIntervalMs: Number(
      overrides.heartbeatIntervalMs ?? process.env.HEARTBEAT_INTERVAL_MS ?? 25000
    ),
    maxPreSetupQueueBytes: Number(
      overrides.maxPreSetupQueueBytes ?? process.env.MAX_PRE_SETUP_QUEUE_BYTES ?? 262144
    ),
    maxOutboundBufferBytes: Number(
      overrides.maxOutboundBufferBytes ?? process.env.MAX_OUTBOUND_BUFFER_BYTES ?? 1048576
    ),
    silent: Boolean(overrides.silent ?? false),
  };

  const modelCandidates = buildModelCandidates(config.primaryModel, config.modelFallbacks);
  const secretsToRedact = [config.geminiApiKey, config.deviceAuthToken].filter(Boolean);

  function log(level, sessionId, message, meta = null) {
    if (config.silent) return;
    const ts = new Date().toISOString();
    const prefix = sessionId ? `[${ts}] [${level}] [${sessionId}]` : `[${ts}] [${level}]`;
    const safeMsg = redactSecrets(message, secretsToRedact);
    const line = meta
      ? `${prefix} ${safeMsg} ${redactSecrets(JSON.stringify(meta), secretsToRedact)}`
      : `${prefix} ${safeMsg}`;
    if (level === 'ERROR') {
      console.error(line);
    } else if (level === 'WARN') {
      console.warn(line);
    } else {
      console.log(line);
    }
  }

  // --------------------------------------------------------------------------
  // Express HTTP Application
  // --------------------------------------------------------------------------
  const app = express();
  app.disable('x-powered-by');
  app.set('trust proxy', 1);

  const activeSessions = new Map();

  app.get('/health', (_req, res) => {
    res.status(200).json({
      status: 'ok',
      service: 'khauka-server',
      version: '2.0.0',
      uptimeSeconds: Math.round(process.uptime()),
      activeConnections: activeSessions.size,
      maxConnections: config.maxConnections,
      geminiKeyConfigured: Boolean(config.geminiApiKey && config.geminiApiKey.trim().length > 0),
      authEnabled: Boolean(config.deviceAuthToken && config.deviceAuthToken.trim().length > 0),
      primaryModel: modelCandidates[0],
      fallbackModels: modelCandidates.slice(1),
      audioProtocol: {
        inputFormat: 'audio/pcm;rate=16000',
        inputChannels: 1,
        inputBitDepth: 16,
        outputFormat: 'audio/pcm;rate=24000',
        outputChannels: 1,
        outputBitDepth: 16,
      },
      timestamp: new Date().toISOString(),
    });
  });

  app.get('/', (_req, res) => {
    res.status(200).json({
      status: 'running',
      message: 'Khau Ka Voice Server is Running!',
      websocketPath: '/',
      healthEndpoint: '/health',
      activeConnections: activeSessions.size,
    });
  });

  const server = http.createServer(app);

  // --------------------------------------------------------------------------
  // WebSocket Server (Root Path '/' & '/ws')
  // --------------------------------------------------------------------------
  const wss = new WebSocket.Server({
    noServer: true,
    perMessageDeflate: false, // Disabled to protect ESP32-S3 heap memory
    maxPayload: 2 * 1024 * 1024, // 2 MB max single frame
  });

  server.on('upgrade', (req, socket, head) => {
    socket.on('error', (err) => {
      log('WARN', null, `Upgrade socket error: ${err.message}`);
    });

    let pathname = '/';
    try {
      const parsedUrl = new URL(req.url || '/', 'http://localhost');
      pathname = parsedUrl.pathname;
    } catch (_) {
      pathname = '/';
    }

    // Accept upgrades on root path '/' (and '/ws' as alias)
    if (pathname !== '/' && pathname !== '/ws') {
      log('WARN', null, `Rejected WebSocket upgrade on invalid path: ${pathname}`);
      socket.write(
        'HTTP/1.1 404 Not Found\r\n' +
          'Content-Type: text/plain\r\n' +
          'Connection: close\r\n\r\n' +
          'WebSocket endpoint is at /\r\n'
      );
      socket.destroy();
      return;
    }

    // Enforce connection limit
    if (activeSessions.size >= config.maxConnections) {
      log('WARN', null, `Rejected WebSocket upgrade: connection limit reached (${config.maxConnections})`);
      socket.write(
        'HTTP/1.1 429 Too Many Requests\r\n' +
          'Content-Type: text/plain\r\n' +
          'Connection: close\r\n\r\n' +
          'Maximum concurrent device connections reached\r\n'
      );
      socket.destroy();
      return;
    }

    // Enforce device authentication if DEVICE_AUTH_TOKEN is configured
    if (config.deviceAuthToken && config.deviceAuthToken.trim().length > 0) {
      const clientToken = extractClientToken(req);
      if (!safeCompareToken(clientToken, config.deviceAuthToken.trim())) {
        const clientIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown';
        log('WARN', null, `Rejected unauthorized WebSocket client from ${clientIp}`);
        socket.write(
          'HTTP/1.1 401 Unauthorized\r\n' +
            'Content-Type: text/plain\r\n' +
            'Connection: close\r\n\r\n' +
            'Unauthorized device token\r\n'
        );
        socket.destroy();
        return;
      }
    }

    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit('connection', ws, req);
    });
  });

  // --------------------------------------------------------------------------
  // Per-Device Session Handler
  // --------------------------------------------------------------------------
  wss.on('connection', (espWs, req) => {
    const sessionId = `esp-${crypto.randomBytes(3).toString('hex')}`;
    const clientIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown';

    const session = {
      id: sessionId,
      clientIp,
      espWs,
      geminiWs: null,
      modelIndex: 0,
      activeModel: modelCandidates[0],
      useTranscriptionConfig: true,
      isSetupComplete: false,
      isClosed: false,
      espAlive: true,
      geminiAlive: true,
      preSetupQueue: [],
      preSetupQueueBytes: 0,
      outboundQueue: [],
      outboundProcessing: false,
      setupTimer: null,
      heartbeatTimer: null,
      audioChunksIn: 0,
      audioBytesIn: 0,
      audioChunksOut: 0,
      audioBytesOut: 0,
      lastWeightGrams: null,
    };

    activeSessions.set(sessionId, session);
    log('INFO', sessionId, `ESP32 connected from ${clientIp} (active=${activeSessions.size})`);

    // Validate server-side API key presence before opening upstream connection
    if (!config.geminiApiKey || !config.geminiApiKey.trim()) {
      log('ERROR', sessionId, 'GEMINI_API_KEY is not configured on the server');
      sendEspJson(session, {
        type: 'error',
        code: 'MISSING_API_KEY',
        message: 'Server GEMINI_API_KEY is not configured.',
      });
      closeSession(session, 1011, 'Server configuration error');
      return;
    }

    // Notify ESP32 that server accepted connection and is setting up Gemini session
    sendEspJson(session, {
      type: 'status',
      status: 'connecting_gemini',
      model: session.activeModel,
      message: 'Connecting to Gemini Live API...',
    });

    // Start upstream Gemini Live API connection
    connectGeminiUpstream(session);

    // Start heartbeat ping/pong timer
    session.heartbeatTimer = setInterval(() => {
      if (session.isClosed) return;

      if (session.espWs.readyState === WebSocket.OPEN) {
        if (!session.espAlive) {
          log('WARN', sessionId, 'ESP32 heartbeat timeout; closing stale connection');
          closeSession(session, 1001, 'ESP32 heartbeat timeout');
          return;
        }
        session.espAlive = false;
        try {
          session.espWs.ping();
        } catch (err) {
          log('WARN', sessionId, `Failed to ping ESP32: ${err.message}`);
        }
      }

      if (session.geminiWs && session.geminiWs.readyState === WebSocket.OPEN) {
        if (!session.geminiAlive) {
          log('WARN', sessionId, 'Gemini upstream heartbeat timeout; closing session');
          closeSession(session, 1011, 'Gemini heartbeat timeout');
          return;
        }
        session.geminiAlive = false;
        try {
          session.geminiWs.ping();
        } catch (err) {
          log('WARN', sessionId, `Failed to ping Gemini: ${err.message}`);
        }
      }
    }, config.heartbeatIntervalMs);

    if (session.heartbeatTimer.unref) {
      session.heartbeatTimer.unref();
    }

    espWs.on('pong', () => {
      session.espAlive = true;
    });

    // ------------------------------------------------------------------------
    // Incoming Messages from ESP32
    // ------------------------------------------------------------------------
    espWs.on('message', (data, isBinary) => {
      session.espAlive = true;
      if (session.isClosed) return;

      if (isBinary) {
        handleEspBinaryAudio(session, Buffer.isBuffer(data) ? data : Buffer.from(data));
      } else {
        const text = Buffer.isBuffer(data) ? data.toString('utf8') : String(data);
        handleEspControlMessage(session, text);
      }
    });

    espWs.on('close', (code, reasonBuf) => {
      const reason = reasonBuf ? reasonBuf.toString('utf8') : '';
      log('INFO', sessionId, `ESP32 WebSocket closed (code=${code}, reason="${reason}")`, {
        audioChunksIn: session.audioChunksIn,
        audioBytesIn: session.audioBytesIn,
        audioChunksOut: session.audioChunksOut,
        audioBytesOut: session.audioBytesOut,
      });
      closeSession(session, code || 1000, 'ESP32 disconnected');
    });

    espWs.on('error', (err) => {
      log('ERROR', sessionId, `ESP32 WebSocket error: ${err.message}`);
    });
  });

  // --------------------------------------------------------------------------
  // Upstream Gemini Live API Connection & Setup Protocol
  // --------------------------------------------------------------------------
  function connectGeminiUpstream(session) {
    if (session.isClosed) return;

    session.activeModel = modelCandidates[session.modelIndex] || modelCandidates[0];
    session.isSetupComplete = false;

    const separator = config.geminiWsUrl.includes('?') ? '&' : '?';
    const upstreamUrl = `${config.geminiWsUrl}${separator}key=${encodeURIComponent(
      config.geminiApiKey.trim()
    )}`;

    log(
      'INFO',
      session.id,
      `Connecting to Gemini Live API (model=${session.activeModel}, attempt=${session.modelIndex + 1}/${
        modelCandidates.length
      })`
    );

    let geminiWs;
    try {
      geminiWs = new WebSocket(upstreamUrl, {
        perMessageDeflate: false,
        handshakeTimeout: config.setupTimeoutMs,
      });
    } catch (err) {
      log('ERROR', session.id, `Failed to construct Gemini WebSocket: ${err.message}`);
      sendEspJson(session, {
        type: 'error',
        code: 'UPSTREAM_INIT_ERROR',
        message: 'Failed to initialize upstream Gemini connection.',
      });
      closeSession(session, 1011, 'Upstream init error');
      return;
    }

    session.geminiWs = geminiWs;
    session.geminiAlive = true;

    clearTimeout(session.setupTimer);
    session.setupTimer = setTimeout(() => {
      if (!session.isSetupComplete && !session.isClosed) {
        log('ERROR', session.id, `Gemini setupComplete timed out after ${config.setupTimeoutMs}ms`);
        sendEspJson(session, {
          type: 'error',
          code: 'SETUP_TIMEOUT',
          message: 'Gemini Live API setup timed out.',
        });
        closeSession(session, 1011, 'Gemini setup timeout');
      }
    }, config.setupTimeoutMs);
    if (session.setupTimer.unref) {
      session.setupTimer.unref();
    }

    geminiWs.on('pong', () => {
      session.geminiAlive = true;
    });

    geminiWs.on('open', () => {
      if (session.isClosed || session.geminiWs !== geminiWs) return;
      log('INFO', session.id, `Gemini WebSocket connected; sending setup for ${session.activeModel}`);

      const setupPayload = {
        setup: {
          model: session.activeModel,
          generationConfig: {
            responseModalities: ['AUDIO'],
            speechConfig: {
              voiceConfig: {
                prebuiltVoiceConfig: {
                  voiceName: config.voiceName,
                },
              },
            },
          },
          systemInstruction: {
            parts: [{ text: config.systemInstruction }],
          },
        },
      };

      if (session.useTranscriptionConfig) {
        setupPayload.setup.inputAudioTranscription = {};
        setupPayload.setup.outputAudioTranscription = {};
      }

      sendGeminiJson(session, setupPayload);
    });

    geminiWs.on('message', (rawData) => {
      if (session.isClosed || session.geminiWs !== geminiWs) return;
      session.geminiAlive = true;
      const text = Buffer.isBuffer(rawData) ? rawData.toString('utf8') : String(rawData);
      handleGeminiMessage(session, text);
    });

    geminiWs.on('close', (code, reasonBuf) => {
      if (session.isClosed || session.geminiWs !== geminiWs) return;
      const reason = redactSecrets(reasonBuf ? reasonBuf.toString('utf8') : '', secretsToRedact);
      log(
        'WARN',
        session.id,
        `Gemini WebSocket closed (code=${code}, reason="${reason}", setupComplete=${session.isSetupComplete})`
      );

      // Automatic fallback if setup failed before setupComplete
      if (!session.isSetupComplete) {
        const lowerReason = reason.toLowerCase();

        // Case 1: Model rejected transcription config fields (code 1007 Invalid Argument)
        if (
          session.useTranscriptionConfig &&
          (code === 1007 || lowerReason.includes('transcription') || lowerReason.includes('unknown name'))
        ) {
          log(
            'WARN',
            session.id,
            `Retrying model ${session.activeModel} without audio transcription config fields`
          );
          session.useTranscriptionConfig = false;
          connectGeminiUpstream(session);
          return;
        }

        // Case 2: Model not found or not supported for BidiGenerateContent -> try next fallback model
        const isModelError =
          code === 1008 ||
          code === 1007 ||
          lowerReason.includes('not found') ||
          lowerReason.includes('not supported') ||
          lowerReason.includes('model') ||
          lowerReason.includes('permission');

        if (isModelError && session.modelIndex + 1 < modelCandidates.length) {
          session.modelIndex += 1;
          session.useTranscriptionConfig = true;
          const nextModel = modelCandidates[session.modelIndex];
          log('INFO', session.id, `Falling back to Gemini model: ${nextModel}`);
          sendEspJson(session, {
            type: 'status',
            status: 'fallback_model',
            model: nextModel,
            message: `Trying fallback model ${nextModel}...`,
          });
          connectGeminiUpstream(session);
          return;
        }
      }

      sendEspJson(session, {
        type: 'error',
        code: `GEMINI_CLOSED_${code || 1006}`,
        message: reason || `Gemini connection closed (code ${code || 1006})`,
      });
      closeSession(session, 1011, 'Gemini connection closed');
    });

    geminiWs.on('error', (err) => {
      if (session.isClosed || session.geminiWs !== geminiWs) return;
      log('ERROR', session.id, `Gemini WebSocket error: ${err.message}`);
    });
  }

  // --------------------------------------------------------------------------
  // Handle ESP32 Binary Audio Input (16 kHz, 16-bit signed LE mono PCM)
  // --------------------------------------------------------------------------
  function handleEspBinaryAudio(session, pcmBuffer) {
    if (!pcmBuffer || pcmBuffer.length === 0) return;

    // Ensure 16-bit PCM alignment (even number of bytes)
    let alignedBuffer = pcmBuffer;
    if (alignedBuffer.length % 2 !== 0) {
      log(
        'WARN',
        session.id,
        `Received odd-length PCM frame (${alignedBuffer.length} bytes); trimming 1 trailing byte for 16-bit alignment`
      );
      alignedBuffer = alignedBuffer.subarray(0, alignedBuffer.length - 1);
      if (alignedBuffer.length === 0) return;
    }

    session.audioChunksIn += 1;
    session.audioBytesIn += alignedBuffer.length;

    const payload = {
      realtimeInput: {
        audio: {
          mimeType: 'audio/pcm;rate=16000',
          data: alignedBuffer.toString('base64'),
        },
      },
    };

    enqueueOrSendToGemini(session, payload, alignedBuffer.length);
  }

  // --------------------------------------------------------------------------
  // Handle ESP32 JSON Control Messages
  // --------------------------------------------------------------------------
  function handleEspControlMessage(session, rawText) {
    const trimmed = rawText.trim();
    if (!trimmed) return;

    let msg;
    try {
      msg = JSON.parse(trimmed);
    } catch (err) {
      log('WARN', session.id, `Invalid JSON control message from ESP32: ${trimmed.slice(0, 120)}`);
      sendEspJson(session, {
        type: 'error',
        code: 'INVALID_JSON',
        message: 'Control frame must be valid JSON.',
      });
      return;
    }

    // 1. Heartbeat ping
    if (msg.type === 'ping') {
      sendEspJson(session, { type: 'pong', timestamp: Date.now() });
      return;
    }

    // 2. Ignore or log legacy client-side setup messages (server handles setup securely)
    if (msg.setup) {
      log('INFO', session.id, 'Ignored client-side setup message (handled by server)');
      if (session.isSetupComplete) {
        sendEspJson(session, {
          type: 'ready',
          setupComplete: true,
          model: session.activeModel,
          inputSampleRate: 16000,
          outputSampleRate: 24000,
          channels: 1,
          bitDepth: 16,
        });
      }
      return;
    }

    // 3. Base64 audio sent inside JSON wrapper
    if (msg.type === 'audio' && typeof msg.data === 'string') {
      const rate = Number(msg.sampleRate) || 16000;
      const approxBytes = Math.floor((msg.data.length * 3) / 4);
      session.audioChunksIn += 1;
      session.audioBytesIn += approxBytes;
      enqueueOrSendToGemini(
        session,
        {
          realtimeInput: {
            audio: {
              mimeType: `audio/pcm;rate=${rate}`,
              data: msg.data,
            },
          },
        },
        approxBytes
      );
      return;
    }

    // 4. End of speech / microphone turn completion
    if (
      msg.type === 'end_of_turn' ||
      msg.type === 'audio_stream_end' ||
      msg.realtimeInput?.audioStreamEnd
    ) {
      log(
        'INFO',
        session.id,
        `ESP32 signaled end_of_turn (chunksIn=${session.audioChunksIn}, bytesIn=${session.audioBytesIn})`
      );
      // Official Gemini Live API audio stream flush signal
      enqueueOrSendToGemini(session, {
        realtimeInput: {
          audioStreamEnd: true,
        },
      });
      // Also send clientContent.turnComplete when requested or when using models that require explicit turn completion
      if (msg.forceTurnComplete !== false) {
        enqueueOrSendToGemini(session, {
          clientContent: {
            turnComplete: true,
          },
        });
      }
      return;
    }

    // 5. Live HX711 load cell weight update from ESP32
    if (msg.type === 'weight_context' && typeof msg.grams === 'number') {
      session.lastWeightGrams = msg.grams;
      log('INFO', session.id, `Updated HX711 weight context: ${msg.grams.toFixed(1)} g`);
      enqueueOrSendToGemini(session, {
        clientContent: {
          turns: [
            {
              role: 'user',
              parts: [
                {
                  text: `[System Sensor Update: वज़न कांटे (HX711 scale) पर वर्तमान वज़न ${msg.grams.toFixed(
                    1
                  )} ग्राम है। जब उपयोगकर्ता वज़न पूछे तब इस मान का उपयोग करें।]`,
                },
              ],
            },
          ],
          turnComplete: Boolean(msg.speakNow),
        },
      });
      return;
    }

    // 6. Direct text message / question
    if (msg.type === 'text' && typeof msg.text === 'string' && msg.text.trim()) {
      const userText = msg.text.trim();
      log('INFO', session.id, `Forwarding text prompt from ESP32: "${userText.slice(0, 80)}"`);
      enqueueOrSendToGemini(session, {
        clientContent: {
          turns: [
            {
              role: 'user',
              parts: [{ text: userText }],
            },
          ],
          turnComplete: true,
        },
      });
      return;
    }

    // 7. Pass-through for standard Gemini Live API JSON structures (realtimeInput / clientContent / toolResponse)
    if (msg.realtimeInput || msg.clientContent || msg.toolResponse) {
      // Upgrade legacy mediaChunks to realtimeInput.audio if needed
      if (
        msg.realtimeInput &&
        Array.isArray(msg.realtimeInput.mediaChunks) &&
        msg.realtimeInput.mediaChunks.length > 0
      ) {
        for (const chunk of msg.realtimeInput.mediaChunks) {
          if (chunk && chunk.data) {
            session.audioChunksIn += 1;
            enqueueOrSendToGemini(session, {
              realtimeInput: {
                audio: {
                  mimeType: chunk.mimeType || 'audio/pcm;rate=16000',
                  data: chunk.data,
                },
              },
            });
          }
        }
        return;
      }

      enqueueOrSendToGemini(session, msg);
      return;
    }

    log('WARN', session.id, `Unrecognized control message type: ${msg.type || 'unknown'}`);
    sendEspJson(session, {
      type: 'error',
      code: 'UNSUPPORTED_MESSAGE_TYPE',
      message: `Unsupported control message type: ${msg.type || 'unknown'}`,
    });
  }

  // --------------------------------------------------------------------------
  // Queueing & Backpressure Management (ESP32 -> Gemini)
  // --------------------------------------------------------------------------
  function enqueueOrSendToGemini(session, payload, estimatedBytes = 256) {
    if (session.isClosed) return;

    if (!session.isSetupComplete) {
      if (session.preSetupQueueBytes + estimatedBytes > config.maxPreSetupQueueBytes) {
        const dropped = session.preSetupQueue.shift();
        if (dropped) {
          session.preSetupQueueBytes = Math.max(0, session.preSetupQueueBytes - dropped.bytes);
        }
        log(
          'WARN',
          session.id,
          `Pre-setup queue limit (${config.maxPreSetupQueueBytes}B) reached; dropped oldest queued item`
        );
        sendEspJson(session, {
          type: 'warning',
          code: 'PRE_SETUP_BUFFER_OVERFLOW',
          message: 'Audio sent before ready exceeded buffer limit; oldest chunk dropped.',
        });
      }
      session.preSetupQueue.push({ payload, bytes: estimatedBytes });
      session.preSetupQueueBytes += estimatedBytes;
      return;
    }

    sendGeminiJson(session, payload);
  }

  function sendGeminiJson(session, payload) {
    if (!session.geminiWs || session.geminiWs.readyState !== WebSocket.OPEN) {
      log('WARN', session.id, 'Attempted to send to Gemini while upstream socket is not OPEN');
      return;
    }

    if (session.geminiWs.bufferedAmount > config.maxOutboundBufferBytes) {
      log(
        'WARN',
        session.id,
        `Upstream Gemini backpressure high (${session.geminiWs.bufferedAmount} bytes buffered); dropping frame`
      );
      sendEspJson(session, {
        type: 'warning',
        code: 'UPSTREAM_BACKPRESSURE',
        message: 'Upstream congestion detected.',
      });
      return;
    }

    try {
      // Always send explicit UTF-8 string with binary: false so Gemini receives Text opcode (0x01)
      session.geminiWs.send(JSON.stringify(payload), { binary: false });
    } catch (err) {
      log('ERROR', session.id, `Error sending payload to Gemini: ${err.message}`);
    }
  }

  // --------------------------------------------------------------------------
  // Handle Gemini Live API Responses (Gemini -> ESP32)
  // --------------------------------------------------------------------------
  function handleGeminiMessage(session, rawText) {
    let response;
    try {
      response = JSON.parse(rawText);
    } catch (err) {
      log('ERROR', session.id, `Failed to parse Gemini JSON message: ${err.message}`);
      return;
    }

    // 1. API Error Frame
    if (response.error) {
      const errCode = response.error.code || response.error.status || 'GEMINI_API_ERROR';
      const errMsg = response.error.message || 'Unknown Gemini API error';
      log('ERROR', session.id, `Gemini API error [${errCode}]: ${errMsg}`);
      enqueueEspOutbound(session, {
        isBinary: false,
        data: JSON.stringify({
          type: 'error',
          code: String(errCode),
          message: errMsg,
        }),
      });
      return;
    }

    // 2. Session Setup Complete
    if (response.setupComplete !== undefined) {
      clearTimeout(session.setupTimer);
      session.isSetupComplete = true;
      log(
        'INFO',
        session.id,
        `Gemini setupComplete received (model=${session.activeModel}); announcing readiness to ESP32`
      );

      // Announce readiness to ESP32 (includes both type:"ready" and setupComplete:true)
      sendEspJson(session, {
        type: 'ready',
        setupComplete: true,
        model: session.activeModel,
        inputSampleRate: 16000,
        outputSampleRate: 24000,
        channels: 1,
        bitDepth: 16,
      });

      // Flush any messages buffered before setupComplete in strict FIFO order
      if (session.preSetupQueue.length > 0) {
        log(
          'INFO',
          session.id,
          `Flushing ${session.preSetupQueue.length} pre-setup queued items (${session.preSetupQueueBytes} bytes)`
        );
        while (session.preSetupQueue.length > 0) {
          const item = session.preSetupQueue.shift();
          sendGeminiJson(session, item.payload);
        }
        session.preSetupQueueBytes = 0;
      }
      return;
    }

    // 3. GoAway notification (server-side session rotation warning)
    if (response.goAway) {
      log('WARN', session.id, 'Received goAway notification from Gemini', response.goAway);
      enqueueEspOutbound(session, {
        isBinary: false,
        data: JSON.stringify({
          type: 'go_away',
          timeLeft: response.goAway.timeLeft || null,
        }),
      });
    }

    // 4. Server Content (Audio, Transcriptions, Text, Interruption, Turn Completion)
    const serverContent = response.serverContent;
    if (!serverContent) return;

    // 4a. User input speech transcription
    if (
      serverContent.inputTranscription &&
      typeof serverContent.inputTranscription.text === 'string' &&
      serverContent.inputTranscription.text.trim()
    ) {
      const transcript = serverContent.inputTranscription.text;
      log('INFO', session.id, `Input transcription: "${transcript}"`);
      enqueueEspOutbound(session, {
        isBinary: false,
        data: JSON.stringify({
          type: 'input_transcript',
          text: transcript,
        }),
      });
    }

    // 4b. Model output audio transcription (Hindi text for LCD / Serial)
    if (
      serverContent.outputTranscription &&
      typeof serverContent.outputTranscription.text === 'string' &&
      serverContent.outputTranscription.text.trim()
    ) {
      const transcript = serverContent.outputTranscription.text;
      log('INFO', session.id, `Output transcription: "${transcript}"`);
      enqueueEspOutbound(session, {
        isBinary: false,
        data: JSON.stringify({
          type: 'output_transcript',
          text: transcript,
        }),
      });
    }

    // 4c. Model Turn Parts (Inline PCM Audio + Text)
    if (serverContent.modelTurn && Array.isArray(serverContent.modelTurn.parts)) {
      for (const part of serverContent.modelTurn.parts) {
        if (part.text && typeof part.text === 'string') {
          enqueueEspOutbound(session, {
            isBinary: false,
            data: JSON.stringify({
              type: 'text',
              text: part.text,
            }),
          });
        }

        if (part.inlineData && typeof part.inlineData.data === 'string') {
          const mimeType = part.inlineData.mimeType || 'audio/pcm;rate=24000';
          if (mimeType.startsWith('audio/pcm')) {
            const rawPcm = Buffer.from(part.inlineData.data, 'base64');
            if (rawPcm.length > 0) {
              // Ensure even byte length (16-bit signed LE samples)
              const evenLength = rawPcm.length - (rawPcm.length % 2);
              const cleanPcm = evenLength === rawPcm.length ? rawPcm : rawPcm.subarray(0, evenLength);

              // Slice into <= 4096-byte chunks so ESP32 WebSocketsClient and I2S DMA never overflow
              for (let offset = 0; offset < cleanPcm.length; offset += ESP32_AUDIO_CHUNK_MAX_BYTES) {
                const slice = cleanPcm.subarray(
                  offset,
                  Math.min(offset + ESP32_AUDIO_CHUNK_MAX_BYTES, cleanPcm.length)
                );
                enqueueEspOutbound(session, {
                  isBinary: true,
                  data: slice,
                });
              }
            }
          }
        }
      }
    }

    // 4d. Interruption handling (user barge-in)
    if (serverContent.interrupted) {
      log('INFO', session.id, 'Gemini turn interrupted; clearing pending outbound audio chunks');
      // Remove any queued binary audio chunks so playback stops immediately
      session.outboundQueue = session.outboundQueue.filter((item) => !item.isBinary);
      enqueueEspOutbound(session, {
        isBinary: false,
        data: JSON.stringify({ type: 'interrupted' }),
      });
    }

    // 4e. Turn Completion (enqueued after all audio slices so ESP32 receives it after final audio byte)
    if (serverContent.turnComplete) {
      log(
        'INFO',
        session.id,
        `Gemini turnComplete (chunksOut=${session.audioChunksOut}, bytesOut=${session.audioBytesOut})`
      );
      enqueueEspOutbound(session, {
        isBinary: false,
        data: JSON.stringify({ type: 'turn_complete' }),
      });
    }
  }

  // --------------------------------------------------------------------------
  // Ordered Outbound Queue & Backpressure Pacing (Server -> ESP32)
  // --------------------------------------------------------------------------
  function enqueueEspOutbound(session, item) {
    if (session.isClosed || session.espWs.readyState !== WebSocket.OPEN) return;
    session.outboundQueue.push(item);
    processEspOutboundQueue(session);
  }

  function processEspOutboundQueue(session) {
    if (session.outboundProcessing || session.isClosed) return;
    session.outboundProcessing = true;

    const drainNext = () => {
      if (session.isClosed || session.espWs.readyState !== WebSocket.OPEN) {
        session.outboundQueue.length = 0;
        session.outboundProcessing = false;
        return;
      }

      if (session.outboundQueue.length === 0) {
        session.outboundProcessing = false;
        return;
      }

      // If ESP32 TCP send buffer is congested, pause briefly to let it drain without dropping audio
      if (session.espWs.bufferedAmount > config.maxOutboundBufferBytes) {
        setTimeout(drainNext, 15);
        return;
      }

      const item = session.outboundQueue.shift();
      try {
        session.espWs.send(item.data, { binary: item.isBinary }, (err) => {
          if (err) {
            log('ERROR', session.id, `Failed to send frame to ESP32: ${err.message}`);
            session.outboundProcessing = false;
            return;
          }
          if (item.isBinary) {
            session.audioChunksOut += 1;
            session.audioBytesOut += item.data.length;
          }
          setImmediate(drainNext);
        });
      } catch (err) {
        log('ERROR', session.id, `Exception sending frame to ESP32: ${err.message}`);
        session.outboundProcessing = false;
      }
    };

    drainNext();
  }

  function sendEspJson(session, obj) {
    if (session.isClosed || session.espWs.readyState !== WebSocket.OPEN) return;
    try {
      session.espWs.send(JSON.stringify(obj), { binary: false });
    } catch (err) {
      log('ERROR', session.id, `Failed to send JSON to ESP32: ${err.message}`);
    }
  }

  // --------------------------------------------------------------------------
  // Session Cleanup & Graceful Shutdown
  // --------------------------------------------------------------------------
  function closeSession(session, closeCode = 1000, reason = 'Session closed') {
    if (session.isClosed) return;
    session.isClosed = true;

    clearTimeout(session.setupTimer);
    clearInterval(session.heartbeatTimer);
    session.preSetupQueue.length = 0;
    session.outboundQueue.length = 0;
    activeSessions.delete(session.id);

    if (
      session.geminiWs &&
      (session.geminiWs.readyState === WebSocket.OPEN ||
        session.geminiWs.readyState === WebSocket.CONNECTING)
    ) {
      try {
        session.geminiWs.close(1000, 'Device session ended');
      } catch (_) {
        session.geminiWs.terminate();
      }
    }

    if (
      session.espWs &&
      (session.espWs.readyState === WebSocket.OPEN ||
        session.espWs.readyState === WebSocket.CONNECTING)
    ) {
      try {
        const safeCode =
          typeof closeCode === 'number' && closeCode >= 1000 && closeCode <= 4999 ? closeCode : 1000;
        session.espWs.close(safeCode, String(reason).slice(0, 120));
      } catch (_) {
        session.espWs.terminate();
      }
    }
  }

  function shutdown(signal = 'SIGTERM') {
    log('INFO', null, `Received ${signal}; shutting down server gracefully...`);
    for (const session of activeSessions.values()) {
      closeSession(session, 1001, 'Server shutting down');
    }
    return new Promise((resolve) => {
      wss.close(() => {
        server.close(() => {
          log('INFO', null, 'HTTP and WebSocket servers closed cleanly');
          resolve();
        });
      });
    });
  }

  return {
    app,
    server,
    wss,
    config,
    activeSessions,
    shutdown,
  };
}

// ----------------------------------------------------------------------------
// Standalone Execution Entrypoint
// ----------------------------------------------------------------------------
if (require.main === module) {
  const instance = createServerInstance();
  const { server, config, shutdown } = instance;

  server.listen(config.port, config.host, () => {
    console.log('============================================================');
    console.log(' KHAU KA! Hindi Voice Assistant — WebSocket Backend Server');
    console.log('============================================================');
    console.log(` Listening on      : http://${config.host}:${config.port}`);
    console.log(` WebSocket Path    : ws://${config.host}:${config.port}/`);
    console.log(` Health Endpoint   : http://${config.host}:${config.port}/health`);
    console.log(` Primary Model     : ${config.primaryModel}`);
    console.log(` Gemini Key Set    : ${Boolean(config.geminiApiKey)}`);
    console.log(` Device Auth Set   : ${Boolean(config.deviceAuthToken)}`);
    console.log(` Max Connections   : ${config.maxConnections}`);
    console.log('============================================================');
  });

  process.on('SIGTERM', async () => {
    await shutdown('SIGTERM');
    process.exit(0);
  });

  process.on('SIGINT', async () => {
    await shutdown('SIGINT');
    process.exit(0);
  });
}

module.exports = {
  createServerInstance,
  redactSecrets,
  safeCompareToken,
  buildModelCandidates,
};
