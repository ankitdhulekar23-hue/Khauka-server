/*
  ============================================================================
  KHAU KA! — ESP32-S3 Hindi Voice Assistant & Smart Weighing Scale Firmware
  ============================================================================
  Target Board : ESP32-S3 Dev Module (esp32:esp32:esp32s3)
  Backend URL  : wss://khauka-server.onrender.com/

  Hardware Pin Assignments (Preserved):
    - 16x2 I2C LCD (0x27) : SDA = GPIO 1,  SCL = GPIO 2
    - INMP441 Microphone  : SCK = GPIO 4,  WS  = GPIO 5,  SD  = GPIO 6  (I2S_NUM_0)
    - MAX98357A Speaker   : BCLK= GPIO 15, LRC = GPIO 16, DIN = GPIO 17 (I2S_NUM_1)
    - HX711 Load Cell     : DOUT= GPIO 13, SCK = GPIO 14  (Cal factor: 331.64)
    - TARE / TALK Button  : GPIO 12 (INPUT_PULLUP to GND)
    - Onboard BOOT Button : GPIO 0  (INPUT_PULLUP to GND)

  Standalone Operation (No Laptop Required After Deployment):
    - Short Press GPIO 12 (< 1.2s) or Press BOOT (GPIO 0) : Start Hindi Voice Assistant
    - Long Press GPIO 12 (>= 1.2s)                        : Tare HX711 Load Cell
    - Hands-Free Voice Wake (Toggleable via 'a')          : Speak loudly near mic when idle
    - Idle Mode                                           : Continuously updates live weight on LCD

  Serial Monitor Commands (115200 baud):
    - v : Record voice and talk to KHAU KA! Hindi Assistant
    - w : Read and display current HX711 weight
    - t : Tare (zero) the HX711 load cell
    - a : Toggle hands-free auto voice trigger ON/OFF
    - p : Send ping to Render backend
    - ? : Print help and diagnostics

  Required Arduino Libraries:
    1. WebSockets by Markus Sattler (Links2004) — v2.7.2+
    2. ArduinoJson by Benoit Blanchon           — v7.x
    3. LiquidCrystal I2C by Frank de Brabander  — v1.1.2+
    4. HX711 Arduino Library by Bogdan Necula   — v0.6.5+
  ============================================================================
*/

#include <WiFi.h>
#include <WiFiMulti.h>
#include <WebSocketsClient.h>
#include <ArduinoJson.h>
#include <Wire.h>
#include <LiquidCrystal_I2C.h>
#include "HX711.h"
#include "driver/i2s.h"
#include "esp_heap_caps.h"

// ============================================================================
// 1. NETWORK & BACKEND CONFIGURATION (NO GEMINI_API_KEY IN FIRMWARE!)
// ============================================================================

struct WifiCredential {
  const char* ssid;
  const char* password;
};

// Configured Wi-Fi networks (WiFiMulti automatically connects to the strongest available)
static const WifiCredential KNOWN_WIFI_NETWORKS[] = {
  { "Sandhya Home", "bhavani$1317" },
  { "Mumma",        "Aai#1234"     },
  // Add or edit your primary Wi-Fi network here if different:
  { "YOUR_WIFI_SSID", "YOUR_WIFI_PASSWORD" }
};
static const size_t KNOWN_WIFI_COUNT = sizeof(KNOWN_WIFI_NETWORKS) / sizeof(KNOWN_WIFI_NETWORKS[0]);

// Render Public WSS Backend Settings
static const char* BACKEND_HOST = "khauka-server.onrender.com";
static const uint16_t BACKEND_PORT = 443;
static const char* BACKEND_PATH = "/";

// Optional Device Authentication Token (must match DEVICE_AUTH_TOKEN on Render if enabled)
static const char* DEVICE_AUTH_TOKEN = "";

// ============================================================================
// 2. HARDWARE PIN CONFIGURATION (PRESERVED)
// ============================================================================

static const int LCD_SDA_PIN     = 1;
static const int LCD_SCL_PIN     = 2;

static const int MIC_SCK_PIN     = 4;
static const int MIC_WS_PIN      = 5;
static const int MIC_SD_PIN      = 6;

static const int HX711_DOUT_PIN  = 13;
static const int HX711_SCK_PIN   = 14;
static const int TARE_BUTTON_PIN = 12;
static const int BOOT_BUTTON_PIN = 0;

static const int SPK_BCLK_PIN    = 15;
static const int SPK_LRC_PIN     = 16;
static const int SPK_DIN_PIN     = 17;

// ============================================================================
// 3. AUDIO & SENSOR PARAMETERS
// ============================================================================

static const i2s_port_t MIC_I2S_PORT = I2S_NUM_0;
static const i2s_port_t SPK_I2S_PORT = I2S_NUM_1;

static const uint32_t MIC_SAMPLE_RATE = 16000; // 16 kHz, 16-bit mono signed LE PCM input
static const uint32_t SPK_SAMPLE_RATE = 24000; // 24 kHz, 16-bit mono signed LE PCM output from Gemini

static const uint32_t MAX_RECORD_MS     = 5000; // Maximum single utterance duration
static const uint32_t MIN_RECORD_MS     = 700;  // Minimum recording duration before silence cutoff
static const uint32_t SILENCE_CUTOFF_MS = 900;  // Stop early after 900ms of silence once speech is heard
static const float    SPEECH_RMS_THRESH = 220.0f;
static const float    WAKE_RMS_THRESH   = 650.0f; // Threshold for hands-free auto-wake
static const float    SPEAKER_GAIN      = 1.6f;   // Volume boost for MAX98357A with clipping protection

static const size_t   STREAM_CHUNK_BYTES = 2048;  // 1024 samples (64 ms @ 16kHz) per binary WS frame
static const float    HX711_CAL_FACTOR   = 331.64f;

// ============================================================================
// 4. GLOBAL STATE & OBJECTS
// ============================================================================

WiFiMulti wifiMulti;
LiquidCrystal_I2C lcd(0x27, 16, 2);
HX711 scale;
WebSocketsClient webSocket;

bool wifiConnected       = false;
bool wsConnected         = false;
bool geminiReady         = false;
bool isRecording         = false;
bool waitingForResponse  = false;
bool speakerPlaying      = false;
bool autoVoiceWake       = false; // Toggle with 'a' in Serial Monitor

uint32_t lastWsActivityMs     = 0;
uint32_t lastWeightUpdateMs   = 0;
uint32_t lastSpeakerAudioMs   = 0;
uint32_t turnWaitStartMs      = 0;
uint32_t binaryChunksReceived = 0;
uint32_t binaryBytesReceived  = 0;
float    latestWeightGrams    = 0.0f;
bool     scaleAvailable       = false;

String lastInputTranscript  = "";
String lastOutputTranscript = "";

// Carry-over byte in case an odd-length binary frame ever arrives
bool    hasPendingOddByte = false;
uint8_t pendingOddByte    = 0;

// ============================================================================
// 5. LCD & DEVANAGARI-TO-ASCII TRANSLITERATION HELPERS
// ============================================================================

// Standard 16x2 HD44780 LCDs only support ASCII characters in ROM.
// This helper transliterates UTF-8 Hindi (Devanagari) into readable Hinglish/ASCII
// for the 16x2 LCD while keeping full UTF-8 Hindi in the Serial Monitor.
String toLcdSafeAscii(const String& utf8Str) {
  String out = "";
  out.reserve(utf8Str.length());

  const uint8_t* bytes = (const uint8_t*)utf8Str.c_str();
  size_t len = utf8Str.length();
  size_t i = 0;

  while (i < len) {
    uint8_t b0 = bytes[i];
    if (b0 < 0x80) {
      if (b0 >= 32 && b0 <= 126) {
        out += (char)b0;
      } else if (b0 == '\n' || b0 == '\r' || b0 == '\t') {
        out += ' ';
      }
      i++;
      continue;
    }

    // Check 3-byte UTF-8 Devanagari block (U+0900 .. U+097F -> 0xE0 0xA4/0xA5 0x80..0xBF)
    if ((b0 == 0xE0) && (i + 2 < len)) {
      uint8_t b1 = bytes[i + 1];
      uint8_t b2 = bytes[i + 2];
      uint16_t cp = ((uint16_t)(b0 & 0x0F) << 12) |
                    ((uint16_t)(b1 & 0x3F) << 6)  |
                    ((uint16_t)(b2 & 0x3F));
      i += 3;

      switch (cp) {
        case 0x0901: case 0x0902: out += "n"; break;
        case 0x0903: out += "h"; break;
        case 0x0905: out += "a"; break;
        case 0x0906: out += "aa"; break;
        case 0x0907: out += "i"; break;
        case 0x0908: out += "ee"; break;
        case 0x0909: out += "u"; break;
        case 0x090A: out += "oo"; break;
        case 0x090F: out += "e"; break;
        case 0x0910: out += "ai"; break;
        case 0x0913: out += "o"; break;
        case 0x0914: out += "au"; break;
        case 0x0915: out += "k"; break;
        case 0x0916: out += "kh"; break;
        case 0x0917: out += "g"; break;
        case 0x0918: out += "gh"; break;
        case 0x091A: out += "ch"; break;
        case 0x091B: out += "chh"; break;
        case 0x091C: out += "j"; break;
        case 0x091D: out += "jh"; break;
        case 0x091F: case 0x0924: out += "t"; break;
        case 0x0920: case 0x0925: out += "th"; break;
        case 0x0921: case 0x0926: out += "d"; break;
        case 0x0922: case 0x0927: out += "dh"; break;
        case 0x0923: case 0x0928: out += "n"; break;
        case 0x092A: out += "p"; break;
        case 0x092B: out += "f"; break;
        case 0x092C: out += "b"; break;
        case 0x092D: out += "bh"; break;
        case 0x092E: out += "m"; break;
        case 0x092F: out += "y"; break;
        case 0x0930: out += "r"; break;
        case 0x0932: out += "l"; break;
        case 0x0935: out += "v"; break;
        case 0x0936: case 0x0937: out += "sh"; break;
        case 0x0938: out += "s"; break;
        case 0x0939: out += "h"; break;
        // Matras
        case 0x093E: out += "a"; break;
        case 0x093F: out += "i"; break;
        case 0x0940: out += "ee"; break;
        case 0x0941: out += "u"; break;
        case 0x0942: out += "oo"; break;
        case 0x0947: out += "e"; break;
        case 0x0948: out += "ai"; break;
        case 0x094B: out += "o"; break;
        case 0x094C: out += "au"; break;
        case 0x094D: break; // Halant / virama
        case 0x0964: out += "."; break;
        // Devanagari digits 0-9
        case 0x0966: out += "0"; break;
        case 0x0967: out += "1"; break;
        case 0x0968: out += "2"; break;
        case 0x0969: out += "3"; break;
        case 0x096A: out += "4"; break;
        case 0x096B: out += "5"; break;
        case 0x096C: out += "6"; break;
        case 0x096D: out += "7"; break;
        case 0x096E: out += "8"; break;
        case 0x096F: out += "9"; break;
        default: break;
      }
      continue;
    }

    // Skip other multi-byte UTF-8 sequences
    if ((b0 & 0xE0) == 0xC0) i += 2;
    else if ((b0 & 0xF0) == 0xE0) i += 3;
    else if ((b0 & 0xF8) == 0xF0) i += 4;
    else i += 1;
  }

  out.trim();
  return out.length() > 0 ? out : utf8Str.substring(0, 16);
}

void lcdMessage(const String& line1, const String& line2 = "") {
  String safe1 = toLcdSafeAscii(line1);
  String safe2 = toLcdSafeAscii(line2);

  // Pad to 16 chars so we overwrite old characters without flicker from lcd.clear()
  while (safe1.length() < 16) safe1 += ' ';
  while (safe2.length() < 16) safe2 += ' ';

  lcd.setCursor(0, 0);
  lcd.print(safe1.substring(0, 16));
  lcd.setCursor(0, 1);
  lcd.print(safe2.substring(0, 16));
}

void showIdleLcdStatus() {
  if (!wifiConnected) {
    lcdMessage("WiFi Connecting", "Please wait...");
    return;
  }
  if (!wsConnected || !geminiReady) {
    lcdMessage("Connecting AI..", BACKEND_HOST);
    return;
  }
  String line1 = "KHAU KA! Ready";
  String line2 = scaleAvailable
                   ? ("Wt: " + String(latestWeightGrams, 1) + " g")
                   : "Btn/v: Talk";
  lcdMessage(line1, line2);
}

// ============================================================================
// 6. I2S MICROPHONE (INMP441) & SPEAKER (MAX98357A) SETUP
// ============================================================================

bool setupMicI2S() {
  i2s_config_t cfg = {};
  cfg.mode = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_RX);
  cfg.sample_rate = MIC_SAMPLE_RATE;
  cfg.bits_per_sample = I2S_BITS_PER_SAMPLE_32BIT;
  cfg.channel_format = I2S_CHANNEL_FMT_ONLY_LEFT;
  cfg.communication_format = I2S_COMM_FORMAT_STAND_I2S;
  cfg.intr_alloc_flags = ESP_INTR_FLAG_LEVEL1;
  cfg.dma_buf_count = 8;
  cfg.dma_buf_len = 256;
  cfg.use_apll = false;
  cfg.tx_desc_auto_clear = false;
  cfg.fixed_mclk = 0;

  esp_err_t err = i2s_driver_install(MIC_I2S_PORT, &cfg, 0, NULL);
  if (err != ESP_OK && err != ESP_ERR_INVALID_STATE) {
    Serial.printf("[I2S] Mic driver install failed: %d\n", (int)err);
    return false;
  }

  i2s_pin_config_t pins = {};
  pins.bck_io_num = MIC_SCK_PIN;
  pins.ws_io_num = MIC_WS_PIN;
  pins.data_out_num = I2S_PIN_NO_CHANGE;
  pins.data_in_num = MIC_SD_PIN;

  err = i2s_set_pin(MIC_I2S_PORT, &pins);
  if (err != ESP_OK) {
    Serial.printf("[I2S] Mic pin setup failed: %d\n", (int)err);
    return false;
  }

  i2s_zero_dma_buffer(MIC_I2S_PORT);
  return true;
}

bool setupSpeakerI2S() {
  i2s_config_t cfg = {};
  cfg.mode = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_TX);
  cfg.sample_rate = SPK_SAMPLE_RATE;
  cfg.bits_per_sample = I2S_BITS_PER_SAMPLE_16BIT;
  cfg.channel_format = I2S_CHANNEL_FMT_RIGHT_LEFT;
  cfg.communication_format = I2S_COMM_FORMAT_STAND_I2S;
  cfg.intr_alloc_flags = ESP_INTR_FLAG_LEVEL1;
  cfg.dma_buf_count = 10;
  cfg.dma_buf_len = 256;
  cfg.use_apll = false;
  cfg.tx_desc_auto_clear = true;
  cfg.fixed_mclk = 0;

  esp_err_t err = i2s_driver_install(SPK_I2S_PORT, &cfg, 0, NULL);
  if (err != ESP_OK && err != ESP_ERR_INVALID_STATE) {
    Serial.printf("[I2S] Speaker driver install failed: %d\n", (int)err);
    return false;
  }

  i2s_pin_config_t pins = {};
  pins.bck_io_num = SPK_BCLK_PIN;
  pins.ws_io_num = SPK_LRC_PIN;
  pins.data_out_num = SPK_DIN_PIN;
  pins.data_in_num = I2S_PIN_NO_CHANGE;

  err = i2s_set_pin(SPK_I2S_PORT, &pins);
  if (err != ESP_OK) {
    Serial.printf("[I2S] Speaker pin setup failed: %d\n", (int)err);
    return false;
  }

  i2s_zero_dma_buffer(SPK_I2S_PORT);
  return true;
}

// ============================================================================
// 7. STREAMING SPEAKER PLAYBACK (24 kHz 16-bit Signed Mono -> Stereo I2S)
// ============================================================================

void streamPcm16Mono24kToSpeaker(const uint8_t* rawBytes, size_t byteLen) {
  if (!rawBytes || byteLen == 0) return;

  speakerPlaying = true;
  lastSpeakerAudioMs = millis();

  // Handle byte alignment across arbitrary network frame boundaries
  const uint8_t* ptr = rawBytes;
  size_t remaining = byteLen;

  int16_t stereoBlock[256 * 2]; // 256 stereo frames = 1024 bytes

  if (hasPendingOddByte && remaining > 0) {
    uint16_t combined = (uint16_t)pendingOddByte | ((uint16_t)ptr[0] << 8);
    int16_t sample = (int16_t)combined;
    int32_t boosted = (int32_t)((float)sample * SPEAKER_GAIN);
    if (boosted > 32767) boosted = 32767;
    if (boosted < -32768) boosted = -32768;
    stereoBlock[0] = (int16_t)boosted;
    stereoBlock[1] = (int16_t)boosted;

    size_t written = 0;
    i2s_write(SPK_I2S_PORT, stereoBlock, 2 * sizeof(int16_t), &written, portMAX_DELAY);

    ptr += 1;
    remaining -= 1;
    hasPendingOddByte = false;
  }

  if (remaining % 2 != 0) {
    pendingOddByte = ptr[remaining - 1];
    hasPendingOddByte = true;
    remaining -= 1;
  }

  size_t totalSamples = remaining / sizeof(int16_t);
  const int16_t* monoSamples = (const int16_t*)ptr;
  size_t offset = 0;

  while (offset < totalSamples) {
    size_t batch = totalSamples - offset;
    if (batch > 256) batch = 256;

    for (size_t i = 0; i < batch; i++) {
      int32_t boosted = (int32_t)((float)monoSamples[offset + i] * SPEAKER_GAIN);
      if (boosted > 32767) boosted = 32767;
      if (boosted < -32768) boosted = -32768;
      int16_t s = (int16_t)boosted;
      stereoBlock[2 * i]     = s; // Left
      stereoBlock[2 * i + 1] = s; // Right
    }

    size_t written = 0;
    i2s_write(SPK_I2S_PORT, stereoBlock, batch * 2 * sizeof(int16_t), &written, portMAX_DELAY);
    offset += batch;
  }
}

// ============================================================================
// 8. HX711 LOAD CELL FUNCTIONS
// ============================================================================

void setupScale() {
  scale.begin(HX711_DOUT_PIN, HX711_SCK_PIN);
  scale.set_scale(HX711_CAL_FACTOR);
  if (scale.wait_ready_timeout(1200)) {
    scale.tare(10);
    latestWeightGrams = scale.get_units(3);
    if (fabs(latestWeightGrams) < 0.5f) latestWeightGrams = 0.0f;
    scaleAvailable = true;
    Serial.printf("[HX711] Ready. Initial weight: %.1f g\n", latestWeightGrams);
  } else {
    scaleAvailable = false;
    Serial.println(F("[HX711] Not detected at startup (check DOUT=13, SCK=14)."));
  }
}

void sendWeightContextToServer(float grams, bool speakNow = false) {
  if (!wsConnected || !geminiReady) return;
  JsonDocument doc;
  doc["type"] = "weight_context";
  doc["grams"] = serialized(String(grams, 1));
  doc["speakNow"] = speakNow;
  String json;
  serializeJson(doc, json);
  webSocket.sendTXT(json);
}

void tareScale() {
  lcdMessage("Scale Tare...", "Keep clear");
  Serial.println(F("[HX711] Taring scale... keep platform empty."));
  if (scale.wait_ready_timeout(1500)) {
    scale.tare(15);
    latestWeightGrams = 0.0f;
    scaleAvailable = true;
    Serial.println(F("[HX711] Tare complete (0.0 g)."));
    lcdMessage("Scale Tared!", "0.0 g");
    sendWeightContextToServer(0.0f, false);
    delay(600);
  } else {
    Serial.println(F("[HX711] Tare failed: sensor not ready."));
    lcdMessage("HX711 Error", "Check wiring");
    delay(800);
  }
  showIdleLcdStatus();
}

void readAndShowWeight(bool sendToAi = true) {
  if (!scale.wait_ready_timeout(800)) {
    scaleAvailable = false;
    Serial.println(F("[HX711] Sensor not ready."));
    lcdMessage("HX711 not ready", "Check wiring");
    return;
  }
  scaleAvailable = true;
  latestWeightGrams = scale.get_units(5);
  if (fabs(latestWeightGrams) < 0.5f) latestWeightGrams = 0.0f;
  Serial.printf("[HX711] Weight: %.1f g\n", latestWeightGrams);
  lcdMessage("Weight Reading:", String(latestWeightGrams, 1) + " g");
  if (sendToAi) {
    sendWeightContextToServer(latestWeightGrams, false);
  }
}

// ============================================================================
// 9. WEBSOCKET CLIENT EVENT HANDLER (JSON CONTROL + BINARY PCM FRAMES)
// ============================================================================

void handleControlJson(const uint8_t* payload, size_t length) {
  JsonDocument doc;
  DeserializationError err = deserializeJson(doc, payload, length);
  if (err) {
    Serial.printf("[WS] JSON parse error: %s\n", err.c_str());
    return;
  }

  const char* msgType = doc["type"] | "";

  // 1. Server Readiness Announcement (after Gemini setupComplete)
  if (strcmp(msgType, "ready") == 0 || doc["setupComplete"] == true) {
    geminiReady = true;
    const char* activeModel = doc["model"] | "gemini-live";
    Serial.printf("[WS] Gemini Session READY (model=%s, in=16kHz, out=24kHz)\n", activeModel);
    showIdleLcdStatus();
    if (scaleAvailable) {
      sendWeightContextToServer(latestWeightGrams, false);
    }
    return;
  }

  // 2. Status updates (connecting / fallback model)
  if (strcmp(msgType, "status") == 0) {
    const char* status = doc["status"] | "";
    const char* message = doc["message"] | "";
    Serial.printf("[WS] Status [%s]: %s\n", status, message);
    lcdMessage("AI Status:", status);
    return;
  }

  // 3. Recognized User Hindi Speech Transcription
  if (strcmp(msgType, "input_transcript") == 0) {
    lastInputTranscript = doc["text"].as<String>();
    Serial.print(F("[You (Hindi)]   : "));
    Serial.println(lastInputTranscript);
    lcdMessage("You said:", lastInputTranscript);
    return;
  }

  // 4. Gemini Hindi Output Audio Transcription
  if (strcmp(msgType, "output_transcript") == 0) {
    lastOutputTranscript = doc["text"].as<String>();
    Serial.print(F("[KHAU KA! Hindi]: "));
    Serial.println(lastOutputTranscript);
    lcdMessage("KHAU KA!:", lastOutputTranscript);
    return;
  }

  // 5. Text part from modelTurn
  if (strcmp(msgType, "text") == 0) {
    String txt = doc["text"].as<String>();
    Serial.print(F("[KHAU KA! Text] : "));
    Serial.println(txt);
    lcdMessage("KHAU KA!:", txt);
    return;
  }

  // 6. Interruption (barge-in)
  if (strcmp(msgType, "interrupted") == 0) {
    Serial.println(F("[WS] Turn interrupted by user."));
    i2s_zero_dma_buffer(SPK_I2S_PORT);
    speakerPlaying = false;
    waitingForResponse = false;
    hasPendingOddByte = false;
    return;
  }

  // 7. Turn Complete (all binary audio chunks for this response have been delivered)
  if (strcmp(msgType, "turn_complete") == 0) {
    waitingForResponse = false;
    speakerPlaying = false;
    hasPendingOddByte = false;
    i2s_zero_dma_buffer(SPK_I2S_PORT);
    Serial.printf("[WS] Turn complete (audio chunks=%u, bytes=%u).\n",
                  (unsigned)binaryChunksReceived,
                  (unsigned)binaryBytesReceived);
    if (lastOutputTranscript.length() > 0) {
      lcdMessage("KHAU KA!:", lastOutputTranscript);
    } else {
      showIdleLcdStatus();
    }
    return;
  }

  // 8. Pong response
  if (strcmp(msgType, "pong") == 0) {
    Serial.println(F("[WS] Pong received from Render server."));
    return;
  }

  // 9. Error or Warning from Backend / Gemini API
  if (strcmp(msgType, "error") == 0 || strcmp(msgType, "warning") == 0) {
    const char* code = doc["code"] | "ERR";
    const char* message = doc["message"] | "Unknown error";
    Serial.printf("[WS] %s [%s]: %s\n", msgType, code, message);
    lcdMessage(String("Err: ") + code, message);
    if (strcmp(msgType, "error") == 0) {
      waitingForResponse = false;
    }
    return;
  }
}

void webSocketEvent(WStype_t type, uint8_t* payload, size_t length) {
  switch (type) {
    case WStype_DISCONNECTED:
      wsConnected = false;
      geminiReady = false;
      waitingForResponse = false;
      speakerPlaying = false;
      hasPendingOddByte = false;
      Serial.println(F("[WS] Disconnected from Render backend."));
      break;

    case WStype_CONNECTED:
      wsConnected = true;
      geminiReady = false;
      lastWsActivityMs = millis();
      Serial.printf("[WS] Connected to wss://%s%s (waiting for Gemini ready...)\n",
                    BACKEND_HOST,
                    BACKEND_PATH);
      lcdMessage("Server Connected", "Waiting AI setup");
      break;

    case WStype_TEXT:
      lastWsActivityMs = millis();
      handleControlJson(payload, length);
      break;

    case WStype_BIN:
      lastWsActivityMs = millis();
      binaryChunksReceived++;
      binaryBytesReceived += length;
      streamPcm16Mono24kToSpeaker(payload, length);
      break;

    case WStype_ERROR:
      Serial.print(F("[WS] Transport/TLS error: "));
      if (payload && length > 0) {
        Serial.write(payload, length);
      }
      Serial.println();
      break;

    case WStype_PING:
    case WStype_PONG:
      lastWsActivityMs = millis();
      break;

    default:
      break;
  }
}

// ============================================================================
// 10. MICROPHONE CAPTURE & REAL-TIME BINARY PCM STREAMING
// ============================================================================

void streamVoiceTurn() {
  if (!wifiConnected) {
    Serial.println(F("[Voice] Wi-Fi not connected."));
    lcdMessage("No WiFi", "Reconnecting...");
    return;
  }

  if (!wsConnected || !geminiReady) {
    Serial.println(F("[Voice] Waiting for Gemini session readiness..."));
    lcdMessage("Wait for AI...", "Connecting...");
    uint32_t waitStart = millis();
    while ((!wsConnected || !geminiReady) && (millis() - waitStart < 8000)) {
      webSocket.loop();
      delay(10);
    }
    if (!wsConnected || !geminiReady) {
      Serial.println(F("[Voice] Backend/Gemini not ready yet."));
      lcdMessage("AI Not Ready", "Check server");
      return;
    }
  }

  // Update live HX711 weight right before starting voice turn so Gemini has fresh weight
  if (scale.wait_ready_timeout(150)) {
    latestWeightGrams = scale.get_units(2);
    if (fabs(latestWeightGrams) < 0.5f) latestWeightGrams = 0.0f;
    scaleAvailable = true;
    sendWeightContextToServer(latestWeightGrams, false);
  }

  isRecording = true;
  binaryChunksReceived = 0;
  binaryBytesReceived = 0;
  lastOutputTranscript = "";
  hasPendingOddByte = false;
  i2s_zero_dma_buffer(MIC_I2S_PORT);

  lcdMessage("Listening Hindi", "Bolna shuru kare");
  Serial.println(F("\n[Voice] Listening (16kHz 16-bit mono PCM)... Speak in Hindi now!"));

  int32_t rawI2s[256];
  int16_t pcmChunk[STREAM_CHUNK_BYTES / sizeof(int16_t)];
  size_t  pcmSampleCount = 0;
  size_t  totalBytesSent = 0;
  uint32_t chunksSent    = 0;

  uint32_t recordStartMs = millis();
  uint32_t lastSpeechMs  = recordStartMs;
  bool     speechDetected = false;

  while ((millis() - recordStartMs) < MAX_RECORD_MS && wsConnected) {
    size_t bytesRead = 0;
    esp_err_t err = i2s_read(MIC_I2S_PORT, rawI2s, sizeof(rawI2s), &bytesRead, pdMS_TO_TICKS(50));
    if (err != ESP_OK || bytesRead < sizeof(int32_t)) {
      webSocket.loop();
      continue;
    }

    size_t samplesRead = bytesRead / sizeof(int32_t);
    double sumSquares = 0.0;

    for (size_t i = 0; i < samplesRead; i++) {
      // INMP441 24-bit signed value in 32-bit slot -> shift to 16-bit signed PCM
      int32_t s = rawI2s[i] >> 14;
      if (s > 32767) s = 32767;
      if (s < -32768) s = -32768;
      int16_t sample16 = (int16_t)s;
      sumSquares += (double)sample16 * (double)sample16;

      pcmChunk[pcmSampleCount++] = sample16;

      if (pcmSampleCount == (STREAM_CHUNK_BYTES / sizeof(int16_t))) {
        if (webSocket.sendBIN((uint8_t*)pcmChunk, STREAM_CHUNK_BYTES)) {
          totalBytesSent += STREAM_CHUNK_BYTES;
          chunksSent++;
        }
        pcmSampleCount = 0;
        webSocket.loop();
      }
    }

    double rms = samplesRead > 0 ? sqrt(sumSquares / samplesRead) : 0.0;
    if (rms >= SPEECH_RMS_THRESH) {
      speechDetected = true;
      lastSpeechMs = millis();
    }

    // Stop early once user finishes speaking and silence persists for SILENCE_CUTOFF_MS
    uint32_t elapsed = millis() - recordStartMs;
    if (speechDetected && elapsed >= MIN_RECORD_MS && (millis() - lastSpeechMs) >= SILENCE_CUTOFF_MS) {
      break;
    }

    webSocket.loop();
  }

  // Flush any remaining PCM samples in buffer
  if (pcmSampleCount > 0 && wsConnected) {
    size_t remainingBytes = pcmSampleCount * sizeof(int16_t);
    if (webSocket.sendBIN((uint8_t*)pcmChunk, remainingBytes)) {
      totalBytesSent += remainingBytes;
      chunksSent++;
    }
  }

  isRecording = false;

  // Signal end_of_turn to backend
  if (wsConnected) {
    webSocket.sendTXT("{\"type\":\"end_of_turn\"}");
  }

  Serial.printf("[Voice] Sent %u binary chunks (%u bytes, %.2f sec). Waiting for Gemini...\n",
                (unsigned)chunksSent,
                (unsigned)totalBytesSent,
                (float)totalBytesSent / (MIC_SAMPLE_RATE * sizeof(int16_t)));

  lcdMessage("Thinking...", "KHAU KA! AI");
  waitingForResponse = true;
  turnWaitStartMs = millis();
}

// ============================================================================
// 11. WI-FI & WEBSOCKET CONNECTION MANAGEMENT
// ============================================================================

void setupWiFiAndWebSocket() {
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);

  for (size_t i = 0; i < KNOWN_WIFI_COUNT; i++) {
    if (strlen(KNOWN_WIFI_NETWORKS[i].ssid) > 0 &&
         strcmp(KNOWN_WIFI_NETWORKS[i].ssid, "YOUR_WIFI_SSID") != 0) {
      wifiMulti.addAP(KNOWN_WIFI_NETWORKS[i].ssid, KNOWN_WIFI_NETWORKS[i].password);
    }
  }

  lcdMessage("Connecting WiFi", "Please wait...");
  Serial.print(F("[WiFi] Scanning and connecting to known networks"));

  uint32_t startMs = millis();
  while (wifiMulti.run() != WL_CONNECTED && (millis() - startMs) < 20000) {
    delay(350);
    Serial.print('.');
  }
  Serial.println();

  wifiConnected = (WiFi.status() == WL_CONNECTED);
  if (wifiConnected) {
    Serial.printf("[WiFi] Connected to \"%s\" | IP: %s | RSSI: %d dBm\n",
                  WiFi.SSID().c_str(),
                  WiFi.localIP().toString().c_str(),
                  WiFi.RSSI());
    lcdMessage("WiFi Connected", WiFi.localIP().toString());
  } else {
    Serial.println(F("[WiFi] Initial connection timed out; will retry automatically in loop."));
    lcdMessage("WiFi Failed", "Retrying...");
  }

  // Configure Secure WebSocket connection to Render backend
  if (strlen(DEVICE_AUTH_TOKEN) > 0) {
    String header = String("x-device-token: ") + DEVICE_AUTH_TOKEN + "\r\n";
    webSocket.setExtraHeaders(header.c_str());
  }

  webSocket.onEvent(webSocketEvent);
  webSocket.setReconnectInterval(4000);
  webSocket.enableHeartbeat(20000, 4000, 2);
  webSocket.beginSSL(BACKEND_HOST, BACKEND_PORT, BACKEND_PATH);
  Serial.printf("[WS] Initialized WSS client -> wss://%s:%u%s\n",
                BACKEND_HOST,
                BACKEND_PORT,
                BACKEND_PATH);
}

void printHelp() {
  Serial.println();
  Serial.println(F("============================================================"));
  Serial.println(F(" KHAU KA! ESP32-S3 Hindi Voice Assistant — Diagnostics/Help"));
  Serial.println(F("============================================================"));
  Serial.printf(" Wi-Fi Status     : %s (%s)\n",
                wifiConnected ? "CONNECTED" : "DISCONNECTED",
                wifiConnected ? WiFi.SSID().c_str() : "N/A");
  Serial.printf(" Backend WSS      : wss://%s:%u%s (%s)\n",
                BACKEND_HOST,
                BACKEND_PORT,
                BACKEND_PATH,
                wsConnected ? "OPEN" : "CLOSED");
  Serial.printf(" Gemini Session   : %s\n", geminiReady ? "READY" : "NOT READY");
  Serial.printf(" HX711 Scale      : %s (%.1f g)\n",
                scaleAvailable ? "ONLINE" : "OFFLINE",
                latestWeightGrams);
  Serial.printf(" Auto Voice Wake  : %s\n", autoVoiceWake ? "ENABLED" : "DISABLED");
  Serial.printf(" Free Heap Memory : %u bytes\n", (unsigned)ESP.getFreeHeap());
  Serial.println(F("------------------------------------------------------------"));
  Serial.println(F(" Controls (Standalone & Serial):"));
  Serial.println(F("   - Short Press GPIO 12 / Press BOOT (GPIO 0) : Talk in Hindi"));
  Serial.println(F("   - Long Press GPIO 12 (>= 1.2 sec)           : Tare Scale"));
  Serial.println(F("   - Serial 'v' : Record voice & talk to KHAU KA!"));
  Serial.println(F("   - Serial 'w' : Read HX711 weight"));
  Serial.println(F("   - Serial 't' : Tare HX711 scale"));
  Serial.println(F("   - Serial 'a' : Toggle hands-free voice wake ON/OFF"));
  Serial.println(F("   - Serial 'p' : Send ping to Render server"));
  Serial.println(F("   - Serial '?' : Show this status & help menu"));
  Serial.println(F("============================================================"));
}

// ============================================================================
// 12. ARDUINO SETUP & MAIN LOOP
// ============================================================================

void setup() {
  Serial.begin(115200);
  delay(600);

  Serial.println();
  Serial.println(F("============================================================"));
  Serial.println(F(" KHAU KA! — ESP32-S3 Hindi Voice Assistant Booting..."));
  Serial.println(F("============================================================"));

  // 1. Initialize 16x2 I2C LCD (SDA=1, SCL=2)
  Wire.begin(LCD_SDA_PIN, LCD_SCL_PIN);
  lcd.init();
  lcd.backlight();
  lcdMessage("KHAU KA! Hindi", "Booting...");

  // 2. Initialize Buttons
  pinMode(TARE_BUTTON_PIN, INPUT_PULLUP);
  pinMode(BOOT_BUTTON_PIN, INPUT_PULLUP);

  // 3. Initialize HX711 Load Cell (DOUT=13, SCK=14)
  setupScale();

  // 4. Initialize I2S Microphone (INMP441) & Speaker (MAX98357A)
  bool micOk = setupMicI2S();
  bool spkOk = setupSpeakerI2S();
  Serial.printf("[Audio] INMP441 Mic: %s | MAX98357A Speaker: %s\n",
                micOk ? "OK" : "FAILED",
                spkOk ? "OK" : "FAILED");

  // 5. Connect to Wi-Fi and Render WSS Backend
  setupWiFiAndWebSocket();
  printHelp();
}

void loop() {
  // 1. Maintain Wi-Fi connection and auto-reconnect if lost
  static uint32_t lastWifiCheckMs = 0;
  if (millis() - lastWifiCheckMs >= 3000) {
    lastWifiCheckMs = millis();
    bool currentlyConnected = (wifiMulti.run() == WL_CONNECTED);
    if (currentlyConnected != wifiConnected) {
      wifiConnected = currentlyConnected;
      if (wifiConnected) {
        Serial.printf("[WiFi] Reconnected! IP: %s\n", WiFi.localIP().toString().c_str());
        showIdleLcdStatus();
      } else {
        wsConnected = false;
        geminiReady = false;
        Serial.println(F("[WiFi] Connection lost; attempting reconnect..."));
        lcdMessage("WiFi Lost", "Reconnecting...");
      }
    }
  }

  // 2. Service WebSocket client
  if (wifiConnected) {
    webSocket.loop();
  }

  // 3. Handle response timeout (25s) so device never gets stuck in waitingForResponse
  if (waitingForResponse && (millis() - turnWaitStartMs > 25000)) {
    waitingForResponse = false;
    speakerPlaying = false;
    Serial.println(F("[Voice] Response timeout; returning to ready state."));
    showIdleLcdStatus();
  }

  // 4. Clear speakerPlaying flag 400ms after last audio chunk if turn_complete was delayed
  if (speakerPlaying && (millis() - lastSpeakerAudioMs > 400)) {
    speakerPlaying = false;
  }

  // 5. Standalone Button Handling:
  //    - GPIO 12 Short Press (< 1200ms) -> Voice Command
  //    - GPIO 12 Long Press (>= 1200ms) -> Tare Scale
  static bool     btn12Pressed = false;
  static uint32_t btn12DownMs  = 0;
  static bool     btn12LongHandled = false;

  if (digitalRead(TARE_BUTTON_PIN) == LOW) {
    if (!btn12Pressed) {
      btn12Pressed = true;
      btn12DownMs = millis();
      btn12LongHandled = false;
    } else if (!btn12LongHandled && (millis() - btn12DownMs >= 1200)) {
      btn12LongHandled = true;
      tareScale();
    }
  } else if (btn12Pressed) {
    uint32_t heldMs = millis() - btn12DownMs;
    btn12Pressed = false;
    if (!btn12LongHandled && heldMs >= 50) {
      // Short press triggers Hindi voice interaction!
      if (!isRecording && !waitingForResponse && !speakerPlaying) {
        streamVoiceTurn();
      }
    }
  }

  //    - GPIO 0 (BOOT button) Short Press -> Voice Command
  static uint32_t lastBootBtnMs = 0;
  if (digitalRead(BOOT_BUTTON_PIN) == LOW && (millis() - lastBootBtnMs > 800)) {
    lastBootBtnMs = millis();
    if (!isRecording && !waitingForResponse && !speakerPlaying) {
      streamVoiceTurn();
    }
  }

  // 6. Periodic Idle HX711 Weight Refresh (every 4 seconds when idle)
  if (!isRecording && !waitingForResponse && !speakerPlaying &&
      (millis() - lastWeightUpdateMs >= 4000)) {
    lastWeightUpdateMs = millis();
    if (scale.is_ready()) {
      float w = scale.get_units(2);
      if (fabs(w) < 0.5f) w = 0.0f;
      if (fabs(w - latestWeightGrams) >= 1.0f || !scaleAvailable) {
        latestWeightGrams = w;
        scaleAvailable = true;
        showIdleLcdStatus();
      }
    }
  }

  // 7. Optional Hands-Free Voice Wake when idle
  if (autoVoiceWake && geminiReady && !isRecording && !waitingForResponse && !speakerPlaying) {
    int32_t rawSample[64];
    size_t bytesRead = 0;
    if (i2s_read(MIC_I2S_PORT, rawSample, sizeof(rawSample), &bytesRead, 0) == ESP_OK &&
        bytesRead >= sizeof(int32_t)) {
      size_t n = bytesRead / sizeof(int32_t);
      double sumSq = 0.0;
      for (size_t i = 0; i < n; i++) {
        int32_t s = rawSample[i] >> 14;
        sumSq += (double)s * (double)s;
      }
      double rms = sqrt(sumSq / n);
      if (rms >= WAKE_RMS_THRESH) {
        Serial.printf("[AutoWake] Voice activity detected (RMS=%.0f)! Starting capture...\n", rms);
        streamVoiceTurn();
      }
    }
  }

  // 8. Serial Monitor Commands
  if (Serial.available()) {
    char c = Serial.read();
    if (c == 'v' || c == 'V') {
      streamVoiceTurn();
    } else if (c == 'w' || c == 'W') {
      readAndShowWeight(true);
    } else if (c == 't' || c == 'T') {
      tareScale();
    } else if (c == 'a' || c == 'A') {
      autoVoiceWake = !autoVoiceWake;
      Serial.printf("[Config] Hands-free Auto Voice Wake: %s\n",
                    autoVoiceWake ? "ENABLED" : "DISABLED");
      lcdMessage("Auto Voice Wake", autoVoiceWake ? "ENABLED" : "DISABLED");
    } else if (c == 'p' || c == 'P') {
      if (wsConnected) {
        webSocket.sendTXT("{\"type\":\"ping\"}");
        Serial.println(F("[WS] Sent ping to Render backend."));
      } else {
        Serial.println(F("[WS] Not connected to backend."));
      }
    } else if (c == '?') {
      printHelp();
    }
  }

  delay(2);
}
