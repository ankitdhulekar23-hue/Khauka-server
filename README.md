# KHAU KA! — Hindi Voice Assistant & Smart Weighing Scale (`Khauka-server`)

Production-ready Node.js WebSocket Audio Bridge connecting the **ESP32-S3** hardware device (**INMP441** I2S microphone, **MAX98357A** I2S speaker amplifier, **16x2 I2C LCD**, and **HX711 10 kg load cell**) to the **Google Gemini Live API (`v1beta` `BidiGenerateContent`)** over Render's public `WSS` infrastructure.

---

## 1. Architecture & Audio Protocol

```
+-----------------------------------------------------------------------+
| ESP32-S3 Device (Standalone Hardware)                                 |
| - INMP441 Mic (GPIO 4/5/6)  : 16 kHz, 16-bit signed LE mono PCM       |
| - MAX98357A Spk (GPIO 15/16/17): 24 kHz, 16-bit signed LE -> Stereo   |
| - 16x2 I2C LCD (GPIO 1/2)   : Status, Weight, & Hindi Transliteration |
| - HX711 Scale (GPIO 13/14)  : Live weight context in grams            |
| - GPIO 12 / GPIO 0 Button   : Short press = Talk, Long press = Tare   |
+-----------------------------------------------------------------------+
                   |                                   ^
   Binary WS Frames| (16 kHz PCM16 Mono)               | Binary WS Frames (24 kHz PCM16 Mono,
   + JSON Control  | (end_of_turn, weight_context)     | <=4096B chunks) + JSON Control
                   v                                   | (ready, transcripts, turn_complete)
+-----------------------------------------------------------------------+
| Render Node.js Backend (Khauka-server)                                |
| - Express HTTP (/health, /) + ws WebSocket Server on root path (/)    |
| - Keeps GEMINI_API_KEY exclusively server-side (never on ESP32)       |
| - Waits for Gemini setupComplete before sending {"type":"ready"}      |
| - Automatic model fallback (gemini-3.8-live -> 3.1-flash-live -> ...) |
| - Pre-setup queueing, 16-bit sample alignment, & backpressure pacing  |
+-----------------------------------------------------------------------+
                   |                                   ^
   JSON Text Frames| (setup, realtimeInput.audio,      | JSON Frames (setupComplete,
   (Opcode 0x01)   |  audioStreamEnd, clientContent)   | serverContent, inlineData, transcripts)
                   v                                   |
+-----------------------------------------------------------------------+
| Google Gemini Live API (v1beta BidiGenerateContent)                   |
+-----------------------------------------------------------------------+
```

### Verified Audio Specifications
| Direction | Hardware / Source | Sample Rate | Bit Depth | Channels | Byte Order | WebSocket Frame Type (ESP32 <-> Render) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Input (Mic)** | INMP441 (`I2S_NUM_0`) | `16,000 Hz` | `16-bit signed` | `1 (Mono)` | Little-Endian | **Binary (`WStype_BIN`)** |
| **Output (Speaker)** | Gemini Live API -> MAX98357A (`I2S_NUM_1`) | `24,000 Hz` | `16-bit signed` | `1 (Mono)` -> expanded to `2 (Stereo)` | Little-Endian | **Binary (`WStype_BIN`)**, sliced `<= 4096` bytes |

---

## 2. Hardware Pin Assignments (ESP32-S3)

All existing hardware pin assignments are preserved:

| Component | Signal | ESP32-S3 Pin | Notes |
| :--- | :--- | :--- | :--- |
| **INMP441 Microphone** | `SCK` (BCLK) | `GPIO 4` | `I2S_NUM_0` Master RX (`32-bit` slot >> 14 -> `16-bit` PCM) |
| **INMP441 Microphone** | `WS` (L/R) | `GPIO 5` | Left channel selected |
| **INMP441 Microphone** | `SD` (DOUT) | `GPIO 6` | Data in |
| **MAX98357A Amplifier** | `BCLK` | `GPIO 15` | `I2S_NUM_1` Master TX (`24,000 Hz`, 16-bit stereo frames) |
| **MAX98357A Amplifier** | `LRC` (WS) | `GPIO 16` | Word select |
| **MAX98357A Amplifier** | `DIN` | `GPIO 17` | Audio data out (1.6x gain with clipping protection) |
| **16x2 I2C LCD (`0x27`)** | `SDA` | `GPIO 1` | I2C Data |
| **16x2 I2C LCD (`0x27`)** | `SCL` | `GPIO 2` | I2C Clock |
| **HX711 Load Cell (10kg)** | `DOUT` (DT) | `GPIO 13` | Calibration factor: `331.64f` |
| **HX711 Load Cell (10kg)** | `SCK` | `GPIO 14` | Clock |
| **TARE / TALK Button** | Button to `GND` | `GPIO 12` | `INPUT_PULLUP`: **Short press (<1.2s) = Talk**, **Long press (>=1.2s) = Tare** |
| **Onboard BOOT Button** | Button to `GND` | `GPIO 0` | `INPUT_PULLUP`: **Press = Talk** |

---

## 3. Environment Variables

Configure these in your Render Dashboard (**Environment** tab) or local `.env` file (see [`.env.example`](.env.example)):

| Variable | Required | Default | Description |
| :--- | :---: | :--- | :--- |
| `GEMINI_API_KEY` | **Yes** | — | Your Google AI Studio Gemini API Key (`AIza...`). Kept strictly on the server. |
| `PORT` | No | `10000` | HTTP/WebSocket listening port (Render injects this automatically). |
| `GEMINI_MODEL` | No | `models/gemini-3.8-live` | Primary Gemini Live API model. |
| `GEMINI_MODEL_FALLBACKS` | No | `models/gemini-3.8-live,models/gemini-3.1-flash-live-preview,models/gemini-2.5-flash-native-audio-preview-12-2025,models/gemini-2.0-flash-exp` | Automatic fallback order if primary model is unavailable for your project tier. |
| `GEMINI_VOICE` | No | `Aoede` | Prebuilt voice name (`Aoede`, `Puck`, `Charon`, `Kore`, `Fenrir`). |
| `DEVICE_AUTH_TOKEN` | No | *(empty)* | Optional shared secret token. When set, ESP32 must send `x-device-token: <token>` or `Authorization: Bearer <token>`. |
| `MAX_CONNECTIONS` | No | `10` | Maximum concurrent WebSocket device sessions. |

---

## 4. Local Setup & Automated Tests

```bash
# 1. Install dependencies
npm install

# 2. Run automated test suite (Tests A through J)
npm test

# 3. Start the server locally
GEMINI_API_KEY=your_key_here npm start
```

### Health Check Endpoint
```bash
curl http://localhost:10000/health
```

---

## 5. ESP32-S3 Firmware Setup

The complete ESP32-S3 sketch is located at:
[`firmware/Khauka_ESP32S3_Hindi_Voice_Assistant/Khauka_ESP32S3_Hindi_Voice_Assistant.ino`](firmware/Khauka_ESP32S3_Hindi_Voice_Assistant/Khauka_ESP32S3_Hindi_Voice_Assistant.ino)

### Required Arduino Libraries
Install via **Arduino IDE -> Library Manager**:
1. **WebSockets** by Markus Sattler (`Links2004`) — `v2.7.2`
2. **ArduinoJson** by Benoit Blanchon — `v7.4.3`
3. **LiquidCrystal I2C** by Frank de Brabander — `v1.1.2`
4. **HX711 Arduino Library** by Bogdan Necula — `v0.6.5`

### Arduino IDE Board Settings
- **Board**: `ESP32S3 Dev Module` (`esp32:esp32:esp32s3`)
- **USB CDC On Boot**: `Enabled` (if using native USB port) or `Disabled` (if using UART bridge port)
- **Upload Speed**: `921600`
- **Serial Monitor Baud Rate**: `115200`
