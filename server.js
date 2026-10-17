const WebSocket = require('ws');
const http = require('http');

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Khau Ka Voice Server is Running!\n');
});

const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  console.log('ESP32 Connected!');

  const geminiWs = new WebSocket('wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1alpha.GenerativeService.BidiGenerateContent?key=' + process.env.GEMINI_API_KEY);

  geminiWs.on('open', () => {
    console.log('Connected to Gemini Live API');
  });

  geminiWs.on('message', (message) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(message);
    }
  });

  ws.on('message', (message) => {
    if (geminiWs.readyState === WebSocket.OPEN) {
      geminiWs.send(message);
    }
  });

  ws.on('close', () => {
    geminiWs.close();
    console.log('ESP32 Disconnected');
  });

  geminiWs.on('close', () => {
    ws.close();
    console.log('Gemini Disconnected');
  });
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
