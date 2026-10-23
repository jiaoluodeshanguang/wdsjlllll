// 实时视频接收服务器
// 运行: node server.js   (需先: npm init -y && npm install ws)
// 访问: http://localhost:8080  查看接收端
// 上传端可在任意页面调用 ws://<本机IP>:8080 发送视频 Blob
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = 8080;
const UPLOAD_DIR = path.join(__dirname, 'uploads');
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR);

const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.webm': 'video/webm', '.mp4': 'video/mp4', '.png': 'image/png' };

const server = http.createServer((req, res) => {
  if (req.method === 'POST' && req.url === '/upload') {
    const name = Date.now() + '-' + (req.headers['x-filename'] || 'video.webm').replace(/[^\w.\-]/g, '_');
    const out = fs.createWriteStream(path.join(UPLOAD_DIR, name));
    req.pipe(out);
    out.on('finish', () => {
      console.log('收到上传:', name);
      wss.clients.forEach(c => { if (c.readyState === 1) c.send(JSON.stringify({ type: 'new-video', name })); });
      res.writeHead(200, { 'Access-Control-Allow-Origin': '*' });
      res.end('OK');
    });
    return;
  }
  // OPTIONS 预检
  if (req.method === 'OPTIONS') {
    res.writeHead(204, { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': '*', 'Access-Control-Allow-Methods': 'POST, GET, OPTIONS' });
    return res.end();
  }
  let urlPath = req.url.split('?')[0];
  if (urlPath === '/') urlPath = '/index.html';
  if (urlPath === '/videos.json') {
    const list = fs.readdirSync(UPLOAD_DIR).filter(f => /\.(webm|mp4|mov|mkv)$/i.test(f));
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    return res.end(JSON.stringify(list));
  }
  if (urlPath.startsWith('/videos/')) {
    const file = path.join(UPLOAD_DIR, path.basename(urlPath));
    if (fs.existsSync(file)) {
      const stat = fs.statSync(file);
      res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream', 'Content-Length': stat.size, 'Accept-Ranges': 'bytes' });
      fs.createReadStream(file).pipe(res);
    } else { res.writeHead(404); res.end('Not Found'); }
    return;
  }
  const file = path.join(__dirname, urlPath === '/' ? 'index.html' : urlPath.replace(/^\/+/, ''));
  if (fs.existsSync(file) && fs.statSync(file).isFile()) {
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream' });
    fs.createReadStream(file).pipe(res);
  } else { res.writeHead(404); res.end('Not Found'); }
});

const wss = new WebSocketServer({ server });
const senders = new Set(); // 发送端连接
const viewers = new Set(); // 观看端连接

function broadcastText(obj, except) {
  const s = JSON.stringify(obj);
  wss.clients.forEach(c => { if (c !== except && c.readyState === 1) c.send(s); });
}

wss.on('connection', (socket) => {
  socket.isReady = false;
  // 文本消息用于握手 / 元数据
  socket.on('message', (data, isBinary) => {
    if (isBinary) {
      // 视频分片：第一个分片是 WebM 头部，缓存下来供新观看端补发
      if (senders.has(socket)) {
        if (!socket.header) socket.header = Buffer.from(data);
        viewers.forEach(v => { if (v.isReady && v.readyState === 1) v.send(data, { binary: true }); });
      }
      return;
    }
    try {
      const msg = JSON.parse(data.toString());
      if (msg.type === 'sender') {
        senders.add(socket);
        console.log('📤 发送端已接入 (共 ' + senders.size + ')');
        broadcastText({ type: 'sender-online', mime: msg.mime || 'video/webm;codecs=vp8' }, socket);
      } else if (msg.type === 'viewer') {
        viewers.add(socket);
        socket.isReady = false;
        // 告知当前编码格式
        const s = [...senders][0];
        socket.send(JSON.stringify({ type: 'mime', mime: (s && s.mime) || 'video/webm;codecs=vp8' }));
        // 若发送端已在推流，通知观看端准备
        if (senders.size > 0) socket.send(JSON.stringify({ type: 'stream-start' }));
      } else if (msg.type === 'viewer-ready') {
        // 先补发缓存的 WebM 头部，再标记就绪接收后续分片
        const s = [...senders][0];
        if (s && s.header) socket.send(s.header, { binary: true });
        socket.isReady = true;
      } else if (msg.type === 'mime') {
        socket.mime = msg.mime; // 发送端声明编码
      }
    } catch (e) { /* 忽略 */ }
  });
  socket.on('close', () => {
    if (senders.delete(socket)) {
      console.log('📤 发送端断开');
      broadcastText({ type: 'stream-end' });
    }
    viewers.delete(socket);
  });
});

console.log(`✅ 实时视频接收服务器已启动: http://localhost:${PORT}`);
console.log(`   WebSocket 地址: ws://<本机IP>:${PORT}`);

server.listen(PORT);
