const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static('public'));

app.get('/ice', (req, res) => {
  const list = [];
  const turn = process.env.TURN_URL;
  if (turn) {
    const item = { urls: turn };
    if (process.env.TURN_USERNAME) item.username = process.env.TURN_USERNAME;
    if (process.env.TURN_PASSWORD) item.credential = process.env.TURN_PASSWORD;
    list.push(item);
  }
  res.json(list);
});

const queue = [];
const partnerOf = new Map();

function removeFromQueue(id) {
  const i = queue.indexOf(id);
  if (i !== -1) queue.splice(i, 1);
}

function tryMatch() {
  while (queue.length >= 2) {
    const a = queue.shift();
    const b = queue.shift();
    if (!io.sockets.sockets.get(a)) continue;
    if (!io.sockets.sockets.get(b)) continue;
    partnerOf.set(a, b);
    partnerOf.set(b, a);
    io.to(a).emit('matched', { partnerId: b, initiator: true });
    io.to(b).emit('matched', { partnerId: a, initiator: false });
  }
}

function leavePair(id) {
  const partnerId = partnerOf.get(id);
  if (partnerId && io.sockets.sockets.get(partnerId)) {
    io.to(partnerId).emit('partner-disconnected');
    removeFromQueue(partnerId);
  }
  partnerOf.delete(id);
  partnerOf.delete(partnerId);
}

io.on('connection', (socket) => {
  socket.on('start', () => {
    leavePair(socket.id);
    removeFromQueue(socket.id);
    queue.push(socket.id);
    tryMatch();
  });

  socket.on('next', () => {
    leavePair(socket.id);
    removeFromQueue(socket.id);
    queue.push(socket.id);
    tryMatch();
  });

  socket.on('stop', () => {
    leavePair(socket.id);
    removeFromQueue(socket.id);
  });

  socket.on('offer', (data) => {
    if (partnerOf.get(socket.id) === data.to) {
      io.to(data.to).emit('offer', { from: socket.id, sdp: data.sdp });
    }
  });

  socket.on('answer', (data) => {
    if (partnerOf.get(socket.id) === data.to) {
      io.to(data.to).emit('answer', { from: socket.id, sdp: data.sdp });
    }
  });

  socket.on('ice-candidate', (data) => {
    if (partnerOf.get(socket.id) === data.to) {
      io.to(data.to).emit('ice-candidate', { from: socket.id, candidate: data.candidate });
    }
  });

  socket.on('heartbeat', () => {});

  socket.on('chat-message', (data) => {
    const partnerId = partnerOf.get(socket.id);
    if (partnerId && data && typeof data.text === 'string' && data.text.trim()) {
      io.to(partnerId).emit('chat-message', { text: data.text.slice(0, 500) });
    }
  });

  socket.on('disconnect', () => {
    removeFromQueue(socket.id);
    leavePair(socket.id);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`ChatRuletka running on http://localhost:${PORT}`);
});

setInterval(() => {
  io.emit('online', io.sockets.sockets.size);
}, 8000);