const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const cors = require("cors");

const app = express();
app.use(cors());

app.get("/", (req, res) => {
  res.send("Server is working ✅");
});

const server = http.createServer(app);

// ============================
// FORCE ERROR CHECK (IMPORTANT FIX)
// ============================
server.on("error", (err) => {
  console.log("❌ SERVER ERROR:", err.message);
});

// ============================
// WEBSOCKET SERVER
// ============================
const wss = new WebSocket.Server({ server });

// ============================
// STORAGE
// ============================
let users = {};      // { username: ws }
let lastSeen = {};
let messages = [];

const MAX_MESSAGES = 200;

// ============================
// BROADCAST USERS
// ============================
function broadcastUsers() {
  const userList = Object.keys(users);

  Object.entries(users).forEach(([name, client]) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(
        JSON.stringify({
          type: "users",
          list: userList.filter((u) => u !== name),
        })
      );
    }
  });
}

// ============================
// BROADCAST LAST SEEN
// ============================
function broadcastLastSeen() {
  Object.values(users).forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(
        JSON.stringify({
          type: "lastSeen",
          data: lastSeen,
        })
      );
    }
  });
}

// ============================
// CONNECTION
// ============================
wss.on("connection", (ws) => {
  console.log("✅ Client connected");

  let currentUser = null;

  ws.on("message", (data) => {
    try {
      const msg = JSON.parse(data.toString());

      // ================= JOIN =================
      if (msg.type === "join") {
        currentUser = msg.user;

        users[currentUser] = ws;

        console.log("👤 Joined:", currentUser);

        ws.send(
          JSON.stringify({
            type: "history",
            data: messages,
          })
        );

        broadcastUsers();
        broadcastLastSeen();
        return;
      }

      // ================= MESSAGE =================
      if (msg.type === "message") {
        const messageData = {
          user: msg.user,
          to: msg.to,
          text: msg.text || "",
          image: msg.image || null,
          audio: msg.audio || null,
          time: msg.time || new Date().toLocaleTimeString(),
        };

        messages.push(messageData);

        if (messages.length > MAX_MESSAGES) {
          messages.shift();
        }

        const receiver = users[msg.to];

        if (receiver && receiver.readyState === WebSocket.OPEN) {
          receiver.send(
            JSON.stringify({
              type: "message",
              data: messageData,
            })
          );
        }

        if (ws.readyState === WebSocket.OPEN) {
          ws.send(
            JSON.stringify({
              type: "message",
              data: messageData,
            })
          );
        }

        return;
      }

      // ================= TYPING =================
      if (msg.type === "typing") {
        const target = users[msg.to];
        if (target && target.readyState === WebSocket.OPEN) {
          target.send(JSON.stringify({ type: "typing", user: msg.user }));
        }
        return;
      }

      // ================= CALL =================
      if (msg.type === "call") {
        const target = users[msg.to];
        if (target && target.readyState === WebSocket.OPEN) {
          target.send(
            JSON.stringify({
              type: "call",
              from: msg.from,
              callType: msg.callType,
            })
          );
        }
        return;
      }

      // ================= ANSWER =================
      if (msg.type === "answer") {
        const target = users[msg.to];
        if (target && target.readyState === WebSocket.OPEN) {
          target.send(JSON.stringify({ type: "answer", answer: msg.answer }));
        }
        return;
      }

      // ================= ICE =================
      if (msg.type === "candidate") {
        const target = users[msg.to];
        if (target && target.readyState === WebSocket.OPEN) {
          target.send(
            JSON.stringify({
              type: "candidate",
              candidate: msg.candidate,
            })
          );
        }
        return;
      }

      // ================= REJECT =================
      if (msg.type === "reject") {
        const target = users[msg.to];
        if (target && target.readyState === WebSocket.OPEN) {
          target.send(JSON.stringify({ type: "reject" }));
        }
        return;
      }
    } catch (err) {
      console.log("❌ MESSAGE ERROR:", err.message);
    }
  });

  // ================= DISCONNECT =================
  ws.on("close", () => {
    if (currentUser) {
      console.log("⚠️ Disconnected:", currentUser);

      delete users[currentUser];
      lastSeen[currentUser] = new Date().toLocaleString();

      broadcastUsers();
      broadcastLastSeen();
    }
  });

  ws.on("error", (err) => {
    console.log("❌ WS ERROR:", err.message);
  });
});

// ============================
// START SERVER (IMPORTANT FIX)
// ============================
const PORT = 8080;

server.listen(PORT, "0.0.0.0", () => {
  console.log("🚀 HTTP + WS Server started");
  console.log(`👉 WebSocket: ws://localhost:${PORT}`);
});