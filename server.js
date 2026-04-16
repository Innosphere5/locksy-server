import Express from "express";
import { createServer } from "http";
import { Server } from "socket.io";
import cors from "cors";
import { v4 as uuidv4 } from "uuid";

const app = Express();
const httpServer = createServer(app);
const io = new Server(httpServer, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
    allowEIO3: true,
  },
  transports: ["websocket", "polling"],
  path: "/socket.io/",
  maxHttpBufferSize: 1e6,
});

const PORT = process.env.PORT || 5000;

// ─────────────────────────────────────────────────────────────
// In-memory storage (Improved for Persistence)
// ─────────────────────────────────────────────────────────────
const users = new Map(); // CID -> { cid, nickname, socketId, avatar, status, connectedAt }
const chatRooms = new Map(); // roomId -> { roomId, userA, userB, createdAt, messages, status }
const userSockets = new Map(); // socketId -> CID mapping
const pendingMessages = new Map(); // roomId -> Array of undelivered messages
const pendingRequests = new Map(); // toCid -> Array of { fromCid, fromNickname, fromAvatar }

app.use(Express.json());
app.use(cors());

// ─────────────────────────────────────────────────────────────
// REST API ENDPOINTS
// ─────────────────────────────────────────────────────────────
app.get("/", (req, res) => {
  res.json({
    status: "Locksy secure chat server is running",
    version: "1.1.0 (Advanced Persistence)",
  });
});

app.get("/api/users/:cid", (req, res) => {
  const { cid } = req.params;
  const user = users.get(cid);
  if (!user) return res.status(404).json({ error: "User not found" });
  return res.json({
    cid: user.cid,
    nickname: user.nickname,
    avatar: user.avatar,
    status: user.status,
  });
});

// ─────────────────────────────────────────────────────────────
// SOCKET.IO EVENT HANDLERS
// ─────────────────────────────────────────────────────────────

io.on("connection", (socket) => {
  console.log(`[Socket] New connection: ${socket.id}`);

  // ─── User Registration ───────────────────────────────
  socket.on("register", (data) => {
    const { cid, nickname, avatar } = data;
    if (!cid) return socket.emit("register:error", { message: "CID is required" });

    // Update existing user or create new
    const existingUser = users.get(cid) || {};
    const newUser = {
      ...existingUser,
      cid,
      nickname: nickname || existingUser.nickname || "Anonymous",
      avatar: avatar || existingUser.avatar || null,
      socketId: socket.id,
      status: "online",
      lastSeen: new Date().toISOString()
    };

    users.set(cid, newUser);
    userSockets.set(socket.id, cid);

    console.log(`[Register] User ${cid} is now online`);

    socket.emit("register:success", { message: "Welcome back!", cid });
    io.emit("user:status", { cid, status: "online" });

    // Check for pending requests
    const requests = pendingRequests.get(cid);
    if (requests && requests.length > 0) {
      requests.forEach(req => socket.emit("contact:request", req));
      pendingRequests.delete(cid); // Clear after delivery
    }

    // Deliver pending messages for all rooms this user is in
    chatRooms.forEach((room, roomId) => {
      if (room.userA === cid || room.userB === cid) {
        socket.join(roomId);
        const undelivered = pendingMessages.get(roomId);
        if (undelivered && undelivered.length > 0) {
            // Sort by timestamp just in case
            undelivered.forEach(msg => socket.emit("message:received", msg));
            pendingMessages.delete(roomId);
        }
      }
    });
  });

  // ─── Search Contact (Discovery Only) ──────────────────
  socket.on("search:cid", (data) => {
    const { otherCid } = data;
    const otherUser = users.get(otherCid);

    if (!otherUser) {
      return socket.emit("search:error", { message: "User not found" });
    }

    socket.emit("search:success", {
      otherUser: {
        cid: otherUser.cid,
        nickname: otherUser.nickname,
        avatar: otherUser.avatar,
        status: otherUser.status,
      }
    });
  });

  // ─── Connection Request ───────────────────────────────
  socket.on("contact:request:send", (data) => {
    const { fromCid, toCid } = data;
    const fromUser = users.get(fromCid);
    const toUser = users.get(toCid);

    if (!toUser) return socket.emit("contact:request:error", { message: "Target user not found" });

    const requestData = {
      fromCid,
      fromNickname: fromUser?.nickname || "Anonymous",
      fromAvatar: fromUser?.avatar || null,
      timestamp: new Date().toISOString()
    };

    if (toUser.status === "online" && toUser.socketId) {
      io.to(toUser.socketId).emit("contact:request", requestData);
    } else {
      // Store for later
      const existing = pendingRequests.get(toCid) || [];
      existing.push(requestData);
      pendingRequests.set(toCid, existing);
    }

    socket.emit("contact:request:success", { message: "Request sent successfully" });
  });

  // ─── Accept Connection Request ────────────────────────
  socket.on("contact:request:accept", (data) => {
    const { fromCid, toCid } = data; // fromCid is the requester, toCid is the accepter
    const requester = users.get(fromCid);
    const accepter = users.get(toCid);

    if (!requester || !accepter) return;

    const roomId = generateRoomId(fromCid, toCid);

    if (!chatRooms.has(roomId)) {
      chatRooms.set(roomId, {
        roomId,
        userA: fromCid,
        userB: toCid,
        createdAt: new Date().toISOString(),
        messages: [],
        status: "active",
      });
    }

    const roomData = { 
        roomId, 
        userA: fromCid, 
        userB: toCid, 
        requester: { cid: requester.cid, nickname: requester.nickname, avatar: requester.avatar },
        accepter: { cid: accepter.cid, nickname: accepter.nickname, avatar: accepter.avatar }
    };

    // Notify requester
    if (requester.socketId) {
      io.to(requester.socketId).emit("contact:accepted", roomData);
      io.sockets.sockets.get(requester.socketId)?.join(roomId);
    }

    // Notify accepter
    if (accepter.socketId) {
      io.to(accepter.socketId).emit("contact:accepted", roomData);
      io.sockets.sockets.get(accepter.socketId)?.join(roomId);
    }
  });

  // ─── Send Message ────────────────────────────────────
  socket.on("message:send", (data) => {
    const { roomId, message, senderCid, senderNickname } = data;
    const room = chatRooms.get(roomId);

    if (!room) return;

    const messageObj = {
      id: uuidv4(),
      roomId,
      senderCid,
      senderNickname,
      message,
      timestamp: new Date().toISOString(),
      status: "delivered",
    };

    room.messages.push(messageObj);
    
    // Broadcast to room (only online members will get it immediately)
    io.to(roomId).emit("message:received", messageObj);

    // Buffer for offline members
    const otherCid = room.userA === senderCid ? room.userB : room.userA;
    const targetUser = users.get(otherCid);
    
    if (!targetUser || targetUser.status !== "online") {
      const undelivered = pendingMessages.get(roomId) || [];
      undelivered.push(messageObj);
      pendingMessages.set(roomId, undelivered);
      console.log(`[Message] Buffered for offline user ${otherCid} in room ${roomId}`);
    }
  });

  // ─── Disconnect Handler ──────────────────────────────
  socket.on("disconnect", () => {
    const cid = userSockets.get(socket.id);
    if (cid) {
      const user = users.get(cid);
      if (user) {
        user.status = "offline";
        user.lastSeen = new Date().toISOString();
        console.log(`[Disconnect] User ${cid} is now offline`);
        io.emit("user:status", { cid, status: "offline" });
      }
      userSockets.delete(socket.id);
    }
  });

  // Existing history and other methods...
  socket.on("room:getHistory", (data) => {
    const { roomId } = data;
    const room = chatRooms.get(roomId);
    if (room) {
        socket.emit("room:history", { roomId, messages: room.messages });
    }
  });

  socket.on("room:join", (data) => {
    const { roomId } = data;
    if (chatRooms.has(roomId)) {
        socket.join(roomId);
        socket.emit("room:joined", { success: true, roomId });
    }
  });
});

function generateRoomId(cidA, cidB) {
  const sorted = [cidA, cidB].sort();
  return `room_${sorted[0]}_${sorted[1]}`;
}

httpServer.listen(PORT, () => {
  console.log(`✅ Locksy Server v1.1.0 running on port ${PORT}`);
});
