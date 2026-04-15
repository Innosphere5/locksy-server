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
// In-memory storage (in production, use a database)
// ─────────────────────────────────────────────────────────────
const users = new Map(); // CID -> { cid, nickname, socketId, avatar, status }
const chatRooms = new Map(); // roomId -> { roomId, userA, userB, createdAt, messages }
const userSockets = new Map(); // socketId -> CID mapping

// ─────────────────────────────────────────────────────────────
// REST API ENDPOINTS
// ─────────────────────────────────────────────────────────────
app.use(Express.json());
app.use(cors());

app.get("/", (req, res) => {
  res.json({
    status: "Locksy secure chat server is running",
    version: "1.0.0",
  });
});

app.get("/api/users/:cid", (req, res) => {
  const { cid } = req.params;
  const user = users.get(cid);

  if (!user) {
    return res.status(404).json({ error: "User not found" });
  }

  return res.json({
    cid: user.cid,
    nickname: user.nickname,
    avatar: user.avatar,
    status: user.status,
  });
});

app.get("/api/health", (req, res) => {
  res.json({
    status: "healthy",
    usersOnline: users.size,
    roomsActive: chatRooms.size,
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

    if (!cid) {
      socket.emit("register:error", { message: "CID is required" });
      return;
    }

    // Register user
    users.set(cid, {
      cid,
      nickname: nickname || "Anonymous",
      socketId: socket.id,
      avatar: avatar || null,
      status: "online",
      connectedAt: new Date().toISOString(),
    });

    userSockets.set(socket.id, cid);

    console.log(`[Register] User ${cid} registered with socket ${socket.id}`);

    socket.emit("register:success", {
      message: "User registered successfully",
      cid,
    });

    // Broadcast user online status
    io.emit("user:status", {
      cid,
      status: "online",
      nickname: nickname || "Anonymous",
    });
  });

  // ─── Search/Add Contact by CID ───────────────────────
  socket.on("search:cid", (data) => {
    const { myCid, otherCid } = data;

    console.log(`[Search] ${myCid} searching for contact: ${otherCid}`);

    // Verify the other user exists
    const otherUser = users.get(otherCid);

    if (!otherUser) {
      socket.emit("search:error", {
        message: "User not found. Check the CID and try again.",
        otherCid,
      });
      return;
    }

    // Both users found, create or retrieve chat room
    const roomId = generateRoomId(myCid, otherCid);

    // Create chat room if it doesn't exist
    if (!chatRooms.has(roomId)) {
      chatRooms.set(roomId, {
        roomId,
        userA: myCid,
        userB: otherCid,
        createdAt: new Date().toISOString(),
        messages: [],
        status: "active",
      });

      console.log(
        `[ChatRoom] Created room ${roomId} between ${myCid} and ${otherCid}`,
      );
    }

    // Join socket to the room
    socket.join(roomId);

    // Emit success with room and user details
    socket.emit("search:success", {
      message: "Contact found and chat room created!",
      roomId,
      otherUser: {
        cid: otherUser.cid,
        nickname: otherUser.nickname,
        avatar: otherUser.avatar,
        status: otherUser.status,
      },
    });

    // Notify other user if they're online
    if (otherUser.socketId) {
      io.to(otherUser.socketId).emit("contact:added", {
        message: "New contact added you",
        roomId,
        newContact: {
          cid: myCid,
          nickname: users.get(myCid)?.nickname || "Anonymous",
          avatar: users.get(myCid)?.avatar || null,
          status: "online",
        },
      });
    }
  });

  // ─── Send Message ────────────────────────────────────
  socket.on("message:send", (data) => {
    const { roomId, message, senderCid, senderNickname } = data;

    console.log("[Server] Received message:send event");
    console.log("[Server] Data:", { roomId, message: message?.substring(0, 50), senderCid, senderNickname });

    if (!roomId || !message || !senderCid) {
      console.error("[Server] Invalid message data - rejecting");
      socket.emit("message:error", {
        message: "Invalid message data",
      });
      return;
    }

    const chatRoom = chatRooms.get(roomId);
    if (!chatRoom) {
      console.error("[Server] Chat room not found:", roomId);
      socket.emit("message:error", {
        message: "Chat room not found",
      });
      return;
    }

    // Create message object
    const messageObj = {
      id: uuidv4(),
      roomId,
      senderCid,
      senderNickname: senderNickname || "Anonymous",
      message,
      timestamp: new Date().toISOString(),
      encrypted: true, // Placeholder — actual encryption happens on client
      status: "delivered",
    };

    // Store message
    chatRoom.messages.push(messageObj);

    console.log(`[Server] Broadcasting message:received to room: ${roomId}`);
    console.log(`[Server] Message ID: ${messageObj.id}`);
    
    // Broadcast to room
    io.to(roomId).emit("message:received", messageObj);

    console.log(
      `[Message] ${senderCid} -> ${roomId}: "${message.substring(0, 50)}..."`,
    );
  });

  // ─── Get Chat History ────────────────────────────────
  socket.on("room:getHistory", (data) => {
    const { roomId } = data;

    console.log(`[Server] Received room:getHistory for ${roomId}`);

    const chatRoom = chatRooms.get(roomId);
    if (!chatRoom) {
      console.error("[Server] Chat room not found:", roomId);
      socket.emit("room:error", {
        message: "Chat room not found",
      });
      return;
    }

    socket.emit("room:history", {
      roomId,
      messages: chatRoom.messages,
    });
  });

  // ─── Join Room (for users entering chat) ────────────
  socket.on("room:join", (data) => {
    const { roomId } = data;
    const userCid = userSockets.get(socket.id);

    console.log(`[Server] room:join event received from socket ${socket.id}`);
    console.log(`[Server] User CID: ${userCid}, roomId: ${roomId}`);

    if (!roomId) {
      console.error("[Server] No roomId provided for room:join");
      socket.emit("room:error", { message: "roomId is required" });
      return;
    }

    const chatRoom = chatRooms.get(roomId);
    if (!chatRoom) {
      console.error("[Server] Chat room not found for roomId:", roomId);
      console.log("[Server] Available rooms:", Array.from(chatRooms.keys()));
      socket.emit("room:error", { message: "Chat room not found" });
      return;
    }

    console.log(`[Server] Room found, user joining: ${roomId}`);
    socket.join(roomId);
    
    console.log(`[Server] Socket ${socket.id} successfully joined room ${roomId}`);
    console.log(`[Server] Emitting room:joined event back to client`);
    
    socket.emit("room:joined", {
      success: true,
      roomId,
      messageCount: chatRoom.messages.length,
    });
    
    console.log(`[Server] room:joined emitted successfully`);
  });

  // ─── Mark Message as Read ────────────────────────────
  socket.on("message:read", (data) => {
    const { roomId, messageId } = data;

    const chatRoom = chatRooms.get(roomId);
    if (chatRoom) {
      const message = chatRoom.messages.find((m) => m.id === messageId);
      if (message) {
        message.status = "read";
      }
    }

    io.to(roomId).emit("message:readStatus", { messageId, status: "read" });
  });

  // ─── User Typing Indicator ────────────────────────────
  socket.on("typing:start", (data) => {
    const { roomId, userCid, nickname } = data;
    socket.to(roomId).emit("typing:active", { userCid, nickname });
  });

  socket.on("typing:stop", (data) => {
    const { roomId, userCid } = data;
    socket.to(roomId).emit("typing:inactive", { userCid });
  });

  // ─── Disconnect Handler ──────────────────────────────
  socket.on("disconnect", () => {
    const cid = userSockets.get(socket.id);

    if (cid) {
      users.delete(cid);
      userSockets.delete(socket.id);

      console.log(`[Disconnect] User ${cid} disconnected`);

      // Broadcast user offline status
      io.emit("user:status", {
        cid,
        status: "offline",
      });
    }
  });

  // ─── Error Handler ───────────────────────────────────
  socket.on("error", (error) => {
    console.error(`[Socket Error] ${socket.id}:`, error);
  });
});

// ─────────────────────────────────────────────────────────────
// UTILITY FUNCTIONS
// ─────────────────────────────────────────────────────────────

/**
 * Generate consistent room ID from two CIDs
 * Ensures that roomId(A, B) === roomId(B, A)
 */
function generateRoomId(cidA, cidB) {
  const sorted = [cidA, cidB].sort();
  return `room_${sorted[0]}_${sorted[1]}`;
}

// ─────────────────────────────────────────────────────────────
// START SERVER
// ─────────────────────────────────────────────────────────────
httpServer.listen(PORT, () => {
  console.log(`
╔════════════════════════════════════════════════════════╗
║                                                        ║
║     🔐 Locksy Secure Chat Server                      ║
║     Version: 1.0.0                                    ║
║                                                        ║
║     ✅ Server running on port ${PORT}                   ║
║     ✅ Socket.io enabled                               ║
║     ✅ CORS enabled                                    ║
║                                                        ║
╚════════════════════════════════════════════════════════╝
  `);
});
