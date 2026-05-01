import Express from "express";
import { createServer } from "http";
import { Server } from "socket.io";
import cors from "cors";
import { v4 as uuidv4 } from "uuid";
import low from 'lowdb';
import FileSync from 'lowdb/adapters/FileSync.js';

const adapter = new FileSync('db.json');
const db = low(adapter);

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
  maxHttpBufferSize: 100e6, 
});

const PORT = process.env.PORT || 5000;

// ─────────────────────────────────────────────────────────────
// In-memory runtime state (Mirrors DB for performance)
// ─────────────────────────────────────────────────────────────
const users = new Map(); 
const chatRooms = new Map(); 
const userSockets = new Map(); 
const pendingMessages = new Map(); 
const pendingRequests = new Map(); 
const groups = new Map(); 
const groupInvites = new Map(); 

// LOAD DATA FROM DB ON STARTUP
const dbUsers = db.get('users').value() || [];
dbUsers.forEach(u => users.set(u.cid, u));

const dbGroups = db.get('groups').value() || [];
dbGroups.forEach(g => {
  // BACKFILL: If group is missing creatorPublicKey, try to find it from users
  if (!g.creatorPublicKey && g.createdBy) {
    const creator = users.get(g.createdBy);
    if (creator && creator.publicKey) {
      g.creatorPublicKey = creator.publicKey;
      db.get('groups').find({ groupId: g.groupId }).assign({ creatorPublicKey: g.creatorPublicKey }).write();
      console.log(`[Startup] Backfilled creatorPublicKey for group: ${g.name}`);
    }
  }
  groups.set(g.groupId, g);
});

const dbRooms = db.get('chatRooms').value() || [];
dbRooms.forEach(r => chatRooms.set(r.roomId, r));

console.log(`[Startup] Loaded ${users.size} users, ${groups.size} groups, and ${chatRooms.size} rooms from DB`);

// Sync Maps with DB on startup
const loadFromDb = () => {
  const dbUsers = db.get('users').value();
  dbUsers.forEach(u => users.set(u.cid, { ...u, socketId: null, status: 'offline' }));
  
  const dbGroups = db.get('groups').value();
  dbGroups.forEach(g => groups.set(g.groupId, g));
  
  const dbRooms = db.get('chatRooms').value();
  dbRooms.forEach(r => chatRooms.set(r.roomId, r));
  
  console.log(`[DB] Loaded ${users.size} users, ${groups.size} groups, ${chatRooms.size} rooms`);
};

loadFromDb();

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
    publicKey: user.publicKey,
    status: user.status,
  });
});

app.get("/api/debug/users", (req, res) => {
  const allUsers = Array.from(users.values()).map(u => ({
    cid: u.cid,
    nickname: u.nickname,
    hasPublicKey: !!u.publicKey,
    publicKey: u.publicKey,
    status: u.status
  }));
  res.json(allUsers);
});

// ─────────────────────────────────────────────────────────────
// SOCKET.IO EVENT HANDLERS
// ─────────────────────────────────────────────────────────────

io.on("connection", (socket) => {
  console.log(`[Socket] New connection: ${socket.id}`);

  // ─── User Registration ───────────────────────────────
  socket.on("register", (data) => {
    const { cid, nickname, avatar, publicKey } = data;
    if (!cid) return socket.emit("register:error", { message: "CID is required" });

    // Update existing user or create new
    const existingUser = users.get(cid) || {};
    const userData = {
      cid,
      socketId: socket.id,
      nickname: (nickname && nickname.trim()) ? nickname : `User-${cid.substring(0, 4)}`,
      avatar: avatar || existingUser.avatar || null,
      publicKey: publicKey || existingUser.publicKey || null,
      status: "online",
      lastSeen: new Date().toISOString()
    };
    
    users.set(cid, userData);
    userSockets.set(socket.id, cid);

    // Save to DB (Exclude socket-specific fields)
    const { socketId, status, ...dbData } = userData;
    const userInDb = db.get('users').find({ cid }).value();
    if (userInDb) {
      db.get('users').find({ cid }).assign(dbData).write();
    } else {
      db.get('users').push(dbData).write();
    }

    console.log(`[Register] User ${cid} is now online`);

    socket.emit("register:success", { message: "Welcome back!", cid });
    io.emit("user:status", { cid, status: "online", publicKey: userData.publicKey });

    // Check for pending requests
    const requests = pendingRequests.get(cid);
    if (requests && requests.length > 0) {
      requests.forEach(req => socket.emit("contact:request", req));
      pendingRequests.delete(cid); // Clear after delivery
    }

    // Check for pending group invites
    const invites = groupInvites.get(cid);
    if (invites && invites.length > 0) {
      invites.forEach(inv => socket.emit("group:invite", inv));
      groupInvites.delete(cid);
    }

    // Auto-join group rooms
    groups.forEach((group, groupId) => {
      if (group.members.some(m => m.cid === cid)) {
        socket.join(groupId);
        console.log(`[Join] User ${cid} auto-joined group ${groupId}`);
      }
    });

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
        publicKey: otherUser.publicKey,
      }
    });
  });

  socket.on("search:nickname", (data) => {
    const { nickname } = data;
    const foundUser = Array.from(users.values()).find(u => u.nickname.toLowerCase() === nickname.toLowerCase());

    if (!foundUser) {
      return socket.emit("search:error", { message: "User not found" });
    }

    socket.emit("search:success", {
      otherUser: {
        cid: foundUser.cid,
        nickname: foundUser.nickname,
        avatar: foundUser.avatar,
        status: foundUser.status,
        publicKey: foundUser.publicKey,
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
      publicKey: fromUser?.publicKey || null,
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
      const newRoom = {
        roomId,
        userA: fromCid,
        userB: toCid,
        createdAt: new Date().toISOString(),
        messages: [],
        status: "active",
      };
      chatRooms.set(roomId, newRoom);
      // Save to DB
      db.get('chatRooms').push(newRoom).write();
    }

    const roomData = {
      roomId,
      userA: fromCid,
      userB: toCid,
      requester: { cid: requester.cid, nickname: requester.nickname, avatar: requester.avatar, publicKey: requester.publicKey },
      accepter: { cid: accepter.cid, nickname: accepter.nickname, avatar: accepter.avatar, publicKey: accepter.publicKey }
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

  // ─── Direct Add Connection (QR Code bypass) ─────────
  socket.on("contact:add_direct", (data) => {
    const { fromCid, toCid } = data;
    const requester = users.get(fromCid);
    const accepter = users.get(toCid);

    if (!requester || !accepter) return;

    const roomId = generateRoomId(fromCid, toCid);

    if (!chatRooms.has(roomId)) {
      const newRoom = {
        roomId,
        userA: fromCid,
        userB: toCid,
        createdAt: new Date().toISOString(),
        messages: [],
        status: "active",
      };
      chatRooms.set(roomId, newRoom);
      // Save to DB
      db.get('chatRooms').push(newRoom).write();
    }

    const roomData = {
      roomId,
      userA: fromCid,
      userB: toCid,
      requester: { cid: requester.cid, nickname: requester.nickname, avatar: requester.avatar, publicKey: requester.publicKey },
      accepter: { cid: accepter.cid, nickname: accepter.nickname, avatar: accepter.avatar, publicKey: accepter.publicKey }
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

// ─── Group Management ────────────────────────────────
socket.on("group:create", (data) => {
  const { name, description, creatorCid, members } = data; // members: [{cid, nickname, role, encryptedKey}]
  const groupId = uuidv4();

  // Find the creator's entry in the members list to get their nickname/role
  const creatorEntry = members.find(m => m.cid === creatorCid) || { cid: creatorCid, nickname: 'Admin', role: 'ADMIN' };

  const groupObj = {
    groupId,
    name,
    description,
    createdBy: creatorCid,
    creatorPublicKey: creatorEntry.publicKey,
    members: [creatorEntry], // Initially only the creator is a member
    createdAt: new Date().toISOString(),
    messages: []
  };

  groups.set(groupId, groupObj);
  socket.join(groupId);

  // Save to DB
  db.get('groups').push(groupObj).write();

  console.log(`[Group] Created: ${name} (${groupId}) by ${creatorCid}`);

  // Send invitations to all other intended members
  const otherMembers = members.filter(m => m.cid !== creatorCid);
  
  otherMembers.forEach(m => {
    const inviteData = {
      groupId,
      groupName: name,
      fromNickname: creatorEntry.nickname,
      nickname: m.nickname,
      encryptedKey: m.encryptedKey,
      timestamp: new Date().toISOString(),
    };

    const targetUser = users.get(m.cid);
    if (targetUser && targetUser.status === "online" && targetUser.socketId) {
      io.to(targetUser.socketId).emit("group:invite", inviteData);
      console.log(`[Group] Sent real-time invite to ${m.cid} for group ${name}`);
    } else {
      const existing = groupInvites.get(m.cid) || [];
      existing.push(inviteData);
      groupInvites.set(m.cid, existing);
      console.log(`[Group] Buffered offline invite for ${m.cid} for group ${name}`);
    }
  });

  socket.emit("group:create:success", { ...groupObj, groupKey: data.groupKey });
});

socket.on("group:invite", (data) => {
  const { groupId, adminCid, memberCid, nickname, encryptedKey } = data;
  const group = groups.get(groupId);

  if (!group) return socket.emit("group:error", { message: "Group not found" });

  const admin = group.members.find(m => m.cid === adminCid && m.role === 'ADMIN');
  if (!admin) return socket.emit("group:error", { message: "Unauthorized" });

  const inviteData = {
    groupId,
    groupName: group.name,
    fromNickname: admin.nickname,
    nickname,
    encryptedKey,
    timestamp: new Date().toISOString(),
  };

  const targetUser = users.get(memberCid);
  if (targetUser && targetUser.status === "online" && targetUser.socketId) {
    io.to(targetUser.socketId).emit("group:invite", inviteData);
  } else {
    const existing = groupInvites.get(memberCid) || [];
    existing.push(inviteData);
    groupInvites.set(memberCid, existing);
  }
});

socket.on("group:invite:accept", (data) => {
  const { groupId, memberCid, nickname, encryptedKey } = data;
  const group = groups.get(groupId);

  if (!group) return;

  if (group.members.some(m => m.cid === memberCid)) return;

  const newMember = { cid: memberCid, nickname, role: 'MEMBER', encryptedKey };
  group.members.push(newMember);

  // Save to DB
  db.get('groups').find({ groupId }).assign({ members: group.members }).write();

  // Join the user to the group room if online
  const user = users.get(memberCid);
  if (user && user.socketId) {
    io.sockets.sockets.get(user.socketId)?.join(groupId);
  }

  // Broadcast update to the group
  io.to(groupId).emit("group:update", { 
    type: 'added_to_group', 
    groupId,
    group, // Send the full group object
    member: { cid: memberCid, nickname } 
  });
  
  // Notify the user they joined successfully
  if (user && user.socketId) {
    io.to(user.socketId).emit("group:update", { type: 'added_to_group', group });
  }
});

socket.on("group:remove_member", (data) => {
  const { groupId, adminCid, memberCid } = data;
  const group = groups.get(groupId);

  if (!group) return socket.emit("group:error", { message: "Group not found" });

  const admin = group.members.find(m => m.cid === adminCid && m.role === 'ADMIN');
  if (!admin && adminCid !== memberCid) {
    return socket.emit("group:error", { message: "Unauthorized" });
  }

  group.members = group.members.filter(m => m.cid !== memberCid);

  // Save to DB
  db.get('groups').find({ groupId }).assign({ members: group.members }).write();

  // Notify removed member
  const user = users.get(memberCid);
  if (user && user.socketId) {
    io.to(user.socketId).emit("group:update", { type: 'removed_from_group', groupId });
    io.sockets.sockets.get(user.socketId)?.leave(groupId);
  }

  // Broadcast to others
  io.to(groupId).emit("group:update", { type: 'member_removed', groupId, memberCid });
});

socket.on("group:promote_admin", (data) => {
  const { groupId, adminCid, memberCid } = data;
  const group = groups.get(groupId);

  if (!group) return socket.emit("group:error", { message: "Group not found" });

  const admin = group.members.find(m => m.cid === adminCid && m.role === 'ADMIN');
  if (!admin) return socket.emit("group:error", { message: "Unauthorized: Admin only" });

  const member = group.members.find(m => m.cid === memberCid);
  if (member) {
    member.role = 'ADMIN';
    // Save to DB
    db.get('groups').find({ groupId }).assign({ members: group.members }).write();
    io.to(groupId).emit("group:update", { type: 'admin_promoted', groupId, memberCid });
  }
});

// ─── Send Message (Updated for Groups) ──────────────────
socket.on("message:send", (data) => {
  const { roomId, groupId, message, senderCid, senderNickname } = data;

  const targetId = groupId || roomId;
  if (!targetId) return;

  const messageObj = {
    id: data.id || uuidv4(),
    roomId: roomId || null,
    groupId: groupId || null,
    senderCid,
    senderNickname: (senderNickname && senderNickname.trim()) ? senderNickname : `User-${senderCid.substring(0, 4)}`,
    senderAvatar: data.senderAvatar || null,
    message, // This is the ENCRYPTED payload from the client
    timestamp: new Date().toISOString(),
    status: "delivered",
    reactions: [],
  };

  if (groupId) {
    const group = groups.get(groupId);
    if (!group) return;
    if (!group.members.some(m => m.cid === senderCid)) return;

    if (!group.messages) group.messages = [];
    group.messages.push(messageObj);

    // Save to DB
    db.get('groups').find({ groupId }).assign({ messages: group.messages }).write();
  } else {
    const room = chatRooms.get(roomId);
    if (!room) return;
    room.messages.push(messageObj);

    // Save to DB
    db.get('chatRooms').find({ roomId }).assign({ messages: room.messages }).write();
  }

  // Broadcast to target (room or group)
  io.to(targetId).emit("message:received", messageObj);

  // Buffer for offline members in 1v1
  if (roomId) {
    const room = chatRooms.get(roomId);
    const otherCid = room.userA === senderCid ? room.userB : room.userA;
    const targetUser = users.get(otherCid);

    if (!targetUser || targetUser.status !== "online") {
      const undelivered = pendingMessages.get(roomId) || [];
      undelivered.push(messageObj);
      pendingMessages.set(roomId, undelivered);
    }
  }
});

// ─── Delete Message ──────────────────────────────────
socket.on("message:delete", (data) => {
  const { roomId, messageId } = data;
  const room = chatRooms.get(roomId);

  if (!room) return;

  // Remove from room's memory history
  room.messages = room.messages.filter(m => m.id !== messageId);

  // Remove from pending offline queue if it was stuck there
  const pending = pendingMessages.get(roomId);
  if (pending) {
    pendingMessages.set(roomId, pending.filter(m => m.id !== messageId));
  }

  // Broadcast delete event to all active clients in the room
  io.to(roomId).emit("message:deleted", { roomId, messageId });
});

// ─── React to Message ────────────────────────────────
socket.on("message:react", (data) => {
  const { roomId, messageId, emoji, action } = data;
  const room = chatRooms.get(roomId);

  if (!room) return;

  // Helper to update reactions
  const updateReactions = (messages) => {
    messages.forEach(m => {
      if (m.id === messageId) {
        if (!m.reactions) m.reactions = [];
        if (action === 'add' && !m.reactions.includes(emoji)) {
          m.reactions.push(emoji);
        } else if (action === 'remove') {
          m.reactions = m.reactions.filter(r => r !== emoji);
        }
      }
    });
  };

  // Update room's memory history
  updateReactions(room.messages);

  // Update pending offline queue
  const pending = pendingMessages.get(roomId);
  if (pending) updateReactions(pending);

  // Broadcast reaction event directly to room
  io.to(roomId).emit("message:reaction:updated", data);
});

// ─── Message Opened (View Once) ──────────────────────
socket.on("message:opened", (data) => {
  const { roomId, messageId } = data;
  const room = chatRooms.get(roomId);

  if (!room) return;

  // Update in-memory history and SCRUB SENSITIVE DATA
  room.messages.forEach(m => {
    if (m.id === messageId) {
      m.isOpened = true;
      // Scrub the payload data for security
      if (m.message && typeof m.message === 'object') {
        m.message.uri = null;
        m.message.image = null;
        m.message.text = "[View Once Content Expired]";
      }
    }
  });

  // Broadcast update to the room
  io.to(roomId).emit("message:opened", { roomId, messageId });
  console.log(`[Message] View-once message ${messageId} opened and scrubbed in room ${roomId}`);
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
  const group = groups.get(roomId);
  
  if (room) {
    socket.emit("room:history", { roomId, messages: room.messages });
  } else if (group) {
    socket.emit("room:history", { roomId, messages: group.messages || [] });
  } else {
    socket.emit("room:error", { message: "History not found for this room/group" });
  }
});

socket.on("room:join", (data) => {
  const { roomId } = data;
  if (chatRooms.has(roomId) || groups.has(roomId)) {
    socket.join(roomId);
    socket.emit("room:joined", { success: true, roomId });
    console.log(`[Socket] Socket ${socket.id} joined room/group: ${roomId}`);
  } else {
    socket.emit("room:error", { message: "Room or Group not found" });
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
