import Express from "express";
import { createServer } from "http";
import { Server } from "socket.io";
import cors from "cors";
import { v4 as uuidv4 } from "uuid";
import low from 'lowdb';
import FileSync from 'lowdb/adapters/FileSync.js';
import dotenv from 'dotenv';
import { 
  S3Client, 
  PutObjectCommand, 
  CreateMultipartUploadCommand, 
  UploadPartCommand, 
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand 
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

dotenv.config();

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

const PORT = 5050; // Force to 5050 to avoid Port 5000 conflicts

// AWS S3 CONFIGURATION
const s3Client = new S3Client({
  region: process.env.AWS_REGION || 'us-east-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
  },
});

const BUCKET_NAME = process.env.AWS_BUCKET_NAME || 'locksy-bucket';

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
const mediaRegistry = new Map(); // Store media metadata

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

  const dbMedia = db.get('media').value() || [];
  dbMedia.forEach(m => mediaRegistry.set(m.id, m));
  
  console.log(`[DB] Loaded ${users.size} users, ${groups.size} groups, ${chatRooms.size} rooms, ${mediaRegistry.size} media items`);
};

loadFromDb();

// ─── S3 CONNECTIVITY TEST ───────────────────────────
import { HeadBucketCommand } from "@aws-sdk/client-s3";
const testS3 = async () => {
  try {
    await s3Client.send(new HeadBucketCommand({ Bucket: BUCKET_NAME }));
    console.log(`✅ S3 Connection Success: Bucket "${BUCKET_NAME}" is reachable.`);
  } catch (err) {
    console.error(`❌ S3 Connection Failed: Cannot reach bucket "${BUCKET_NAME}". 
      Check your AWS_REGION, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY.`);
    console.error(`Error details: ${err.message}`);
  }
};
testS3();

app.use(Express.json());
app.use(cors());

// Global error handler for JSON parsing errors
app.use((err, req, res, next) => {
  if (err instanceof SyntaxError && err.status === 400 && 'body' in err) {
    console.error('[Server] Bad JSON received:', err.message);
    return res.status(400).json({ error: 'Malformed JSON request' });
  }
  next();
});

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
// MEDIA HANDLING API (AWS S3)
// ─────────────────────────────────────────────────────────────

/**
 * Generate Pre-signed URL for direct upload
 */
app.post("/api/media/upload-url", async (req, res) => {
  try {
    const { fileName, fileType, fileSize, userId } = req.body;

    if (!fileName || !fileType || !userId) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    // Validation: Reject > 200MB
    const MAX_SIZE = 200 * 1024 * 1024; // 200MB
    if (fileSize > MAX_SIZE) {
      return res.status(400).json({ error: "File too large (max 200MB)" });
    }

    // Allow only image/video/pdf/doc
    const allowedTypes = ['image/', 'video/', 'application/pdf', 'application/msword', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'];
    const isAllowed = allowedTypes.some(type => fileType.startsWith(type));
    if (!isAllowed) {
      return res.status(400).json({ error: "File type not allowed" });
    }

    const timestamp = Date.now();
    const cleanFileName = fileName.replace(/[^a-zA-Z0-9.-]/g, '_');
    const key = `uploads/${userId}/${timestamp}-${cleanFileName}`;

    console.log(`[Media] Generating Signed URL:
      - Key: ${key}
      - ContentType: ${fileType}
      - User: ${userId}
      - Expiry: 300s`);

    const command = new PutObjectCommand({
      Bucket: BUCKET_NAME,
      Key: key,
      ContentType: fileType,
    });

    const uploadUrl = await getSignedUrl(s3Client, command, { expiresIn: 300 });
    const fileUrl = `https://${BUCKET_NAME}.s3.${process.env.AWS_REGION}.amazonaws.com/${key}`;

    res.json({
      uploadUrl,
      fileUrl,
      key
    });
  } catch (error) {
    console.error("[Media] Error generating upload URL:", error);
    res.status(500).json({ error: "Failed to generate upload URL" });
  }
});

/**
 * Save file metadata to DB
 */
app.post("/api/media/save", (req, res) => {
  try {
    const { key, fileUrl, type, size, sender_id, chat_id } = req.body;

    if (!key || !fileUrl || !type || !sender_id) {
      return res.status(400).json({ error: "Missing required metadata fields" });
    }

    const mediaId = uuidv4();
    const mediaObj = {
      id: mediaId,
      key,
      url: fileUrl,
      type, // image, video, doc
      size,
      user_id: sender_id,
      chat_id: chat_id || null,
      created_at: new Date().toISOString(),
    };

    mediaRegistry.set(mediaId, mediaObj);

    // Save to DB
    if (!db.has('media').value()) {
      db.set('media', []).write();
    }
    db.get('media').push(mediaObj).write();

    res.json({ success: true, media: mediaObj });
  } catch (error) {
    console.error("[Media] Error saving metadata:", error);
    res.status(500).json({ error: "Failed to save media metadata" });
  }
});

/**
 * Fetch media for a specific chat
 */
app.get("/api/media", (req, res) => {
  const { chat_id } = req.query;
  if (!chat_id) return res.status(400).json({ error: "chat_id is required" });

  const media = Array.from(mediaRegistry.values()).filter(m => m.chat_id === chat_id);
  res.json(media);
});

// ─────────────────────────────────────────────────────────────
// E2EE MEDIA HANDLING API (STRICT PRIVACY)
// ─────────────────────────────────────────────────────────────

/**
 * Generate Pre-signed URL for E2EE Upload (Direct to S3)
 * Backend NEVER receives file data or keys.
 */
app.post("/api/media/e2ee/upload-url", async (req, res) => {
  try {
    const { fileName, userId } = req.body;

    if (!fileName || !userId) {
      return res.status(400).json({ error: "Missing fileName or userId" });
    }

    const timestamp = Date.now();
    const cleanFileName = fileName.replace(/[^a-zA-Z0-9.-]/g, '_');
    // Store as binary blob (encrypted cipher)
    const key = `e2ee/${userId}/${timestamp}-${cleanFileName}.bin`;

    console.log(`[E2EE-Media] Generating Upload URL for ${key}`);

    const command = new PutObjectCommand({
      Bucket: BUCKET_NAME,
      Key: key,
      ContentType: 'application/octet-stream', // Force binary for encrypted data
    });

    const uploadUrl = await getSignedUrl(s3Client, command, { expiresIn: 60 }); // Short expiry (60s)

    res.json({
      uploadUrl,
      key
    });
  } catch (error) {
    console.error("[E2EE-Media] Error generating upload URL:", error);
    res.status(500).json({ error: "Failed to generate upload URL" });
  }
});

/**
 * Generate Pre-signed URL for E2EE Download
 */
app.get("/api/media/e2ee/download-url", async (req, res) => {
  try {
    const { media_id } = req.query;

    if (!media_id) {
      return res.status(400).json({ error: "Missing media_id" });
    }

    // Look up metadata in DB
    const media = db.get('media').find({ id: media_id }).value();
    if (!media) {
      return res.status(404).json({ error: "Media metadata not found" });
    }

    console.log(`[E2EE-Media] Generating Download URL for ID: ${media_id} (Key: ${media.s3_key})`);

    const { GetObjectCommand } = await import("@aws-sdk/client-s3");
    const command = new GetObjectCommand({
      Bucket: BUCKET_NAME,
      Key: media.s3_key,
    });

    const downloadUrl = await getSignedUrl(s3Client, command, { expiresIn: 60 });

    res.json({
      downloadUrl,
      s3_key: media.s3_key,
      nonce: media.nonce,
      file_name: media.file_name,
      mime_type: media.mime_type
    });
  } catch (error) {
    console.error("[E2EE-Media] Error generating download URL:", error);
    res.status(500).json({ error: "Failed to generate download URL" });
  }
});

/**
 * Save E2EE Metadata (Cipher Metadata)
 * Store everything EXCEPT content and keys.
 */
app.post("/api/media/e2ee/save-metadata", (req, res) => {
  try {
    const { 
      key, 
      nonce, 
      file_name, 
      mime_type, 
      sender_id, 
      receiver_id, 
      chat_id, 
      size 
    } = req.body;

    if (!key || !nonce || !sender_id || !file_name) {
      return res.status(400).json({ error: "Missing required E2EE metadata" });
    }

    const mediaId = uuidv4();
    const mediaObj = {
      id: mediaId,
      s3_key: key,
      nonce, // Base64 encoded nonce
      file_name,
      mime_type,
      size,
      sender_id,
      receiver_id: receiver_id || null,
      chat_id: chat_id || null,
      is_e2ee: true,
      timestamp: new Date().toISOString(),
    };

    // Store in media registry and DB
    mediaRegistry.set(mediaId, mediaObj);
    
    if (!db.has('media').value()) {
      db.set('media', []).write();
    }
    db.get('media').push(mediaObj).write();

    console.log(`[E2EE-Media] Metadata saved for ${file_name} (ID: ${mediaId})`);

    res.json({ success: true, media: mediaObj });
  } catch (error) {
    console.error("[E2EE-Media] Error saving metadata:", error);
    res.status(500).json({ error: "Failed to save E2EE metadata" });
  }
});

// ─────────────────────────────────────────────────────────────
// MULTIPART UPLOAD API
// ─────────────────────────────────────────────────────────────

/**
 * Initiate Multipart Upload
 */
app.post("/api/media/multipart/initiate", async (req, res) => {
  try {
    const { fileName, fileType, userId } = req.body;
    const timestamp = Date.now();
    const cleanFileName = fileName.replace(/[^a-zA-Z0-9.-]/g, '_');
    const key = `uploads/${userId}/${timestamp}-${cleanFileName}`;

    const command = new CreateMultipartUploadCommand({
      Bucket: BUCKET_NAME,
      Key: key,
      ContentType: fileType,
    });

    const response = await s3Client.send(command);
    res.json({
      uploadId: response.UploadId,
      key: response.Key,
    });
  } catch (error) {
    console.error("[Media] Multipart initiate error:", error);
    res.status(500).json({ error: "Failed to initiate multipart upload" });
  }
});

/**
 * Get Pre-signed URLs for parts
 */
app.post("/api/media/multipart/get-presigned-urls", async (req, res) => {
  try {
    const { key, uploadId, partNumbers } = req.body; // partNumbers: array of numbers

    const urls = await Promise.all(
      partNumbers.map(async (partNumber) => {
        const command = new UploadPartCommand({
          Bucket: BUCKET_NAME,
          Key: key,
          UploadId: uploadId,
          PartNumber: partNumber,
        });
        return {
          partNumber,
          url: await getSignedUrl(s3Client, command, { expiresIn: 3600 }),
        };
      })
    );

    res.json({ urls });
  } catch (error) {
    console.error("[Media] Multipart URLs error:", error);
    res.status(500).json({ error: "Failed to generate part URLs" });
  }
});

/**
 * Complete Multipart Upload
 */
app.post("/api/media/multipart/complete", async (req, res) => {
  try {
    const { key, uploadId, parts } = req.body; // parts: [{ ETag, PartNumber }]

    const command = new CompleteMultipartUploadCommand({
      Bucket: BUCKET_NAME,
      Key: key,
      UploadId: uploadId,
      MultipartUpload: {
        Parts: parts.sort((a, b) => a.PartNumber - b.PartNumber),
      },
    });

    await s3Client.send(command);
    const fileUrl = `https://${BUCKET_NAME}.s3.${process.env.AWS_REGION}.amazonaws.com/${key}`;

    res.json({ success: true, fileUrl, key });
  } catch (error) {
    console.error("[Media] Multipart complete error:", error);
    res.status(500).json({ error: "Failed to complete multipart upload" });
  }
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
