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
  GetObjectCommand,
  CreateMultipartUploadCommand, 
  UploadPartCommand, 
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand 
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import admin from "firebase-admin";
import { readFile } from "fs/promises";

dotenv.config();

// Initialize Firebase Admin
const serviceAccount = JSON.parse(
  await readFile(new URL("./serviceAccountKey.json", import.meta.url))
);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
});


const adapter = new FileSync('db.json');
const db = low(adapter);

// Initialize DB defaults
db.defaults({
  users: [],
  chatRooms: [],
  groups: [],
  media: [],
  groupInvites: []
}).write();

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
// In-memory runtime state (Mirrors Persistent Store for performance)
// ─────────────────────────────────────────────────────────────
const users = new Map(); 
const chatRooms = new Map(); 
const userSockets = new Map(); 
const pendingMessages = new Map(); 
const pendingRequests = new Map(); 
const groups = new Map(); 
const groupInvites = new Map(); 
const mediaRegistry = new Map();

// S3 Persistence Helper for Groups
const syncGroupsToS3 = async () => {
  try {
    const groupsArray = Array.from(groups.values());
    
    // 1. Sync to local DB for fast fallback
    db.set('groups', groupsArray).write();
    
    // 2. Sync to S3 for remote persistence
    await s3Client.send(new PutObjectCommand({
      Bucket: BUCKET_NAME,
      Key: 'persistence/groups.json',
      Body: JSON.stringify(groupsArray),
      ContentType: 'application/json'
    }));
    console.log('[S3] Groups synced to bucket and local DB');
  } catch (err) {
    console.error('[S3] Sync failed:', err);
  }
};

const loadGroupsFromS3 = async () => {
  try {
    console.log('[S3] Attempting to load groups from bucket...');
    const command = new GetObjectCommand({
      Bucket: BUCKET_NAME,
      Key: 'persistence/groups.json',
    });
    
    const response = await s3Client.send(command);
    const stream = response.Body;
    const data = await new Promise((resolve, reject) => {
      const chunks = [];
      stream.on('data', chunk => chunks.push(chunk));
      stream.on('error', reject);
      stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    });

    const groupsArray = JSON.parse(data);
    groupsArray.forEach(g => groups.set(g.groupId, g));
    console.log(`✅ [S3] Successfully loaded ${groups.size} groups from bucket`);
  } catch (err) {
    console.warn('[S3] Load from bucket failed (using local fallback):', err.message);
    // FALLBACK: Load from db.json if S3 is not yet established
    const dbGroups = db.get('groups').value() || [];
    dbGroups.forEach(g => groups.set(g.groupId, g));
    console.log(`[Startup] Loaded ${groups.size} groups from local persistent store`);
  }
};

// LOAD DATA ON STARTUP
const dbUsers = db.get('users').value() || [];
dbUsers.forEach(u => users.set(u.cid, u));

// Await S3 load (with local DB fallback)
await loadGroupsFromS3();

// Sync Maps with DB on startup
const loadFromDb = () => {
  const dbUsers = db.get('users').value() || [];
  dbUsers.forEach(u => users.set(u.cid, { ...u, socketId: null, status: 'offline' }));
  
  const dbGroups = db.get('groups').value() || [];
  dbGroups.forEach(g => groups.set(g.groupId, g));
  
  const dbRooms = db.get('chatRooms').value() || [];
  dbRooms.forEach(r => chatRooms.set(r.roomId, r));

  const dbMedia = db.get('media').value() || [];
  dbMedia.forEach(m => mediaRegistry.set(m.id, m));
  
  const dbInvites = db.get('groupInvites').value() || [];
  dbInvites.forEach(inv => {
    const existing = groupInvites.get(inv.cid) || [];
    existing.push(...inv.invites);
    groupInvites.set(inv.cid, existing);
  });
  
  console.log(`[DB] Loaded ${users.size} users, ${groups.size} groups, ${chatRooms.size} rooms, ${mediaRegistry.size} media items, ${groupInvites.size} users with invites`);
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

// ─── Push Notification Helper ───────────────────────
const sendPushNotification = async (token, title, body, data = {}) => {
  if (!token) return;

  const message = {
    notification: { title, body },
    data: {
      ...data,
      click_action: "FLUTTER_NOTIFICATION_CLICK", // For older Android versions
    },
    android: {
      priority: 'high',
      notification: {
        channelId: "default",
        priority: "high",
        sound: "default",
      },
    },
    token: token,
  };

  try {
    const response = await admin.messaging().send(message);
    console.log(`[Push] Successfully sent to ${token.substring(0, 10)}... :`, response);
  } catch (error) {
    console.error("[Push] Error sending notification:", error);
    if (error.code === 'messaging/registration-token-not-registered') {
      console.warn(`[Push] Token no longer valid, should prune from DB`);
    }
  }
};

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

    const uploadUrl = await getSignedUrl(s3Client, command, { expiresIn: 300 }); // 5 minutes expiry

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

    const downloadUrl = await getSignedUrl(s3Client, command, { expiresIn: 300 });

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
    const { cid, nickname, avatar, publicKey, pushToken } = data;

    if (!cid) return socket.emit("register:error", { message: "CID is required" });

    // Store CID on socket for validation in later events
    socket.cid = cid;

    // Update existing user or create new
    const existingUser = users.get(cid) || {};
    const userData = {
      cid,
      socketId: socket.id,
      nickname: (nickname && nickname.trim()) ? nickname : `User-${cid.substring(0, 4)}`,
      avatar: avatar || existingUser.avatar || null,
      publicKey: publicKey || existingUser.publicKey || null,
      pushToken: pushToken || existingUser.pushToken || null,
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

    // Check for pending group invites (NEW: groep:invite)
    const invites = groupInvites.get(cid);
    if (invites && invites.length > 0) {
      invites.forEach(inv => socket.emit("groep:invite", inv));
      groupInvites.delete(cid);
    }

    // Auto-rejoin rooms & collect active groups for the user
    const userGroups = [];
    groups.forEach((group, groupId) => {
      if (group.members.some(m => m.cid === cid)) {
        socket.join(`group_${groupId}`);
        userGroups.push({
          groupId: group.groupId,
          name: group.name,
          groupLogo: group.groupLogo,
          adminId: group.adminId,
          members: group.members,
          createdAt: group.createdAt
        });
        console.log(`[Join] User ${cid} auto-rejoined group_${groupId}`);
      }
    });

    // Send the current list of groups to the client for reconciliation
    socket.emit("groep:list", userGroups);

    // Deliver pending messages for all rooms this user is in
    chatRooms.forEach((room, roomId) => {
      if (room.userA === cid || room.userB === cid) {
        socket.join(roomId);
        const undelivered = pendingMessages.get(roomId);
        if (undelivered && undelivered.length > 0) {
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

// ─── Group Management (REFINED) ─────────────────────
socket.on("groep:create", (data) => {
  const { groupName: name, description, adminId, members: memberList } = data; 
  const groupId = uuidv4();

  const adminUser = users.get(adminId);
  const adminEntry = { 
    cid: adminId, 
    nickname: adminUser?.nickname || 'Admin', 
    role: 'ADMIN',
    encryptedKey: null // Admin has the raw key
  };

  const groupObj = {
    groupId,
    name,
    description,
    groupLogo: data.groupLogo || null, // NEW: Support for group logo
    adminId,
    adminPublicKey: adminUser?.publicKey || null,
    members: [adminEntry], // Only admin is a member initially
    pendingInvites: memberList.filter(m => m.cid !== adminId),
    createdAt: new Date().toISOString(),
    messages: []
  };

  groups.set(groupId, groupObj);
  socket.join(`group_${groupId}`);

  // Sync to S3 (instead of db.json)
  syncGroupsToS3();

  console.log(`[Group] Created: ${name} (${groupId}) by ${adminId}`);

  // Send invitations to all pending members
  groupObj.pendingInvites.forEach(invitee => {
    const inviteData = {
      groupId,
      groupName: name,
      fromNickname: adminEntry.nickname,
      adminId: adminId, // Include admin's CID
      adminPublicKey: adminUser?.publicKey || null,
      encryptedKey: invitee.encryptedKey, // Include the key for E2EE
      timestamp: new Date().toISOString(),
    };

    const targetUser = users.get(invitee.cid);
    if (targetUser && targetUser.status === "online" && targetUser.socketId) {
      io.to(targetUser.socketId).emit("groep:invite", inviteData);
      console.log(`[Group] Sent real-time invite to ${invitee.cid}`);
    } else {
      const existing = groupInvites.get(invitee.cid) || [];
      existing.push(inviteData);
      groupInvites.set(invitee.cid, existing);
      
      // Persist invites to DB
      const allInvites = Array.from(groupInvites.entries()).map(([cid, invites]) => ({ cid, invites }));
      db.set('groupInvites', allInvites).write();
      
      console.log(`[Group] Buffered offline invite for ${invitee.cid}`);
    }
  });

  socket.emit("groep:created", { groupId, name });
});

socket.on("groep:accept", (data) => {
  const { groupId, userId } = data; // userId is CID
  const group = groups.get(groupId);

  if (!group) return socket.emit("groep:error", { message: "Group not found" });

  // Find user in pendingInvites
  const inviteEntry = group.pendingInvites.find(m => m.cid === userId);
  if (!inviteEntry) {
    return socket.emit("groep:error", { message: "No pending invite for this user" });
  }

  // Move from pendingInvites to members
  group.pendingInvites = group.pendingInvites.filter(m => m.cid !== userId);
  
  const user = users.get(userId);
  const newMember = { 
    cid: userId, 
    nickname: user?.nickname || `User-${userId.substring(0, 4)}`, 
    role: 'MEMBER',
    encryptedKey: inviteEntry.encryptedKey
  };
  
  group.members.push(newMember);

  // Sync to S3
  syncGroupsToS3();

  // Join the user to the socket room
  const targetSocketId = user?.socketId;
  if (targetSocketId) {
    const targetSocket = io.sockets.sockets.get(targetSocketId);
    if (targetSocket) {
      targetSocket.join(`group_${groupId}`);
      console.log(`[Group] User ${userId} accepted and joined room group_${groupId}`);
    }
  }

  // Notify the user they joined successfully
  socket.emit("groep:joined", { groupId, groupName: group.name });

  // Broadcast update to the group
  io.to(`group_${groupId}`).emit("group:update", { 
    type: 'member_added', 
    groupId,
    member: { cid: userId, nickname: newMember.nickname } 
  });
});
socket.on("groep:invite:send", (data) => {
  const { groupId, adminCid, memberCid, encryptedKey } = data;
  const group = groups.get(groupId);

  if (!group) return socket.emit("groep:error", { message: "Group not found" });

  const admin = group.members.find(m => m.cid === adminCid && m.role === 'ADMIN');
  if (!admin) return socket.emit("groep:error", { message: "Unauthorized: Admin only" });

  // Add to pendingInvites if not already there or a member
  if (group.members.some(m => m.cid === memberCid)) return;
  if (!group.pendingInvites.some(m => m.cid === memberCid)) {
    group.pendingInvites.push({ cid: memberCid, encryptedKey });
    db.get('groups').find({ groupId }).assign({ pendingInvites: group.pendingInvites }).write();
  }

  const inviteData = {
    groupId,
    groupName: group.name,
    fromNickname: admin.nickname,
    adminId: adminCid,
    adminPublicKey: admin.publicKey || group.adminPublicKey,
    encryptedKey,
    timestamp: new Date().toISOString(),
  };

  const targetUser = users.get(memberCid);
  if (targetUser && targetUser.status === "online" && targetUser.socketId) {
    io.to(targetUser.socketId).emit("groep:invite", inviteData);
    console.log(`[Group] Invite sent to ${memberCid} for group ${group.name}`);
    } else {
      const existing = groupInvites.get(memberCid) || [];
      existing.push(inviteData);
      groupInvites.set(memberCid, existing);
      
      // Persist invites to DB
      const allInvites = Array.from(groupInvites.entries()).map(([cid, invites]) => ({ cid, invites }));
      db.set('groupInvites', allInvites).write();
      
      console.log(`[Group] Buffered offline invite for ${memberCid}`);
    }
});

socket.on("groep:remove_member", (data) => {
  const { groupId, adminCid, memberCid } = data;
  const group = groups.get(groupId);

  if (!group) return socket.emit("groep:error", { message: "Group not found" });

  // 1. Validate Admin
  if (group.adminId !== adminCid) {
    return socket.emit("groep:error", { message: "Unauthorized: Admin only" });
  }

  // Find the member to get their nickname before removing
  const removedMember = group.members.find(m => m.cid === memberCid);

  // 2. Remove Member
  group.members = group.members.filter(m => m.cid !== memberCid);
  
  // 3. Save & Sync
  syncGroupsToS3();

  // 4. Notify & Force Leave Socket Room
  io.to(`group_${groupId}`).emit("group:update", { 
    type: 'member_removed', 
    groupId,
    groupName: group.name,
    memberCid,
    memberNickname: removedMember?.nickname || 'A member'
  });

  // Find the socket of the removed member and make them leave the room
  const targetUser = users.get(memberCid);
  if (targetUser && targetUser.socketId) {
    const targetSocket = io.sockets.sockets.get(targetUser.socketId);
    if (targetSocket) {
      targetSocket.leave(`group_${groupId}`);
      targetSocket.emit("groep:removed", { groupId, groupName: group.name });
    }
  }
});

socket.on("groep:leave", (data) => {
  const { groupId, userId } = data;
  const group = groups.get(groupId);

  if (!group) return;

  // Find the member to get their nickname before removing
  const leftMember = group.members.find(m => m.cid === userId);

  // 1. Remove from members
  group.members = group.members.filter(m => m.cid !== userId);

  // 2. Handle Admin Leaving
  if (group.adminId === userId && group.members.length > 0) {
    // Pass admin rights to the first remaining member
    const newAdmin = group.members[0];
    group.adminId = newAdmin.cid;
    
    // FETCH NEW ADMIN'S PUBLIC KEY to ensure E2EE continues working
    const newAdminUser = users.get(newAdmin.cid);
    if (newAdminUser && newAdminUser.publicKey) {
      group.adminPublicKey = newAdminUser.publicKey;
    }
  }

  // 3. Delete group if empty
  if (group.members.length === 0) {
    groups.delete(groupId);
  }

  // 4. Save & Sync
  syncGroupsToS3();

  // 5. Notify
  socket.leave(`group_${groupId}`);
  io.to(`group_${groupId}`).emit("group:update", { 
    type: 'member_left', 
    groupId,
    memberCid: userId,
    memberNickname: leftMember?.nickname || 'A member',
    newAdminId: group.adminId
  });

  socket.emit("groep:left:success", { groupId });
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

// ─── Send Message (Strict Group Validation) ──────────
socket.on("message:send", (data) => {
  const { roomId, groupId, message, senderCid, senderNickname } = data;

  // Use the CID stored on the socket for absolute verification
  const verifiedSenderCid = socket.cid || senderCid;

  if (groupId) {
    const group = groups.get(groupId);
    if (!group) return console.warn(`[Send] Group ${groupId} not found`);

    // STRICT VALIDATION: Is the sender a member?
    if (!group.members.some(m => m.cid === verifiedSenderCid)) {
      console.error(`[Security] Unauthorized message attempt by ${verifiedSenderCid} in group ${groupId}`);
      return socket.emit("message:error", { message: "You are not a member of this group" });
    }

    const messageObj = {
      id: data.id || uuidv4(),
      groupId,
      senderCid: verifiedSenderCid,
      senderNickname: senderNickname || `User-${verifiedSenderCid.substring(0, 4)}`,
      senderAvatar: data.senderAvatar || null,
      message, // Encrypted payload
      timestamp: new Date().toISOString(),
      status: "delivered"
    };

    if (!group.messages) group.messages = [];
    group.messages.push(messageObj);

    // Sync to S3
    syncGroupsToS3();

    // Broadcast ONLY to accepted members in the room
    io.to(`group_${groupId}`).emit("message:received", messageObj);
    
  } else if (roomId) {
    const room = chatRooms.get(roomId);
    if (!room) return;

    const messageObj = {
      id: data.id || uuidv4(),
      roomId,
      senderCid: verifiedSenderCid,
      senderNickname: senderNickname || `User-${verifiedSenderCid.substring(0, 4)}`,
      senderAvatar: data.senderAvatar || null,
      message, 
      timestamp: new Date().toISOString(),
      status: "delivered",
      reactions: [],
    };

    room.messages.push(messageObj);
    db.get('chatRooms').find({ roomId }).assign({ messages: room.messages }).write();

    // Broadcast to room
    io.to(roomId).emit("message:received", messageObj);

    // Buffer for offline members in 1v1
    const otherCid = room.userA === verifiedSenderCid ? room.userB : room.userA;
    const targetUser = users.get(otherCid);

    console.log(`[Push-Debug] Processing message in ${roomId}`);
    console.log(`[Push-Debug] Sender: ${verifiedSenderCid}, Target: ${otherCid}`);

    // If recipient is not actively in the socket room, send a push notification
    const roomMembers = io.sockets.adapter.rooms.get(roomId);
    const roomMemberCount = roomMembers ? roomMembers.size : 0;
    const isRecipientInRoom = targetUser && targetUser.socketId && roomMembers && roomMembers.has(targetUser.socketId);

    console.log(`[Push-Debug] Room ${roomId} has ${roomMemberCount} members. Recipient in room? ${isRecipientInRoom}`);

    if (!isRecipientInRoom) {
      // Buffer if offline
      if (!targetUser || targetUser.status !== "online") {
        console.log(`[Push-Debug] Target ${otherCid} is offline. Buffering message.`);
        const undelivered = pendingMessages.get(roomId) || [];
        undelivered.push(messageObj);
        pendingMessages.set(roomId, undelivered);
      }

      // Send push notification
      if (targetUser && targetUser.pushToken) {
        console.log(`[Push-Debug] Triggering push to ${otherCid} (Token: ${targetUser.pushToken.substring(0, 10)}...)`);
        const bodyPreview = typeof message === 'string' ? message : (message.text || "Sent an attachment");
        sendPushNotification(
          targetUser.pushToken,
          senderNickname || "Locksy",
          bodyPreview,
          { roomId, senderCid: verifiedSenderCid, type: 'chat' }
        );
      } else {
        console.log(`[Push-Debug] Cannot send push to ${otherCid}: ${!targetUser ? 'User not found' : 'No pushToken'}`);
      }
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

// ─── Call Signaling (Push Notifications) ────────────
socket.on("call:signal", (data) => {
  const { toCid, callerName, callType, callId } = data;
  const targetUser = users.get(toCid);

  // 1. Deliver via Socket (Real-time foreground)
  if (targetUser && targetUser.socketId) {
    console.log(`[Call-Socket] Forwarding call signal to ${toCid}`);
    io.to(targetUser.socketId).emit("call:signal", { 
      type: 'call', 
      callId, 
      callerName, 
      callType,
      senderCid: socket.cid || data.fromCid,
      fromCid: socket.cid || data.fromCid
    });
  }

  // 2. Deliver via Push Notification (Background/Killed)
  if (targetUser && targetUser.pushToken) {
    console.log(`[Call-Push] Sending call invite push to ${toCid}`);
    sendPushNotification(
      targetUser.pushToken,
      `Incoming ${callType} call`,
      `${callerName} is calling you...`,
      { 
        type: 'call', 
        callId, 
        callerName, 
        callType,
        senderCid: socket.cid || data.fromCid 
      }
    );
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
  const group = groups.get(roomId);
  
  if (room) {
    socket.emit("room:history", { roomId, messages: room.messages });
  } else if (group) {
    socket.emit("room:history", { roomId, messages: group.messages || [] });
  } else {
    socket.emit("room:error", { message: "History not found for this room/group" });
  }
});

socket.on("room:leave", (data) => {
  const { roomId } = data;
  if (chatRooms.has(roomId)) {
    socket.leave(roomId);
    console.log(`[Socket] Left 1v1 room: ${roomId}`);
  } else if (groups.has(roomId)) {
    socket.leave(`group_${roomId}`);
    console.log(`[Socket] Left group room: group_${roomId}`);
  }
});

socket.on("room:join", (data) => {
  const { roomId } = data;
  if (chatRooms.has(roomId)) {
    socket.join(roomId);
    socket.emit("room:joined", { success: true, roomId });
    console.log(`[Socket] Joined 1v1 room: ${roomId}`);
  } else if (groups.has(roomId)) {
    socket.join(`group_${roomId}`);
    socket.emit("room:joined", { success: true, roomId });
    console.log(`[Socket] Joined group room: group_${roomId}`);
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
