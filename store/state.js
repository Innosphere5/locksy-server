import { db } from "../config/db.js";

// In-memory runtime state (Mirrors Persistent Store for performance)
export const users = new Map();
export const chatRooms = new Map();
export const userSockets = new Map();
export const pendingMessages = new Map();
export const pendingRequests = new Map();
export const groups = new Map();
export const groupInvites = new Map();
export const mediaRegistry = new Map();
export const groupUnreadMessages = new Map(); // cid -> { groupId: [messageIds] }
export const userMutes = new Map(); // `${cid}_${roomId}` -> until_timestamp
export const activeCalls = new Map(); // callId -> { callerId, receiverId, status }

// Helper to retrieve user data from memory or Lowdb persistent storage
// IMPORTANT: Caches DB results back into the users Map so pushToken and other
// fields are immediately available for push notification delivery without
// repeated DB lookups.
export function getUserData(cid) {
  if (!cid) return null;
  const cleanCid = String(cid).trim().toUpperCase();
  const memoryUser = users.get(cleanCid) || users.get(cid) || Array.from(users.values()).find(u => u && u.cid && String(u.cid).trim().toUpperCase() === cleanCid);
  if (memoryUser) return memoryUser;

  const dbUsers = db.get("users").value() || [];
  const dbUser = dbUsers.find(u => u && u.cid && String(u.cid).trim().toUpperCase() === cleanCid);
  if (dbUser) {
    const userData = {
      ...dbUser,
      status: "offline",
      socketId: null
    };
    // Cache back into the in-memory Map so subsequent lookups (especially
    // pushToken lookups during message:send) hit memory instead of DB.
    users.set(dbUser.cid, userData);
    return userData;
  }
  return null;
}

export function generateRoomId(cidA, cidB) {
  const sorted = [cidA, cidB].sort();
  return `room_${sorted[0]}_${sorted[1]}`;
}

// Sync Maps with DB on startup
export const loadFromDb = () => {
  const dbUsers = db.get('users').value() || [];
  dbUsers.forEach(u => users.set(u.cid, { ...u, socketId: null, status: 'offline' }));

  const dbGroups = db.get('groups').value() || [];
  dbGroups.forEach(g => groups.set(g.groupId, g));

  const dbRooms = db.get('chatRooms').value() || [];
  dbRooms.forEach(r => chatRooms.set(r.roomId, r));

  const dbMedia = db.get('media').value() || [];
  dbMedia.forEach(m => {
    if (m && m.id) mediaRegistry.set(m.id, m);
    if (m && m.s3_key) mediaRegistry.set(m.s3_key, m);
    if (m && m.key) mediaRegistry.set(m.key, m);
  });

  const dbInvites = db.get('groupInvites').value() || [];
  dbInvites.forEach(inv => {
    const existing = groupInvites.get(inv.cid) || [];
    existing.push(...inv.invites);
    groupInvites.set(inv.cid, existing);
  });

  const dbGroupUnread = db.get('groupUnreadMessages').value() || [];
  dbGroupUnread.forEach(entry => {
    groupUnreadMessages.set(entry.cid, entry.unread);
  });

  const dbMutes = db.get('userMutes').value() || {};
  Object.keys(dbMutes).forEach(k => userMutes.set(k, dbMutes[k]));

  console.log(`[DB] Loaded ${users.size} users, ${groups.size} groups, ${chatRooms.size} rooms, ${mediaRegistry.size} media items, ${groupInvites.size} users with invites, ${groupUnreadMessages.size} users with unread group msgs, ${userMutes.size} active mutes`);
};
