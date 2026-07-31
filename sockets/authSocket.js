import jwt from "jsonwebtoken";
import { db, flushDb } from "../config/db.js";
import { JWT_SECRET } from "../middleware/authMiddleware.js";
import {
  users,
  userSockets,
  pendingRequests,
  groupInvites,
  groups,
  chatRooms,
  pendingMessages,
  groupUnreadMessages,
  getUserData,
  activeCalls
} from "../store/state.js";

export function handleAuthSocket(io, socket) {
  // ─── User Registration ───────────────────────────────
  socket.on("register", (data) => {
    const { cid, nickname, avatar, publicKey, pushToken } = data;

    if (!cid) return socket.emit("register:error", { message: "CID is required" });

    // Force Single Session: Disconnect any existing socket for this CID
    const oldUser = users.get(cid);
    if (oldUser && oldUser.socketId && oldUser.socketId !== socket.id) {
      console.log(`[Register] User ${cid} already has an active socket ${oldUser.socketId}. Disconnecting old session.`);
      const oldSocket = io.sockets.sockets.get(oldUser.socketId);
      if (oldSocket) {
        oldSocket.emit("error", { message: "New login detected. You have been disconnected." });
        oldSocket.disconnect(true);
      }
    }

    // Store CID on socket for validation in later events
    socket.cid = cid;

    // Update existing user or create new
    const existingUser = users.get(cid) || {};
    const dbUser = db.get('users').find({ cid }).value();
    const userInDb = dbUser || {};

    // Do not overwrite existing stored nickname with fallback string if incoming nickname is empty/default
    const validIncomingNickname = (nickname && typeof nickname === 'string' && nickname.trim() && !nickname.startsWith('User-')) ? nickname.trim() : null;
    const finalNickname = validIncomingNickname || existingUser.nickname || userInDb.nickname || (nickname && nickname.trim()) || `User-${cid.substring(0, 4)}`;

    const userData = {
      cid,
      socketId: socket.id,
      nickname: finalNickname,
      avatar: avatar || existingUser.avatar || userInDb.avatar || null,
      publicKey: publicKey || existingUser.publicKey || userInDb.publicKey || null,
      pushToken: pushToken || existingUser.pushToken || userInDb.pushToken || null,
      status: "online",
      lastSeen: new Date().toISOString()
    };

    users.set(cid, userData);
    userSockets.set(socket.id, cid);

    // Save to DB (Exclude socket-specific fields)
    const { socketId, status, ...dbData } = userData;
    if (dbUser) {
      db.get('users').find({ cid }).assign(dbData).write();
    } else {
      db.get('users').push(dbData).write();
    }

    // Force-flush to disk when pushToken is present — this is critical data
    // that must survive a server crash/restart for notification delivery.
    if (userData.pushToken) {
      flushDb();
    }

    console.log(`[Register] User ${cid} is now online`);

    const token = jwt.sign({ cid }, JWT_SECRET, { expiresIn: "30d" });
    socket.emit("register:success", { message: "Welcome back!", cid, token });
    io.emit("user:status", {
      cid,
      status: "online",
      publicKey: userData.publicKey,
      nickname: userData.nickname,
      avatar: userData.avatar
    });

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
      const allInvites = Array.from(groupInvites.entries()).map(([c, invs]) => ({ cid: c, invites: invs }));
      db.set('groupInvites', allInvites).write();
    }

    // Collect active groups for the user (without auto-joining socket room)
    const userGroups = [];
    const cleanCid = String(cid || '').trim().toUpperCase();
    groups.forEach((group, groupId) => {
      if (group.members && group.members.some(m => m && m.cid && String(m.cid).trim().toUpperCase() === cleanCid)) {
        userGroups.push({
          groupId: group.groupId,
          name: group.name,
          description: group.description || null,
          groupLogo: group.groupLogo || null,
          adminId: group.adminId,
          members: group.members,
          createdAt: group.createdAt
        });
      }
    });

    // Send the current list of groups to the client for reconciliation
    socket.emit("groep:list", userGroups);

    // Collect and send the list of 1-on-1 contacts to the client
    const userContacts = [];
    chatRooms.forEach((room) => {
      if (room.userA === cid || room.userB === cid) {
        const otherCid = room.userA === cid ? room.userB : room.userA;
        const otherUser = getUserData(otherCid);
        if (otherUser) {
          userContacts.push({
            cid: otherUser.cid,
            nickname: otherUser.nickname,
            avatar: otherUser.avatar,
            publicKey: otherUser.publicKey,
            status: otherUser.status || "offline",
            verified: true,
            roomId: room.roomId
          });
        }
      }
    });
    socket.emit("contact:list", userContacts);

    // Deliver pending messages for all rooms this user is in
    chatRooms.forEach((room, roomId) => {
      if (room.userA === cid || room.userB === cid) {
        const undelivered = pendingMessages.get(roomId);
        if (undelivered && undelivered.length > 0) {
          undelivered.forEach(msg => socket.emit("message:received", msg));
          pendingMessages.delete(roomId);
        }
      }
    });

    // Deliver buffered group messages
    const unreadMap = groupUnreadMessages.get(cid);
    if (unreadMap) {
      Object.keys(unreadMap).forEach(groupId => {
        const group = groups.get(groupId);
        if (group && group.messages) {
          const messageIds = unreadMap[groupId];
          messageIds.forEach(msgId => {
            const msg = group.messages.find(m => m.id === msgId);
            if (msg) {
              socket.emit("message:received", msg);
            }
          });
        }
      });
      groupUnreadMessages.delete(cid);
      // Persist cleanup
      const allUnread = Array.from(groupUnreadMessages.entries()).map(([cid, unread]) => ({ cid, unread }));
      db.set('groupUnreadMessages', allUnread).write();
    }
  });

  // ─── Push Token Update (Fast Path) ────────────────────
  // Allows the client to update its push token without a full re-registration.
  // This is critical because the push token may arrive AFTER the initial
  // registration (race condition between socket connect and FCM token fetch).
  socket.on("pushToken:update", (data) => {
    const { pushToken } = data;
    const cid = socket.cid || userSockets.get(socket.id);

    if (!cid || !pushToken) {
      console.warn(`[PushToken] Invalid update: cid=${cid}, token=${pushToken ? 'present' : 'missing'}`);
      return;
    }

    // Update in-memory user
    const user = users.get(cid);
    if (user) {
      const oldToken = user.pushToken;
      user.pushToken = pushToken;
      console.log(`[PushToken] Updated in-memory token for ${cid} (changed: ${oldToken !== pushToken})`);
    } else {
      console.warn(`[PushToken] User ${cid} not found in memory — creating minimal entry`);
      users.set(cid, { cid, pushToken, socketId: socket.id, status: 'online' });
    }

    // Persist to DB and force-flush (bypass debounce) — pushToken must survive crash
    const dbUser = db.get('users').find({ cid }).value();
    if (dbUser) {
      db.get('users').find({ cid }).assign({ pushToken }).write();
    } else {
      db.get('users').push({ cid, pushToken }).write();
    }
    flushDb();

    socket.emit("pushToken:updated", { success: true });
    console.log(`[PushToken] Token persisted and flushed for ${cid}: ${pushToken.substring(0, 15)}...`);
  });

  // ─── Disconnect Handler ──────────────────────────────
  socket.on("disconnect", () => {
    const cid = userSockets.get(socket.id);
    if (cid) {
      const user = users.get(cid);
      if (user) {
        // ONLY mark offline if the disconnecting socket is the user's active socket!
        if (user.socketId === socket.id) {
          user.status = "offline";
          user.lastSeen = new Date().toISOString();
          console.log(`[Disconnect] User ${cid} is now offline`);
          io.emit("user:status", { cid, status: "offline" });
        } else {
          console.log(`[Disconnect] Ignoring stale disconnect for user ${cid} (active: ${user.socketId}, disconnecting: ${socket.id})`);
        }
      }
      userSockets.delete(socket.id);

      // Clean up any active calls this user was in
      for (const [callId, call] of activeCalls) {
        if (call.callerId === cid || call.receiverId === cid) {
          console.log(`[Call-Cleanup] Cleaning up active call ${callId} due to disconnect of ${cid}`);
          activeCalls.delete(callId);
          
          // Notify the other party if still connected
          const otherId = call.callerId === cid ? call.receiverId : call.callerId;
          const otherUser = users.get(otherId);
          if (otherUser && otherUser.socketId) {
            io.to(otherUser.socketId).emit("call:ended", { callId, reason: "Partner disconnected" });
          }
        }
      }
    }
  });
}
