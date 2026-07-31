import { v4 as uuidv4 } from "uuid";
import { db } from "../config/db.js";
import {
  users,
  chatRooms,
  groups,
  userSockets,
  pendingMessages,
  groupUnreadMessages,
  userMutes,
  getUserData,
  generateRoomId
} from "../store/state.js";
import { syncChatRoomsToS3, syncGroupsToS3, deleteS3Object } from "../services/s3Service.js";
import { sendPushNotification } from "../services/pushService.js";

export function handleChatSocket(io, socket) {
  // ─── Send Message (Strict Group Validation) ──────────
  socket.on("message:send", (data) => {
    const { roomId, groupId, message, senderCid, senderNickname } = data;

    // Use the CID stored on the socket for absolute verification
    const verifiedSenderCid = socket.cid || senderCid;

    if (groupId) {
      let group = groups.get(groupId);
      if (!group) {
        group = db.get('groups').find({ groupId }).value();
        if (group) groups.set(groupId, group);
      }
      if (!group) return console.warn(`[Send] Group ${groupId} not found`);

      const cleanSenderCid = String(verifiedSenderCid || '').trim().toUpperCase();

      // STRICT VALIDATION: Is the sender a member or admin?
      const isMember = group.members && group.members.some(m => {
        if (!m) return false;
        const mCid = typeof m === 'object' ? m.cid : m;
        return mCid && String(mCid).trim().toUpperCase() === cleanSenderCid;
      });

      if (!isMember) {
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
        status: "sent" // INITIAL STATUS
      };

      if (!group.messages) group.messages = [];
      group.messages.push(messageObj);

      // Sync to S3
      syncGroupsToS3();

      // 1. ACK to sender
      socket.emit("message:sent", { id: messageObj.id, tempId: data.id, groupId });

      // 2. Broadcast ONLY to members in the room
      const roomName = `group_${groupId}`;
      io.to(roomName).emit("message:received", messageObj);

      // 3. Buffer for members NOT in the room
      const activeSocketIds = io.sockets.adapter.rooms.get(roomName) || new Set();
      group.members.forEach(member => {
        const memberCid = typeof member === 'object' ? member.cid : member;
        if (!memberCid || String(memberCid).trim().toUpperCase() === cleanSenderCid) return;

        const memberUser = users.get(memberCid);
        const isActive = memberUser && memberUser.socketId && activeSocketIds.has(memberUser.socketId);

        if (!isActive) {
          // Emit directly to their socket if they are online but not in the room
          if (memberUser && memberUser.socketId) {
            io.to(memberUser.socketId).emit("message:received", messageObj);
          }
          // Buffer it
          const userUnread = groupUnreadMessages.get(memberCid) || {};
          const groupMsgs = userUnread[groupId] || [];
          groupMsgs.push(messageObj.id);
          userUnread[groupId] = groupMsgs;
          groupUnreadMessages.set(memberCid, userUnread);

          // Persist unread map to DB
          const allUnread = Array.from(groupUnreadMessages.entries()).map(([cid, unread]) => ({ cid, unread }));
          db.set('groupUnreadMessages', allUnread).write();

          // Send push notification if they have a token (check memory & LowDB fallback)
          const targetToken = (memberUser && memberUser.pushToken) || db.get('users').find({ cid: member.cid }).value()?.pushToken;
          if (targetToken) {
            const targetMuteKey = `${member.cid}_${groupId}`;
            const muteUntil = userMutes.get(targetMuteKey);
            if (muteUntil && muteUntil > Date.now()) {
              console.log(`[Push-Debug] Target ${member.cid} has muted group ${groupId}. Skipping push.`);
            } else {
              // Privacy-safe body: messages are encrypted, never send ciphertext
              const bodyPreview = 'New message';
              try {
                sendPushNotification(
                  targetToken,
                  `${group.name}: ${senderNickname || "Locksy"}`,
                  bodyPreview,
                  { groupId: String(groupId), senderCid: String(verifiedSenderCid), type: 'message' }
                );
              } catch (pushErr) {
                console.error(`[Push-Error] Failed to send group push to ${member.cid}:`, pushErr.message);
              }
            }
          } else {
            console.warn(`[Push-Debug] No pushToken found for group member ${memberCid} (memory or DB)`);
          }
        }
      });

    } else if (roomId) {
      let room = chatRooms.get(roomId);
      if (!room) {
        // Dynamic room creation/recovery for 1v1 room
        let userA = null;
        let userB = null;
        if (typeof roomId === 'string' && roomId.startsWith("room_")) {
          const parts = roomId.replace("room_", "").split("_");
          if (parts.length === 2) {
            userA = parts[0];
            userB = parts[1];
          }
        }
        if (!userA && data.receiverCid) {
          userA = verifiedSenderCid;
          userB = data.receiverCid;
        }
        if (userA && userB) {
          const canonicalId = generateRoomId(userA, userB);
          room = chatRooms.get(canonicalId);
          if (!room) {
            room = {
              roomId: canonicalId,
              userA,
              userB,
              createdAt: new Date().toISOString(),
              messages: [],
              status: "active",
            };
            chatRooms.set(canonicalId, room);
            syncChatRoomsToS3();
            console.log(`[Send-Recovery] Dynamically created 1v1 room ${canonicalId} for message sending`);
          }
          socket.join(canonicalId);
        } else {
          console.warn(`[Send] Unable to recover room for roomId: ${roomId}`);
          return;
        }
      }

      const messageObj = {
        id: data.id || uuidv4(),
        roomId: room.roomId,
        senderCid: verifiedSenderCid,
        senderNickname: senderNickname || `User-${verifiedSenderCid.substring(0, 4)}`,
        senderAvatar: data.senderAvatar || null,
        message,
        timestamp: new Date().toISOString(),
        status: "sent",
        reactions: [],
      };

      room.messages.push(messageObj);
      syncChatRoomsToS3();

      // 1. ACK to sender
      socket.emit("message:sent", { id: messageObj.id, tempId: data.id, roomId: room.roomId });

      // 2. Broadcast to room
      io.to(room.roomId).emit("message:received", messageObj);

      // Buffer for offline members in 1v1
      const otherCid = room.userA === verifiedSenderCid ? room.userB : room.userA;
      let targetUser = users.get(otherCid) || getUserData(otherCid);

      console.log(`[Push-Debug] Processing message in ${room.roomId}`);
      console.log(`[Push-Debug] Sender: ${verifiedSenderCid}, Target: ${otherCid}`);
      console.log(`[Push-Debug] Target user in memory: ${!!targetUser}, pushToken: ${targetUser?.pushToken ? targetUser.pushToken.substring(0, 15) + '...' : 'NONE'}`);

      // Resolve live socket ID for recipient if stale
      if (targetUser) {
        if (!targetUser.socketId || !io.sockets.sockets.has(targetUser.socketId)) {
          for (const [sockId, cid] of userSockets.entries()) {
            if (cid === otherCid && io.sockets.sockets.has(sockId)) {
              targetUser.socketId = sockId;
              break;
            }
          }
        }
      }

      // If recipient is not actively in the socket room, send direct socket payload or push notification
      const roomMembers = io.sockets.adapter.rooms.get(room.roomId);
      const roomMemberCount = roomMembers ? roomMembers.size : 0;
      const isRecipientInRoom = targetUser && targetUser.socketId && roomMembers && roomMembers.has(targetUser.socketId);

      console.log(`[Push-Debug] Room ${room.roomId} has ${roomMemberCount} members. Recipient in room? ${isRecipientInRoom}`);

      if (!isRecipientInRoom) {
        // Emit directly to their socket if they are online but not in the room
        if (targetUser && targetUser.socketId) {
          io.to(targetUser.socketId).emit("message:received", messageObj);
        }
        // Buffer the message whenever recipient is NOT actively in the room
        console.log(`[Push-Debug] Target ${otherCid} is not in room. Buffering message.`);
        const undelivered = pendingMessages.get(room.roomId) || [];
        undelivered.push(messageObj);
        pendingMessages.set(room.roomId, undelivered);

        // Send push notification (check memory & LowDB fallback)
        // Double-fallback: check targetUser.pushToken, then query DB directly
        let targetToken = targetUser?.pushToken || null;
        if (!targetToken) {
          const dbUserRecord = db.get('users').find({ cid: otherCid }).value();
          targetToken = dbUserRecord?.pushToken || null;
          if (targetToken) {
            console.log(`[Push-Debug] Found pushToken in DB fallback for ${otherCid}`);
            // Cache it back so next message doesn't need DB lookup
            if (targetUser) {
              targetUser.pushToken = targetToken;
            }
          }
        }

        if (targetToken) {
          const targetMuteKey = `${otherCid}_${room.roomId}`;
          const muteUntil = userMutes.get(targetMuteKey);
          if (muteUntil && muteUntil > Date.now()) {
            console.log(`[Push-Debug] Target ${otherCid} has muted room ${room.roomId}. Skipping push.`);
          } else {
            console.log(`[Push-Debug] Triggering push to ${otherCid} (Token: ${targetToken.substring(0, 15)}...)`);
            // Privacy-safe body: messages are encrypted, never send ciphertext
            const bodyPreview = 'New message';
            try {
              sendPushNotification(
                targetToken,
                senderNickname || "Locksy",
                bodyPreview,
                { roomId: String(room.roomId), senderCid: String(verifiedSenderCid), type: 'message' }
              );
            } catch (pushErr) {
              console.error(`[Push-Error] Failed to send 1v1 push to ${otherCid}:`, pushErr.message);
            }
          }
        } else {
          console.warn(`[Push-Debug] CANNOT send push to ${otherCid}: No pushToken found in memory or DB. User may not have granted notification permissions.`);
        }
      }
    }
  });

  // ─── Message Status Updates (NEW) ────────────────────
  socket.on("message:delivered", (data) => {
    const { roomId, groupId, messageId, receiverCid } = data;
    console.log(`[Status] Message ${messageId} delivered to ${receiverCid}`);

    if (groupId) {
      const group = groups.get(groupId);
      if (group && group.messages) {
        const msg = group.messages.find(m => m.id === messageId);
        if (msg && msg.status !== 'read') {
          msg.status = 'delivered';
          syncGroupsToS3();
          io.to(`group_${groupId}`).emit("message:status:update", { groupId, messageId, status: 'delivered' });
        }
      }
    } else if (roomId) {
      const room = chatRooms.get(roomId);
      if (room && room.messages) {
        const msg = room.messages.find(m => m.id === messageId);
        if (msg && msg.status !== 'read') {
          msg.status = 'delivered';
          syncChatRoomsToS3();
          io.to(roomId).emit("message:status:update", { roomId, messageId, status: 'delivered' });
        }
      }
    }
  });

  socket.on("message:read", (data) => {
    const { roomId, groupId, messageId, receiverCid } = data;
    console.log(`[Status] Message ${messageId} read by ${receiverCid}`);

    if (groupId) {
      const group = groups.get(groupId);
      if (group && group.messages) {
        const msg = group.messages.find(m => m.id === messageId);
        if (msg) {
          msg.status = 'read';
          syncGroupsToS3();
          io.to(`group_${groupId}`).emit("message:status:update", { groupId, messageId, status: 'read' });
        }
      }
    } else if (roomId) {
      const room = chatRooms.get(roomId);
      if (room && room.messages) {
        const msg = room.messages.find(m => m.id === messageId);
        if (msg) {
          msg.status = 'read';
          syncChatRoomsToS3();
          io.to(roomId).emit("message:status:update", { roomId, messageId, status: 'read' });
        }
      }
    }
  });

  // ─── Delete Message ──────────────────────────────────
  socket.on("message:delete", async (data) => {
    const { roomId, messageId } = data;
    const room = chatRooms.get(roomId);
    const group = groups.get(roomId);

    const msgs = room ? room.messages : (group ? group.messages : []);
    if (!msgs) return;

    // Check for media to cleanup before filtering
    const msgToDelete = msgs.find(m => m.id === messageId);
    if (msgToDelete) {
      // Extract mediaId from payload structure
      const msgPayload = msgToDelete.message;
      const mediaId = msgToDelete.media_id || (msgPayload && typeof msgPayload === 'object' ? msgPayload.media_id : null);

      if (mediaId) {
        await deleteS3Object(mediaId);
      }
    }

    // Remove from room's memory history
    if (room) {
      room.messages = room.messages.filter(m => m.id !== messageId);
      syncChatRoomsToS3();
    } else if (group) {
      group.messages = group.messages.filter(m => m.id !== messageId);
      syncGroupsToS3();
    }

    // Remove from pending offline queue if it was stuck there
    const pending = pendingMessages.get(roomId);
    if (pending) {
      pendingMessages.set(roomId, pending.filter(m => m.id !== messageId));
    }

    // Broadcast delete event to all active clients in the room
    io.to(roomId).emit("message:deleted", { roomId, messageId });
  });

  socket.on("message:delete_bulk", async (data) => {
    const { roomId, messageIds } = data;
    if (!messageIds || !Array.isArray(messageIds)) return;

    console.log(`[Delete-Bulk] Pruning ${messageIds.length} messages in ${roomId}`);

    const room = chatRooms.get(roomId);
    const group = groups.get(roomId);
    const msgs = room ? room.messages : (group ? group.messages : []);
    if (!msgs) return;

    for (const messageId of messageIds) {
      const msgToDelete = msgs.find(m => m.id === messageId);
      if (msgToDelete) {
        const msgPayload = msgToDelete.message;
        const mediaId = msgToDelete.media_id || (msgPayload && typeof msgPayload === 'object' ? msgPayload.media_id : null);
        if (mediaId) await deleteS3Object(mediaId);
      }
    }

    if (room) {
      room.messages = room.messages.filter(m => !messageIds.includes(m.id));
      syncChatRoomsToS3();
    } else if (group) {
      group.messages = group.messages.filter(m => !messageIds.includes(m.id));
      syncGroupsToS3();
    }

    const pending = pendingMessages.get(roomId);
    if (pending) {
      pendingMessages.set(roomId, pending.filter(m => !messageIds.includes(m.id)));
    }

    io.to(roomId).emit("message:deleted_bulk", { roomId, messageIds });
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
    const group = groups.get(roomId);

    if (room) {
      room.messages.forEach(m => {
        if (m.id === messageId) {
          m.isOpened = true;
          if (m.message && typeof m.message === 'object') {
            m.message.isOpened = true;
            m.message.uri = null;
            m.message.image = null;
            m.message.media_id = null;
            m.message.text = "[View Once Content Expired]";
          }
        }
      });
      syncChatRoomsToS3();
      io.to(roomId).emit("message:opened", { roomId, groupId: roomId, messageId });
      console.log(`[Message] View-once message ${messageId} opened and scrubbed in room ${roomId}`);
    } else if (group) {
      if (group.messages) {
        group.messages.forEach(m => {
          if (m.id === messageId) {
            m.isOpened = true;
            if (m.message && typeof m.message === 'object') {
              m.message.isOpened = true;
              m.message.uri = null;
              m.message.image = null;
              m.message.media_id = null;
              m.message.text = "[View Once Content Expired]";
            }
          }
        });
        syncGroupsToS3();
      }
      io.to(`group_${roomId}`).emit("message:opened", { roomId, groupId: roomId, messageId });
      io.to(roomId).emit("message:opened", { roomId, groupId: roomId, messageId });
      console.log(`[Group] View-once message ${messageId} opened and scrubbed in group ${roomId}`);
    }
  });

  // ─── Room Timer Event ─────────────────────────────────────────────
  socket.on("room:timer:update", (data) => {
    const { roomId, timerMs } = data;
    const room = chatRooms.get(roomId);
    const group = groups.get(roomId);

    if (room) {
      room.timer = timerMs;
      syncChatRoomsToS3();
      io.to(roomId).emit("room:timer:updated", { roomId, timerMs });
    } else if (group) {
      group.timer = timerMs;
      syncGroupsToS3();
      io.to(`group_${roomId}`).emit("room:timer:updated", { roomId, timerMs });
    }
  });

  // ─── History and Room Management Methods ─────────────
  socket.on("room:getHistory", (data) => {
    const { roomId } = data;
    let room = chatRooms.get(roomId) || db.get('chatRooms').find({ roomId }).value();
    let group = groups.get(roomId) || db.get('groups').find({ groupId: roomId }).value();

    if (group && !groups.has(roomId)) {
      groups.set(roomId, group);
    }
    if (room && !chatRooms.has(roomId)) {
      chatRooms.set(roomId, room);
    }

    if (!room && !group && roomId && typeof roomId === 'string' && roomId.startsWith("room_")) {
      const parts = roomId.split("_");
      if (parts.length === 3) {
        room = {
          roomId,
          userA: parts[1],
          userB: parts[2],
          createdAt: new Date().toISOString(),
          messages: [],
          status: "active",
        };
        chatRooms.set(roomId, room);
        syncChatRoomsToS3();
      }
    }

    if (room) {
      pendingMessages.delete(roomId);
      socket.emit("room:history", { roomId, messages: room.messages || [], timer: room.timer || null });
    } else if (group) {
      socket.emit("room:history", { roomId, messages: group.messages || [], timer: group.timer || null });
    } else {
      socket.emit("room:error", { message: "History not found for this room/group" });
    }
  });

  socket.on("room:leave", (data) => {
    const { roomId } = data;
    const isGroup = groups.has(roomId) || !!db.get('groups').find({ groupId: roomId }).value();
    if (isGroup) {
      socket.leave(`group_${roomId}`);
      console.log(`[Socket] Left group room: group_${roomId}`);
    } else {
      socket.leave(roomId);
      console.log(`[Socket] Left 1v1 room: ${roomId}`);
    }
  });

  socket.on("room:join", (data) => {
    const { roomId } = data;
    let room = chatRooms.get(roomId) || db.get('chatRooms').find({ roomId }).value();
    let group = groups.get(roomId) || db.get('groups').find({ groupId: roomId }).value();

    if (room) {
      if (!chatRooms.has(roomId)) chatRooms.set(roomId, room);
      socket.join(roomId);
      pendingMessages.delete(roomId);
      socket.emit("room:joined", { success: true, roomId });
      console.log(`[Socket] Joined 1v1 room: ${roomId}`);
    } else if (group) {
      if (!groups.has(roomId)) groups.set(roomId, group);
      socket.join(`group_${roomId}`);
      socket.emit("room:joined", { success: true, roomId });
      console.log(`[Socket] Joined group room: group_${roomId}`);
    } else if (roomId && typeof roomId === 'string' && roomId.startsWith("room_")) {
      const parts = roomId.split("_");
      if (parts.length === 3) {
        const userA = parts[1];
        const userB = parts[2];
        const newRoom = {
          roomId,
          userA,
          userB,
          createdAt: new Date().toISOString(),
          messages: [],
          status: "active",
        };
        chatRooms.set(roomId, newRoom);
        syncChatRoomsToS3();
        socket.join(roomId);
        pendingMessages.delete(roomId);
        socket.emit("room:joined", { success: true, roomId });
        console.log(`[Socket] Auto-created & joined 1v1 room: ${roomId}`);
      } else {
        socket.emit("room:error", { message: "Room or Group not found" });
      }
    } else {
      socket.emit("room:error", { message: "Room or Group not found" });
    }
  });

  socket.on("room:clearHistory", async (data) => {
    const { roomId } = data;
    const room = chatRooms.get(roomId);
    const group = groups.get(roomId);

    console.log(`[Soft-Clear] Clearing room messages for: ${roomId} (Preserving S3 objects)`);

    if (room) {
      room.messages = [];
      db.get('chatRooms').find({ roomId }).assign({ messages: [] }).write();
      console.log(`[Socket] 1v1 history cleared for room: ${roomId}`);
      socket.emit("room:historyCleared", { success: true, roomId });
    } else if (group) {
      group.messages = [];
      syncGroupsToS3();
      console.log(`[Socket] Group history cleared for room: ${roomId}`);
      socket.emit("room:historyCleared", { success: true, roomId });
    } else {
      socket.emit("room:error", { message: "Room or Group not found" });
    }
  });
}
