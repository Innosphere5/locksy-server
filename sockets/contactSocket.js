import { pendingRequests, chatRooms, userMutes, getUserData, generateRoomId } from "../store/state.js";
import { syncChatRoomsToS3, syncMutesToDb } from "../services/s3Service.js";

export function handleContactSocket(io, socket) {
  // ─── Connection Request ───────────────────────────────
  socket.on("contact:request:send", (data) => {
    const { fromCid, toCid } = data;
    const fromUser = getUserData(fromCid);
    const toUser = getUserData(toCid);

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
    const requester = getUserData(fromCid);
    const accepter = getUserData(toCid);

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
      // Save to S3
      syncChatRoomsToS3();
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
    const requester = getUserData(fromCid);
    const accepter = getUserData(toCid);

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
      // Save to S3
      syncChatRoomsToS3();
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

  // ─── Contact Mute Event ───────────────────────────────────────────
  socket.on("contact:mute", (data) => {
    const { cid: targetCid, roomId, until } = data;
    const verifiedCid = socket.cid || targetCid;
    const key = `${verifiedCid}_${roomId}`;
    if (until === null || until === 0) {
      userMutes.delete(key);
      console.log(`[Mute] User ${verifiedCid} unmuted ${roomId}`);
    } else {
      userMutes.set(key, until);
      console.log(`[Mute] User ${verifiedCid} muted ${roomId} until ${new Date(until).toISOString()}`);
    }
    syncMutesToDb();
  });
}
