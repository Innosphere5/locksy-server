import { users, userSockets, activeCalls } from "../store/state.js";
import { sendPushNotification } from "../services/pushService.js";

export function handleCallSocket(io, socket) {
  // Helper: Resolve live socket for a CID (handles stale socket IDs after reconnect)
  const resolveUserSocket = (cid) => {
    const user = users.get(cid);
    if (!user) return null;
    // If stored socketId is still alive, use it
    if (user.socketId && io.sockets.sockets.has(user.socketId)) {
      return user.socketId;
    }
    // Scan userSockets for a live mapping
    for (const [sockId, mappedCid] of userSockets.entries()) {
      if (mappedCid === cid && io.sockets.sockets.has(sockId)) {
        user.socketId = sockId; // update stale reference
        return sockId;
      }
    }
    return null;
  };

  socket.on("call:offer", (data) => {
    const { callId, receiverId, offerSDP, callType, callerName } = data;
    const callerId = socket.cid || data.callerId;
    const targetUser = users.get(receiverId);

    console.log(`[Call] Offer from ${callerId} to ${receiverId} (Type: ${callType})`);

    if (callerId === receiverId) {
      console.warn(`[Call] Self-call attempt by ${callerId}`);
      return socket.emit("call:busy", { callId, message: "Self-calling not supported" });
    }

    if (!targetUser) {
      return socket.emit("call:error", { message: "Target user not found" });
    }

    // 1. Check if receiver is busy
    let isBusy = false;
    activeCalls.forEach((call) => {
      if (call.receiverId === receiverId || call.callerId === receiverId) {
        if (call.status !== 'ended') isBusy = true;
      }
    });

    if (isBusy) {
      console.log(`[Call] Receiver ${receiverId} is busy`);
      return socket.emit("call:busy", { receiverId, callId });
    }

    // 2. Track the call session
    activeCalls.set(callId, {
      callerId,
      receiverId,
      status: 'calling',
      startTime: Date.now(),
      offerSDP,
      callerName,
      callType
    });

    // 3. Forward offer via Socket (Real-time foreground)
    const targetSocketId = resolveUserSocket(receiverId);
    if (targetSocketId && targetSocketId !== socket.id) {
      io.to(targetSocketId).emit("call:offer", {
        callId,
        callerId,
        callerName,
        offerSDP,
        callType
      });
    }

    // 4. Send Push Notification (Background/Wake-up)
    if (targetUser.pushToken) {
      sendPushNotification(
        targetUser.pushToken,
        `Incoming ${callType} call`,
        `${callerName || 'Someone'} is calling you...`,
        {
          type: 'call_offer',
          callId: String(callId),
          callerId: String(callerId || ''),
          callerName: String(callerName || 'Locksy User'),
          callType: String(callType)
        }
      );
    }
  });

  socket.on("call:request_offer", (data) => {
    const { callId } = data;
    console.log(`[Call] Request offer received for callId: ${callId}`);
    const call = activeCalls.get(callId);
    if (call && call.offerSDP) {
      socket.emit("call:offer", {
        callId,
        callerId: call.callerId,
        callerName: call.callerName,
        offerSDP: call.offerSDP,
        callType: call.callType
      });
    }
  });

  socket.on("call:ringing", (data) => {
    const { callId, callerId } = data;
    const targetSocketId = resolveUserSocket(callerId);
    if (targetSocketId) {
      io.to(targetSocketId).emit("call:ringing", { callId });
    }
  });

  socket.on("call:answer", (data) => {
    const { callId, callerId, answerSDP } = data;
    const receiverId = socket.cid;

    console.log(`[Call] Answer from ${receiverId} to ${callerId}`);

    const call = activeCalls.get(callId);
    if (call) {
      call.status = 'connected';
    }

    const targetSocketId = resolveUserSocket(callerId);
    if (targetSocketId) {
      io.to(targetSocketId).emit("call:answer", {
        callId,
        receiverId,
        answerSDP
      });
    }
  });

  socket.on("call:ice-candidate", (data) => {
    const { callId, receiverId, candidate } = data;
    const targetSocketId = resolveUserSocket(receiverId);

    if (targetSocketId) {
      io.to(targetSocketId).emit("call:ice-candidate", {
        callId,
        senderId: socket.cid,
        candidate
      });
    }
  });

  socket.on("call:reject", (data) => {
    const { callId, callerId, reason } = data;

    console.log(`[Call] Rejected by ${socket.cid} (Reason: ${reason})`);

    activeCalls.delete(callId);

    const targetSocketId = resolveUserSocket(callerId);
    if (targetSocketId) {
      io.to(targetSocketId).emit("call:rejected", {
        callId,
        receiverId: socket.cid,
        reason
      });
    }
  });

  socket.on("call:end", (data) => {
    const { callId, otherId } = data;

    console.log(`[Call] Ended by ${socket.cid} for ${callId}`);

    activeCalls.delete(callId);

    const targetSocketId = resolveUserSocket(otherId);
    if (targetSocketId) {
      io.to(targetSocketId).emit("call:ended", {
        callId,
        senderId: socket.cid
      });
    }
  });

  socket.on("call:busy", (data) => {
    const { callId, callerId } = data;
    const targetSocketId = resolveUserSocket(callerId);

    if (targetSocketId) {
      io.to(targetSocketId).emit("call:busy", {
        callId,
        receiverId: socket.cid
      });
    }
  });
}
