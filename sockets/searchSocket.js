import { db } from "../config/db.js";
import { users, getUserData } from "../store/state.js";

export function handleSearchSocket(io, socket) {
  // ─── Search Contact (Discovery Only) ──────────────────
  socket.on("search:cid", (data) => {
    const { otherCid } = data || {};
    if (!otherCid) {
      return socket.emit("search:error", { message: "Target CID is required" });
    }
    const cleanCid = String(otherCid).trim().toUpperCase();
    const otherUser = getUserData(cleanCid);

    if (!otherUser) {
      return socket.emit("search:error", { message: "User not found" });
    }

    socket.emit("search:success", {
      otherUser: {
        cid: otherUser.cid,
        nickname: otherUser.nickname,
        avatar: otherUser.avatar,
        status: otherUser.status || "offline",
        publicKey: otherUser.publicKey,
      }
    });
  });

  socket.on("search:nickname", (data) => {
    const { nickname } = data;
    let foundUser = Array.from(users.values()).find(u => u.nickname && u.nickname.toLowerCase() === nickname.toLowerCase());
    if (!foundUser) {
      const dbUser = db.get("users").find(u => u.nickname && u.nickname.toLowerCase() === nickname.toLowerCase()).value();
      if (dbUser) {
        foundUser = { ...dbUser, status: "offline" };
      }
    }

    if (!foundUser) {
      return socket.emit("search:error", { message: "User not found" });
    }

    socket.emit("search:success", {
      otherUser: {
        cid: foundUser.cid,
        nickname: foundUser.nickname,
        avatar: foundUser.avatar,
        status: foundUser.status || "offline",
        publicKey: foundUser.publicKey,
      }
    });
  });
}
