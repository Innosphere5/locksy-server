import jwt from "jsonwebtoken";
import { handleAuthSocket } from "./authSocket.js";
import { handleSearchSocket } from "./searchSocket.js";
import { handleContactSocket } from "./contactSocket.js";
import { handleGroupSocket } from "./groupSocket.js";
import { handleChatSocket } from "./chatSocket.js";
import { handleCallSocket } from "./callSocket.js";
import { JWT_SECRET } from "../middleware/authMiddleware.js";

export function registerSocketHandlers(io) {
  // Socket Authentication Middleware
  io.use((socket, next) => {
    const token = socket.handshake.auth?.token || socket.handshake.headers?.authorization?.replace(/^Bearer\s+/, "");

    if (!token) {
      // Allow unauthenticated connection temporarily if no token provided during registration phase
      console.log(`[Socket Auth] Connection ${socket.id} proceeding without handshake token (registration pending)`);
      return next();
    }

    try {
      const decoded = jwt.verify(token, JWT_SECRET);
      socket.user = decoded;
      socket.cid = decoded.cid || decoded.employee_id;
      console.log(`[Socket Auth] Verified socket ${socket.id} for CID: ${socket.cid}`);
      next();
    } catch (err) {
      console.warn(`[Socket Auth] Invalid token on socket ${socket.id}:`, err.message);
      // Return error to client
      next(new Error("Authentication error: Invalid or expired token"));
    }
  });

  io.on("connection", (socket) => {
    console.log(`[Socket] New connection established: ${socket.id}`);

    handleAuthSocket(io, socket);
    handleSearchSocket(io, socket);
    handleContactSocket(io, socket);
    handleGroupSocket(io, socket);
    handleChatSocket(io, socket);
    handleCallSocket(io, socket);
  });
}

