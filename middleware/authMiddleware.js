import jwt from "jsonwebtoken";
import { users } from "../store/state.js";
import { db } from "../config/db.js";

const JWT_SECRET = process.env.JWT_SECRET || "locksy_production_jwt_secret_key_2026";

export const authenticateToken = (req, res, next) => {
  const authHeader = req.get("Authorization") || "";
  const token = authHeader.startsWith("Bearer ")
    ? authHeader.slice(7)
    : req.body?.token || req.query?.token;

  if (!token) {
    return res.status(401).json({ error: "Access token required" });
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    const cid = decoded.cid || decoded.employee_id;

    // Check if user has been disabled by admin
    const existingUser = users.get(cid) || db.get("users").find({ cid }).value();
    if (existingUser && existingUser.disabled === true) {
      return res.status(403).json({ error: "Account disabled", action: "nuke" });
    }

    req.user = decoded;
    next();
  } catch (err) {
    console.error("[AuthMiddleware] Token verification failed:", err.message);
    return res.status(401).json({ error: "Invalid or expired token" });
  }
};

export { JWT_SECRET };
