import jwt from "jsonwebtoken";
import { db } from "../config/db.js";
import { users } from "../store/state.js";
import { supabase } from "../config/supabase.js";
import { JWT_SECRET } from "../middleware/authMiddleware.js";

export const getRoot = (req, res) => {
  res.json({
    status: "Locksy secure chat server is running",
    version: "1.1.0 (Advanced Persistence)",
  });
};

export const getAppConfig = (req, res) => {
  res.json({
    is_app_active: true,
    force_update: false,
    message: "local-backend",
  });
};

export const verifyUser = async (req, res) => {
  const { employee_id, device_id } = req.body || {};

  if (!employee_id || !device_id) {
    return res.status(400).json({ error: "employee_id and device_id are required" });
  }

  // 1. Fast path: check local in-memory map and lowdb
  let existingUser = users.get(employee_id) || db.get("users").find({ cid: employee_id }).value();

  // 2. If not in local store, check Supabase (source of truth for registered employees)
  if (!existingUser && supabase) {
    try {
      const { data: employee, error } = await supabase
        .from("employees")
        .select("*")
        .eq("id", employee_id)
        .single();

      if (!error && employee) {
        // 'Pending enrollment' is the dashboard's placeholder for unbound devices
        const deviceValue = employee.device && employee.device !== "Pending enrollment"
          ? employee.device
          : null;

        existingUser = {
          cid: employee.id,
          disabled: !employee.authorized,
          boundDeviceId: deviceValue,
          action: employee.action || null,
        };
      }
    } catch (err) {
      console.error("[AuthController] Supabase lookup failed:", err.message);
    }
  }

  // 3. If employee not found in ANY store, reject
  if (!existingUser) {
    return res.status(403).json({
      allowed: false,
      error: "Employee not found. Access denied.",
      action: "nuke"
    });
  }

  // 4. Admin has explicitly disabled this user — hard block
  if (existingUser.disabled === true) {
    return res.status(403).json({ allowed: false, error: "Account disabled", action: "nuke" });
  }

  // 5. Device binding check: if a bound device exists and it doesn't match, reject
  if (existingUser.boundDeviceId && existingUser.boundDeviceId !== device_id) {
    return res.status(403).json({ allowed: false, error: "Device mismatch", action: "nuke" });
  }

  // 6. First-time device binding — update Supabase if device not yet bound
  if (!existingUser.boundDeviceId && supabase) {
    try {
      await supabase
        .from("employees")
        .update({ device: device_id })
        .eq("id", employee_id);
    } catch (err) {
      console.error("[AuthController] Device binding update failed:", err.message);
    }
  }

  // 7. Employee exists, is active, and device is valid — issue signed JWT
  const token = jwt.sign(
    { cid: employee_id, deviceId: device_id },
    JWT_SECRET,
    { expiresIn: "30d" }
  );

  return res.json({ allowed: true, token });
};

export const validateSession = (req, res) => {
  const authHeader = req.get("Authorization") || "";
  const token = authHeader.startsWith("Bearer ")
    ? authHeader.slice(7)
    : req.body?.token || "";

  if (!token) {
    return res.status(401).json({ valid: false, error: "Missing auth token" });
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    const cid = decoded.cid || decoded.employee_id;

    // Check if user has been disabled in the system
    const existingUser = users.get(cid) || db.get("users").find({ cid }).value();
    if (existingUser && existingUser.disabled === true) {
      return res.status(403).json({ valid: false, error: "Account disabled", action: "nuke" });
    }

    return res.json({ valid: true, cid, message: "session valid" });
  } catch (err) {
    console.error("[AuthController] Session validation token error:", err.message);
    return res.status(401).json({ valid: false, error: "Invalid or expired token" });
  }
};

export const logout = (req, res) => {
  res.json({ ok: true });
};

export const getUserByCid = (req, res) => {
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
};

export const getDebugUsers = (req, res) => {
  if (process.env.NODE_ENV === "production") {
    return res.status(403).json({ error: "Forbidden in production" });
  }

  const allUsers = Array.from(users.values()).map(u => ({
    cid: u.cid,
    nickname: u.nickname,
    hasPublicKey: !!u.publicKey,
    status: u.status
  }));
  res.json(allUsers);
};

