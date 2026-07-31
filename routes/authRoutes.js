import { Router } from "express";
import {
  getRoot,
  getAppConfig,
  verifyUser,
  validateSession,
  logout,
  getUserByCid,
  getDebugUsers
} from "../controllers/authController.js";
import { authenticateToken } from "../middleware/authMiddleware.js";

const router = Router();

router.get("/", getRoot);
router.get("/api/app/config", getAppConfig);
router.post("/api/auth/verify-user", verifyUser);
router.post("/api/auth/validate-session", validateSession);
router.post("/api/auth/logout", logout);
router.get("/api/users/:cid", authenticateToken, getUserByCid);
router.get("/api/debug/users", authenticateToken, getDebugUsers);

export default router;

