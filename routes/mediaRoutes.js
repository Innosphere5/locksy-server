import { Router } from "express";
import {
  getUploadUrl,
  saveMedia,
  getMedia,
  getE2eeUploadUrl,
  getE2eeDownloadUrl,
  saveE2eeMetadata,
  initiateMultipart,
  getMultipartPresignedUrls,
  completeMultipart
} from "../controllers/mediaController.js";
import { authenticateToken } from "../middleware/authMiddleware.js";

const router = Router();

router.post("/api/media/upload-url", authenticateToken, getUploadUrl);
router.post("/api/media/save", authenticateToken, saveMedia);
router.get("/api/media", authenticateToken, getMedia);
router.post("/api/media/e2ee/upload-url", authenticateToken, getE2eeUploadUrl);
router.get("/api/media/e2ee/download-url", authenticateToken, getE2eeDownloadUrl);
router.post("/api/media/e2ee/save-metadata", authenticateToken, saveE2eeMetadata);
router.post("/api/media/multipart/initiate", authenticateToken, initiateMultipart);
router.post("/api/media/multipart/get-presigned-urls", authenticateToken, getMultipartPresignedUrls);
router.post("/api/media/multipart/complete", authenticateToken, completeMultipart);

export default router;

