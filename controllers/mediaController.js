import {
  PutObjectCommand,
  GetObjectCommand,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { v4 as uuidv4 } from "uuid";
import { s3Client, BUCKET_NAME } from "../config/s3.js";
import { db, flushDb } from "../config/db.js";
import { mediaRegistry } from "../store/state.js";
import { syncMediaToS3 } from "../services/s3Service.js";

/**
 * Generate Pre-signed URL for direct upload
 */
export const getUploadUrl = async (req, res) => {
  try {
    const { fileName, fileType, fileSize, userId } = req.body;

    if (!fileName) return res.status(400).json({ error: "Missing required field: fileName" });
    if (!fileType) return res.status(400).json({ error: "Missing required field: fileType" });
    if (!userId) return res.status(400).json({ error: "Missing required field: userId" });

    // Validation: Reject > 200MB
    const MAX_SIZE = 200 * 1024 * 1024; // 200MB
    if (fileSize > MAX_SIZE) {
      return res.status(400).json({ error: "File too large (max 200MB)" });
    }

    // Allow only image/video/pdf/doc
    const allowedTypes = ['image/', 'video/', 'application/pdf', 'application/msword', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'];
    const isAllowed = allowedTypes.some(type => fileType.startsWith(type));
    if (!isAllowed) {
      return res.status(400).json({ error: `File type not allowed: ${fileType}` });
    }

    const timestamp = Date.now();
    const cleanFileName = fileName.replace(/[^a-zA-Z0-9.-]/g, '_');
    const key = `uploads/${userId}/${timestamp}-${cleanFileName}`;

    console.log(`[Media] Generating Signed URL:
      - Key: ${key}
      - ContentType: ${fileType}
      - User: ${userId}
      - Expiry: 300s`);

    const command = new PutObjectCommand({
      Bucket: BUCKET_NAME,
      Key: key,
      ContentType: fileType,
    });

    const uploadUrl = await getSignedUrl(s3Client, command, { expiresIn: 300 });
    const region = process.env.AWS_REGION || 'us-east-1';
    const fileUrl = `https://${BUCKET_NAME}.s3.${region}.amazonaws.com/${key}`;

    res.json({
      uploadUrl,
      fileUrl,
      key
    });
  } catch (error) {
    console.error("[Media] Error generating upload URL:", error);
    res.status(500).json({ error: "Failed to generate upload URL" });
  }
};

/**
 * Save file metadata to DB
 */
export const saveMedia = (req, res) => {
  try {
    const { key, fileUrl, type, size, sender_id, chat_id } = req.body;

    if (!key || !fileUrl || !type || !sender_id) {
      return res.status(400).json({ error: "Missing required metadata fields" });
    }

    const mediaId = uuidv4();
    const mediaObj = {
      id: mediaId,
      key,
      url: fileUrl,
      type, // image, video, doc
      size,
      user_id: sender_id,
      chat_id: chat_id || null,
      created_at: new Date().toISOString(),
    };

    mediaRegistry.set(mediaId, mediaObj);
    if (key) mediaRegistry.set(key, mediaObj);

    // Save to DB
    if (!db.has('media').value()) {
      db.set('media', []).write();
    }
    db.get('media').push(mediaObj).write();
    flushDb();
    syncMediaToS3();

    res.json({ success: true, media: mediaObj });
  } catch (error) {
    console.error("[Media] Error saving metadata:", error);
    res.status(500).json({ error: "Failed to save media metadata" });
  }
};

/**
 * Fetch media for a specific chat
 */
export const getMedia = (req, res) => {
  const { chat_id } = req.query;
  if (!chat_id) return res.status(400).json({ error: "chat_id is required" });

  const media = Array.from(mediaRegistry.values()).filter(m => m.chat_id === chat_id);
  res.json(media);
};

/**
 * Generate Pre-signed URL for E2EE Upload (Direct to S3)
 * Backend NEVER receives file data or keys.
 */
export const getE2eeUploadUrl = async (req, res) => {
  try {
    const { fileName, userId } = req.body;

    if (!fileName || !userId) {
      return res.status(400).json({ error: "Missing fileName or userId" });
    }

    const timestamp = Date.now();
    const cleanFileName = fileName.replace(/[^a-zA-Z0-9.-]/g, '_');
    // Store as binary blob (encrypted cipher)
    const key = `e2ee/${userId}/${timestamp}-${cleanFileName}.bin`;

    console.log(`[E2EE-Media] Generating Upload URL for ${key}`);

    const command = new PutObjectCommand({
      Bucket: BUCKET_NAME,
      Key: key,
      ContentType: 'application/octet-stream', // Force binary for encrypted data
    });

    const uploadUrl = await getSignedUrl(s3Client, command, { expiresIn: 300 }); // 5 minutes expiry

    res.json({
      uploadUrl,
      key
    });
  } catch (error) {
    console.error("[E2EE-Media] Error generating upload URL:", error);
    res.status(500).json({ error: "Failed to generate upload URL" });
  }
};

/**
 * Generate Pre-signed URL for E2EE Download
 */
export const getE2eeDownloadUrl = async (req, res) => {
  try {
    const targetQuery = req.query.media_id || req.query.s3_key || req.query.key || req.query.id;
    const fallbackS3Key = req.query.s3_key || req.query.key;

    if (!targetQuery && !fallbackS3Key) {
      return res.status(400).json({ error: "Missing media_id or s3_key" });
    }

    // Look up metadata in DB or Registry (Try Registry first for speed)
    let media = targetQuery ? mediaRegistry.get(targetQuery) : null;
    if (!media && fallbackS3Key) {
      media = mediaRegistry.get(fallbackS3Key);
    }
    
    if (!media) {
      // Fallback: Search DB by ID or S3 Key (to handle all formats)
      const allMedia = db.get('media').value() || [];
      media = allMedia.find(m => m && (
        m.id === targetQuery || 
        m.s3_key === targetQuery || 
        m.key === targetQuery || 
        m.id === fallbackS3Key ||
        m.s3_key === fallbackS3Key ||
        (m.s3_key && targetQuery && m.s3_key.endsWith(targetQuery))
      ));
    }

    // S3 KEY DYNAMIC FALLBACK: If still not found, check if fallbackS3Key or targetQuery looks like an S3 path
    const pathCandidate = fallbackS3Key || (typeof targetQuery === 'string' && (targetQuery.includes('/') || targetQuery.endsWith('.bin') || targetQuery.startsWith('e2ee/')) ? targetQuery : null);

    if (!media && pathCandidate) {
      console.log(`[E2EE-Media] S3 Fallback for path ID: ${pathCandidate}`);
      media = {
        s3_key: pathCandidate,
        key: pathCandidate,
        nonce: null,
        file_name: pathCandidate.split('/').pop(),
        mime_type: 'application/octet-stream'
      };
    }

    if (!media) {
      console.warn(`[E2EE-Media] 404: Media metadata not found for ID: ${targetQuery}`);
      return res.status(404).json({ error: "Media metadata not found" });
    }

    const targetKey = media.s3_key || media.key || pathCandidate || targetQuery;
    console.log(`[E2EE-Media] Generating Download URL for ID: ${targetQuery} (Key: ${targetKey})`);

    const command = new GetObjectCommand({
      Bucket: BUCKET_NAME,
      Key: targetKey,
    });

    const downloadUrl = await getSignedUrl(s3Client, command, { expiresIn: 3600 });

    res.json({
      downloadUrl,
      s3_key: targetKey,
      key: targetKey,
      nonce: media.nonce || null,
      file_name: media.file_name || targetKey.split('/').pop(),
      mime_type: media.mime_type || 'application/octet-stream'
    });
  } catch (error) {
    console.error("[E2EE-Media] Error generating download URL:", error);
    res.status(500).json({ error: "Failed to generate download URL" });
  }
};

/**
 * Save E2EE Metadata (Cipher Metadata)
 * Store everything EXCEPT content and keys.
 */
export const saveE2eeMetadata = (req, res) => {
  try {
    const {
      id,
      key,
      nonce,
      file_name,
      mime_type,
      sender_id,
      receiver_id,
      chat_id,
      size
    } = req.body;

    if (!key || !nonce || !sender_id || !file_name) {
      return res.status(400).json({ error: "Missing required E2EE metadata" });
    }

    const mediaId = id || uuidv4();
    const mediaObj = {
      id: mediaId,
      s3_key: key,
      key: key,
      nonce, // Base64 encoded nonce
      file_name,
      mime_type,
      size,
      sender_id,
      receiver_id: receiver_id || null,
      chat_id: chat_id || null,
      is_e2ee: true,
      timestamp: new Date().toISOString(),
    };

    // Store in media registry under both ID and S3 key
    mediaRegistry.set(mediaId, mediaObj);
    if (key) mediaRegistry.set(key, mediaObj);

    if (!db.has('media').value()) {
      db.set('media', []).write();
    }
    db.get('media').push(mediaObj).write();
    flushDb();
    syncMediaToS3();

    console.log(`[E2EE-Media] Metadata saved for ${file_name} (ID: ${mediaId}, Key: ${key})`);

    res.json({ success: true, media: mediaObj });
  } catch (error) {
    console.error("[E2EE-Media] Error saving metadata:", error);
    res.status(500).json({ error: "Failed to save E2EE metadata" });
  }
};

/**
 * Initiate Multipart Upload
 */
export const initiateMultipart = async (req, res) => {
  try {
    const { fileName, fileType, userId } = req.body;
    const timestamp = Date.now();
    const cleanFileName = fileName.replace(/[^a-zA-Z0-9.-]/g, '_');
    const key = `uploads/${userId}/${timestamp}-${cleanFileName}`;

    const command = new CreateMultipartUploadCommand({
      Bucket: BUCKET_NAME,
      Key: key,
      ContentType: fileType,
    });

    const response = await s3Client.send(command);
    res.json({
      uploadId: response.UploadId,
      key: response.Key,
    });
  } catch (error) {
    console.error("[Media] Multipart initiate error:", error);
    res.status(500).json({ error: "Failed to initiate multipart upload" });
  }
};

/**
 * Get Pre-signed URLs for parts
 */
export const getMultipartPresignedUrls = async (req, res) => {
  try {
    const { key, uploadId, partNumbers } = req.body; // partNumbers: array of numbers

    const urls = await Promise.all(
      partNumbers.map(async (partNumber) => {
        const command = new UploadPartCommand({
          Bucket: BUCKET_NAME,
          Key: key,
          UploadId: uploadId,
          PartNumber: partNumber,
        });
        return {
          partNumber,
          url: await getSignedUrl(s3Client, command, { expiresIn: 3600 }),
        };
      })
    );

    res.json({ urls });
  } catch (error) {
    console.error("[Media] Multipart URLs error:", error);
    res.status(500).json({ error: "Failed to generate part URLs" });
  }
};

/**
 * Complete Multipart Upload
 */
export const completeMultipart = async (req, res) => {
  try {
    const { key, uploadId, parts } = req.body; // parts: [{ ETag, PartNumber }]

    const command = new CompleteMultipartUploadCommand({
      Bucket: BUCKET_NAME,
      Key: key,
      UploadId: uploadId,
      MultipartUpload: {
        Parts: parts.sort((a, b) => a.PartNumber - b.PartNumber),
      },
    });

    await s3Client.send(command);
    const region = process.env.AWS_REGION || 'us-east-1';
    const fileUrl = `https://${BUCKET_NAME}.s3.${region}.amazonaws.com/${key}`;

    res.json({ success: true, fileUrl, key });
  } catch (error) {
    console.error("[Media] Multipart complete error:", error);
    res.status(500).json({ error: "Failed to complete multipart upload" });
  }
};
