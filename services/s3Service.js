import {
  PutObjectCommand,
  GetObjectCommand,
  DeleteObjectCommand,
  HeadBucketCommand
} from "@aws-sdk/client-s3";
import { s3Client, BUCKET_NAME } from "../config/s3.js";
import { db } from "../config/db.js";
import { chatRooms, groups, mediaRegistry, userMutes } from "../store/state.js";

// Debounce Mechanism for S3 Sync
const syncDebounces = new Map();

export const debounceSync = (key, fn, delay = 2000) => {
  if (syncDebounces.has(key)) {
    clearTimeout(syncDebounces.get(key));
  }
  const timeout = setTimeout(() => {
    fn();
    syncDebounces.delete(key);
  }, delay);
  syncDebounces.set(key, timeout);
};

export const syncMutesToDb = () => {
  debounceSync('userMutes', () => {
    const mutesObj = Object.fromEntries(userMutes);
    db.set('userMutes', mutesObj).write();
  }, 1000);
};

export const syncChatRoomsToS3 = async () => {
  debounceSync('chatRooms', async () => {
    try {
      const roomsArray = Array.from(chatRooms.values());
      db.set('chatRooms', roomsArray).write();
      await s3Client.send(new PutObjectCommand({
        Bucket: BUCKET_NAME,
        Key: 'persistence/chatRooms.json',
        Body: JSON.stringify(roomsArray),
        ContentType: 'application/json'
      }));
      console.log('[S3] ChatRooms synced to bucket');
    } catch (err) {
      console.error('[S3] ChatRoom Sync failed:', err);
    }
  });
};

export const syncGroupsToS3 = async () => {
  debounceSync('groups', async () => {
    try {
      const groupsArray = Array.from(groups.values());
      db.set('groups', groupsArray).write();
      await s3Client.send(new PutObjectCommand({
        Bucket: BUCKET_NAME,
        Key: 'persistence/groups.json',
        Body: JSON.stringify(groupsArray),
        ContentType: 'application/json'
      }));
      console.log('[S3] Groups synced to bucket');
    } catch (err) {
      console.error('[S3] Group Sync failed:', err);
    }
  });
};

export const syncMediaToS3 = async () => {
  debounceSync('media', async () => {
    try {
      const mediaArray = Array.from(mediaRegistry.values());
      db.set('media', mediaArray).write();
      await s3Client.send(new PutObjectCommand({
        Bucket: BUCKET_NAME,
        Key: 'persistence/media.json',
        Body: JSON.stringify(mediaArray),
        ContentType: 'application/json'
      }));
      console.log('[S3] Media metadata synced to bucket');
    } catch (err) {
      console.error('[S3] Media Sync failed:', err);
    }
  });
};

// S3 Cleanup Helper
export async function deleteS3Object(mediaId) {
  try {
    const media = db.get('media').find({ id: mediaId }).value();
    if (!media) return;

    console.log(`[S3-Cleanup] Deleting file: ${media.s3_key} (ID: ${mediaId})`);

    const command = new DeleteObjectCommand({
      Bucket: BUCKET_NAME,
      Key: media.s3_key,
    });

    await s3Client.send(command);

    // Remove from DB
    db.get('media').remove({ id: mediaId }).write();
    mediaRegistry.delete(mediaId);
    syncMediaToS3();

    console.log(`[S3-Cleanup] Successfully deleted ${mediaId} from S3 and Registry`);
  } catch (error) {
    console.error(`[S3-Cleanup] Error deleting ${mediaId}:`, error);
  }
}

export const loadGroupsFromS3 = async () => {
  try {
    console.log('[S3] Attempting to load groups from bucket...');
    const command = new GetObjectCommand({
      Bucket: BUCKET_NAME,
      Key: 'persistence/groups.json',
    });

    const response = await s3Client.send(command);
    const stream = response.Body;
    const data = await new Promise((resolve, reject) => {
      const chunks = [];
      stream.on('data', chunk => chunks.push(chunk));
      stream.on('error', reject);
      stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    });

    const groupsArray = JSON.parse(data);
    groupsArray.forEach(g => groups.set(g.groupId, g));
    console.log(`✅ [S3] Successfully loaded ${groups.size} groups from bucket`);
  } catch (err) {
    console.warn('[S3] Load from bucket failed (using local fallback):', err.message);
    // FALLBACK: Load from db.json if S3 is not yet established
    const dbGroups = db.get('groups').value() || [];
    dbGroups.forEach(g => groups.set(g.groupId, g));
    console.log(`[Startup] Loaded ${groups.size} groups from local persistent store`);
  }
};

export const loadChatRoomsFromS3 = async () => {
  try {
    console.log('[S3] Attempting to load chatRooms from bucket...');
    const command = new GetObjectCommand({
      Bucket: BUCKET_NAME,
      Key: 'persistence/chatRooms.json',
    });

    const response = await s3Client.send(command);
    const stream = response.Body;
    const data = await new Promise((resolve, reject) => {
      const chunks = [];
      stream.on('data', chunk => chunks.push(chunk));
      stream.on('error', reject);
      stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    });

    const roomsArray = JSON.parse(data);
    roomsArray.forEach(r => chatRooms.set(r.roomId, r));
    console.log(`✅ [S3] Successfully loaded ${chatRooms.size} chatRooms from bucket`);
  } catch (err) {
    console.warn('[S3] ChatRooms load failed (using local fallback):', err.message);
    const dbRooms = db.get('chatRooms').value() || [];
    dbRooms.forEach(r => chatRooms.set(r.roomId, r));
    console.log(`[Startup] Loaded ${chatRooms.size} chatRooms from local persistent store`);
  }
};

export const registerMediaObject = (m) => {
  if (!m) return;
  if (m.id) mediaRegistry.set(m.id, m);
  if (m.s3_key) mediaRegistry.set(m.s3_key, m);
  if (m.key) mediaRegistry.set(m.key, m);
};

export const loadMediaFromS3 = async () => {
  // Always seed local DB items first so memory is immediately populated
  try {
    const dbMedia = db.get('media').value() || [];
    dbMedia.forEach(registerMediaObject);
  } catch (e) {}

  try {
    console.log('[S3] Attempting to load media from bucket...');
    const command = new GetObjectCommand({
      Bucket: BUCKET_NAME,
      Key: 'persistence/media.json',
    });

    const response = await s3Client.send(command);
    const stream = response.Body;
    const data = await new Promise((resolve, reject) => {
      const chunks = [];
      stream.on('data', chunk => chunks.push(chunk));
      stream.on('error', reject);
      stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    });

    const mediaArray = JSON.parse(data);
    mediaArray.forEach(registerMediaObject);
    console.log(`✅ [S3] Successfully loaded ${mediaRegistry.size} media items from bucket`);
  } catch (err) {
    console.warn('[S3] Media load from bucket skipped/failed (using local store):', err.message);
  }
};

export const testS3 = async () => {
  try {
    await s3Client.send(new HeadBucketCommand({ Bucket: BUCKET_NAME }));
    console.log(`✅ S3 Connection Success: Bucket "${BUCKET_NAME}" is reachable.`);
  } catch (err) {
    console.error(`❌ S3 Connection Failed: Cannot reach bucket "${BUCKET_NAME}". 
      Check your AWS_REGION, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY.`);
    console.error(`Error details: ${err.message}`);
  }
};
