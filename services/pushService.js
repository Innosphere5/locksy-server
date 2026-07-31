import { Expo } from 'expo-server-sdk';
import { admin, expo, isFirebaseAdminInitialized } from '../config/firebase.js';

// Expo Push Notification helper
export const sendExpoPushNotification = async (token, title, body, data = {}) => {
  if (!Expo.isExpoPushToken(token)) {
    console.error(`[Expo-Push] Token ${token} is not a valid Expo push token`);
    return;
  }

  // Ensure all data payload fields are strings to avoid Expo protocol validation errors
  const serializedData = {};
  for (const key of Object.keys(data)) {
    serializedData[key] = String(data[key]);
  }

  const messages = [{
    to: token,
    sound: 'default',
    title: title,
    body: body,
    data: serializedData,
    priority: 'high',
    channelId: 'default',
    _displayInForeground: true,
  }];

  try {
    const chunks = expo.chunkPushNotifications(messages);
    for (let chunk of chunks) {
      const ticketChunk = await expo.sendPushNotificationsAsync(chunk);
      console.log('[Expo-Push] Tickets response:', ticketChunk);
      if (Array.isArray(ticketChunk)) {
        for (const ticket of ticketChunk) {
          if (ticket.status === 'error') {
            console.error(`[Expo-Push] Delivery error: ${ticket.message} (${ticket.details?.error})`);
            // DeviceNotRegistered means the token is stale — caller should clean it up
            if (ticket.details?.error === 'DeviceNotRegistered') {
              console.warn(`[Expo-Push] Token ${token.substring(0, 20)}... is stale (DeviceNotRegistered)`);
            }
          }
        }
      }
    }
  } catch (error) {
    console.error('[Expo-Push] Error sending via Expo Push API:', error);
  }
};

// Push Notification Helper — unified entry point for both Expo and raw FCM tokens
export const sendPushNotification = async (token, title, body, data = {}) => {
  if (!token) {
    console.warn('[Push] Cannot send notification: token is null/undefined');
    return;
  }

  // Sanitize body — messages in Locksy are encrypted, so never send ciphertext
  // as the notification body. Use a generic privacy-safe preview.
  let safeBody = body;
  if (typeof body === 'string' && body.length > 200) {
    // Likely encrypted ciphertext — truncate/replace
    safeBody = 'New message';
  }
  // Detect base64-encoded or ciphertext patterns (common in encrypted chat payloads)
  if (typeof safeBody === 'string' && /^[A-Za-z0-9+/=]{50,}$/.test(safeBody.trim())) {
    safeBody = 'New message';
  }

  console.log(`[Push] Attempting push: title="${title}", body="${safeBody?.substring(0, 30)}...", token="${token.substring(0, 15)}..."`);

  // If it is an Expo Push Token, use the Expo Push API
  if (token.startsWith('ExponentPushToken') || token.startsWith('ExpoPushToken') || Expo.isExpoPushToken(token)) {
    console.log(`[Push] Sending via Expo Push Service to: ${token.substring(0, 20)}...`);
    await sendExpoPushNotification(token, title, safeBody, data);
    return;
  }

  // Ensure all data fields are strings for raw FCM
  const serializedData = {};
  for (const key of Object.keys(data)) {
    serializedData[key] = String(data[key]);
  }

  // Otherwise, use raw FCM
  const message = {
    token: token,
    notification: {
      title: title,
      body: safeBody
    },
    data: serializedData,
    android: {
      priority: 'high',
      notification: {
        channelId: 'default',
        sound: 'default',
        priority: 'high',
        visibility: 'public'
      }
    }
  };

  try {
    if (!isFirebaseAdminInitialized) {
      console.error('[Push] CRITICAL: Firebase Admin SDK is not initialized. Push notification WILL NOT be delivered. Check service account JSON file.');
      return;
    }
    const response = await admin.messaging().send(message);
    console.log(`[Push] Successfully sent via Firebase Admin SDK to ${token.substring(0, 10)}... :`, response);
  } catch (error) {
    const errorCode = error.code || '';
    const errorMsg = error.message || String(error);
    console.error(`[Push] FCM send error (code: ${errorCode}):`, errorMsg);

    // Handle specific FCM error codes
    if (errorCode === 'messaging/registration-token-not-registered' ||
        errorCode === 'messaging/invalid-registration-token') {
      console.warn(`[Push] Token ${token.substring(0, 10)}... is invalid/expired. It should be cleaned up.`);
    }
  }
};
