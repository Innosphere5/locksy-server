import 'dotenv/config';
import admin from 'firebase-admin';
import { readFileSync } from 'fs';
import { Expo } from 'expo-server-sdk';

const expo = new Expo();
let isFirebaseAdminInitialized = false;

// Initialize Firebase Admin with the provided service account
try {
  let serviceAccount;
  const candidateFiles = [
    './serviceAccountKey.json',
    './locksy-notification-firebase-adminsdk-fbsvc-b6249d695c.json',
    './locksy-notification-firebase-adminsdk-fbsvc-d9f8ea8f8b.json'
  ];

  for (const file of candidateFiles) {
    try {
      serviceAccount = JSON.parse(readFileSync(file, 'utf8'));
      console.log(`[Firebase] Loaded service account from JSON file: ${file}`);
      break;
    } catch (_) {}
  }

  if (!serviceAccount && process.env.FIREBASE_SERVICE_ACCOUNT) {
    try {
      serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
      console.log('[Firebase] Loaded service account from FIREBASE_SERVICE_ACCOUNT environment variable.');
    } catch (parseError) {
      console.error(`[Firebase] Failed to parse FIREBASE_SERVICE_ACCOUNT env variable as JSON: ${parseError.message}`);
    }
  }

  if (serviceAccount && serviceAccount.private_key) {
    serviceAccount.private_key = serviceAccount.private_key
      .replace(/\\n/g, '\n')
      .replace(/\r/g, '');
  }

  if (serviceAccount && admin.apps.length === 0) {
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount)
    });
    isFirebaseAdminInitialized = true;
    console.log('[Firebase] Admin SDK initialized successfully.');
  }
} catch (error) {
  console.error('[Firebase] Failed to initialize Admin SDK:', error.message || error);
}

export { admin, expo, isFirebaseAdminInitialized };
