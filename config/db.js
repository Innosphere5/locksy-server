import low from 'lowdb';
import FileSync from 'lowdb/adapters/FileSync.js';
import fs from 'fs';

const adapter = new FileSync('db.json');
const db = low(adapter);

// Initialize DB defaults synchronously on startup
db.defaults({
  users: [],
  chatRooms: [],
  groups: [],
  media: [],
  groupInvites: [],
  groupUnreadMessages: [],
  userMutes: {}
}).write();

// Override db.write for runtime writes to be debounced to prevent event loop blocks
let dbWriteTimeout = null;

const performDbWrite = () => {
  try {
    const data = db.getState();
    fs.writeFile('db.json', JSON.stringify(data, null, 2), 'utf8', (err) => {
      if (err) console.error('[DB-Async] Write failed:', err);
    });
  } catch (err) {
    console.error('[DB-Async] Exception during write:', err);
  }
};

db.write = function() {
  if (dbWriteTimeout) clearTimeout(dbWriteTimeout);
  dbWriteTimeout = setTimeout(performDbWrite, 500);
  return db;
};

// Force-flush: immediately write to disk synchronously, bypassing debounce.
const flushDb = () => {
  if (dbWriteTimeout) clearTimeout(dbWriteTimeout);
  try {
    const data = db.getState();
    fs.writeFileSync('db.json', JSON.stringify(data, null, 2), 'utf8');
  } catch (err) {
    console.error('[DB-Flush] Synchronous write failed:', err);
  }
};

export { db, flushDb };
