import "dotenv/config";
import Express from "express";
import { createServer } from "http";
import { Server } from "socket.io";
import cors from "cors";
import rateLimit from "express-rate-limit";

import authRoutes from "./routes/authRoutes.js";
import mediaRoutes from "./routes/mediaRoutes.js";
import { registerSocketHandlers } from "./sockets/index.js";
import { loadFromDb } from "./store/state.js";
import {
  loadGroupsFromS3,
  loadChatRoomsFromS3,
  loadMediaFromS3,
  testS3
} from "./services/s3Service.js";

const app = Express();
const httpServer = createServer(app);
const allowedOrigins = process.env.ALLOWED_ORIGINS 
  ? process.env.ALLOWED_ORIGINS.split(',').map(o => o.trim()) 
  : "*";

const io = new Server(httpServer, {
  cors: {
    origin: allowedOrigins,
    methods: ["GET", "POST"],
    allowEIO3: true,
  },
  transports: ["websocket", "polling"],
  path: "/socket.io/",
  maxHttpBufferSize: 10e6, // 10MB limit
});

const PORT = process.env.PORT || 5050; // Use process.env.PORT for Render/Production

// Express Rate Limiting
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 300, // Limit each IP to 300 requests per 15 mins
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: "Too many requests, please try again later." }
});

// Express Middlewares
app.use(Express.json({ limit: "10mb" }));
app.use(Express.urlencoded({ limit: "10mb", extended: true }));
app.use("/api/", apiLimiter);

app.use(cors({
  origin: allowedOrigins,
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  credentials: true
}));

// Global error handler for JSON parsing errors
app.use((err, req, res, next) => {
  if (err instanceof SyntaxError && err.status === 400 && 'body' in err) {
    console.error('[Server] Bad JSON received:', err.message);
    return res.status(400).json({ error: 'Malformed JSON request' });
  }
  next();
});

// REST API Routes
app.use("/", authRoutes);
app.use("/", mediaRoutes);

// Socket.IO Handlers
registerSocketHandlers(io);

// Startup Data Loading
const withTimeout = (promise, ms) => Promise.race([
  promise,
  new Promise((_, reject) => setTimeout(() => reject(new Error('S3 Connection Timeout')), ms))
]);

try { await withTimeout(loadGroupsFromS3(), 3000); } catch(e) { console.warn('[S3] Groups Load Timeout:', e.message); }
try { await withTimeout(loadChatRoomsFromS3(), 3000); } catch(e) { console.warn('[S3] ChatRooms Load Timeout:', e.message); }
try { await withTimeout(loadMediaFromS3(), 3000); } catch(e) { console.warn('[S3] Media Load Timeout:', e.message); }

loadFromDb();
testS3();

httpServer.listen(PORT, () => {
  console.log(`✅ Locksy Server v1.1.0 running on port ${PORT}`);
});
