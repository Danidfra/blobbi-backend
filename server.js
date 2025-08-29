import "dotenv/config";
import express from "express";
import cors from "cors";
import webpush from "web-push";
import pg from "pg";
import { SimplePool, nip19, getPublicKey } from "nostr-tools";

const { Pool } = pg;

// Environment variables
const PORT = process.env.PORT || 8080;
const DATABASE_URL = process.env.DATABASE_URL;
const VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY;
const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY;
const VAPID_SUBJECT = process.env.VAPID_SUBJECT;
const NOSTR_RELAYS = (process.env.NOSTR_RELAYS || "wss://relay.damus.io,wss://nostr.wine").split(",");
const MONITOR_INTERVAL_MS =
  Number(process.env.MONITOR_INTERVAL_MS) ||
  (process.env.DEBUG_MONITOR ? 60_000 : 30 * 60 * 1000);

// Rate limiting storage (in-memory)
const lastSent = new Map(); // endpoint -> { messageKey -> timestamp }

// New caches for notification logic
const latestByNpub = new Map(); // npub -> { blobbies: Map, lastEvaluatedAt: timestamp }
const lastNotified = new Map(); // npub -> { care?: timestamp, serious?: timestamp }

// Database connection
const pool = new Pool({
  connectionString: DATABASE_URL,
});

// Web push configuration
webpush.setVapidDetails(VAPID_SUBJECT, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);

// Nostr pool
const nostrPool = new SimplePool();
let activeSubscriptions = new Set(); // Set of hex pubkeys to monitor

const lastSeenByPubkey = new Map();
const processedEventIds = new Set();

const app = express();
app.use(cors());
app.use(express.json());

// Helper functions
function npubToHex(npub) {
  try {
    const { type, data } = nip19.decode(npub);
    return type === "npub" ? data : null;
  } catch (error) {
    console.error("Invalid npub:", npub, error.message);
    return null;
  }
}

async function loadActiveAuthorsHex() {
  const rows = await pool.query(
    "SELECT DISTINCT npub FROM webpush_subscriptions WHERE npub IS NOT NULL AND muted = FALSE"
  );
  const authors = [];
  for (const r of rows.rows) {
    const hex = npubToHex(r.npub);
    if (hex) authors.push(hex);
  }
  return authors;
}

function normalizeValue(value) {
  if (typeof value === "string") {
    value = parseFloat(value);
  }
  if (isNaN(value)) return null;
  
  // Normalize to percentage (0-100)
  if (value <= 1) {
    return value * 100;
  }
  return Math.min(100, Math.max(0, value));
}

function fetchEventsOnce(relays, filters, timeoutMs = 4000) {
  return new Promise((resolve) => {
    const out = [];
    const sub = nostrPool.subscribeMany(relays, filters, {
      onevent: (ev) => out.push(ev),
      oneose: () => {
        try { sub.close(); } catch {}
        resolve(out);
      },
    });
    // guarantee: close even if some relay doesn't send EOSE
    setTimeout(() => {
      try { sub.close(); } catch {}
      resolve(out);
    }, timeoutMs);
  });
}

/**
 * Extracts Blobbi ID and name from a kind 31124 event
 * @param {Object} event - Nostr event
 * @returns {Object} { id: string, name: string }
 */
function extractBlobbiIdAndName(event) {
  let blobbiId = null;
  let explicitName = null;

  // Step 1: Check for NIP-33 ["d",...] tag first (any "d" tag value)
  for (const tag of event.tags || []) {
    if (tag.length >= 2 && tag[0] === "d") {
      blobbiId = tag[1]; // Use the "d" tag value as blobbiId
      break; // Found NIP-33 tag, stop searching for other IDs
    }
  }

  // Step 2: If no NIP-33 tag, check for blobbi_id, id, name, or blobbi_name tags
  if (!blobbiId) {
    for (const tag of event.tags || []) {
      if (tag.length >= 2) {
        const [key, value] = tag;
        if ((key === "blobbi_id" || key === "id") && !blobbiId) {
          blobbiId = value;
        }
        if ((key === "name" || key === "blobbi_name") && !explicitName) {
          explicitName = value;
        }
      }
    }
  }

  // Step 3: Check content JSON for additional name/ID info
  try {
    const content = JSON.parse(event.content || "{}");
    if (!blobbiId && (content.blobbi_id || content.id)) {
      blobbiId = content.blobbi_id || content.id;
    }
    if (!explicitName && (content.name || content.blobbi_name)) {
      explicitName = content.name || content.blobbi_name;
    }
  } catch (error) {
    // Content is not valid JSON
  }

  // Step 4: Determine final display name
  let finalName;
  
  // If blobbiId exists, derive displayName by removing "blobbi-" prefix
  if (blobbiId) {
    finalName = blobbiId.startsWith("blobbi-") ? blobbiId.slice(7) : blobbiId;
  }
  // Else, if explicit name exists, use that
  else if (explicitName) {
    finalName = explicitName;
  }
  // Else, fall back to "Blobbi"
  else {
    finalName = "Blobbi";
  }

  // Debug log to verify name selection
  console.log(`🔍 parse: { blobbiId: "${blobbiId || 'null'}", finalName: "${finalName}" }`);

  return {
    id: blobbiId || "default",
    name: finalName
  };
}

/**
 * Collects and normalizes statuses of all Blobbies by event
 * @param {Array} events - Array of kind 31124 events
 * @returns {Map} npub -> Map(blobbiId -> { name, statuses })
 */
function collectStatusesByBlobbi(events) {
  const result = new Map(); // npub -> Map(blobbiId -> { name, statuses })

  for (const event of events) {
    const npub = nip19.npubEncode(event.pubkey);
    const { id: blobbiId, name: blobbiName } = extractBlobbiIdAndName(event);
    
    if (!result.has(npub)) {
      result.set(npub, new Map());
    }
    
    const userBlobbies = result.get(npub);
    
    // Parse statuses
    const statuses = {};

    const setMetric = (key, raw) => {
      if (key === "awake") {
        const s = String(raw).toLowerCase();
        statuses.awake = (s === "1" || s === "true");
        return;
      }
      const normalized = normalizeValue(raw);
      if (normalized !== null) statuses[key] = normalized;
    };

    // Process tags
    for (const tag of event.tags || []) {
      if (tag.length >= 2) {
        const [key, value] = tag;
        if (["health", "energy", "hygiene", "happiness", "hunger", "awake"].includes(key)) {
          setMetric(key, value);
        }
      }
    }

    // Process content JSON
    try {
      const content = JSON.parse(event.content || "{}");
      for (const [key, value] of Object.entries(content)) {
        if (["health", "energy", "hygiene", "happiness", "hunger", "awake"].includes(key)) {
          setMetric(key, value);
        }
      }
    } catch (error) {
      // Content is not valid JSON
    }

    // Store or update the Blobbi
    if (!userBlobbies.has(blobbiId) || event.created_at > (userBlobbies.get(blobbiId).lastUpdate || 0)) {
      userBlobbies.set(blobbiId, {
        name: blobbiName,
        statuses,
        lastUpdate: event.created_at
      });
    }
  }

  return result;
}

/**
 * Computes notification decision for a user based on their Blobbies
 * @param {Map} blobbies - Map(blobbiId -> { name, statuses })
 * @returns {string|null} Notification message or null if no notification needed
 */
function computeGroupDecision(blobbies) {
  const blobbiesBelow60 = [];
  const blobbiesBelow30 = [];

  for (const [blobbiId, { name, statuses }] of blobbies) {
    const { health, energy, hygiene, happiness, hunger, awake } = statuses;
    
    // Collect valid values (energy only counts if awake !== false)
    const values = [];
    if (health !== undefined) values.push(health);
    if (energy !== undefined && awake !== false) values.push(energy);
    if (hygiene !== undefined) values.push(hygiene);
    if (happiness !== undefined) values.push(happiness);
    if (hunger !== undefined) values.push(hunger);

    // Check if any status is < 60 or < 30
    const hasBelow60 = values.some(v => v < 60);
    const hasBelow30 = values.some(v => v < 30);

    if (hasBelow60) {
      blobbiesBelow60.push({ id: blobbiId, name });
    }
    if (hasBelow30) {
      blobbiesBelow30.push({ id: blobbiId, name });
    }
  }

  // Apply notification rules
  const countBelow60 = blobbiesBelow60.length;
  const countBelow30 = blobbiesBelow30.length;

  if (countBelow60 === 0) {
    return null; // No Blobbi needs care
  }

  if (countBelow60 === 1 && countBelow30 === 0) {
    // Only 1 Blobbi below 60 and none below 30
    const blobbiName = blobbiesBelow60[0].name;
    if (blobbiName === "Blobbi") {
      return "Your Blobbi needs care";
    } else {
      return `The Blobbi ${blobbiName} needs care`;
    }
  }

  if (countBelow30 === 1 && countBelow60 === 1) {
    // Only 1 Blobbi with some status < 30 (and it's the same one that's < 60)
    const blobbiName = blobbiesBelow30[0].name;
    if (blobbiName === "Blobbi") {
      return "Your Blobbi needs serious care";
    } else {
      return `The Blobbi ${blobbiName} needs serious care`;
    }
  }

  if (countBelow60 >= 2 && countBelow30 === 0) {
    // 2+ Blobbies with some status < 60 and none < 30
    return "Your Blobbies need care";
  }

  if (countBelow60 >= 2 && countBelow30 >= 1) {
    // 2+ Blobbies with some status < 60 and at least 1 with some status < 30
    return "Your Blobbies need serious care";
  }

  // Special case: only 1 Blobbi < 60, but there are others < 30
  if (countBelow60 === 1 && countBelow30 >= 1) {
    const blobbiName = blobbiesBelow30[0].name;
    if (blobbiName === "Blobbi") {
      return "Your Blobbi needs serious care";
    } else {
      return `The Blobbi ${blobbiName} needs serious care`;
    }
  }

  return null;
}

function createMessageKey(message) {
  return Buffer.from(message).toString("base64").slice(0, 32);
}

const RATE_LIMIT_MS = Number(process.env.RATE_LIMIT_MS || MONITOR_INTERVAL_MS);

function canSendMessage(endpoint, messageKey) {
  const now = Date.now();
  const endpointMessages = lastSent.get(endpoint) || {};
  const lastSentTime = endpointMessages[messageKey];
  
  if (!lastSentTime || (now - lastSentTime) > RATE_LIMIT_MS) {
    return true;
  }
  return false;
}

function markMessageSent(endpoint, messageKey) {
  const now = Date.now();
  if (!lastSent.has(endpoint)) {
    lastSent.set(endpoint, {});
  }
  lastSent.get(endpoint)[messageKey] = now;
}

async function sendPushNotification(endpoint, p256dh, auth, title, body, data = {}) {
  try {
    const subscription = { endpoint, keys: { p256dh, auth } };

    const sev = data?.severity || "care";
    const tag = (data?.tag) ?? `blobbi-status:${sev}`;
    const renotify = (typeof data?.renotify === "boolean")
      ? data.renotify
      : (sev === "serious"); // padrão: só renotify em “serious”

    const payload = JSON.stringify({
      title,
      body,
      data: { ...data, timestamp: data?.timestamp ?? Date.now() },
      tag,
      renotify,
      timestamp: Date.now()
    });

    await webpush.sendNotification(subscription, payload);
    return true;
  } catch (error) {
    console.error("Push notification failed:", error.message);
    if (error.statusCode === 404 || error.statusCode === 410) {
      try {
        await pool.query("DELETE FROM webpush_subscriptions WHERE endpoint = $1", [endpoint]);
        console.log("Removed invalid subscription:", endpoint);
      } catch (dbError) {
        console.error("Failed to remove invalid subscription:", dbError.message);
      }
    }
    return false;
  }
}

/**
 * Periodic notification handler that sends notifications based on cached data
 */
async function tickNotifications() {
  console.log(`🔔 [tick] Checking ${latestByNpub.size} cached users for notifications`);
  
  const now = Date.now();
  let notificationsSent = 0;
  
  for (const [npub, { blobbies, lastEvaluatedAt }] of latestByNpub) {
    try {
      // Compute the decision message
      const message = computeGroupDecision(blobbies);
      
      // Determine severity
      let severity = null;
      if (message) {
        if (message.includes("serious care")) {
          severity = "serious";
        } else if (message.includes("needs care") || message.includes("need care")) {
          severity = "care";
        }
      }
      
      // If no message needed, reset cooldowns and skip
      if (severity === null) {
        if (lastNotified.has(npub)) {
          lastNotified.delete(npub);
          console.log(`✅ [tick] ${npub} recovered - cleared cooldowns`);
        }
        continue;
      }
      
      // Get or initialize last notified timestamps for this user
      const userNotified = lastNotified.get(npub) || {};
      
      // Check if we should send notification
      let shouldSend = false;
      
      if (!userNotified[severity]) {
        // Never notified with this severity before
        shouldSend = true;
      } else {
        const lastNotifiedTime = userNotified[severity];
        const timeSinceLastNotified = now - lastNotifiedTime;
        
        // Check for escalation (care -> serious)
        if (severity === "serious" && userNotified.care && !userNotified.serious) {
          // Escalation from care to serious - send immediately
          shouldSend = true;
        } else {
          // Normal cooldown check
          shouldSend = timeSinceLastNotified >= MONITOR_INTERVAL_MS;
        }
      }
      
      if (shouldSend) {
        // Send the notification
        await broadcastToNpub(npub, message, { force: true, data: { severity } });
        
        // Update the timestamp
        if (!lastNotified.has(npub)) {
          lastNotified.set(npub, {});
        }
        lastNotified.get(npub)[severity] = now;
        
        console.log(`📤 [tick] Sent "${message}" to ${npub} (severity: ${severity})`);
        notificationsSent++;
      }
      
    } catch (err) {
      console.error(`❌ [tick] Error processing ${npub}:`, err.message);
    }
  }
  
  if (notificationsSent > 0) {
    console.log(`✅ [tick] Sent ${notificationsSent} notification(s)`);
  }
}

async function broadcastToNpub(npub, message, { force = false, data = {} } = {}) {
  if (!message) return;

  try {
    const result = await pool.query(
      "SELECT * FROM webpush_subscriptions WHERE npub = $1 AND muted = FALSE",
      [npub]
    );
    
    const messageKey = createMessageKey(message);
    let sentCount = 0;
    
    for (const subscription of result.rows) {
      if (force || canSendMessage(subscription.endpoint, messageKey)) {
        const success = await sendPushNotification(
          subscription.endpoint,
          subscription.p256dh,
          subscription.auth,
          "Blobbi",
          message,
          data
        );
        
        if (success) {
          markMessageSent(subscription.endpoint, messageKey);
          sentCount++;
        }
      }
    }
    
    if (sentCount > 0) {
      console.log(`📤 Sent "${message}" to ${sentCount} endpoint(s) for ${npub}`);
    }
  } catch (error) {
    console.error("Failed to broadcast to npub:", npub, error.message);
  }
}

async function updateActiveSubscriptions() {
  try {
    const result = await pool.query(
      "SELECT DISTINCT npub FROM webpush_subscriptions WHERE npub IS NOT NULL AND muted = FALSE"
    );
    const newSubscriptions = new Set();
    
    for (const row of result.rows) {
      const hexPubkey = npubToHex(row.npub);
      if (hexPubkey) newSubscriptions.add(hexPubkey);
    }
    
    activeSubscriptions = newSubscriptions;
    console.log(`📡 Monitoring ${activeSubscriptions.size} npubs for Nostr events`);
  } catch (error) {
    console.error("Failed to update active subscriptions:", error.message);
  }
}

// Nostr monitoring
function startNostrMonitoring() {
  let reconnectDelay = 1000;
  const maxReconnectDelay = 30000;

  const connect = () => {
    try {
      const relays = NOSTR_RELAYS;
      console.log("🔗 Connecting to Nostr relays:", relays);

      const authors = Array.from(activeSubscriptions); // hex pubkeys
      if (authors.length === 0) {
        console.log("⏳ No npubs to monitor yet; retrying soon...");
        setTimeout(connect, 5000);
        return;
      }

      // Filter (last 10 min for restart tolerance)
      const filters = [{ kinds: [31124], authors, since: Math.floor(Date.now() / 1000) - 600 }];

      // subscribeMany: receives callbacks
      const sub = nostrPool.subscribeMany(relays, filters, {
        onevent: async (event) => {
          try {
            await processNostrEvents([event]);
          } catch (err) {
            console.error("❌ Error processing Nostr event:", err.message);
          }
        },
        oneose: () => {
          console.log("✅ Nostr subscription established (EOSE)");
          reconnectDelay = 1000; // reset backoff
        },
      });

      // Reconnect every 5 min to refresh filters/connections
      setTimeout(() => {
        try { sub.close(); } catch {}
        setTimeout(connect, reconnectDelay);
        reconnectDelay = Math.min(maxReconnectDelay, reconnectDelay * 2);
      }, 5 * 60 * 1000);

    } catch (err) {
      console.error("❌ Nostr connection error:", err.message);
      setTimeout(connect, reconnectDelay);
      reconnectDelay = Math.min(maxReconnectDelay, reconnectDelay * 2);
    }
  };

  connect();
}

/**
 * Processes Nostr kind 31124 events and updates the cache (no notifications sent)
 * @param {Array} events - Array of events to process
 */
async function processNostrEvents(events) {
  if (!events || events.length === 0) return;

  // Group events by npub and Blobbi
  const statusesByNpub = collectStatusesByBlobbi(events);

  // Process each user
  for (const [npub, blobbies] of statusesByNpub) {
    try {
      // Count Blobbies that need care
      let countBelow60 = 0;
      let countBelow30 = 0;
      const blobbiNames = [];

      for (const [blobbiId, { name, statuses }] of blobbies) {
        const { health, energy, hygiene, happiness, hunger, awake } = statuses;
        
        // Collect valid values (energy only counts if awake !== false)
        const values = [];
        if (health !== undefined) values.push(health);
        if (energy !== undefined && awake !== false) values.push(energy);
        if (hygiene !== undefined) values.push(hygiene);
        if (happiness !== undefined) values.push(happiness);
        if (hunger !== undefined) values.push(hunger);

        const hasBelow60 = values.some(v => v < 60);
        const hasBelow30 = values.some(v => v < 30);

        if (hasBelow60) {
          countBelow60++;
          blobbiNames.push(name);
        }
        if (hasBelow30) {
          countBelow30++;
        }
      }

      // Log status counts
      if (countBelow60 > 0 || countBelow30 > 0) {
        console.log(`📊 ${npub}: ${blobbies.size} blobbies | ${countBelow60} <60 | ${countBelow30} <30`);
      }

      // Update cache instead of sending notifications
      latestByNpub.set(npub, {
        blobbies,
        lastEvaluatedAt: Date.now()
      });
      
      console.log(`🔄 [stream] Updated cache for ${npub} with ${blobbies.size} blobbies`);

    } catch (err) {
      console.error(`❌ Error processing events for ${npub}:`, err.message);
    }
  }
}

// HTTP Endpoints

// Health check
app.get("/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true, db: "up" });
  } catch (error) {
    res.status(500).json({ ok: false, db: "down", error: error.message });
  }
});

// Get VAPID public key
app.get("/vapid-public-key", (req, res) => {
  res.json({ key: VAPID_PUBLIC_KEY });
});

// Subscribe endpoint
app.post("/subscribe", async (req, res) => {
  try {
    const { subscription, npub, segment, label } = req.body;
    console.log(`📝 New subscription request: npub=${npub ? npub.slice(0, 16) + '...' : 'none'}, segment=${segment}`);
    
    if (!subscription || !subscription.endpoint || !subscription.keys) {
      return res.status(400).json({ error: "Invalid subscription object" });
    }
    
    const { endpoint, keys } = subscription;
    const { p256dh, auth } = keys;
    
    if (!endpoint || !p256dh || !auth) {
      return res.status(400).json({ error: "Missing required subscription fields" });
    }
    
    // Validate npub if provided
    if (npub && !npubToHex(npub)) {
      return res.status(400).json({ error: "Invalid npub format" });
    }
    
    // Use a more reliable method to detect new vs updated subscriptions
    const result = await pool.query(`
      INSERT INTO webpush_subscriptions (endpoint, p256dh, auth, ua, segment, npub, label, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, now())
      ON CONFLICT (endpoint) 
      DO UPDATE SET 
        p256dh = EXCLUDED.p256dh,
        auth = EXCLUDED.auth,
        ua = EXCLUDED.ua,
        segment = EXCLUDED.segment,
        npub = EXCLUDED.npub,
        label = EXCLUDED.label,
        updated_at = now()
      RETURNING id, endpoint, created_at, updated_at
    `, [endpoint, p256dh, auth, req.headers["user-agent"], segment, npub, label]);
    
    // Check if this was a new subscription by comparing created_at and updated_at
    const subscriptionRecord = result.rows[0];
    const isNewSubscription = Math.abs(new Date(subscriptionRecord.created_at) - new Date(subscriptionRecord.updated_at)) < 1000; // Within 1 second
    
    // Update active subscriptions if npub was provided
    if (npub) {
      await updateActiveSubscriptions();
    }
    
    // Send immediate confirmation push notification only for new subscriptions
    if (isNewSubscription) {
      try {
        console.log(`📤 Sending confirmation notification to new subscriber: ${endpoint.slice(-20)}...`);
        
        const confirmationSuccess = await sendPushNotification(
          endpoint,
          p256dh,
          auth,
          "Blobbi Notifications",
          "Notifications enabled! You'll receive alerts when your Blobbies need care.",
          { 
            type: "confirmation",
            tag: "blobbi-confirmation",
            renotify: false,
            timestamp: Date.now(),
            icon: "/icon-192x192.png",
            badge: "/badge-72x72.png"
          }
        );
        
        if (confirmationSuccess) {
          console.log(`✅ Successfully sent confirmation notification to: ${endpoint.slice(-20)}...`);
        } else {
          console.log(`⚠️ Failed to send confirmation notification to: ${endpoint.slice(-20)}...`);
        }
      } catch (confirmationError) {
        console.error("❌ Confirmation notification error:", confirmationError.message);
        // Don't fail the subscription if confirmation fails
      }
    } else {
      console.log(`🔄 Updated existing subscription: ${endpoint.slice(-20)}...`);
    }
    
    res.json({
      ok: true,
      id: subscriptionRecord.id,
      endpoint: subscriptionRecord.endpoint,
      isNew: isNewSubscription
    });
  } catch (error) {
    console.error("Subscribe error:", error.message);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Update segment
app.post("/subscribe/segment", async (req, res) => {
  try {
    const { endpoint, segment } = req.body;
    
    if (!endpoint) {
      return res.status(400).json({ error: "Missing endpoint" });
    }
    
    await pool.query(
      "UPDATE webpush_subscriptions SET segment = $1, updated_at = now() WHERE endpoint = $2",
      [segment, endpoint]
    );
    
    res.json({ ok: true });
  } catch (error) {
    console.error("Update segment error:", error.message);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Send to specific endpoint
app.post("/send", async (req, res) => {
  try {
    const { endpoint, title, body, data } = req.body;
    
    if (!endpoint || !title || !body) {
      return res.status(400).json({ error: "Missing required fields" });
    }
    
    const result = await pool.query(
      "SELECT * FROM webpush_subscriptions WHERE endpoint = $1",
      [endpoint]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Subscription not found" });
    }
    
    const subscription = result.rows[0];
    const success = await sendPushNotification(
      subscription.endpoint,
      subscription.p256dh,
      subscription.auth,
      title,
      body,
      data || {}
    );
    
    res.json({ ok: success });
  } catch (error) {
    console.error("Send error:", error.message);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Broadcast to all or segment
app.post("/broadcast", async (req, res) => {
  try {
    const { title, body, segment, data } = req.body;
    
    if (!title || !body) {
      return res.status(400).json({ error: "Missing required fields" });
    }
    
    let query = "SELECT * FROM webpush_subscriptions WHERE muted = FALSE";
    const params = [];
    
    if (segment) {
      query += " AND segment = $1";
      params.push(segment);
    }
    
    const result = await pool.query(query, params);
    let successCount = 0;
    
    for (const subscription of result.rows) {
      const success = await sendPushNotification(
        subscription.endpoint,
        subscription.p256dh,
        subscription.auth,
        title,
        body,
        data || {}
      );
      
      if (success) successCount++;
    }
    
    res.json({ 
      ok: true, 
      sent: successCount, 
      total: result.rows.length 
    });
  } catch (error) {
    console.error("Broadcast error:", error.message);
    res.status(500).json({ error: "Internal server error" });
  }
});

// List user subscriptions
app.get("/subs", async (req, res) => {
  try {
    const { npub } = req.query;
    
    if (!npub) {
      return res.status(400).json({ error: "Missing npub parameter" });
    }
    
    if (!npubToHex(npub)) {
      return res.status(400).json({ error: "Invalid npub format" });
    }
    
    const result = await pool.query(
      "SELECT id, endpoint, segment, label, muted, created_at, updated_at FROM webpush_subscriptions WHERE npub = $1",
      [npub]
    );
    
    res.json({ subscriptions: result.rows });
  } catch (error) {
    console.error("List subscriptions error:", error.message);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.post("/admin/poll-now", async (req, res) => {
  try {
    await pollNostrOnce();
    await tickNotifications();
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Test endpoint to send confirmation notifications to all subscribers
app.post("/admin/test-confirmation", async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT * FROM webpush_subscriptions WHERE muted = FALSE LIMIT 10"
    );
    
    let successCount = 0;
    for (const subscription of result.rows) {
      try {
        const success = await sendPushNotification(
          subscription.endpoint,
          subscription.p256dh,
          subscription.auth,
          "Blobbi Test",
          "Test confirmation notification - your push notifications are working!",
          { type: "test", timestamp: Date.now() }
        );
        if (success) successCount++;
      } catch (error) {
        console.error(`Failed to send test notification to ${subscription.endpoint.slice(-20)}:`, error.message);
      }
    }
    
    res.json({ 
      ok: true, 
      sent: successCount, 
      total: result.rows.length,
      message: `Sent test confirmations to ${successCount}/${result.rows.length} subscribers`
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Manual confirmation test endpoint
app.post("/admin/send-confirmation", async (req, res) => {
  try {
    const { endpoint } = req.body;
    
    if (!endpoint) {
      return res.status(400).json({ error: "Missing endpoint parameter" });
    }
    
    const result = await pool.query(
      "SELECT * FROM webpush_subscriptions WHERE endpoint = $1",
      [endpoint]
    );
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Subscription not found" });
    }
    
    const subscription = result.rows[0];
    const success = await sendPushNotification(
      subscription.endpoint,
      subscription.p256dh,
      subscription.auth,
      "Blobbi Notifications",
      "Notifications enabled! You'll receive alerts when your Blobbies need care.",
      { 
        type: "confirmation",
        tag: "blobbi-confirmation",
        renotify: false,
        timestamp: Date.now()
      }
    );
    
    res.json({ 
      ok: success,
      message: success ? "Confirmation sent successfully" : "Failed to send confirmation"
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Start server
app.listen(PORT, async () => {
  console.log(`🚀 Blobbi Push Backend running on port ${PORT}`);
  console.log(`📊 Monitor interval: ${MONITOR_INTERVAL_MS}ms (${MONITOR_INTERVAL_MS/1000/60} min)`);

  // Initialize active authors (npubs) and start live streaming
  await updateActiveSubscriptions();
  startNostrMonitoring(); // real-time streaming

  // Update npubs list periodically
  setInterval(updateActiveSubscriptions, 5 * 60 * 1000); // every 5 min

  // Periodic polling to ensure we don't miss events
  try {
    await pollNostrOnce(); // first immediate round
  } catch (e) {
    console.error("❌ [poll] first run error:", e.message);
  }
  setInterval(pollNostrOnce, MONITOR_INTERVAL_MS); // then periodic

  // Periodic notification checks
  try {
    await tickNotifications(); // first immediate check
  } catch (e) {
    console.error("❌ [tick] first run error:", e.message);
  }
  setInterval(tickNotifications, MONITOR_INTERVAL_MS); // then periodic
});

// Graceful shutdown
process.on("SIGTERM", () => {
  console.log("👋 Shutting down gracefully...");
  nostrPool.close(NOSTR_RELAYS);
  pool.end();
  process.exit(0);
});

async function pollNostrOnce() {
  try {
    const relays = NOSTR_RELAYS;
    // active authors (filter muted=false)
    const authors = await loadActiveAuthorsHex();

    if (authors.length === 0) {
      console.log("🔍 [poll] No authors to monitor yet");
      return;
    }

    // global since: earliest lastSeen per author; fallback 0 (gets history on first boot)
    const fallbackSince = 0; // or use ENV: Number(process.env.HISTORY_WINDOW ?? 0);
    let globalSince = Infinity;
    for (const a of authors) {
      const seen = lastSeenByPubkey.get(a);
      if (typeof seen === "number") globalSince = Math.min(globalSince, seen);
    }
    if (!Number.isFinite(globalSince)) globalSince = fallbackSince;

    console.log(`🔍 [poll] Fetching kind 31124 for ${authors.length} authors since ${new Date(globalSince * 1000).toISOString()}`);

    const filters = [{ kinds: [31124], authors, since: globalSince }];
    const events = await fetchEventsOnce(relays, filters, 4000);

    if (events.length === 0) {
      console.log("🔍 [poll] No new events found");
      return;
    }

    // dedup by id + get only the NEWEST per author+blobbi
    processedEventIds.clear();
    const latestByAuthor = new Map(); // hexPubkey -> Array<event> (all newest blobbies)

    // first keep the newest per (author, blobbiId)
    const newestPerBlobbi = new Map(); // key `${pubkey}:${blobbiId}` -> event

    // local helper to find blobbi id
    const getBlobbiId = (ev) => {
      // reuse your function:
      const { id } = extractBlobbiIdAndName(ev);
      return id || "default";
    };

    for (const ev of events) {
      if (processedEventIds.has(ev.id)) continue;
      processedEventIds.add(ev.id);

      const author = ev.pubkey;
      const key = `${author}:${getBlobbiId(ev)}`;
      const prev = newestPerBlobbi.get(key);
      if (!prev || ev.created_at > prev.created_at) {
        newestPerBlobbi.set(key, ev);
      }
    }

    // group by author
    for (const [key, ev] of newestPerBlobbi) {
      const [author] = key.split(":");
      const arr = latestByAuthor.get(author) || [];
      arr.push(ev);
      latestByAuthor.set(author, arr);
    }

    console.log(`🔍 [poll] Found ${newestPerBlobbi.size} unique blobbi events`);

    // process and send
    let processedAuthors = 0;
    for (const [pubkeyHex, evs] of latestByAuthor) {
      try {
        const npub = nip19.npubEncode(pubkeyHex);

        // use the existing new pipeline:
        const statusesByNpub = collectStatusesByBlobbi(evs); // Map(npub -> Map(blobbiId -> {name,statuses}))
        const blobbies = statusesByNpub.get(npub);
        if (!blobbies || blobbies.size === 0) continue;

        // simple count logging
        let countBelow60 = 0, countBelow30 = 0;
        for (const [, { statuses }] of blobbies) {
          const { health, energy, hygiene, happiness, hunger, awake } = statuses;
          const vals = [];
          if (health !== undefined) vals.push(health);
          if (energy !== undefined && awake !== false) vals.push(energy);
          if (hygiene !== undefined) vals.push(hygiene);
          if (happiness !== undefined) vals.push(happiness);
          if (hunger !== undefined) vals.push(hunger);
          if (vals.some(v => v < 60)) countBelow60++;
          if (vals.some(v => v < 30)) countBelow30++;
        }
        if (countBelow60 > 0 || countBelow30 > 0) {
          console.log(`📊 [poll] ${npub}: ${blobbies.size} blobbies | ${countBelow60} <60 | ${countBelow30} <30`);
        }

        // Update cache instead of sending notifications
        latestByNpub.set(npub, {
          blobbies,
          lastEvaluatedAt: Date.now()
        });
        
        console.log(`🔄 [poll] Updated cache for ${npub} with ${blobbies.size} blobbies`);

        // update lastSeen for author with the highest created_at among processed
        const maxCreated = evs.reduce((m, x) => Math.max(m, x.created_at || 0), 0);
        if (maxCreated) lastSeenByPubkey.set(pubkeyHex, maxCreated);

        processedAuthors++;
      } catch (e) {
        console.error("❌ [poll] process error:", e.message);
      }
    }

    console.log(`✅ [poll] Processed events for ${processedAuthors} author(s), ${events.length} raw event(s)`);
  } catch (err) {
    console.error("❌ [poll] error:", err.message);
  }
}