# Blobbi Push Backend

A Node.js/Express server that provides web push notifications for Blobbi status updates via Nostr monitoring.

## Quick Start

1. Install dependencies: `npm install`
2. Configure your environment variables (`.env`)
3. Generate VAPID keys: `npx web-push generate-vapid-keys`
4. Start server: `npm start`

## Features

- Web push subscription management with npub support
- Instant confirmation notifications upon successful subscription
- Real-time Nostr monitoring for kind 31124 events (Blobbi status)
- Periodic re-notification system for persistent low Blobbi status
- Smart escalation detection (care → serious care) with immediate alerts
- Rate limiting and endpoint-level deduplication
- Automatic reconnection to Nostr relays with exponential backoff