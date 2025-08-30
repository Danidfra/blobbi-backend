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
- **Time-based stat decay system** with last_interaction support
- **Sleeping Blobbi support** with is_sleeping tag and energy regeneration
- Periodic re-notification system for persistent low Blobbi status
- Smart escalation detection (care → serious care) with immediate alerts
- Rate limiting and endpoint-level deduplication
- Automatic reconnection to Nostr relays with exponential backoff

## Stat Decay System

The backend now computes real-time Blobbi stats by applying time-based decay from the last interaction timestamp:

### Decay Rates (per hour)
- **Baby**: Hunger -5, Happiness -3, Energy -6 (awake) or +4 (sleeping), Hygiene -4
- **Adult**: Hunger -4, Happiness -3, Energy -5 (awake) or +4 (sleeping), Hygiene -4
- **Health**: Baseline -1, with additional penalties for critical stats (-1.5 to -1.0)
- **Health Regeneration**: +2/h when all stats ≥ 80 (instead of baseline/penalties)

### Sleep Support
- Parse `is_sleeping` tag or content (boolean, "1"/"0", "true"/"false", "yes"/"no")
- When `is_sleeping=true`, forces `awake=false` for energy regeneration
- Energy regenerates at +4/h when sleeping, decays when awake
- Sleeping Blobbis don't trigger "low energy" notifications

### Required Event Fields
- `last_interaction`: UNIX timestamp (seconds) for decay calculation
- Base stats: `health`, `energy`, `hygiene`, `happiness`, `hunger`
- State: `awake` (boolean), `stage` (baby|adult|egg), `is_sleeping` (optional)

### Notification Logic
Notifications are based on **real stats** (after decay), not raw values:
- **<60 threshold**: "needs care" notifications
- **<30 threshold**: "needs serious care" notifications  
- Energy only counts when `awake !== false` (sleeping Blobbis exempt)