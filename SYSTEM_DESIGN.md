# SYSTEM_DESIGN — Discord-Style Real-Time Communication Platform

## 1. Overview

A real-time community platform for creator and fan communities: text chat, voice/video rooms (~50 participants), invite-link-based community membership, username-based friend connections, email notifications, and moderator tooling (per-channel roles/permissions, timeouts, kicks, bans, audit log).

**Architecture style:** six domain-oriented backend services, all **TypeScript**, each packaged as a **standard container image** and deployed as **stateless replicas** (`replicas >= 1`) exposing `GET /healthz` and `GET /readyz` and draining connections gracefully on `SIGTERM`/`SIGINT`.

**Portable infrastructure:**
- **PostgreSQL**: one logical database per stateful service (`identity_service`, `community_service`, `chat_service`, `notification_service`). No cross-service table sharing.
- **Kafka**: asynchronous domain events and real-time fan-out to WebSocket gateway pods.
- **S3-compatible object storage**: *not required for the MVP* (no file attachments; avatars are served from the identity provider's image URLs).

**External providers:**
| Capability | Ownership | Provider | Owning service |
|---|---|---|---|
| Authentication (email/password, Google, Apple, Discord) | External | **Clerk** | All services verify tokens; `identity_service` mirrors users |
| Transactional email | External | **Resend** | `notification_service` |
| Voice/video media transport (WebRTC SFU) | External | **LiveKit Cloud** | `voice_service` |
| Real-time text chat | In-house | — | `chat_service` + `realtime_gateway` |
| Communities, roles, moderation, audit log | In-house | — | `community_service` |

### 1.1 Resolved product decisions (MVP)
1. **Voice/video media** uses an external real-time media provider (**LiveKit Cloud**). Running an in-house SFU means stateful media sessions and UDP port ranges, which does not fit the stateless-pod model. The backend stays in control of *who* may join and *with which capabilities* by issuing short-lived room access tokens.
2. **Text chat** is built in-house: messages are stored in PostgreSQL, delivered live over WebSockets, and fanned out across gateway pods through Kafka.
3. **Friends** are for finding each other across communities only. There are no DMs or direct calls in the MVP (DMs can later be modeled as a private channel type).
4. **Message reporting** is out of scope. Moderation consists of actions moderators take themselves.
5. **Usernames are owned by Clerk.** Username is required at sign-up and unique per Clerk instance. A custom session-token claim carries `username` so services can attribute actions without calling another service.
6. **Community invite emails** are sent to an email address entered by the inviter (works for non-users). Invite links remain the primary join mechanism.

---

## 2. Authentication & Trust Model

- Clients sign up and sign in through **Clerk** (email/password plus Google, Apple and Discord social connections). That is exactly 3 social connections, which fits Clerk's free tier; the paid tier is only needed if more providers are added.
- Clerk issues short-lived **session JWTs**. The Clerk session token is customized to include the claims `sub` (Clerk user id = platform `user_id`), `username`, and `email`.
- **Every service verifies session JWTs offline** using Clerk's published public key/JWKS. No per-request call to Clerk or to `identity_service` is made for authentication.
  - REST: `Authorization: Bearer <session_jwt>`.
  - WebSocket: the token is sent in the initial `identify` frame. The gateway re-validates it on `refresh_token` frames before expiry.
- **Internal endpoints** (`/internal/v1/*`) are reachable only on the cluster network (not exposed via ingress) and require a service-to-service bearer token (shared secret per caller, rotated through your platform secret store).
- **Inbound webhooks** (Clerk, Resend, LiveKit) are publicly routed and authenticated **only** by provider signature verification (Svix signatures for Clerk and Resend; signed JWT + body hash for LiveKit). Payloads that fail verification are rejected with `401`.

---

## 3. Service Catalog

| Service | Runtime | PostgreSQL | Kafka | External provider |
|---|---|---|---|---|
| `identity_service` | TypeScript | ✅ | produces | Clerk (webhooks) |
| `community_service` | TypeScript | ✅ | produces | — |
| `chat_service` | TypeScript | ✅ | produces + consumes | — |
| `realtime_gateway` | TypeScript | ❌ (stateless) | consumes | — |
| `voice_service` | TypeScript | ❌ (stateless) | consumes | LiveKit Cloud |
| `notification_service` | TypeScript | ✅ | consumes | Resend |

**Runtime rationale (all services):** the user explicitly asked for TypeScript for every service. This also fits the integrations: Clerk, Resend and LiveKit each publish an **official, actively maintained TypeScript/Node server SDK** (token verification and webhook verification for Clerk; send with idempotency keys and webhook verification for Resend; access tokens, room service API and webhook receiver for LiveKit). TypeScript's event-loop model also suits the I/O-bound, connection-heavy WebSocket gateway.

---

## 4. Services

### 4.1 `identity_service`

**Runtime:** TypeScript. Official Clerk backend SDK for token and webhook verification.

**Responsibilities**
- Keep a local **user profile** mirror of Clerk users (username, display name, avatar URL, primary email, status) from Clerk webhooks. Lazily upsert from JWT claims on first authenticated request if the webhook hasn't arrived yet.
- Exact-match **username lookup** (case-insensitive) for adding friends.
- Batch profile lookup so clients can render author names and avatars for messages and member lists.
- **Friend requests** lifecycle: send, accept, decline, cancel. **Friendships**: list, remove. Duplicate pending requests, self-requests and already-friends cases are rejected with clear error codes.
- Emit friend events for email notification and live in-app updates.

**Owned domain data & infrastructure**
- PostgreSQL: `UserProfile` (keyed by Clerk user id; username, display name, avatar URL, email, deleted flag), `FriendRequest` (sender, recipient, status `pending|accepted|declined|cancelled`, timestamps), `Friendship` (unordered user pair, created at), `ProcessedWebhook` (idempotency of Clerk deliveries).
- Kafka: produces `identity_friend_events`.

**External provider integration: Clerk**
- Inbound webhook `POST /v1/webhooks/clerk` (Svix-signed): handles `user.created`, `user.updated` and `user.deleted`. Idempotent by Svix message id. On `user.deleted` the profile is soft-deleted (username becomes unsearchable) and pending friend requests are cancelled.
- No outbound Clerk calls are needed in the request path.

**API endpoints** (public; Clerk JWT required unless noted)
| Method | Path | Input → Output | Business goal |
|---|---|---|---|
| GET | `/v1/me` | → own profile (username, display name, avatar, email) | First-run experience / session bootstrap |
| GET | `/v1/users/by-username/{username}` | → public profile `{user_id, username, display_name, avatar_url}` or `404` | Find a user by exact username |
| POST | `/v1/users:batch` | `{user_ids[≤100]}` → public profiles | Render authors/members |
| POST | `/v1/friend-requests` | `{username}` → `FriendRequest` (`201`); `404` unknown user, `409` duplicate/already friends | Send friend request |
| GET | `/v1/friend-requests?direction=incoming\|outgoing&status=pending\|accepted\|declined` | → paginated list | Show pending/accepted/declined states |
| POST | `/v1/friend-requests/{request_id}/accept` | → `Friendship` | Accept request (recipient only) |
| POST | `/v1/friend-requests/{request_id}/decline` | → `FriendRequest` | Decline request (recipient only) |
| DELETE | `/v1/friend-requests/{request_id}` | → `204` | Cancel outgoing request (sender only) |
| GET | `/v1/friends` | → paginated friend profiles | Friends list |
| DELETE | `/v1/friends/{user_id}` | → `204` | Remove friend |
| POST | `/v1/webhooks/clerk` | Clerk event (Svix-signed) → `200` | Profile sync (no JWT; signature auth) |
| GET | `/healthz`, `/readyz` | → status | Liveness / readiness (DB + Kafka producer) |

**Kafka topics produced:** `identity_friend_events` (see §5).

**Inter-service dependencies:** none synchronous. Async producer only.

**Observability**
- Business: `identity_friend_requests_total{outcome=sent|accepted|declined|cancelled}` (counter), `identity_username_lookups_total{result=found|not_found}` (counter), `identity_users_synced_total{event=created|updated|deleted}` (counter).
- Performance: `http_request_duration_seconds{route,method,status_class}` (histogram), `identity_clerk_webhook_failures_total{reason}` (counter), `kafka_produce_errors_total{topic}` (counter).
- SLOs: p95 API latency < 150 ms; 99.9% availability; Clerk webhook processing success ≥ 99.9%.

---

### 4.2 `community_service`

**Runtime:** TypeScript.

**Responsibilities**
- **Communities**: create (creator becomes owner), rename, delete (owner only).
- **Channels**: text and voice/video channels with ordering and optional categories. Create, update and delete require `MANAGE_CHANNELS`.
- **Roles & permissions**: community roles carry a permission set. The implicit `@everyone` role applies to all members, and the owner implicitly has all permissions. Permissions: `VIEW_CHANNEL`, `SEND_MESSAGES`, `READ_MESSAGE_HISTORY`, `CONNECT`, `SPEAK`, `VIDEO`, `CREATE_INVITE`, `MANAGE_CHANNELS`, `MANAGE_ROLES`, `TIMEOUT_MEMBERS`, `KICK_MEMBERS`, `BAN_MEMBERS`, `VIEW_AUDIT_LOG`, `ADMINISTRATOR`. A role hierarchy (position) prevents moderators from acting on equal or higher roles.
- **Per-channel permission overwrites** (allow/deny per role or per member), resolved Discord-style: base role permissions → channel `@everyone` overwrite → role overwrites → member overwrite.
- **Memberships**: join via invite, leave, role assignment, nickname.
- **Invites**: shareable invite links (random code, optional expiry and max uses, revocable) and **email invites** to an address (emits an event for `notification_service`). Invite preview and accept flows. Expired, revoked, exhausted or invalid codes return clear error codes plus the community name and inviter username where known, so the UI can guide the user to ask for a new link. Banned users cannot rejoin.
- **Moderation**: timeout (duration, up to 28 days), clear timeout, kick, ban (optionally with expiry), unban. Each action takes an internal `reason` and an optional **member-facing explanation** (non-punitive wording), which is delivered live to the affected member and returned when they hit a blocked action.
- **Audit log**: every moderation action and every role, permission-overwrite, channel and invite administration change is written **in the same transaction** as the change itself (actor, action, target, reason, before/after summary, timestamp). Viewable with `VIEW_AUDIT_LOG`.
- **Authoritative access decisions** for peer services through the internal access endpoints.

**Owned domain data & infrastructure**
- PostgreSQL: `Community`, `Channel` (type `text|voice`), `Role`, `MemberRole` assignment, `PermissionOverwrite` (channel × role|member, allow/deny sets), `Membership` (incl. nickname, `timed_out_until`), `Ban`, `Invite` (code, creator, expiry, max/used count, revoked flag), `EmailInvite` record, `AuditLogEntry`, `OutboxEvent` (transactional outbox for reliable event publishing).
- Kafka: produces `community_events` and `community_invite_events`.

**External provider integrations:** none.

**API endpoints** (public; Clerk JWT required; permission checked per route)
| Method | Path | Input → Output | Business goal |
|---|---|---|---|
| POST | `/v1/communities` | `{name}` → `Community` with default `#general` text and `General` voice channel and `@everyone` role | Create community |
| GET | `/v1/me/communities` | → communities the caller belongs to (with channels visible to the caller) | Sidebar bootstrap |
| GET | `/v1/communities/{community_id}` | → community, visible channels, caller's effective permissions | Load community |
| PATCH / DELETE | `/v1/communities/{community_id}` | `{name}` → `Community` / `204` (owner) | Manage community |
| POST | `/v1/communities/{community_id}/channels` | `{name, type: text\|voice, position?}` → `Channel` | Create channel |
| PATCH / DELETE | `/v1/communities/{community_id}/channels/{channel_id}` | `{name?, position?}` → `Channel` / `204` | Manage channel |
| GET / POST | `/v1/communities/{community_id}/roles` | `{name, permissions[], position}` → `Role` | Manage roles |
| PATCH / DELETE | `/v1/communities/{community_id}/roles/{role_id}` | → `Role` / `204` | Manage roles |
| PUT / DELETE | `/v1/communities/{community_id}/channels/{channel_id}/permission-overwrites/{target_type}/{target_id}` | `{allow[], deny[]}` → `PermissionOverwrite` / `204` | Per-channel permissions |
| GET | `/v1/communities/{community_id}/members?cursor=` | → paginated memberships (user_id, roles, nickname, timeout state) | Member list |
| PUT / DELETE | `/v1/communities/{community_id}/members/{user_id}/roles/{role_id}` | → `204` | Assign/remove role |
| DELETE | `/v1/communities/{community_id}/members/me` | → `204` | Leave community |
| POST | `/v1/communities/{community_id}/invites` | `{expires_in_seconds?, max_uses?}` → `Invite {code, url, expires_at}` | Generate invite link |
| GET | `/v1/communities/{community_id}/invites` | → active invites | Manage invites |
| DELETE | `/v1/communities/{community_id}/invites/{code}` | → `204` | Revoke invite |
| POST | `/v1/communities/{community_id}/email-invites` | `{email, expires_in_seconds?}` → `202` (invite created, email queued) | Email invite |
| GET | `/v1/invites/{code}` | → preview `{community_name, member_count, inviter_username, expires_at}`; `410 invite_expired\|invite_exhausted\|invite_revoked`, `404 invite_invalid` | Invite landing page |
| POST | `/v1/invites/{code}/accept` | → `Membership` (idempotent if already a member); `403 banned` with explanation | Join via invite |
| POST | `/v1/communities/{community_id}/moderation/timeouts` | `{user_id, duration_seconds, reason, member_message?}` → `Membership` | Time out member |
| DELETE | `/v1/communities/{community_id}/moderation/timeouts/{user_id}` | `{reason?}` → `204` | Clear timeout |
| POST | `/v1/communities/{community_id}/moderation/kicks` | `{user_id, reason, member_message?}` → `204` | Kick member |
| POST | `/v1/communities/{community_id}/moderation/bans` | `{user_id, reason, member_message?, expires_at?}` → `Ban` | Ban member |
| GET | `/v1/communities/{community_id}/moderation/bans` | → paginated bans | Review bans |
| DELETE | `/v1/communities/{community_id}/moderation/bans/{user_id}` | `{reason?}` → `204` | Unban |
| GET | `/v1/communities/{community_id}/audit-log?action=&actor_id=&target_id=&cursor=` | → paginated `AuditLogEntry` (actor, action, target, reason, created_at) | Moderator accountability |
| GET | `/internal/v1/channels/{channel_id}/access/{user_id}` | → `{community_id, channel_type, is_member, can_view, can_read_history, can_send, can_connect, can_speak, can_video, timed_out_until, banned}` | Authoritative per-channel access for chat/voice |
| GET | `/internal/v1/communities/{community_id}/members/{user_id}/access` | → `{is_member, banned, visible_channel_ids[], timed_out_until}` | Gateway subscription authorization |
| GET | `/healthz`, `/readyz` | → status | Probes (DB + Kafka producer) |

**Kafka topics produced:** `community_events`, `community_invite_events` (see §5). Published through a transactional outbox so state changes and events can never diverge.

**Inter-service dependencies:** none outbound. It is the synchronous dependency of `chat_service`, `voice_service` and `realtime_gateway` (internal access endpoints).

**Observability**
- Business: `community_created_total` (counter), `community_invites_created_total{kind=link|email}` (counter), `community_invite_accepts_total{result=joined|expired|invalid|banned}` (counter), `community_members_joined_total` (counter), `community_moderation_actions_total{action=timeout|timeout_clear|kick|ban|unban}` (counter), `community_audit_log_views_total` (counter).
- Performance: `http_request_duration_seconds{route,method,status_class}` (histogram), `community_access_check_duration_seconds{endpoint}` (histogram), `community_outbox_lag_seconds` (gauge), `kafka_produce_errors_total{topic}` (counter).
- SLOs: internal access check p95 < 25 ms and 99.95% availability (on the hot path of chat and voice); public API p95 < 200 ms; outbox publish lag p95 < 2 s.

---

### 4.3 `chat_service`

**Runtime:** TypeScript.

**Responsibilities**
- Persist and serve **text channel messages**: send, author-only edit, author-only delete, and cursor-paginated history (newest-first, `before`/`after` cursors for reconnect backfill).
- **Idempotent sends**: clients attach a `client_nonce`; retries of a failed send return the original message instead of duplicating it (supports the "retry failed message" UX).
- Enforce access through `community_service`'s internal access endpoint on every send (`can_send`, not timed out, not banned) and every history read (`can_view` + `can_read_history`). Timed-out members get `403 member_timed_out` with `timed_out_until`.
- Message ids are time-ordered so they double as pagination cursors.
- Publish message events for live delivery.
- Delete message history when a channel or community is deleted (consumed from `community_events`).
- Content validation: text only, max 4,000 characters, no attachments in the MVP.

**Owned domain data & infrastructure**
- PostgreSQL: `Message` (id, channel id, community id, author id, content, client nonce, created/edited/deleted timestamps), `ProcessedEvent` (consumer idempotency), `OutboxEvent`.
- Kafka: produces `chat_message_events`; consumes `community_events` (consumer group `chat_service`).

**External provider integrations:** none.

**API endpoints** (public; Clerk JWT required)
| Method | Path | Input → Output | Business goal |
|---|---|---|---|
| GET | `/v1/channels/{channel_id}/messages?before=&after=&limit≤100` | → `{messages[], next_cursor}` | Load history / backfill after reconnect |
| POST | `/v1/channels/{channel_id}/messages` | `{content, client_nonce}` → `Message` (`201`, or `200` on nonce replay); `403 member_timed_out\|missing_permission` | Send message in real time |
| PATCH | `/v1/channels/{channel_id}/messages/{message_id}` | `{content}` → `Message` (author only) | Edit own message |
| DELETE | `/v1/channels/{channel_id}/messages/{message_id}` | → `204` (author only) | Delete own message |
| GET | `/healthz`, `/readyz` | → status | Probes (DB + Kafka) |

**Kafka topics:** produces `chat_message_events`; consumes `community_events` (`channel_deleted`, `community_deleted`).

**Inter-service dependencies**
- Sync REST → `community_service` `GET /internal/v1/channels/{channel_id}/access/{user_id}`. This is a genuine real-time authorization dependency. Short timeout (300 ms) with one retry; fails closed (`503`) if unavailable.
- Async → Kafka for fan-out and deletion handling.

**Observability**
- Business: `chat_messages_sent_total{channel_type=text}` (counter), `chat_messages_edited_total` (counter), `chat_messages_deleted_total` (counter), `chat_send_rejections_total{reason=timed_out|missing_permission|validation}` (counter), `chat_send_retries_deduplicated_total` (counter).
- Performance: `http_request_duration_seconds{route,method,status_class}` (histogram), `chat_access_check_duration_seconds` (histogram), `kafka_consumer_lag{topic,group}` (gauge), `kafka_produce_errors_total{topic}` (counter).
- SLOs: send p95 < 150 ms (incl. access check and persistence); history read p95 < 200 ms; 99.9% availability.

---

### 4.4 `realtime_gateway`

**Runtime:** TypeScript (event-loop model suits many concurrent long-lived WebSocket connections).

**Responsibilities**
- Terminate client **WebSocket** connections at `GET /v1/gateway`. Protocol frames:
  - client → server: `identify {token}`, `heartbeat`, `subscribe_community {community_id}`, `unsubscribe_community`, `refresh_token`.
  - server → client: `ready`, `heartbeat_ack`, `event {type, data}`, `error`, `reconnect`.
- Authenticate by verifying the Clerk JWT offline. Close the connection on expiry unless refreshed.
- Authorize subscriptions by calling `community_service`'s member-access endpoint and keep, for each connection, the set of channel ids the user may see. Re-fetch access when `community_events` indicate a role, overwrite, membership or channel change for that community.
- **Cross-pod fan-out:** every gateway pod consumes the event topics with a **pod-unique consumer group** starting at the latest offset, so every pod sees every event. Each pod delivers an event only to its locally connected, authorized sessions:
  - `chat_message_events` → sessions subscribed to the community with visibility of the channel.
  - `community_events` → community subscribers (channel/member changes). Moderation events also push a `moderation_notice` (with the member-facing explanation) to the affected user's sessions and force-unsubscribe them on kick or ban.
  - `identity_friend_events` → the sessions of the recipient/sender user (live friend request status).
- **Stateless:** connection registries are ephemeral per pod. On disconnect, clients reconnect to any pod, re-identify, re-subscribe, and backfill missed messages through `chat_service` history (`after=<last_seen_message_id>`), which drives the "connection lost / reconnecting" UX. On `SIGTERM` the pod sends `reconnect` frames and drains.

**Owned domain data & infrastructure**
- No PostgreSQL (owns no persistent state).
- Kafka: consumes `chat_message_events`, `community_events`, `identity_friend_events` (pod-unique consumer groups, broadcast semantics).

**External provider integrations:** none.

**API endpoints**
| Method | Path | Input → Output | Business goal |
|---|---|---|---|
| GET | `/v1/gateway` | WebSocket upgrade; frames as above | Real-time delivery of messages, community changes, friend and moderation notices |
| GET | `/healthz`, `/readyz` | → status (readiness requires Kafka consumers assigned) | Probes |

**Kafka topics consumed:** `chat_message_events`, `community_events`, `identity_friend_events`.

**Inter-service dependencies**
- Sync REST → `community_service` `GET /internal/v1/communities/{community_id}/members/{user_id}/access` on subscribe and on access-changing events. This is a real-time authorization dependency.
- Async ← Kafka for all pushed events.

**Observability**
- Business: `gateway_connections_active` (gauge), `gateway_sessions_identified_total{result=ok|invalid_token}` (counter), `gateway_daily_active_users_signal_total` (counter, incremented on each user's first identify per UTC day per pod; aggregated downstream for daily-return tracking), `gateway_events_delivered_total{event_type}` (counter).
- Performance: `gateway_fanout_latency_seconds{source_topic}` (histogram, from event produce time to socket write), `gateway_reconnects_total{reason}` (counter), `kafka_consumer_lag{topic}` (gauge), `gateway_access_fetch_duration_seconds` (histogram).
- SLOs: end-to-end message delivery (send accepted → recipient socket) p95 < 500 ms; connection success ≥ 99.5%; consumer lag p95 < 1 s.

---

### 4.5 `voice_service`

**Runtime:** TypeScript. Official LiveKit Node server SDK for access tokens, room service API and webhook verification.

**Responsibilities**
- Issue **short-lived LiveKit room access tokens** (TTL ~10 min to join; session continues after join) for voice/video channels. The room name is derived from the channel id and the participant identity is the platform `user_id`.
- Map `community_service` access to LiveKit grants:
  - `can_connect` → room join allowed;
  - `can_speak` → microphone publishing;
  - `can_video` → camera/screen publishing;
  - always subscribe allowed when connected.
  - Timed-out members get a **listen-only** token (no publish). Banned members or non-members get `403`.
- Room capacity: deny token issuance with `409 room_full` when the room already has ~50 participants (checked via LiveKit participant listing). Mute and camera toggles are client-side over LiveKit.
- List current participants of a voice channel.
- **Enforce moderation live** by consuming `community_events`:
  - `member_kicked` / `member_banned` → remove the participant from all of the community's voice rooms listed in the event payload.
  - `member_timed_out` → revoke publish permissions.
  - `timeout_cleared` → restore publish permissions per the current access check.
  - `channel_deleted` → remove all participants from that room.
  - `permission_overwrites_changed` / `member_roles_changed` → re-check access for affected participants in active rooms.
- Receive **LiveKit webhooks** (`participant_joined`, `participant_left`, `room_finished`) to drive session and participation metrics.

**Owned domain data & infrastructure**
- No PostgreSQL. Live room state is owned by LiveKit and queried on demand; the service is fully stateless.
- Kafka: consumes `community_events` (consumer group `voice_service`).

**External provider integration: LiveKit Cloud**
- Outbound (server API, authenticated with API key/secret): list participants, remove participant, update participant permissions.
- Token minting is done locally with the API secret (no network call).
- Inbound webhook `POST /v1/webhooks/livekit`: verified via the signed authorization header (JWT + body SHA-256).
- Client media flows directly between clients and LiveKit Cloud. The backend never relays media.

**API endpoints** (public; Clerk JWT required unless noted)
| Method | Path | Input → Output | Business goal |
|---|---|---|---|
| POST | `/v1/voice/channels/{channel_id}/token` | → `{server_url, token, room, expires_at, grants: {can_speak, can_video}}`; `403 missing_permission\|banned`, `409 room_full` | Join voice/video room |
| GET | `/v1/voice/channels/{channel_id}/participants` | → `[{user_id, is_muted, has_video, joined_at}]` (requires `can_view`) | Show who is in the room |
| POST | `/v1/webhooks/livekit` | LiveKit event (signed) → `200` | Session metrics (signature auth) |
| GET | `/healthz`, `/readyz` | → status | Probes (Kafka consumer assigned) |

**Kafka topics consumed:** `community_events`.

**Inter-service dependencies**
- Sync REST → `community_service` `GET /internal/v1/channels/{channel_id}/access/{user_id}` before issuing a token or re-evaluating a participant. This is a real-time authorization dependency.
- Async ← Kafka `community_events` for live moderation enforcement.

**Observability**
- Business: `voice_tokens_issued_total{grant=full|listen_only}` (counter), `voice_participants_joined_total` (counter, from webhooks), `voice_sessions_minutes_total` (counter), `voice_room_full_rejections_total` (counter), `voice_moderation_evictions_total{action=kick|ban|timeout|channel_deleted|permission_revoked}` (counter).
- Performance: `http_request_duration_seconds{route,method,status_class}` (histogram), `voice_livekit_api_duration_seconds{operation}` (histogram), `voice_livekit_api_errors_total{operation}` (counter), `kafka_consumer_lag{topic,group}` (gauge).
- SLOs: token issuance p95 < 200 ms; moderation eviction applied within 5 s of the action (p95); 99.9% availability.

---

### 4.6 `notification_service`

**Runtime:** TypeScript. Official Resend Node SDK for sending (with idempotency keys) and webhook verification.

**Responsibilities**
- Consume `identity_friend_events` (`friend_request_sent`) and `community_invite_events` (`invite_email_requested`). Render the transactional email templates (friend request received, community invitation with invite link) and send them through **Resend**.
- **Exactly-once-effect delivery**: the Kafka event id is used both as the consumer dedupe key and as the Resend idempotency key, so redeliveries never send duplicate emails.
- Track each email delivery record and update its status from **Resend webhooks** (`email.sent`, `email.delivered`, `email.bounced`, `email.complained`). Suppress future sends to addresses that hard-bounced or complained.
- **Quota awareness**: track daily and monthly send counts against the configured Resend plan limits (free tier: 100/day, 3,000/month). When the daily cap is reached, defer remaining emails to the next UTC day (retry with backoff) instead of dropping them, and raise an alert metric.
- Retry transient Resend failures with exponential backoff. Events that keep failing go to a dead-letter status in the delivery record (visible in metrics).

**Owned domain data & infrastructure**
- PostgreSQL: `EmailDelivery` (source event id, kind `friend_request|community_invite`, recipient email, provider message id, status `queued|sent|delivered|bounced|complained|deferred|failed`, attempts, timestamps), `EmailSuppression` (address, reason), `DailySendCounter`.
- Kafka: consumes `identity_friend_events` and `community_invite_events` (consumer group `notification_service`).

**External provider integration: Resend**
- Outbound: send email API (API key auth, idempotency key per event).
- Inbound webhook `POST /v1/webhooks/resend` (Svix-signed): delivery status updates, idempotent by Svix message id.

**API endpoints**
| Method | Path | Input → Output | Business goal |
|---|---|---|---|
| POST | `/v1/webhooks/resend` | Resend event (Svix-signed) → `200` | Delivery tracking (signature auth) |
| GET | `/healthz`, `/readyz` | → status (DB + Kafka consumer assigned) | Probes |

**Kafka topics consumed:** `identity_friend_events`, `community_invite_events`.

**Inter-service dependencies:** none synchronous. Event payloads carry everything needed to render the email (recipient email, usernames, community name, invite URL).

**Observability**
- Business: `notification_emails_sent_total{kind}` (counter), `notification_emails_delivered_total{kind}` (counter), `notification_emails_bounced_total{kind,type=hard|soft}` (counter), `notification_emails_complained_total{kind}` (counter), `notification_emails_suppressed_total{reason}` (counter).
- Performance: `notification_resend_api_duration_seconds` (histogram), `notification_resend_api_errors_total{status_class}` (counter), `notification_daily_quota_used_ratio` (gauge, alert ≥ 0.8), `notification_emails_deferred_total{reason=quota|transient}` (counter), `kafka_consumer_lag{topic,group}` (gauge).
- SLOs: event-to-send p95 < 30 s (when under quota); delivery success ≥ 98%; zero duplicate sends.

---

## 5. Kafka Topics & Event Contracts

All events share an envelope:
`{event_id (uuid), event_type, occurred_at (ISO-8601), producer, version, trace_parent, data}`.
- Partition keys are chosen to keep per-entity ordering.
- Consumers are idempotent by `event_id`.
- Producers with database state publish through a transactional outbox.

| Topic | Producer | Consumers | Partition key | Event types & `data` contract |
|---|---|---|---|---|
| `identity_friend_events` | `identity_service` | `notification_service`, `realtime_gateway` | `recipient_user_id` | `friend_request_sent {request_id, sender_user_id, sender_username, recipient_user_id, recipient_username, recipient_email}`; `friend_request_accepted` / `friend_request_declined` / `friend_request_cancelled {request_id, sender_user_id, recipient_user_id}`; `friendship_removed {user_id_a, user_id_b}` |
| `community_invite_events` | `community_service` | `notification_service` | `community_id` | `invite_email_requested {invite_code, invite_url, community_id, community_name, inviter_user_id, inviter_username, recipient_email, expires_at}` |
| `community_events` | `community_service` | `realtime_gateway`, `voice_service`, `chat_service` | `community_id` | `community_updated {community_id, name}`; `community_deleted {community_id, channel_ids[]}`; `channel_created` / `channel_updated {community_id, channel_id, name, type, position}`; `channel_deleted {community_id, channel_id, type}`; `member_joined` / `member_left {community_id, user_id}`; `member_roles_changed {community_id, user_id, role_ids[]}`; `roles_changed {community_id, role_id}`; `permission_overwrites_changed {community_id, channel_id}`; `member_timed_out {community_id, user_id, moderator_user_id, timed_out_until, member_message, voice_channel_ids[]}`; `member_timeout_cleared {community_id, user_id, voice_channel_ids[]}`; `member_kicked` / `member_banned {community_id, user_id, moderator_user_id, member_message, voice_channel_ids[], ban_expires_at?}`; `member_unbanned {community_id, user_id}` |
| `chat_message_events` | `chat_service` | `realtime_gateway` | `channel_id` | `message_created {message_id, channel_id, community_id, author_user_id, author_username, content, client_nonce, created_at}`; `message_edited {message_id, channel_id, community_id, content, edited_at}`; `message_deleted {message_id, channel_id, community_id}` |

No orphan topics: every topic above has at least one consumer.

---

## 6. Communication Map

**Synchronous REST (internal, real-time authorization only):**
- `chat_service` → `community_service`: `GET /internal/v1/channels/{channel_id}/access/{user_id}`
- `voice_service` → `community_service`: `GET /internal/v1/channels/{channel_id}/access/{user_id}`
- `realtime_gateway` → `community_service`: `GET /internal/v1/communities/{community_id}/members/{user_id}/access`

**Asynchronous Kafka:**
- `identity_service` → `identity_friend_events` → `notification_service` (email), `realtime_gateway` (live update)
- `community_service` → `community_invite_events` → `notification_service` (email)
- `community_service` → `community_events` → `realtime_gateway` (live UI + access refresh), `voice_service` (moderation enforcement), `chat_service` (history purge)
- `chat_service` → `chat_message_events` → `realtime_gateway` (live message fan-out)

**External:**
- Clients ↔ Clerk (sign-up/in UI and session tokens); Clerk → `identity_service` (webhooks)
- Clients ↔ LiveKit Cloud (WebRTC media); `voice_service` ↔ LiveKit (server API + webhooks)
- `notification_service` → Resend (send); Resend → `notification_service` (webhooks)

### 6.1 Key flows
1. **Send message:** client `POST /v1/channels/{id}/messages` → `chat_service` checks access with `community_service` → persists and writes to outbox → `chat_message_events` → every `realtime_gateway` pod → delivered to authorized subscribed sessions.
2. **Join via invite:** `GET /v1/invites/{code}` preview → sign in through Clerk if needed → `POST /v1/invites/{code}/accept` → membership created → `member_joined` on `community_events` → gateways update live member lists.
3. **Join voice room:** `POST /v1/voice/channels/{id}/token` → access check → LiveKit token with grants → client connects to LiveKit Cloud directly. On a dropped connection the client rejoins and requests a fresh token if the old one has expired.
4. **Ban member:** moderator `POST .../moderation/bans` → membership removed, ban and audit entry written in one transaction → `member_banned` → gateway pushes `moderation_notice` and unsubscribes the member; voice_service removes them from active rooms; future access checks deny.
5. **Friend request:** `POST /v1/friend-requests {username}` → `friend_request_sent` → notification_service emails the recipient through Resend; the gateway shows it live if the recipient is online.

---

## 7. Cross-Cutting Concerns

- **Tracing:** W3C `traceparent` propagated on all HTTP calls and in Kafka message headers (`trace_parent` in the envelope); spans for external provider calls.
- **Error contract:** JSON `{error: {code, message, details?}}` with stable machine-readable codes (e.g. `invite_expired`, `member_timed_out`, `room_full`, `username_not_found`) so the UI can show clear, non-punitive messages.
- **Rate limiting:** per-user limits on message sends, friend requests and invite creation, enforced in each owning service without shared state (per-pod token buckets, sized to the replica count).
- **Pagination:** cursor-based on all list endpoints.
- **Idempotency:** `client_nonce` for message sends; `event_id` dedupe for all consumers; provider message ids for webhooks.
- **Metric labels:** low-cardinality only. Never `user_id`, `community_id`, `channel_id`, `request_id` or timestamps.
- **Product success metric (daily return):** derived from `gateway_daily_active_users_signal_total` and `chat_messages_sent_total` / `voice_participants_joined_total` trends during the pilot.

## 8. Known Risks & Follow-ups
- **Clerk tier:** Google + Apple + Discord = 3 social connections, which fits the free plan. Adding a 4th requires the paid plan.
- **Resend quota:** the free tier's 100/day cap could defer invite or friend emails during growth spikes. Monitored through `notification_daily_quota_used_ratio`. Upgrade path: $20/month for 50k emails.
- **LiveKit Cloud plan limits:** concurrent participant quotas must be sized from pilot data. Rooms are capped at ~50 by the backend.
- **Gateway broadcast fan-out:** every pod consumes every event. Fine at MVP scale; evolve to partition-aware routing if event volume grows.
- **Open product questions deferred:** friend DMs/direct calls and message reporting are explicitly excluded from this MVP design.
