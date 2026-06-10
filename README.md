# Orpheus

Shared music, synchronized.

A real-time listening platform where multiple users can join a session, share a queue, and stay synchronized while listening together.

## Start

```bash
cargo run
```

Server runs on:

```text
http://localhost:4000
```

## REST API

### Health Check

```http
GET /health
```

### Create Session

```http
POST /sessions
Content-Type: application/json

{
  "id": "jazz-room"
}
```

### Get Session

```http
GET /sessions/jazz-room
```

Returns:

```json
{
  "users": {},
  "queue": [],
  "current_song_index": 0,
  "position_ms": 0,
  "playing": false
}
```

## WebSocket

Connect:

```text
ws://localhost:4000/ws/sessions/jazz-room
```

### Join Session
User is supposed to send it at start  of joining session

```json
{
  "type": "join",
  "username": "alice",
  "client_id": "1"
}
```

### User Joined
Other users recieves it 

```json
{
  "type": "UserJoined",
  "username": "alice",
  "client_id": "1"
}
```

### User Left
It automatically fires up when user disconnects

```json
{
  "type": "UserLeft",
  "username": "alice",
  "client_id": "1"
}
```

### Add Songs To Queue
Frontend/user sends it

```json
{
  "type": "AddInQueue",
  "songs": [...],
  "client_id": "1"
}
```

### Queue Update
server Fires it,when new song are added by other user, so other users are aware of updated queue

```json
{
  "type": "UpdateQueue",
  "songs": [...],
  "client_id": "1"
}
```

### Play

```json
{
  "type": "PlaybackCmds",
  "command": "Play",
  "client_id": "1"
}
```

### Pause

```json
{
  "type": "PlaybackCmds",
  "command": "Pause",
  "client_id": "1"
}
```

### Next Track

```json
{
  "type": "PlaybackCmds",
  "command": "Next",
  "client_id": "1"
}
```

### Previous Track

```json
{
  "type": "PlaybackCmds",
  "command": "Prev",
  "client_id": "1"
}
```

### Playback Sync

```json
{
  "type": "PlaybackSync",
  "position": 52341,
  "playing": true,
  "client_id": "1"
}
```

### Song Ended
Server fires it

```json
{
  "type": "SongEnded",
  "client_id": "1"
}
```

When a song ends, the server automatically:
- Advances to the next track if available.
- Stops playback if the queue has ended.

## Tech Stack

- Rust
- Axum
- Tokio
- WebSockets
- Serde
- a lot of pain
