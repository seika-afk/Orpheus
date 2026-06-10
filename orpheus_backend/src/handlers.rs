use ax_extract_ws::{WebSocket, WebSocketUpgrade};
use axum::extract::ws as ax_extract_ws;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{
    Json,
    extract::{Path, State},
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::broadcast;

use crate::{AppState, PlaybackCmd, Session, SyncMessage, User};

#[derive(Serialize)]
pub struct HealthResponse {
    status: &'static str,
}

#[derive(Serialize)]
pub struct SessionResponse {
    pub users: HashMap<String, User>,
}

#[derive(Deserialize)]
pub struct CreateSessionRequest {
    pub id: String,
}

#[derive(Serialize)]
pub struct CreateSessionResponse {
    pub success: bool,
}

#[derive(Deserialize, Serialize)]
pub struct JsonMessage {
    #[serde(rename = "type")]
    pub kind: String,
    pub username: String,
    pub client_id: String,
}

pub async fn root() -> &'static str {
    " Orpheus : Shared Music, Synchronized"
}

pub async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

pub async fn get_session(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<SessionResponse>, StatusCode> {
    let sessions = state.sessions.read().await;

    let session = sessions.get(&id).ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(SessionResponse {
        users: session.users.clone(),
    }))
}

pub async fn create_session(
    State(state): State<AppState>,
    Json(payload): Json<CreateSessionRequest>,
) -> (StatusCode, Json<CreateSessionResponse>) {
    let mut session = state.sessions.write().await;
    if session.contains_key(&payload.id) {
        return (
            StatusCode::CONFLICT,
            Json(CreateSessionResponse { success: false }),
        );
    }
    let (tx, _rx) = broadcast::channel::<SyncMessage>(100);
    session.insert(
        payload.id,
        Session {
            users: HashMap::new(),
            queue: Vec::new(),
            playing: false,
            tx: tx,
        },
    );
    (
        StatusCode::CREATED,
        Json(CreateSessionResponse { success: true }),
    )
}

pub async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| websocket(socket, state, id))
}
pub async fn websocket(socket: WebSocket, state: AppState, session_id: String) {
    let sessions = state.sessions.clone();
    let (mut sender, mut receiver) = socket.split();

    if let Some(Ok(ax_extract_ws::Message::Text(text))) = receiver.next().await {
        if let Ok(join) = serde_json::from_str::<JsonMessage>(&text) {
            {
                let mut sessions = sessions.write().await;

                if let Some(session) = sessions.get_mut(&session_id) {
                    session.users.insert(
                        join.client_id.clone(),
                        User {
                            username: join.username.clone(),
                        },
                    );
                } else {
                    return;
                }
            }

            let session_snap = {
                let sessions = sessions.read().await;
                sessions.get(&session_id).cloned()
            };

            if let Some(session) = session_snap {
                let json = serde_json::to_string(&session).unwrap();

                let _ = sender.send(ax_extract_ws::Message::Text(json.into())).await;
            }

            let tx = {
                let sessions = sessions.read().await;

                match sessions.get(&session_id) {
                    Some(session) => session.tx.clone(),
                    None => return,
                }
            };

            let mut rx = tx.subscribe();

            let tx_for_recv = tx.clone();
            let _ = tx.send(SyncMessage::UserJoined {
                username: join.username.clone(),
                client_id: join.client_id.clone(),
            });
            let id_ = join.client_id.clone();
            let mut send_task = tokio::spawn(async move {
                while let Ok(msg) = rx.recv().await {
                    let sender_id = match &msg {
                        SyncMessage::UserJoined { client_id, .. } => client_id.clone(),
                        SyncMessage::UserLeft { client_id, .. } => client_id.clone(),
                        SyncMessage::PlaybackCmds { client_id, .. } => client_id.clone(),
                        SyncMessage::PlaybackSync { client_id, .. } => client_id.clone(),
                        SyncMessage::AddInQueue { client_id, .. } => client_id.clone(),
                        SyncMessage::UpdateQueue { client_id, .. } => client_id.clone(),
                    };

                    if sender_id == id_ {
                        continue;
                    }
                    let json = match serde_json::to_string(&msg) {
                        Ok(json) => json,
                        Err(_) => continue,
                    };

                    if sender
                        .send(ax_extract_ws::Message::Text(json.into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });

            let session_id = session_id.clone();
            let sessions_for_recv = sessions.clone();
            let session_id_for_recv = session_id.clone();

            let mut recv_task = tokio::spawn(async move {
                while let Some(Ok(ax_extract_ws::Message::Text(text))) = receiver.next().await {
                    if let Ok(msg) = serde_json::from_str::<SyncMessage>(&text) {
                        match &msg {
                            SyncMessage::AddInQueue { songs, client_id } => {
                                println!("ADDING SONGS IN QUEUE");
                                let mut session = sessions_for_recv.write().await;
                                if let Some(session) = session.get_mut(&session_id_for_recv) {
                                    for song in songs {
                                        session.queue.push(song.clone());
                                        println!("Added :: {}", song.name);
                                    }
                                    let tex = SyncMessage::UpdateQueue {
                                        songs: songs.clone(),
                                        client_id: client_id.clone(),
                                    };
                                    let _ = tx_for_recv.send(tex);
                                }
                            }
                            SyncMessage::PlaybackCmds { command, client_id } => match command {
                                PlaybackCmd::Play => {
                                    println!("{client_id} pressed play");

                                    let mut session = sessions_for_recv.write().await;
                                    if let Some(session) = session.get_mut(&session_id_for_recv) {
                                        session.playing = true;
                                    }
                                }

                                PlaybackCmd::Pause => {
                                    println!("{client_id} pressed pause");
                                    let mut session = sessions_for_recv.write().await;
                                    if let Some(session) = session.get_mut(&session_id_for_recv) {
                                        session.playing = false;
                                    }
                                }

                                PlaybackCmd::Next => {
                                    println!("{client_id} pressed Next");
                                }

                                PlaybackCmd::Prev => {
                                    println!("{client_id} pressed Prev");
                                }
                            },

                            _ => {}
                        }
                        let _ = tx_for_recv.send(msg);
                    }
                }
            });
            let username = join.username.clone();
            let client_id = join.client_id.clone();
            tokio::select! {
                _ = (&mut send_task) => recv_task.abort(),
                _ = (&mut recv_task) => send_task.abort(),
            };
            {
                let mut sessions = sessions.write().await;
                if let Some(session) = sessions.get_mut(&session_id) {
                    session.users.remove(&client_id);
                }
            }
            let _ = tx.send(SyncMessage::UserLeft {
                username: username,
                client_id: join.client_id.clone(),
            });

            {
                let mut sessions = sessions.write().await;
                if let Some(session) = sessions.get(&session_id) {
                    if session.users.is_empty() {
                        sessions.remove(&session_id);
                    }
                }
            }
        }
    }
}
