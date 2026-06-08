use axum::extract::ws::WebSocketUpgrade;
use axum::http::StatusCode;
use axum::{
    Json,
    extract::{Path, State},
    http,
};
use serde::{Deserialize, Serialize};

// /
// /health
// /sessions/:id
// /sessions Post

use crate::{AppState, Session};
//structs
#[derive(Serialize)]
pub struct HealthResponse {
    status: &'static str,
}

#[derive(Serialize)]
pub struct SessionResponse {
    users: Vec<String>,
}

#[derive(Deserialize)]
pub struct CreateSessionRequest {
    id: String,
}
#[derive(Serialize)]
pub struct CreateSessionResponse {
    success: bool,
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
    session.insert(payload.id, Session { users: vec![] });
    (
        StatusCode::CREATED,
        Json(CreateSessionResponse { success: true }),
    )
}

pub async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Path(id): Path<String>,
) {

    
}
