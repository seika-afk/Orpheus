use axum::middleware;
use axum::{Router, routing::get, routing::post};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// Local imports
mod self_middlewares;
use self_middlewares::timing_middleware;

mod handlers;
use handlers::{create_session, get_session, health, root, websocket_handler};
#[derive(Clone, Serialize, Deserialize)]
pub struct User {
    pub username: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Song {
    pub name: String,
    pub url: String,
    pub client_id: String,
}
#[derive(Clone, Serialize)]
pub struct Session {
    pub users: HashMap<String, User>,
    pub queue: Vec<Song>,
    pub playing: bool,
    #[serde(skip)]
    pub tx: broadcast::Sender<SyncMessage>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SyncMessage {
    UserJoined {
        username: String,
        client_id: String,
    },
    UserLeft {
        username: String,
        client_id: String,
    },
    AddInQueue {
        songs: Vec<Song>,
        client_id: String,
    },
    PlaybackCmds {
        command: PlaybackCmd,
        client_id: String,
    },
    PlaybackSync {
        position: u64,
        playing: bool,
        client_id: String,
    },
    UpdateQueue {
        songs: Vec<Song>,
        client_id: String,
    },
}
#[derive(Clone, Serialize, Deserialize)]
pub enum PlaybackCmd {
    Play,
    Pause,
    Next,
    Prev,
}

#[derive(Clone)]
pub struct AppState {
    pub sessions: Arc<RwLock<HashMap<String, Session>>>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("{}=trace", env!("CARGO_CRATE_NAME")).into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    let state = AppState {
        sessions: Arc::new(RwLock::new(HashMap::new())),
    };

    let app: Router = (Router::new()
        .route("/", get(root))
        .route("/health", get(health))
        .route("/sessions/{id}", get(get_session))
        .route("/sessions", post(create_session)))
    .route("/ws/sessions/{id}", get(websocket_handler))
    .layer(middleware::from_fn(timing_middleware))
    .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 4000));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    tracing::debug!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}
