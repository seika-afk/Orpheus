use axum::middleware;
use axum::serve::Listener;
use axum::{Router, routing::get, routing::post};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::{RwLock, broadcast};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// Local imports
mod self_middlewares;
use self_middlewares::timing_middleware;

mod handlers;
use handlers::{create_session, get_session, health, root, websocket_handler};
#[derive(Clone)]
struct Session {
    users: Vec<String>,
}

#[derive(Clone)]
struct SyncMessage {
    text: String,
}

#[derive(Clone)]
struct AppState {
    sessions: Arc<RwLock<HashMap<String, Session>>>,
    tx: broadcast::Sender<SyncMessage>,
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
    let (tx_, _) = broadcast::channel::<SyncMessage>(100);
    let state = AppState {
        sessions: Arc::new(RwLock::new(HashMap::new())),
        tx: tx_,
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
