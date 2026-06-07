use axum::{extract::Request, extract::State, middleware::Next, response::Response};

use crate::AppState;
use std::{sync::atomic::Ordering, time::Instant};

pub async fn timing_middleware(req: Request, next: Next) -> Response {
    let start = Instant::now();
    let response = next.run(req).await;
    let end = start.elapsed();
    println!("Request took : {:?}", end);
    response
}
