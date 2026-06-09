use axum::{extract::Request, middleware::Next, response::Response};

use std::time::Instant;

pub async fn timing_middleware(req: Request, next: Next) -> Response {
    let start = Instant::now();

    tracing::info!("{} {}", req.method(), req.uri());
    let response = next.run(req).await;
    let end = start.elapsed();
    println!("Request took : {:?}", end);
    response
}
