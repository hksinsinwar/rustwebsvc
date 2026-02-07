use std::sync::Arc;

use rustwebsvc::{app, AppState, RealBackend};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
    let mysql_url = std::env::var("MYSQL_URL")
        .unwrap_or_else(|_| "mysql://root:password@127.0.0.1:3306/app".to_string());
    let kafka_brokers =
        std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "127.0.0.1:9092".to_string());
    let kafka_topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "events".to_string());

    let backend = RealBackend::new(&redis_url, &mysql_url, &kafka_brokers, &kafka_topic).await?;
    let state = AppState::new(Arc::new(backend));

    let router = app(state);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    axum::serve(listener, router).await?;
    Ok(())
}
