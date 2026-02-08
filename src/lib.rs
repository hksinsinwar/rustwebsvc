use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use prost::Message;
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use sqlx::{MySqlPool, Row};
use tokio::sync::Mutex;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/rustwebsvc.rs"));
}

pub mod reqwest_jni;

const CONTENT_TYPE_PROTO: &str = "application/x-protobuf";
const CONTENT_TYPE_JSON: &str = "application/json";

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("not found")]
    NotFound,
    #[error("backend error: {0}")]
    Backend(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AppError::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            AppError::NotFound => (StatusCode::NOT_FOUND, self.to_string()),
            AppError::Backend(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
        };
        let body = Json(serde_json::json!({ "error": message }));
        (status, body).into_response()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachePayload {
    pub value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UserPayload {
    pub name: String,
    pub email: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UserRecord {
    pub id: i64,
    pub name: String,
    pub email: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventPayload {
    pub key: String,
    pub payload: String,
}

#[async_trait]
pub trait Backend: Send + Sync {
    async fn cache_set(&self, key: &str, value: &str) -> Result<(), AppError>;
    async fn cache_get(&self, key: &str) -> Result<Option<String>, AppError>;
    async fn mysql_create_user(&self, payload: UserPayload) -> Result<UserRecord, AppError>;
    async fn mysql_get_user(&self, id: i64) -> Result<Option<UserRecord>, AppError>;
    async fn kafka_publish(&self, payload: EventPayload) -> Result<(), AppError>;
}

#[derive(Clone)]
pub struct RealBackend {
    redis_client: redis::Client,
    mysql_pool: MySqlPool,
    kafka_producer: FutureProducer,
    kafka_topic: String,
}

impl RealBackend {
    pub async fn new(
        redis_url: &str,
        mysql_url: &str,
        kafka_brokers: &str,
        kafka_topic: &str,
    ) -> Result<Self, AppError> {
        let redis_client = redis::Client::open(redis_url)
            .map_err(|err| AppError::Backend(format!("redis: {err}")))?;
        let mysql_pool = MySqlPool::connect(mysql_url)
            .await
            .map_err(|err| AppError::Backend(format!("mysql: {err}")))?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS users (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL
            )",
        )
        .execute(&mysql_pool)
        .await
        .map_err(|err| AppError::Backend(format!("mysql: {err}")))?;

        let kafka_producer = ClientConfig::new()
            .set("bootstrap.servers", kafka_brokers)
            .create()
            .map_err(|err| AppError::Backend(format!("kafka: {err}")))?;

        Ok(Self {
            redis_client,
            mysql_pool,
            kafka_producer,
            kafka_topic: kafka_topic.to_string(),
        })
    }
}

#[async_trait]
impl Backend for RealBackend {
    async fn cache_set(&self, key: &str, value: &str) -> Result<(), AppError> {
        let mut conn = self
            .redis_client
            .get_async_connection()
            .await
            .map_err(|err| AppError::Backend(format!("redis: {err}")))?;
        conn.set(key, value)
            .await
            .map_err(|err| AppError::Backend(format!("redis: {err}")))?;
        Ok(())
    }

    async fn cache_get(&self, key: &str) -> Result<Option<String>, AppError> {
        let mut conn = self
            .redis_client
            .get_async_connection()
            .await
            .map_err(|err| AppError::Backend(format!("redis: {err}")))?;
        let value: Option<String> = conn
            .get(key)
            .await
            .map_err(|err| AppError::Backend(format!("redis: {err}")))?;
        Ok(value)
    }

    async fn mysql_create_user(&self, payload: UserPayload) -> Result<UserRecord, AppError> {
        let result = sqlx::query("INSERT INTO users (name, email) VALUES (?, ?)")
            .bind(&payload.name)
            .bind(&payload.email)
            .execute(&self.mysql_pool)
            .await
            .map_err(|err| AppError::Backend(format!("mysql: {err}")))?;
        let id = result.last_insert_id() as i64;
        Ok(UserRecord {
            id,
            name: payload.name,
            email: payload.email,
        })
    }

    async fn mysql_get_user(&self, id: i64) -> Result<Option<UserRecord>, AppError> {
        let row = sqlx::query("SELECT id, name, email FROM users WHERE id = ?")
            .bind(id)
            .fetch_optional(&self.mysql_pool)
            .await
            .map_err(|err| AppError::Backend(format!("mysql: {err}")))?;
        let record = match row {
            Some(row) => Some(UserRecord {
                id: row
                    .try_get::<i64, _>("id")
                    .map_err(|err| AppError::Backend(format!("mysql: {err}")))?,
                name: row
                    .try_get::<String, _>("name")
                    .map_err(|err| AppError::Backend(format!("mysql: {err}")))?,
                email: row
                    .try_get::<String, _>("email")
                    .map_err(|err| AppError::Backend(format!("mysql: {err}")))?,
            }),
            None => None,
        };
        Ok(record)
    }

    async fn kafka_publish(&self, payload: EventPayload) -> Result<(), AppError> {
        self.kafka_producer
            .send(
                FutureRecord::to(&self.kafka_topic)
                    .key(&payload.key)
                    .payload(&payload.payload),
                std::time::Duration::from_secs(5),
            )
            .await
            .map_err(|(err, _)| AppError::Backend(format!("kafka: {err}")))?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct AppState {
    backend: Arc<dyn Backend>,
}

impl AppState {
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        Self { backend }
    }
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/cache/:key", post(cache_set).get(cache_get))
        .route("/users", post(users_create))
        .route("/users/:id", get(users_get))
        .route("/events", post(events_publish))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    StatusCode::OK
}

async fn cache_set(
    State(state): State<AppState>,
    Path(key): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
    let payload = parse_cache_payload(&headers, body)?;
    state.backend.cache_set(&key, &payload.value).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn cache_get(
    State(state): State<AppState>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let value = state.backend.cache_get(&key).await?;
    let payload = value.map(|value| CachePayload { value });
    respond_optional(payload, &headers)
}

async fn users_create(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
    let payload = parse_user_payload(&headers, body)?;
    let record = state.backend.mysql_create_user(payload).await?;
    respond_user(record, &headers)
}

async fn users_get(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    let record = state.backend.mysql_get_user(id).await?;
    respond_optional_user(record, &headers)
}

async fn events_publish(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
    let payload = parse_event_payload(&headers, body)?;
    state.backend.kafka_publish(payload).await?;
    Ok(StatusCode::ACCEPTED.into_response())
}

fn parse_cache_payload(headers: &HeaderMap, body: Bytes) -> Result<CachePayload, AppError> {
    if is_protobuf(headers) {
        let decoded = proto::CacheValue::decode(body)
            .map_err(|err| AppError::BadRequest(format!("invalid protobuf: {err}")))?;
        Ok(CachePayload {
            value: decoded.value,
        })
    } else {
        serde_json::from_slice(&body)
            .map_err(|err| AppError::BadRequest(format!("invalid json: {err}")))
    }
}

fn parse_user_payload(headers: &HeaderMap, body: Bytes) -> Result<UserPayload, AppError> {
    if is_protobuf(headers) {
        let decoded = proto::UserRequest::decode(body)
            .map_err(|err| AppError::BadRequest(format!("invalid protobuf: {err}")))?;
        Ok(UserPayload {
            name: decoded.name,
            email: decoded.email,
        })
    } else {
        serde_json::from_slice(&body)
            .map_err(|err| AppError::BadRequest(format!("invalid json: {err}")))
    }
}

fn parse_event_payload(headers: &HeaderMap, body: Bytes) -> Result<EventPayload, AppError> {
    if is_protobuf(headers) {
        let decoded = proto::KafkaEvent::decode(body)
            .map_err(|err| AppError::BadRequest(format!("invalid protobuf: {err}")))?;
        Ok(EventPayload {
            key: decoded.key,
            payload: decoded.payload,
        })
    } else {
        serde_json::from_slice(&body)
            .map_err(|err| AppError::BadRequest(format!("invalid json: {err}")))
    }
}

fn respond_optional(
    payload: Option<CachePayload>,
    headers: &HeaderMap,
) -> Result<Response, AppError> {
    match payload {
        Some(payload) => respond_cache(payload, headers),
        None => Err(AppError::NotFound),
    }
}

fn respond_cache(payload: CachePayload, headers: &HeaderMap) -> Result<Response, AppError> {
    if wants_protobuf(headers) {
        let message = proto::CacheValue {
            value: payload.value,
        };
        let mut buf = Vec::new();
        message
            .encode(&mut buf)
            .map_err(|err| AppError::Backend(format!("encode protobuf: {err}")))?;
        Ok(response_with_body(buf, CONTENT_TYPE_PROTO))
    } else {
        Ok((StatusCode::OK, Json(payload)).into_response())
    }
}

fn respond_user(record: UserRecord, headers: &HeaderMap) -> Result<Response, AppError> {
    if wants_protobuf(headers) {
        let message = proto::UserResponse {
            id: record.id,
            name: record.name,
            email: record.email,
        };
        let mut buf = Vec::new();
        message
            .encode(&mut buf)
            .map_err(|err| AppError::Backend(format!("encode protobuf: {err}")))?;
        Ok(response_with_body(buf, CONTENT_TYPE_PROTO))
    } else {
        Ok((StatusCode::OK, Json(record)).into_response())
    }
}

fn respond_optional_user(
    record: Option<UserRecord>,
    headers: &HeaderMap,
) -> Result<Response, AppError> {
    match record {
        Some(record) => respond_user(record, headers),
        None => Err(AppError::NotFound),
    }
}

fn response_with_body(body: Vec<u8>, content_type: &str) -> Response {
    let mut response = Response::new(body.into());
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
}

fn is_protobuf(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.starts_with(CONTENT_TYPE_PROTO))
        .unwrap_or(false)
}

fn wants_protobuf(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.contains(CONTENT_TYPE_PROTO))
        .unwrap_or(false)
}

#[derive(Default)]
struct MockStore {
    cache: HashMap<String, String>,
    users: HashMap<i64, UserRecord>,
    next_id: i64,
    events: Vec<EventPayload>,
}

#[derive(Clone, Default)]
struct MockBackend {
    store: Arc<Mutex<MockStore>>,
}

#[async_trait]
impl Backend for MockBackend {
    async fn cache_set(&self, key: &str, value: &str) -> Result<(), AppError> {
        let mut store = self.store.lock().await;
        store.cache.insert(key.to_string(), value.to_string());
        Ok(())
    }

    async fn cache_get(&self, key: &str) -> Result<Option<String>, AppError> {
        let store = self.store.lock().await;
        Ok(store.cache.get(key).cloned())
    }

    async fn mysql_create_user(&self, payload: UserPayload) -> Result<UserRecord, AppError> {
        let mut store = self.store.lock().await;
        store.next_id += 1;
        let record = UserRecord {
            id: store.next_id,
            name: payload.name,
            email: payload.email,
        };
        store.users.insert(record.id, record.clone());
        Ok(record)
    }

    async fn mysql_get_user(&self, id: i64) -> Result<Option<UserRecord>, AppError> {
        let store = self.store.lock().await;
        Ok(store.users.get(&id).cloned())
    }

    async fn kafka_publish(&self, payload: EventPayload) -> Result<(), AppError> {
        let mut store = self.store.lock().await;
        store.events.push(payload);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn test_app() -> Router {
        let backend = Arc::new(MockBackend::default());
        app(AppState::new(backend))
    }

    #[tokio::test]
    async fn json_cache_roundtrip() {
        let app = test_app();

        let request = Request::builder()
            .method("POST")
            .uri("/cache/demo")
            .header(header::CONTENT_TYPE, CONTENT_TYPE_JSON)
            .body(Body::from(r#"{"value":"hello"}"#))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let request = Request::builder()
            .method("GET")
            .uri("/cache/demo")
            .header(header::ACCEPT, CONTENT_TYPE_JSON)
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let payload: CachePayload = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload.value, "hello");
    }

    #[tokio::test]
    async fn protobuf_user_roundtrip() {
        let app = test_app();

        let create = proto::UserRequest {
            name: "Ada".to_string(),
            email: "ada@example.com".to_string(),
        };
        let mut buf = Vec::new();
        create.encode(&mut buf).unwrap();
        let request = Request::builder()
            .method("POST")
            .uri("/users")
            .header(header::CONTENT_TYPE, CONTENT_TYPE_PROTO)
            .header(header::ACCEPT, CONTENT_TYPE_PROTO)
            .body(Body::from(buf))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let created = proto::UserResponse::decode(body).unwrap();

        let request = Request::builder()
            .method("GET")
            .uri(format!("/users/{}", created.id))
            .header(header::ACCEPT, CONTENT_TYPE_PROTO)
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let fetched = proto::UserResponse::decode(body).unwrap();
        assert_eq!(fetched.email, "ada@example.com");
    }

    #[tokio::test]
    async fn kafka_event_accepts_json() {
        let app = test_app();

        let request = Request::builder()
            .method("POST")
            .uri("/events")
            .header(header::CONTENT_TYPE, CONTENT_TYPE_JSON)
            .body(Body::from(r#"{"key":"demo","payload":"event"}"#))
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }
}
