use std::{
    collections::HashSet,
    env,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use axum::{
    Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::{error, info, warn};
use uuid::Uuid;

const REDIS_CHANNEL: &str = "blazechat:v1:messages";

#[derive(Clone)]
struct AppState {
    instance_id: Arc<str>,
    redis: redis::Client,
    publisher: redis::aio::ConnectionManager,
    fanout: broadcast::Sender<Arc<ServerMessage>>,
    metrics: Arc<Metrics>,
}

#[derive(Default)]
struct Metrics {
    connections: AtomicU64,
    accepted: AtomicU64,
    published: AtomicU64,
    delivered: AtomicU64,
    dropped: AtomicU64,
}

#[derive(Debug, Deserialize)]
struct ClientMessage {
    room: String,
    user: String,
    text: String,
    #[serde(default)]
    client_ts_ns: u64,
    #[serde(default)]
    sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ServerMessage {
    id: Uuid,
    instance: String,
    room: String,
    user: String,
    text: String,
    client_ts_ns: u64,
    sequence: u64,
    server_ts_ns: u64,
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
    instance: String,
    connections: u64,
    accepted: u64,
    published: u64,
    delivered: u64,
    dropped: u64,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let bind_addr: SocketAddr = env::var("BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".into())
        .parse()
        .context("invalid BIND_ADDR")?;
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".into());
    let instance_id: Arc<str> = env::var("INSTANCE_ID")
        .unwrap_or_else(|_| Uuid::new_v4().to_string())
        .into();
    let fanout_capacity = env::var("FANOUT_CAPACITY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(65_536);

    let redis = redis::Client::open(redis_url).context("invalid REDIS_URL")?;
    wait_for_redis(&redis).await?;

    let publisher = redis.get_connection_manager().await?;
    let (fanout, _) = broadcast::channel(fanout_capacity);
    let state = AppState {
        instance_id,
        redis,
        publisher,
        fanout,
        metrics: Arc::new(Metrics::default()),
    };
    tokio::spawn(redis_subscriber(state.clone()));

    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/ws", get(websocket))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    info!(%bind_addr, instance = %state.instance_id, "BlazeChat listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn wait_for_redis(client: &redis::Client) -> Result<()> {
    for attempt in 1..=30 {
        match client.get_multiplexed_async_connection().await {
            Ok(mut conn) => {
                let pong: String = redis::cmd("PING").query_async(&mut conn).await?;
                if pong == "PONG" {
                    return Ok(());
                }
            }
            Err(err) if attempt < 30 => warn!(attempt, %err, "waiting for Redis"),
            Err(err) => return Err(err.into()),
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    anyhow::bail!("Redis did not become ready")
}

async fn redis_subscriber(state: AppState) {
    loop {
        let result = async {
            let mut pubsub = state.redis.get_async_pubsub().await?;
            pubsub.subscribe(REDIS_CHANNEL).await?;
            let mut stream = pubsub.on_message();
            while let Some(message) = stream.next().await {
                let payload: Vec<u8> = message.get_payload_bytes().to_vec();
                match serde_json::from_slice::<ServerMessage>(&payload) {
                    Ok(message) => {
                        if state.fanout.send(Arc::new(message)).is_err() {
                            // Having no local receivers is normal.
                        }
                    }
                    Err(err) => warn!(%err, "discarding invalid Redis message"),
                }
            }
            Ok::<(), redis::RedisError>(())
        }
        .await;

        if let Err(err) = result {
            error!(%err, "Redis subscriber disconnected; retrying");
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    }
}

async fn websocket(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.max_message_size(64 * 1024)
        .max_frame_size(64 * 1024)
        .on_upgrade(move |socket| client_session(socket, state))
}

async fn client_session(socket: WebSocket, state: AppState) {
    state.metrics.connections.fetch_add(1, Ordering::Relaxed);
    state.metrics.accepted.fetch_add(1, Ordering::Relaxed);
    let (mut ws_tx, mut ws_rx) = socket.split();
    let mut local_rx = state.fanout.subscribe();
    let mut rooms = HashSet::new();

    loop {
        tokio::select! {
            incoming = ws_rx.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        match publish_client_message(&state, text.as_str()).await {
                            Ok(room) => {
                                rooms.insert(room);
                            }
                            Err(err) => warn!(%err, "message rejected"),
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if ws_tx.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    _ => {}
                }
            }
            outgoing = local_rx.recv() => {
                match outgoing {
                    Ok(message) => {
                        if !rooms.contains(&message.room) {
                            continue;
                        }
                        match serde_json::to_string(message.as_ref()) {
                            Ok(json) => {
                                if ws_tx.send(Message::Text(json.into())).await.is_err() {
                                    break;
                                }
                                state.metrics.delivered.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(err) => error!(%err, "serialization failed"),
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        state.metrics.dropped.fetch_add(skipped, Ordering::Relaxed);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
    state.metrics.connections.fetch_sub(1, Ordering::Relaxed);
}

async fn publish_client_message(state: &AppState, json: &str) -> Result<String> {
    let input: ClientMessage = serde_json::from_str(json).context("invalid JSON")?;
    if input.room.is_empty()
        || input.room.len() > 128
        || input.user.is_empty()
        || input.user.len() > 128
        || input.text.is_empty()
        || input.text.len() > 16_384
    {
        anyhow::bail!("invalid field length");
    }
    let room = input.room.clone();
    let message = ServerMessage {
        id: Uuid::new_v4(),
        instance: state.instance_id.to_string(),
        room: input.room,
        user: input.user,
        text: input.text,
        client_ts_ns: input.client_ts_ns,
        sequence: input.sequence,
        server_ts_ns: unix_time_ns(),
    };
    let payload = serde_json::to_vec(&message)?;
    let mut conn = state.publisher.clone();
    let _: usize = conn.publish(REDIS_CHANNEL, payload).await?;
    state.metrics.published.fetch_add(1, Ordering::Relaxed);
    Ok(room)
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    axum::Json(Health {
        status: "ok",
        instance: state.instance_id.to_string(),
        connections: state.metrics.connections.load(Ordering::Relaxed),
        accepted: state.metrics.accepted.load(Ordering::Relaxed),
        published: state.metrics.published.load(Ordering::Relaxed),
        delivered: state.metrics.delivered.load(Ordering::Relaxed),
        dropped: state.metrics.dropped.load(Ordering::Relaxed),
    })
}

async fn metrics(State(state): State<AppState>) -> (StatusCode, String) {
    let m = &state.metrics;
    let body = format!(
        "blazechat_connections {}\nblazechat_connections_accepted_total {}\nblazechat_messages_published_total {}\nblazechat_messages_delivered_total {}\nblazechat_messages_dropped_total {}\n",
        m.connections.load(Ordering::Relaxed),
        m.accepted.load(Ordering::Relaxed),
        m.published.load(Ordering::Relaxed),
        m.delivered.load(Ordering::Relaxed),
        m.dropped.load(Ordering::Relaxed),
    );
    (StatusCode::OK, body)
}

fn unix_time_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("ctrl-c handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
