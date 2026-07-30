use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use clap::{Parser, ValueEnum};
use futures_util::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use tokio::sync::Barrier;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Parser, Debug)]
#[command(about = "BlazeChat WebSocket load generator")]
struct Args {
    #[arg(long, default_value = "ws://127.0.0.1:8080/ws")]
    url: String,
    #[arg(long, default_value_t = 1_000)]
    connections: usize,
    #[arg(long, default_value_t = 30)]
    duration: u64,
    #[arg(long, default_value_t = 10)]
    warmup: u64,
    #[arg(long, default_value_t = 10)]
    messages_per_second: u64,
    #[arg(long, default_value_t = 16)]
    rooms: usize,
    #[arg(long, value_enum, default_value_t = Mode::Throughput)]
    mode: Mode,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq)]
enum Mode {
    Connections,
    Throughput,
}

#[derive(Serialize)]
struct Outgoing<'a> {
    room: String,
    user: String,
    text: &'a str,
    client_ts_ns: u64,
    sequence: u64,
}

#[derive(Deserialize)]
struct Incoming {
    user: String,
    client_ts_ns: u64,
}

#[derive(Default)]
struct Counters {
    connected: AtomicU64,
    sent: AtomicU64,
    wire_received: AtomicU64,
    received: AtomicU64,
    errors: AtomicU64,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Arc::new(Args::parse());
    let counters = Arc::new(Counters::default());
    let barrier = Arc::new(Barrier::new(args.connections + 1));
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(args.connections);

    for id in 0..args.connections {
        let args = args.clone();
        let counters = counters.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            run_client(id, args, counters, barrier).await
        }));
        if id % 250 == 249 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    barrier.wait().await;
    let connect_seconds = started.elapsed().as_secs_f64();
    let mut histogram = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;
    for task in tasks {
        if let Ok(Ok(client_histogram)) = task.await {
            histogram.add(&client_histogram)?;
        }
    }

    let received = counters.received.load(Ordering::Relaxed);
    println!("mode={:?}", args.mode);
    println!("requested_connections={}", args.connections);
    println!("connected={}", counters.connected.load(Ordering::Relaxed));
    println!("connect_seconds={connect_seconds:.3}");
    println!("sent={}", counters.sent.load(Ordering::Relaxed));
    println!("received={received}");
    println!(
        "wire_received={}",
        counters.wire_received.load(Ordering::Relaxed)
    );
    println!("errors={}", counters.errors.load(Ordering::Relaxed));
    println!(
        "published_messages_per_second={:.2}",
        counters.sent.load(Ordering::Relaxed) as f64 / args.duration as f64
    );
    println!(
        "delivered_messages_per_second={:.2}",
        counters.wire_received.load(Ordering::Relaxed) as f64 / args.duration as f64
    );
    if !histogram.is_empty() {
        println!(
            "latency_p50_ms={:.3}",
            histogram.value_at_quantile(0.50) as f64 / 1000.0
        );
        println!(
            "latency_p95_ms={:.3}",
            histogram.value_at_quantile(0.95) as f64 / 1000.0
        );
        println!(
            "latency_p99_ms={:.3}",
            histogram.value_at_quantile(0.99) as f64 / 1000.0
        );
        println!("latency_max_ms={:.3}", histogram.max() as f64 / 1000.0);
    }
    Ok(())
}

async fn run_client(
    id: usize,
    args: Arc<Args>,
    counters: Arc<Counters>,
    barrier: Arc<Barrier>,
) -> Result<Histogram<u64>> {
    let mut histogram = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;
    let result = connect_async(&args.url).await;
    let (socket, _) = match result {
        Ok(value) => value,
        Err(err) => {
            counters.errors.fetch_add(1, Ordering::Relaxed);
            barrier.wait().await;
            return Err(err.into());
        }
    };
    counters.connected.fetch_add(1, Ordering::Relaxed);
    barrier.wait().await;
    let (mut tx, mut rx) = socket.split();
    let user = format!("bench-{id}");
    let room = format!("room-{}", id % args.rooms.max(1));
    let deadline = Instant::now() + Duration::from_secs(args.warmup + args.duration);
    let measure_after = Instant::now() + Duration::from_secs(args.warmup);
    let interval = if args.mode == Mode::Throughput && args.messages_per_second > 0 {
        Some(Duration::from_nanos(
            1_000_000_000 / args.messages_per_second,
        ))
    } else {
        None
    };
    let mut ticker = interval.map(tokio::time::interval);
    let mut sequence = 0_u64;

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline.into()) => break,
            incoming = rx.next() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if Instant::now() >= measure_after {
                            counters.wire_received.fetch_add(1, Ordering::Relaxed);
                        }
                        if let Ok(message) = serde_json::from_str::<Incoming>(&text)
                            && message.user == user && Instant::now() >= measure_after {
                            let latency_us = unix_time_ns().saturating_sub(message.client_ts_ns) / 1_000;
                            if latency_us > 0 {
                                let _ = histogram.record(latency_us);
                                counters.received.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => {
                        counters.errors.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
            }
            _ = async {
                match ticker.as_mut() {
                    Some(value) => value.tick().await,
                    None => std::future::pending().await,
                }
            } => {
                if Instant::now() >= deadline {
                    break;
                }
                sequence += 1;
                let payload = serde_json::to_string(&Outgoing {
                    room: room.clone(),
                    user: user.clone(),
                    text: "BlazeChat benchmark payload",
                    client_ts_ns: unix_time_ns(),
                    sequence,
                })?;
                if tx.send(Message::Text(payload.into())).await.is_err() {
                    counters.errors.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                if Instant::now() >= measure_after {
                    counters.sent.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    Ok(histogram)
}

fn unix_time_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}
