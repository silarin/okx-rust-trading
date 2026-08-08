use std::{hint::black_box, time::Duration};

use rust_decimal::Decimal;
use tokio::{sync::watch, time::Instant};

const EVENT_COUNT: u64 = 500;
const EVENT_PERIOD: Duration = Duration::from_millis(1);
const SCALED_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Clone, Copy, Debug)]
struct SyntheticMarketEvent {
    generation: u64,
    created_at: Instant,
}

#[derive(Debug)]
struct ResultSummary {
    produced: u64,
    processed: usize,
    coalesced: u64,
    min_micros: u64,
    p50_micros: u64,
    p95_micros: u64,
    p99_micros: u64,
    max_micros: u64,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let polling = run_scaled_polling().await;
    let event_driven = run_event_driven().await;
    println!("synthetic workload: {EVENT_COUNT} events at {EVENT_PERIOD:?}");
    println!("scaled polling interval: {SCALED_POLL_INTERVAL:?}");
    print_summary("scaled_polling", &polling);
    print_summary("event_driven", &event_driven);
}

async fn run_scaled_polling() -> ResultSummary {
    let (sender, receiver) = watch::channel(None);
    let producer = tokio::spawn(produce(sender));
    let mut interval = tokio::time::interval(SCALED_POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_generation = 0;
    let mut samples = Vec::new();
    while last_generation < EVENT_COUNT {
        interval.tick().await;
        if let Some(event) = *receiver.borrow()
            && event.generation > last_generation
        {
            black_box(shadow_decision_work(event.generation));
            samples.push(event.created_at.elapsed());
            last_generation = event.generation;
        }
    }
    producer.await.expect("polling producer");
    summarize(EVENT_COUNT, samples)
}

async fn run_event_driven() -> ResultSummary {
    let (sender, mut receiver) = watch::channel(None);
    let producer = tokio::spawn(produce(sender));
    let mut last_generation = 0;
    let mut samples = Vec::with_capacity(EVENT_COUNT as usize);
    while last_generation < EVENT_COUNT {
        receiver.changed().await.expect("event producer open");
        let event = (*receiver.borrow_and_update()).expect("market event");
        black_box(shadow_decision_work(event.generation));
        samples.push(event.created_at.elapsed());
        last_generation = event.generation;
    }
    producer.await.expect("event producer");
    summarize(EVENT_COUNT, samples)
}

async fn produce(sender: watch::Sender<Option<SyntheticMarketEvent>>) {
    for generation in 1..=EVENT_COUNT {
        sender.send_replace(Some(SyntheticMarketEvent {
            generation,
            created_at: Instant::now(),
        }));
        tokio::time::sleep(EVENT_PERIOD).await;
    }
}

fn shadow_decision_work(generation: u64) -> Decimal {
    let bid = Decimal::from(generation.saturating_add(2));
    let ask = Decimal::from(generation.saturating_add(1));
    (bid - ask) / (bid + ask)
}

fn summarize(produced: u64, samples: Vec<Duration>) -> ResultSummary {
    let mut micros = samples
        .into_iter()
        .map(|sample| sample.as_micros().min(u128::from(u64::MAX)) as u64)
        .collect::<Vec<_>>();
    micros.sort_unstable();
    let processed = micros.len();
    ResultSummary {
        produced,
        processed,
        coalesced: produced.saturating_sub(processed as u64),
        min_micros: micros.first().copied().unwrap_or_default(),
        p50_micros: percentile(&micros, 50),
        p95_micros: percentile(&micros, 95),
        p99_micros: percentile(&micros, 99),
        max_micros: micros.last().copied().unwrap_or_default(),
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let rank = sorted.len().saturating_mul(percentile).saturating_add(99) / 100;
    sorted
        .get(rank.saturating_sub(1).min(sorted.len().saturating_sub(1)))
        .copied()
        .unwrap_or_default()
}

fn print_summary(name: &str, summary: &ResultSummary) {
    println!(
        "{name}: produced={} processed={} coalesced={} min={}us p50={}us p95={}us p99={}us max={}us",
        summary.produced,
        summary.processed,
        summary.coalesced,
        summary.min_micros,
        summary.p50_micros,
        summary.p95_micros,
        summary.p99_micros,
        summary.max_micros,
    );
}
