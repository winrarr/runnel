use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, black_box, criterion_group, criterion_main};
use runnel_core::{Broker, BrokerConfig, PollResult};
use tempfile::TempDir;

const MESSAGE_COUNT: u64 = 100;
const PAYLOAD: &[u8] = &[b'x'; 100];

fn durable_publish(c: &mut Criterion) {
    let mut group = c.benchmark_group("durable_publish");
    group.sample_size(20);
    group.throughput(Throughput::Elements(MESSAGE_COUNT));
    group.bench_function("100-byte_messages", |benchmark| {
        benchmark.iter_batched(
            || {
                let directory = TempDir::new().unwrap();
                let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
                (directory, broker)
            },
            |(_directory, broker)| {
                for _ in 0..MESSAGE_COUNT {
                    black_box(broker.publish("bench", None, PAYLOAD.to_vec()).unwrap());
                }
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn publish_poll_ack(c: &mut Criterion) {
    let mut group = c.benchmark_group("publish_poll_ack");
    group.sample_size(20);
    group.throughput(Throughput::Elements(MESSAGE_COUNT));
    group.bench_function("100-byte_messages", |benchmark| {
        benchmark.iter_batched(
            || {
                let directory = TempDir::new().unwrap();
                let broker = Broker::open(
                    directory.path(),
                    BrokerConfig {
                        ack_timeout: Duration::from_secs(60),
                    },
                )
                .unwrap();
                for _ in 0..MESSAGE_COUNT {
                    broker.publish("bench", None, PAYLOAD.to_vec()).unwrap();
                }
                (directory, broker)
            },
            |(_directory, broker)| {
                for offset in 0..MESSAGE_COUNT {
                    assert!(matches!(
                        broker.poll("bench", "consumer").unwrap(),
                        PollResult::Message(message) if message.offset == offset
                    ));
                    black_box(broker.ack("bench", "consumer", offset).unwrap());
                }
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(benches, durable_publish, publish_poll_ack);
criterion_main!(benches);
