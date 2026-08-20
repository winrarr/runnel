use std::time::Duration;

use std::hint::black_box;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use runnel_core::{Broker, BrokerConfig, PollResult};
use tempfile::TempDir;

const MESSAGE_COUNT: u64 = 100;
const PAYLOAD: &[u8] = &[b'x'; 100];

fn durable_publish(c: &mut Criterion) {
    let mut group = c.benchmark_group("durable_publish");
    group.sample_size(20);
    group.throughput(Throughput::ElementsAndBytes {
        elements: MESSAGE_COUNT,
        bytes: MESSAGE_COUNT * PAYLOAD.len() as u64,
    });
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
    group.throughput(Throughput::ElementsAndBytes {
        elements: MESSAGE_COUNT,
        bytes: MESSAGE_COUNT * PAYLOAD.len() as u64,
    });
    group.bench_function("100-byte_messages", |benchmark| {
        benchmark.iter_batched(
            || {
                let directory = TempDir::new().unwrap();
                let broker = Broker::open(
                    directory.path(),
                    BrokerConfig {
                        ack_timeout: Duration::from_secs(60),
                        max_delivery_attempts: None,
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

fn shared_consumer_poll_ack(c: &mut Criterion) {
    let mut group = c.benchmark_group("shared_consumer_poll_ack");
    group.sample_size(20);
    group.throughput(Throughput::ElementsAndBytes {
        elements: MESSAGE_COUNT,
        bytes: MESSAGE_COUNT * PAYLOAD.len() as u64,
    });
    group.bench_function("100-byte_messages_2_members", |benchmark| {
        benchmark.iter_batched(
            || {
                let directory = TempDir::new().unwrap();
                let broker = Broker::open(
                    directory.path(),
                    BrokerConfig {
                        ack_timeout: Duration::from_secs(60),
                        max_delivery_attempts: None,
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
                    let member = if offset % 2 == 0 {
                        "member-a"
                    } else {
                        "member-b"
                    };
                    let (message_offset, token) =
                        grouped_delivery(broker.poll_group("bench", "workers", member).unwrap());
                    assert_eq!(message_offset, offset);
                    black_box(
                        broker
                            .ack_group("bench", "workers", member, offset, &token)
                            .unwrap(),
                    );
                }
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn shared_consumer_keyed_poll_ack(c: &mut Criterion) {
    let mut group = c.benchmark_group("shared_consumer_keyed_poll_ack");
    group.sample_size(20);
    group.throughput(Throughput::ElementsAndBytes {
        elements: MESSAGE_COUNT,
        bytes: MESSAGE_COUNT * PAYLOAD.len() as u64,
    });
    group.bench_function("100-byte_messages_4_keys_4_members", |benchmark| {
        benchmark.iter_batched(
            || {
                let directory = TempDir::new().unwrap();
                let broker = Broker::open(
                    directory.path(),
                    BrokerConfig {
                        ack_timeout: Duration::from_secs(60),
                        max_delivery_attempts: None,
                    },
                )
                .unwrap();
                for offset in 0..MESSAGE_COUNT {
                    broker
                        .publish(
                            "bench",
                            Some(format!("key-{}", offset % 4)),
                            PAYLOAD.to_vec(),
                        )
                        .unwrap();
                }
                (directory, broker)
            },
            |(_directory, broker)| {
                for offset in 0..MESSAGE_COUNT {
                    let member = match offset % 4 {
                        0 => "member-a",
                        1 => "member-b",
                        2 => "member-c",
                        _ => "member-d",
                    };
                    let (message_offset, token) =
                        grouped_delivery(broker.poll_group("bench", "workers", member).unwrap());
                    assert_eq!(message_offset, offset);
                    black_box(
                        broker
                            .ack_group("bench", "workers", member, offset, &token)
                            .unwrap(),
                    );
                }
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn grouped_delivery(result: PollResult) -> (u64, String) {
    match result {
        PollResult::Message(message) => (
            message.offset,
            message
                .delivery_token
                .expect("grouped benchmark messages should include a token"),
        ),
        PollResult::Empty => panic!("grouped benchmark should have a message"),
    }
}

criterion_group!(
    benches,
    durable_publish,
    publish_poll_ack,
    shared_consumer_poll_ack,
    shared_consumer_keyed_poll_ack
);
criterion_main!(benches);
