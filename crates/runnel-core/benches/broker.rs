use std::time::{Duration, Instant};

use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use runnel_core::{Broker, BrokerConfig, PollResult};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

const MESSAGE_COUNT: u64 = 100;
const CONCURRENT_MESSAGES_PER_WORKER: u64 = 64;
const CONCURRENT_WORKER_COUNTS: &[usize] = &[1, 2, 4, 8];
const PAYLOAD: &[u8] = &[b'x'; 100];
const RECOVERY_PAYLOAD: &[u8] = &[b'r'; 100];
const RECOVERY_RETAINED_MESSAGE_COUNTS: &[u64] = &[100, 1_000, 5_000, 20_000];

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

fn concurrent_publish_same_stream(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_publish_same_stream");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));

    for &worker_count in CONCURRENT_WORKER_COUNTS {
        let message_count = worker_count as u64 * CONCURRENT_MESSAGES_PER_WORKER;
        group.throughput(Throughput::ElementsAndBytes {
            elements: message_count,
            bytes: message_count * PAYLOAD.len() as u64,
        });
        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "{worker_count}_workers_{CONCURRENT_MESSAGES_PER_WORKER}_messages_each"
            )),
            &worker_count,
            |benchmark, &worker_count| {
                benchmark.iter_batched(
                    || {
                        let directory = TempDir::new().unwrap();
                        let broker =
                            Broker::open(directory.path(), BrokerConfig::default()).unwrap();
                        broker.create_stream("bench").unwrap();
                        (directory, broker)
                    },
                    |(_directory, broker)| {
                        let start = Arc::new(Barrier::new(worker_count));
                        thread::scope(|scope| {
                            for _ in 0..worker_count {
                                let broker = broker.clone();
                                let start = Arc::clone(&start);
                                scope.spawn(move || {
                                    start.wait();
                                    for _ in 0..CONCURRENT_MESSAGES_PER_WORKER {
                                        black_box(
                                            broker
                                                .publish("bench", None, PAYLOAD.to_vec())
                                                .unwrap(),
                                        );
                                    }
                                });
                            }
                        });
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

fn reopen_recovery_retained_messages(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_recovery_retained_messages");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    for &retained_message_count in RECOVERY_RETAINED_MESSAGE_COUNTS {
        // The local publish path calls sync_data for every record. Keep all records
        // retained and prepare the bounded log outside the measured reopen loop.
        let directory = TempDir::new().unwrap();
        let broker = Broker::open(directory.path(), BrokerConfig::default()).unwrap();
        for offset in 0..retained_message_count {
            assert_eq!(
                broker
                    .publish("recovery", None, RECOVERY_PAYLOAD.to_vec())
                    .unwrap(),
                offset
            );
        }
        drop(broker);

        group.throughput(Throughput::Elements(retained_message_count));
        group.bench_function(
            BenchmarkId::new(
                "streaming_scan_100_byte_payload",
                format!("{retained_message_count}_retained_messages"),
            ),
            |benchmark| {
                benchmark.iter_custom(|iterations| {
                    let started = Instant::now();
                    for _ in 0..iterations {
                        // Dropping each reopened broker closes its log descriptor before
                        // the next iteration and keeps recovery input size unchanged.
                        let reopened =
                            Broker::open(directory.path(), BrokerConfig::default()).unwrap();
                        black_box(reopened);
                    }
                    started.elapsed()
                });
            },
        );
    }
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
    shared_consumer_keyed_poll_ack,
    concurrent_publish_same_stream,
    reopen_recovery_retained_messages,
);
criterion_main!(benches);
