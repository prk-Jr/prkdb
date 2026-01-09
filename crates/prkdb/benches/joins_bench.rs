use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use futures::{stream, StreamExt};
use prkdb::joins::{JoinConfig, JoinType, JoinedStream};
use std::time::Duration;
use tokio::runtime::Runtime;

#[derive(Clone, Debug, PartialEq)]
struct LeftEvent {
    id: u32,
    value: String,
}

#[derive(Clone, Debug, PartialEq)]
struct RightEvent {
    id: u32,
    data: i32,
}

fn create_left_events(count: usize) -> Vec<LeftEvent> {
    (0..count)
        .map(|i| LeftEvent {
            id: i as u32,
            value: format!("left_{}", i),
        })
        .collect()
}

fn create_right_events(count: usize, overlap_factor: f32) -> Vec<RightEvent> {
    (0..(count as f32 * overlap_factor) as usize)
        .map(|i| RightEvent {
            id: i as u32,
            data: i as i32 * 10,
        })
        .collect()
}

fn bench_inner_join_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("inner_join_throughput");
    group.sample_size(10);

    for left_count in [100, 1_000, 5_000] {
        for overlap_factor in [0.5, 1.0, 2.0] {
            let right_count = (left_count as f32 * overlap_factor) as usize;

            group.bench_with_input(
                BenchmarkId::new("inner_join", format!("{}L_{}R", left_count, right_count)),
                &(left_count, overlap_factor),
                |b, &(left_count, overlap_factor)| {
                    b.to_async(&rt).iter(|| async {
                        let left_events = create_left_events(left_count);
                        let right_events = create_right_events(left_count, overlap_factor);

                        let left_stream = stream::iter(left_events.into_iter());
                        let right_stream = stream::iter(right_events.into_iter());

                        let config = JoinConfig {
                            join_type: JoinType::Inner,
                            window: Duration::from_millis(100),
                        };

                        let mut joined_stream = JoinedStream::new(
                            left_stream,
                            right_stream,
                            config,
                            |left: &LeftEvent| left.id,
                            |right: &RightEvent| right.id,
                        );

                        let mut join_count = 0;
                        while let Some(result) = joined_stream.next().await {
                            match result {
                                Ok((left, right)) => {
                                    if right.is_some() {
                                        join_count += 1;
                                    }
                                    black_box((left, right));
                                }
                                Err(_) => break,
                            }
                        }

                        black_box(join_count)
                    });
                },
            );
        }
    }
    group.finish();
}

fn bench_left_join_performance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("left_join_performance");
    group.sample_size(10);

    for left_count in [500, 2_000] {
        for overlap_factor in [0.3, 0.7, 1.5] {
            let right_count = (left_count as f32 * overlap_factor) as usize;

            group.bench_with_input(
                BenchmarkId::new("left_join", format!("{}L_{}R", left_count, right_count)),
                &(left_count, overlap_factor),
                |b, &(left_count, overlap_factor)| {
                    b.to_async(&rt).iter(|| async {
                        let left_events = create_left_events(left_count);
                        let right_events = create_right_events(left_count, overlap_factor);

                        let left_stream = stream::iter(left_events.into_iter());
                        let right_stream = stream::iter(right_events.into_iter());

                        let config = JoinConfig {
                            join_type: JoinType::Left,
                            window: Duration::from_millis(100),
                        };

                        let mut joined_stream = JoinedStream::new(
                            left_stream,
                            right_stream,
                            config,
                            |left: &LeftEvent| left.id,
                            |right: &RightEvent| right.id,
                        );

                        let mut total_results = 0;
                        let mut matched_results = 0;
                        let mut unmatched_results = 0;

                        while let Some(result) = joined_stream.next().await {
                            match result {
                                Ok((left, right)) => {
                                    total_results += 1;
                                    if right.is_some() {
                                        matched_results += 1;
                                    } else {
                                        unmatched_results += 1;
                                    }
                                    black_box((left, right));
                                }
                                Err(_) => break,
                            }
                        }

                        black_box((total_results, matched_results, unmatched_results))
                    });
                },
            );
        }
    }
    group.finish();
}

fn bench_join_buffer_efficiency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("join_buffer_efficiency");
    group.sample_size(5);

    for buffer_size in [100, 1_000, 10_000] {
        group.bench_with_input(
            BenchmarkId::new("buffer_memory", buffer_size),
            &buffer_size,
            |b, &buffer_size| {
                b.to_async(&rt).iter(|| async {
                    // Create large events to test memory pressure
                    let left_events: Vec<LeftEvent> = (0..buffer_size)
                        .map(|i| LeftEvent {
                            id: i as u32,
                            value: "x".repeat(1024), // 1KB string
                        })
                        .collect();

                    let right_events: Vec<RightEvent> = (0..buffer_size)
                        .map(|i| RightEvent {
                            id: i as u32,
                            data: i,
                        })
                        .collect();

                    let left_stream = stream::iter(left_events);
                    let right_stream = stream::iter(right_events);

                    let config = JoinConfig {
                        join_type: JoinType::Inner,
                        window: Duration::from_millis(50),
                    };

                    let mut joined_stream = JoinedStream::new(
                        left_stream,
                        right_stream,
                        config,
                        |left: &LeftEvent| left.id,
                        |right: &RightEvent| right.id,
                    );

                    let mut processed_bytes = 0;
                    let mut join_count = 0;

                    while let Some(result) = joined_stream.next().await {
                        match result {
                            Ok((left, right)) => {
                                join_count += 1;
                                processed_bytes += left.value.len();
                                if let Some(right_val) = right {
                                    processed_bytes += std::mem::size_of_val(&right_val.data);
                                }
                            }
                            Err(_) => break,
                        }
                    }

                    black_box((join_count, processed_bytes))
                });
            },
        );
    }
    group.finish();
}

fn bench_join_key_complexity(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("join_key_complexity");
    group.sample_size(10);

    for key_cardinality in [10, 100, 1_000] {
        group.bench_with_input(
            BenchmarkId::new("key_distribution", key_cardinality),
            &key_cardinality,
            |b, &key_cardinality| {
                b.to_async(&rt).iter(|| async {
                    // Create events with varying key distribution
                    let left_events: Vec<LeftEvent> = (0..1_000)
                        .map(|i| LeftEvent {
                            id: (i % key_cardinality) as u32,
                            value: format!("left_{}", i),
                        })
                        .collect();

                    let right_events: Vec<RightEvent> = (0..1_000)
                        .map(|i| RightEvent {
                            id: (i % key_cardinality) as u32,
                            data: i,
                        })
                        .collect();

                    let left_stream = stream::iter(left_events);
                    let right_stream = stream::iter(right_events);

                    let config = JoinConfig {
                        join_type: JoinType::Inner,
                        window: Duration::from_millis(100),
                    };

                    let mut joined_stream = JoinedStream::new(
                        left_stream,
                        right_stream,
                        config,
                        |left: &LeftEvent| left.id,
                        |right: &RightEvent| right.id,
                    );

                    let mut join_count = 0;

                    while let Some(result) = joined_stream.next().await {
                        match result {
                            Ok((_left, right)) => {
                                if right.is_some() {
                                    join_count += 1;
                                }
                            }
                            Err(_) => break,
                        }
                    }

                    black_box(join_count)
                });
            },
        );
    }
    group.finish();
}

fn bench_concurrent_joins(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("concurrent_joins");
    group.sample_size(5);

    for num_joins in [1, 2, 4] {
        group.bench_with_input(
            BenchmarkId::new("concurrent_processing", num_joins),
            &num_joins,
            |b, &num_joins| {
                b.to_async(&rt).iter(|| async {
                    let mut handles = Vec::new();

                    for i in 0..num_joins {
                        let handle = tokio::spawn(async move {
                            let left_events = create_left_events(500);
                            let right_events = create_right_events(500, 1.0);

                            let left_stream = stream::iter(left_events.into_iter());
                            let right_stream = stream::iter(right_events.into_iter());

                            let config = JoinConfig {
                                join_type: JoinType::Inner,
                                window: Duration::from_millis(50),
                            };

                            let mut joined_stream = JoinedStream::new(
                                left_stream,
                                right_stream,
                                config,
                                |left: &LeftEvent| left.id + (i as u32 * 10000), // Ensure unique keys
                                |right: &RightEvent| right.id + (i as u32 * 10000),
                            );

                            let mut join_count = 0;

                            while let Some(result) = joined_stream.next().await {
                                match result {
                                    Ok((_left, right)) => {
                                        if right.is_some() {
                                            join_count += 1;
                                        }
                                    }
                                    Err(_) => break,
                                }
                            }

                            join_count
                        });
                        handles.push(handle);
                    }

                    let mut total_joins = 0;
                    for handle in handles {
                        total_joins += handle.await.unwrap_or(0);
                    }

                    black_box(total_joins)
                });
            },
        );
    }
    group.finish();
}

fn bench_join_window_sizes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("join_window_sizes");
    group.sample_size(10);

    for window_ms in [10, 50, 100, 500] {
        group.bench_with_input(
            BenchmarkId::new("window_duration", window_ms),
            &window_ms,
            |b, &window_ms| {
                b.to_async(&rt).iter(|| async {
                    let left_events = create_left_events(1_000);
                    let right_events = create_right_events(1_000, 1.0);

                    let left_stream = stream::iter(left_events.into_iter());
                    let right_stream = stream::iter(right_events.into_iter());

                    let config = JoinConfig {
                        join_type: JoinType::Inner,
                        window: Duration::from_millis(window_ms),
                    };

                    let mut joined_stream = JoinedStream::new(
                        left_stream,
                        right_stream,
                        config,
                        |left: &LeftEvent| left.id,
                        |right: &RightEvent| right.id,
                    );

                    let mut join_count = 0;

                    while let Some(result) = joined_stream.next().await {
                        match result {
                            Ok((_left, right)) => {
                                if right.is_some() {
                                    join_count += 1;
                                }
                            }
                            Err(_) => break,
                        }
                    }

                    black_box(join_count)
                });
            },
        );
    }
    group.finish();
}

fn bench_outer_join_performance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("outer_join_comprehensive", |b| {
        b.to_async(&rt).iter(|| async {
            let left_events = create_left_events(1_000);
            let right_events = create_right_events(800, 1.0); // Partial overlap

            let left_stream = stream::iter(left_events);
            let right_stream = stream::iter(right_events);

            let config = JoinConfig {
                join_type: JoinType::Outer,
                window: Duration::from_millis(100),
            };

            let mut joined_stream = JoinedStream::new(
                left_stream,
                right_stream,
                config,
                |left: &LeftEvent| left.id,
                |right: &RightEvent| right.id,
            );

            let mut total_results = 0;
            let mut matched_results = 0;

            while let Some(result) = joined_stream.next().await {
                match result {
                    Ok((_left, right)) => {
                        total_results += 1;
                        if right.is_some() {
                            matched_results += 1;
                        }
                    }
                    Err(_) => break,
                }
            }

            black_box((total_results, matched_results))
        });
    });
}

criterion_group!(
    benches,
    bench_inner_join_throughput,
    bench_left_join_performance,
    bench_join_buffer_efficiency,
    bench_join_key_complexity,
    bench_concurrent_joins,
    bench_join_window_sizes,
    bench_outer_join_performance
);
criterion_main!(benches);
