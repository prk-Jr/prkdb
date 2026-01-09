use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use futures::{stream, StreamExt};
use prkdb::windowing::{TumblingWindow, WindowConfig, WindowExt, WindowedStream};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

fn bench_tumbling_windows(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("tumbling_windows");
    group.sample_size(10);

    for window_size_ms in [10, 50, 100, 500] {
        for item_count in [100, 1_000, 10_000] {
            group.bench_with_input(
                BenchmarkId::new(
                    "window_processing",
                    format!("{}ms_{}items", window_size_ms, item_count),
                ),
                &(window_size_ms, item_count),
                |b, &(window_size_ms, item_count)| {
                    b.to_async(&rt).iter(|| async {
                        let items: Vec<i32> = (0..item_count).collect();
                        let stream = stream::iter(items.into_iter());

                        let config = WindowConfig::tumbling(Duration::from_millis(window_size_ms));
                        let mut windowed_stream = WindowedStream::new(stream, config);

                        let mut window_count = 0;
                        let mut total_items = 0;

                        while let Some(window_result) = windowed_stream.next().await {
                            match window_result {
                                Ok(window) => {
                                    window_count += 1;
                                    total_items += window.len();
                                }
                                Err(_) => break,
                            }
                        }

                        black_box((window_count, total_items))
                    });
                },
            );
        }
    }
    group.finish();
}

fn bench_sliding_windows(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("sliding_windows");
    group.sample_size(10);

    for window_size_ms in [100, 500] {
        for slide_ms in [25, 50] {
            for item_count in [500, 2_000] {
                group.bench_with_input(
                    BenchmarkId::new(
                        "sliding_processing",
                        format!("{}ms_{}ms_{}items", window_size_ms, slide_ms, item_count),
                    ),
                    &(window_size_ms, slide_ms, item_count),
                    |b, &(window_size_ms, slide_ms, item_count)| {
                        b.to_async(&rt).iter(|| async {
                            let items: Vec<i32> = (0..item_count).collect();
                            let stream = stream::iter(items.into_iter());

                            let config = WindowConfig::sliding(
                                Duration::from_millis(window_size_ms),
                                Duration::from_millis(slide_ms),
                            );
                            let mut windowed_stream = WindowedStream::new(stream, config);

                            let mut window_count = 0;
                            let mut total_items = 0;

                            while let Some(window_result) = windowed_stream.next().await {
                                match window_result {
                                    Ok(window) => {
                                        window_count += 1;
                                        total_items += window.len();
                                    }
                                    Err(_) => break,
                                }
                            }

                            black_box((window_count, total_items))
                        });
                    },
                );
            }
        }
    }
    group.finish();
}

fn bench_window_aggregations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("window_aggregations");
    group.sample_size(10);

    for item_count in [1_000, 5_000, 10_000] {
        group.bench_with_input(
            BenchmarkId::new("sum_aggregation", item_count),
            &item_count,
            |b, &item_count| {
                b.to_async(&rt).iter(|| async {
                    let items: Vec<f64> = (0..item_count).map(|i| i as f64).collect();
                    let stream = stream::iter(items.into_iter());

                    let config = WindowConfig::tumbling(Duration::from_millis(100));
                    let mut windowed_stream = WindowedStream::new(stream, config);

                    let mut aggregated_results = Vec::new();

                    while let Some(window_result) = windowed_stream.next().await {
                        match window_result {
                            Ok(window) => {
                                let sum: f64 = window.iter().sum();
                                let count = window.len();
                                let avg = if count > 0 { sum / count as f64 } else { 0.0 };
                                let min = window.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                                let max = window.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));

                                aggregated_results.push((sum, avg, min, max, count));
                            }
                            Err(_) => break,
                        }
                    }

                    black_box(aggregated_results)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("count_aggregation", item_count),
            &item_count,
            |b, &item_count| {
                b.to_async(&rt).iter(|| async {
                    let items: Vec<i32> = (0..item_count).map(|i| i % 10).collect(); // Values 0-9
                    let stream = stream::iter(items.into_iter());

                    let config = WindowConfig::tumbling(Duration::from_millis(100));
                    let mut windowed_stream = WindowedStream::new(stream, config);

                    let mut count_results = Vec::new();

                    while let Some(window_result) = windowed_stream.next().await {
                        match window_result {
                            Ok(window) => {
                                let mut counts = std::collections::HashMap::new();
                                for item in window {
                                    *counts.entry(item).or_insert(0) += 1;
                                }
                                count_results.push(counts);
                            }
                            Err(_) => break,
                        }
                    }

                    black_box(count_results)
                });
            },
        );
    }
    group.finish();
}

fn bench_window_memory_usage(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("window_memory");
    group.sample_size(5);

    for buffer_size in [100, 1_000, 10_000] {
        group.bench_with_input(
            BenchmarkId::new("memory_pressure", buffer_size),
            &buffer_size,
            |b, &buffer_size| {
                b.to_async(&rt).iter(|| async {
                    // Create large data items to test memory pressure
                    let items: Vec<Vec<u8>> = (0..buffer_size)
                        .map(|i| vec![i as u8; 1024]) // 1KB per item
                        .collect();

                    let stream = stream::iter(items.into_iter());

                    let config = WindowConfig::tumbling(Duration::from_millis(50));
                    let mut windowed_stream = WindowedStream::new(stream, config);

                    let mut window_count = 0;
                    let mut total_bytes = 0;

                    while let Some(window_result) = windowed_stream.next().await {
                        match window_result {
                            Ok(window) => {
                                window_count += 1;
                                total_bytes += window.iter().map(|v| v.len()).sum::<usize>();
                            }
                            Err(_) => break,
                        }
                    }

                    black_box((window_count, total_bytes))
                });
            },
        );
    }
    group.finish();
}

fn bench_window_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("window_throughput");
    group.sample_size(10);

    for items_per_second in [1_000, 10_000, 100_000] {
        group.bench_with_input(
            BenchmarkId::new("items_per_second", items_per_second),
            &items_per_second,
            |b, &items_per_second| {
                b.to_async(&rt).iter(|| async {
                    let items: Vec<i32> = (0..items_per_second).collect();
                    let stream = stream::iter(items.into_iter());

                    let config = WindowConfig::tumbling(Duration::from_millis(100));
                    let mut windowed_stream = WindowedStream::new(stream, config);

                    let start = Instant::now();
                    let mut processed_items = 0;

                    while let Some(window_result) = windowed_stream.next().await {
                        match window_result {
                            Ok(window) => {
                                processed_items += window.len();
                            }
                            Err(_) => break,
                        }
                    }

                    let duration = start.elapsed();
                    let throughput = processed_items as f64 / duration.as_secs_f64();

                    black_box((processed_items, throughput))
                });
            },
        );
    }
    group.finish();
}

fn bench_window_ext_trait(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("window_ext_tumbling", |b| {
        b.to_async(&rt).iter(|| async {
            let items: Vec<i32> = (0..1_000).collect();
            let stream = stream::iter(items);

            let tumbling_config = TumblingWindow::new(Duration::from_millis(50));
            let mut windowed_stream = stream.window(tumbling_config);

            let mut window_count = 0;

            while let Some(window_result) = windowed_stream.next().await {
                match window_result {
                    Ok(window) => {
                        window_count += 1;
                        black_box(window.len());
                    }
                    Err(_) => break,
                }
            }

            black_box(window_count)
        });
    });
}

fn bench_concurrent_windows(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("concurrent_windows");
    group.sample_size(5);

    for num_streams in [1, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("concurrent_processing", num_streams),
            &num_streams,
            |b, &num_streams| {
                b.to_async(&rt).iter(|| async {
                    let mut handles = Vec::new();

                    for i in 0..num_streams {
                        let handle = tokio::spawn(async move {
                            let items: Vec<i32> = (i * 1000..(i + 1) * 1000).collect();
                            let stream = stream::iter(items.into_iter());

                            let config = WindowConfig::tumbling(Duration::from_millis(50));
                            let mut windowed_stream = WindowedStream::new(stream, config);

                            let mut window_count = 0;

                            while let Some(window_result) = windowed_stream.next().await {
                                match window_result {
                                    Ok(_window) => {
                                        window_count += 1;
                                    }
                                    Err(_) => break,
                                }
                            }

                            window_count
                        });
                        handles.push(handle);
                    }

                    let mut total_windows = 0;
                    for handle in handles {
                        total_windows += handle.await.unwrap_or(0);
                    }

                    black_box(total_windows)
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_tumbling_windows,
    bench_sliding_windows,
    bench_window_aggregations,
    bench_window_memory_usage,
    bench_window_throughput,
    bench_window_ext_trait,
    bench_concurrent_windows
);
criterion_main!(benches);
