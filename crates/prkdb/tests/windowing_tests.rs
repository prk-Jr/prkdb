use futures::stream::{self, StreamExt};
use prkdb::windowing::{TumblingWindow, WindowConfig, WindowExt, WindowedStream};
use std::time::Duration;

#[test]
fn test_tumbling_window_config() {
    let duration = Duration::from_secs(10);
    let config = WindowConfig::tumbling(duration);
    assert_eq!(config.window_type, prkdb::windowing::WindowType::Tumbling);
    assert_eq!(config.duration, duration);
    assert!(config.slide.is_none());
}

#[test]
fn test_sliding_window_config() {
    let duration = Duration::from_secs(10);
    let slide = Duration::from_secs(5);
    let config = WindowConfig::sliding(duration, slide);
    assert_eq!(config.window_type, prkdb::windowing::WindowType::Sliding);
    assert_eq!(config.duration, duration);
    assert_eq!(config.slide, Some(slide));
}

#[tokio::test]
async fn test_windowing_stream_basic() {
    // Create a simple stream with numbers
    let data = vec![1, 2, 3, 4, 5];
    let stream = stream::iter(data);

    // Create a 100ms tumbling window (short for testing)
    let config = WindowConfig::tumbling(Duration::from_millis(100));
    let mut windowed_stream = WindowedStream::new(stream, config);

    // Collect first window (should get all items since they arrive "instantly")
    let first_window = windowed_stream.next().await;
    assert!(first_window.is_some());

    let window = first_window.unwrap().unwrap();
    assert_eq!(window, vec![1, 2, 3, 4, 5]);

    // No more windows since stream ended
    let second_window = windowed_stream.next().await;
    assert!(second_window.is_none());
}

#[tokio::test]
async fn test_window_ext_trait() {
    let data = vec![10, 20, 30];
    let stream = stream::iter(data);

    // Use the WindowExt trait
    let tumbling_window = TumblingWindow::new(Duration::from_millis(50));
    let mut windowed_stream = stream.window(tumbling_window);

    let window_result = windowed_stream.next().await;
    assert!(window_result.is_some());

    let window = window_result.unwrap().unwrap();
    assert_eq!(window, vec![10, 20, 30]);
}

#[tokio::test]
async fn test_empty_stream_windowing() {
    let stream = stream::empty::<i32>();
    let config = WindowConfig::tumbling(Duration::from_millis(100));
    let mut windowed_stream = WindowedStream::new(stream, config);

    let result = windowed_stream.next().await;
    assert!(result.is_none());
}

#[tokio::test]
async fn test_single_item_windowing() {
    let stream = stream::once(async { 42 });
    let config = WindowConfig::tumbling(Duration::from_millis(100));
    let mut windowed_stream = WindowedStream::new(stream, config);

    let window_result = windowed_stream.next().await;
    assert!(window_result.is_some());

    let window = window_result.unwrap().unwrap();
    assert_eq!(window, vec![42]);

    // No more windows
    let next_result = windowed_stream.next().await;
    assert!(next_result.is_none());
}
