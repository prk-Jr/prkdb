//! Windowing functions for PrkDB streams.
use futures::Stream;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// Defines the type of window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowType {
    /// Fixed-size, non-overlapping windows.
    Tumbling,
    /// Fixed-size, overlapping windows.
    Sliding,
}

/// A tumbling window configuration.
#[derive(Debug, Clone)]
pub struct TumblingWindow {
    pub duration: Duration,
}

impl TumblingWindow {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

/// Configuration for a window.
#[derive(Debug, Clone)]
pub struct WindowConfig {
    pub window_type: WindowType,
    /// The duration of the window.
    pub duration: Duration,
    /// The slide interval for sliding windows.
    pub slide: Option<Duration>,
}

impl<S: Stream> Stream for WindowedStream<S> {
    type Item = Result<Vec<S::Item>, prkdb_core::error::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        let now = Instant::now();

        // Check if we should emit the current window first
        if this.should_emit_window(now) {
            if let Some(window) = this.emit_current_window() {
                return Poll::Ready(Some(Ok(window)));
            }
        }

        // Poll the inner stream for new items
        loop {
            match this.inner_stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let timestamped = TimestampedItem {
                        item,
                        timestamp: now,
                    };

                    // Initialize window start if this is the first item
                    if this.window_start.is_none() {
                        this.window_start = Some(now);
                    }

                    this.buffer.push_back(timestamped);

                    // Check if current window should be emitted after adding this item
                    if this.should_emit_window(now) {
                        if let Some(window) = this.emit_current_window() {
                            return Poll::Ready(Some(Ok(window)));
                        }
                    }
                }
                Poll::Ready(None) => {
                    // Stream ended, emit any remaining window
                    if let Some(window) = this.emit_current_window() {
                        return Poll::Ready(Some(Ok(window)));
                    }
                    return Poll::Ready(None);
                }
                Poll::Pending => {
                    // Check if we should emit based on time even if no new items
                    if this.should_emit_window(now) {
                        if let Some(window) = this.emit_current_window() {
                            return Poll::Ready(Some(Ok(window)));
                        }
                    }
                    return Poll::Pending;
                }
            }
        }
    }
}

impl WindowConfig {
    /// Creates a new tumbling window configuration.
    pub fn tumbling(duration: Duration) -> Self {
        Self {
            window_type: WindowType::Tumbling,
            duration,
            slide: None,
        }
    }

    /// Creates a new sliding window configuration.
    pub fn sliding(duration: Duration, slide: Duration) -> Self {
        Self {
            window_type: WindowType::Sliding,
            duration,
            slide: Some(slide),
        }
    }
}

/// A stream that is windowed according to a `WindowConfig`.
pub struct WindowedStream<S: Stream> {
    inner_stream: Pin<Box<S>>,
    config: WindowConfig,
    buffer: VecDeque<TimestampedItem<S::Item>>,
    window_start: Option<Instant>,
}

/// Wrapper for items with timestamps for windowing
#[derive(Debug, Clone)]
struct TimestampedItem<T> {
    item: T,
    timestamp: Instant,
}

impl<S: Stream> WindowedStream<S> {
    pub fn new(inner_stream: S, config: WindowConfig) -> Self {
        Self {
            inner_stream: Box::pin(inner_stream),
            config,
            buffer: VecDeque::new(),
            window_start: None,
        }
    }

    fn current_window_end(&self) -> Option<Instant> {
        self.window_start.map(|start| start + self.config.duration)
    }

    fn should_emit_window(&self, now: Instant) -> bool {
        if let Some(window_end) = self.current_window_end() {
            now >= window_end
        } else {
            false
        }
    }

    fn emit_current_window(&mut self) -> Option<Vec<S::Item>> {
        if self.buffer.is_empty() {
            return None;
        }

        let window_end = self.current_window_end()?;
        let mut window_items = Vec::new();

        // Collect items that belong to the current window
        while let Some(front) = self.buffer.front() {
            if front.timestamp < window_end {
                let item = self.buffer.pop_front().unwrap();
                window_items.push(item.item);
            } else {
                break;
            }
        }

        // Start next window if we have more items
        if !self.buffer.is_empty() {
            self.window_start = Some(self.buffer.front().unwrap().timestamp);
        } else {
            self.window_start = None;
        }

        if window_items.is_empty() {
            None
        } else {
            Some(window_items)
        }
    }
}

pub trait WindowExt: Stream {
    fn window(self, config: TumblingWindow) -> WindowedStream<Self>
    where
        Self: Sized,
    {
        WindowedStream::new(
            self,
            WindowConfig {
                window_type: WindowType::Tumbling,
                duration: config.duration,
                slide: None,
            },
        )
    }
}

impl<S: Stream + Sized> WindowExt for S {}
