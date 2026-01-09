//! Streaming API for PrkDB
//!
//! Provides a `futures::Stream` implementation for reactive event consumption
//! with backpressure support and graceful shutdown handling.

use futures::stream::Stream;
use prkdb_core::error::Error;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::mpsc;

use prkdb_core::collection::Collection;
use prkdb_core::consumer::{AutoOffsetReset, Consumer, ConsumerConfig, ConsumerRecord};

use crate::consumer::ConsumerExt;
use crate::db::PrkDb;

/// Configuration for event streams
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Buffer size for the internal channel
    pub buffer_size: usize,
    /// Poll interval for checking new events
    pub poll_interval: Duration,
    /// Consumer group ID
    pub group_id: String,
    /// Auto offset reset strategy
    pub auto_offset_reset: AutoOffsetReset,
    /// Enable auto-commit
    pub auto_commit: bool,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            buffer_size: 1000,
            poll_interval: Duration::from_millis(100),
            group_id: format!("stream-{}", uuid::Uuid::new_v4()),
            auto_offset_reset: AutoOffsetReset::Latest,
            auto_commit: true,
        }
    }
}

impl StreamConfig {
    /// Create a new stream configuration with a specific group ID
    pub fn with_group_id(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            ..Default::default()
        }
    }
}

/// A stream of events from a collection
///
/// Implements `futures::Stream` for reactive consumption with backpressure.
///
/// # Example
///
/// ```rust,no_run
/// use prkdb::prelude::*;
/// use prkdb::streaming::{EventStream, StreamConfig};
/// use futures::StreamExt;
///
/// # #[derive(Collection, serde::Serialize, serde::Deserialize, Clone, Debug)]
/// # struct Order { #[id] id: u64, amount: f64 }
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let db = PrkDb::builder()
///     .with_storage(prkdb::storage::InMemoryAdapter::new())
///     .register_collection::<Order>()
///     .build()?;
///
/// let config = StreamConfig::default();
/// let mut stream = EventStream::<Order>::new(db, config).await?;
///
/// while let Some(result) = stream.next().await {
///     match result {
///         Ok(record) => println!("Event: {:?}", record.value),
///         Err(e) => eprintln!("Error: {}", e),
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub struct EventStream<C: Collection> {
    rx: mpsc::Receiver<Result<ConsumerRecord<C>, Error>>,
}

impl<C: Collection + 'static> EventStream<C> {
    /// Create a new event stream
    pub async fn new(db: PrkDb, config: StreamConfig) -> Result<Self, prkdb_core::error::Error> {
        let (tx, rx) = mpsc::channel(config.buffer_size);

        let consumer_config = ConsumerConfig {
            group_id: config.group_id,
            auto_offset_reset: config.auto_offset_reset,
            auto_commit: config.auto_commit,
            max_poll_records: config.buffer_size,
            ..Default::default()
        };

        let mut consumer = db.consumer::<C>(consumer_config).await?;

        tokio::spawn(async move {
            loop {
                match consumer.poll().await {
                    Ok(records) => {
                        if records.is_empty() {
                            tokio::time::sleep(config.poll_interval).await;
                            continue;
                        }

                        for record in records {
                            if tx.send(Ok(record)).await.is_err() {
                                // Receiver dropped, shutdown the task
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        if tx.send(Err(e)).await.is_err() {
                            // Receiver dropped, shutdown the task
                            return;
                        }
                        // Stop polling on critical errors
                        break;
                    }
                }
            }
        });

        Ok(Self { rx })
    }

    /// Create a new event stream with default configuration
    pub async fn with_defaults(db: PrkDb) -> Result<Self, prkdb_core::error::Error> {
        Self::new(db, StreamConfig::default()).await
    }
}

impl<C: Collection> Stream for EventStream<C>
where
    C: Unpin,
{
    type Item = Result<ConsumerRecord<C>, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().rx.poll_recv(cx)
    }
}

/// Extension trait for adding stream combinators
pub trait EventStreamExt<C: Collection>:
    Stream<Item = Result<ConsumerRecord<C>, prkdb_core::error::Error>>
{
    /// Map events to a new type
    fn map_events<F, T>(self, f: F) -> MapEvents<Self, F>
    where
        Self: Sized,
        F: FnMut(C) -> T,
    {
        MapEvents { stream: self, f }
    }

    /// Filter events based on a predicate
    fn filter_events<F>(self, f: F) -> FilterEvents<Self, F>
    where
        Self: Sized,
        F: FnMut(&C) -> bool,
    {
        FilterEvents { stream: self, f }
    }
}

impl<C: Collection, S> EventStreamExt<C> for S where
    S: Stream<Item = Result<ConsumerRecord<C>, prkdb_core::error::Error>>
{
}

/// Stream combinator for mapping events
pub struct MapEvents<S, F> {
    stream: S,
    f: F,
}

impl<S, F, C, T> Stream for MapEvents<S, F>
where
    S: Stream<Item = Result<ConsumerRecord<C>, prkdb_core::error::Error>>,
    C: Collection,
    F: FnMut(C) -> T,
{
    type Item = Result<T, prkdb_core::error::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        let stream = unsafe { Pin::new_unchecked(&mut this.stream) };

        match stream.poll_next(cx) {
            Poll::Ready(Some(Ok(record))) => Poll::Ready(Some(Ok((this.f)(record.value)))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Stream combinator for filtering events
pub struct FilterEvents<S, F> {
    stream: S,
    f: F,
}

impl<S, F, C> Stream for FilterEvents<S, F>
where
    S: Stream<Item = Result<ConsumerRecord<C>, prkdb_core::error::Error>>,
    C: Collection,
    F: FnMut(&C) -> bool,
{
    type Item = Result<ConsumerRecord<C>, prkdb_core::error::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            let stream = unsafe { Pin::new_unchecked(&mut this.stream) };
            match stream.poll_next(cx) {
                Poll::Ready(Some(Ok(record))) => {
                    if (this.f)(&record.value) {
                        return Poll::Ready(Some(Ok(record)));
                    }
                    // Continue to next iteration if filtered out
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_config_defaults() {
        let config = StreamConfig::default();
        assert_eq!(config.buffer_size, 1000);
        assert_eq!(config.poll_interval, Duration::from_millis(100));
        assert!(config.auto_commit);
    }

    #[test]
    fn stream_config_with_group_id() {
        let config = StreamConfig::with_group_id("test-group");
        assert_eq!(config.group_id, "test-group");
    }
}
