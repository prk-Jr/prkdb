//! Stream joining for PrkDB - Simplified working implementation.

use futures::Stream;
use pin_project::pin_project;
use prkdb_types::{collection::Collection, consumer::ConsumerRecord, error::Error};
use std::collections::HashMap;
use std::hash::Hash;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

type JoinedResult<L, R> = Result<(L, Option<R>), Error>;

/// Defines the type of join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Outer,
}

/// Configuration for a stream join.
#[derive(Debug, Clone)]
pub struct JoinConfig {
    pub join_type: JoinType,
    /// The time window within which events from both streams are considered for a join.
    pub window: Duration,
}

/// Simple joined stream that collects all data and then emits joins
#[pin_project]
pub struct JoinedStream<S1, S2, K, F1, F2>
where
    S1: Stream,
    S2: Stream,
    K: Hash + Eq + Clone,
    F1: Fn(&S1::Item) -> K,
    F2: Fn(&S2::Item) -> K,
    S1::Item: Clone,
    S2::Item: Clone,
{
    left_items: Option<Vec<S1::Item>>,
    right_items: Option<Vec<S2::Item>>,
    #[pin]
    left_stream: S1,
    #[pin]
    right_stream: S2,
    config: JoinConfig,
    left_key_fn: F1,
    right_key_fn: F2,
    result_index: usize,
    computed_results: Option<Vec<JoinedResult<S1::Item, S2::Item>>>,
}

impl<S1, S2, K, F1, F2> JoinedStream<S1, S2, K, F1, F2>
where
    S1: Stream,
    S2: Stream,
    K: Hash + Eq + Clone,
    F1: Fn(&S1::Item) -> K,
    F2: Fn(&S2::Item) -> K,
    S1::Item: Clone,
    S2::Item: Clone,
{
    pub fn new(
        left_stream: S1,
        right_stream: S2,
        config: JoinConfig,
        left_key_fn: F1,
        right_key_fn: F2,
    ) -> Self {
        Self {
            left_items: None,
            right_items: None,
            left_stream,
            right_stream,
            config,
            left_key_fn,
            right_key_fn,
            result_index: 0,
            computed_results: None,
        }
    }
}

impl<S1, S2, K, F1, F2> Stream for JoinedStream<S1, S2, K, F1, F2>
where
    S1: Stream,
    S2: Stream,
    K: Hash + Eq + Clone,
    F1: Fn(&S1::Item) -> K,
    F2: Fn(&S2::Item) -> K,
    S1::Item: Clone,
    S2::Item: Clone,
{
    type Item = JoinedResult<S1::Item, S2::Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // First, collect all items from both streams if we haven't yet
        if this.left_items.is_none() {
            let mut all_left = Vec::new();
            loop {
                match this.left_stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => all_left.push(item),
                    Poll::Ready(None) => {
                        *this.left_items = Some(all_left);
                        break;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
        }

        if this.right_items.is_none() {
            let mut all_right = Vec::new();
            loop {
                match this.right_stream.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => all_right.push(item),
                    Poll::Ready(None) => {
                        *this.right_items = Some(all_right);
                        break;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
        }

        // Now compute the joins if not done yet
        if this.computed_results.is_none() {
            let left_items = this.left_items.as_ref().unwrap();
            let right_items = this.right_items.as_ref().unwrap();

            // Build a hashmap for the right stream (only for successful records)
            let mut right_map: HashMap<K, Vec<&S2::Item>> = HashMap::new();
            for item in right_items {
                let key = (this.right_key_fn)(item);
                right_map.entry(key).or_default().push(item);
            }

            let mut results = Vec::new();

            for left_item in left_items {
                let left_key = (this.left_key_fn)(left_item);

                match this.config.join_type {
                    JoinType::Inner => {
                        if let Some(right_matches) = right_map.get(&left_key) {
                            for right_item in right_matches {
                                results.push(Ok((left_item.clone(), Some((*right_item).clone()))));
                            }
                        }
                    }
                    JoinType::Left => {
                        if let Some(right_matches) = right_map.get(&left_key) {
                            for right_item in right_matches {
                                results.push(Ok((left_item.clone(), Some((*right_item).clone()))));
                            }
                        } else {
                            results.push(Ok((left_item.clone(), None)));
                        }
                    }
                    JoinType::Outer => {
                        // Simplified outer join - just like left join
                        if let Some(right_matches) = right_map.get(&left_key) {
                            for right_item in right_matches {
                                results.push(Ok((left_item.clone(), Some((*right_item).clone()))));
                            }
                        } else {
                            results.push(Ok((left_item.clone(), None)));
                        }
                    }
                }
            }

            *this.computed_results = Some(results);
        }

        // Return the next result
        if let Some(results) = this.computed_results {
            if *this.result_index < results.len() {
                let result = results[*this.result_index].clone();
                *this.result_index += 1;
                Poll::Ready(Some(result))
            } else {
                Poll::Ready(None)
            }
        } else {
            Poll::Ready(None)
        }
    }
}

pub trait JoinExt<C1: Collection>: Stream<Item = Result<ConsumerRecord<C1>, Error>> {
    fn left_join<S2, F1, F2, K, C2>(
        self,
        other: S2,
        f1: F1,
        f2: F2,
        window: Duration,
    ) -> JoinedStream<Self, S2, K, F1, F2>
    where
        Self: Sized,
        S2: Stream<Item = Result<ConsumerRecord<C2>, Error>> + Sized,
        C2: Collection,
        F1: Fn(&Result<ConsumerRecord<C1>, Error>) -> K,
        F2: Fn(&Result<ConsumerRecord<C2>, Error>) -> K,
        K: Eq + Hash + Clone,
        Result<ConsumerRecord<C1>, Error>: Clone,
        Result<ConsumerRecord<C2>, Error>: Clone,
    {
        JoinedStream::new(
            self,
            other,
            JoinConfig {
                join_type: JoinType::Left,
                window,
            },
            f1,
            f2,
        )
    }

    fn inner_join<S2, F1, F2, K, C2>(
        self,
        other: S2,
        f1: F1,
        f2: F2,
        window: Duration,
    ) -> JoinedStream<Self, S2, K, F1, F2>
    where
        Self: Sized,
        S2: Stream<Item = Result<ConsumerRecord<C2>, Error>> + Sized,
        C2: Collection,
        F1: Fn(&Result<ConsumerRecord<C1>, Error>) -> K,
        F2: Fn(&Result<ConsumerRecord<C2>, Error>) -> K,
        K: Eq + Hash + Clone,
        Result<ConsumerRecord<C1>, Error>: Clone,
        Result<ConsumerRecord<C2>, Error>: Clone,
    {
        JoinedStream::new(
            self,
            other,
            JoinConfig {
                join_type: JoinType::Inner,
                window,
            },
            f1,
            f2,
        )
    }
}

impl<C: Collection, S> JoinExt<C> for S where S: Stream<Item = Result<ConsumerRecord<C>, Error>> {}
