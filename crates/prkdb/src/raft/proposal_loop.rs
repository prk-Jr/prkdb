use lazy_static::lazy_static;
use prometheus::{register_histogram, register_int_counter, Histogram, IntCounter};

lazy_static! {
    static ref PROPOSAL_BATCHES_TOTAL: IntCounter = register_int_counter!(
        "raft_proposal_batches_total",
        "Total number of proposal batches processed"
    )
    .unwrap();
    static ref PROPOSAL_BATCH_SIZE: Histogram =
        register_histogram!("raft_proposal_batch_size", "Size of each proposal batch").unwrap();
    static ref PROPOSAL_LATENCY: Histogram = register_histogram!(
        "raft_proposal_latency_seconds",
        "Proposal processing latency"
    )
    .unwrap();
}

async fn run_proposal_loop(
    self: Arc<Self>,
    mut rx: mpsc::Receiver<(Vec<u8>, oneshot::Sender<Result<u64, RaftError>>)>,
) {
    let max_batch_size = 1000;
    let linger_time = std::time::Duration::from_millis(10);

    let mut batch = Vec::with_capacity(max_batch_size);

    loop {
        // Collect batch
        let size = batch.len();
        if size < max_batch_size {
            let timeout = if size > 0 {
                linger_time
            } else {
                std::time::Duration::from_secs(3600)
            };

            tokio::select! {
                res = rx.recv() => {
                    match res {
                        Some(req) => batch.push(req),
                        None => return, // Channel closed
                    }
                }
                _ = tokio::time::sleep(timeout), if size > 0 => {
                    // Linger timeout, flush batch
                }
            }

            // If we just got one and buffer is not full, loop again to try to get more immediately
            // without waiting? No, select handles it.
            // But if we received one, we want to try to receive more *immediately* if available,
            // up to max_batch_size, before waiting/lingering.
            // The current loop structure with select will wait again.
            // To drain channel efficiently:
            while batch.len() < max_batch_size {
                match rx.try_recv() {
                    Ok(req) => batch.push(req),
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => return,
                }
            }
        }

        if batch.is_empty() {
            continue;
        }

        // Measure batch processing time
        let batch_start = std::time::Instant::now();

        // Process batch
        let current_batch = std::mem::take(&mut batch);
        let batch_size = current_batch.len();

        // Record batch metrics
        PROPOSAL_BATCHES_TOTAL.inc();
        PROPOSAL_BATCH_SIZE.observe(batch_size as f64);

        // 1. Append to log (holding lock once)
        let term = *self.current_term.read().await;
        let mut log = self.log.write().await;
        let start_index = log.len() as u64 + 1;

        let mut senders = Vec::with_capacity(batch_size);
        let mut batch_data = Vec::with_capacity(batch_size);

        for (i, (data, tx)) in current_batch.into_iter().enumerate() {
            let index = start_index + i as u64;
            let entry = LogEntry {
                term,
                index,
                data: data.clone(),
            };
            log.push(entry);

            batch_data.push(data);
            senders.push((tx, index));
        }
        drop(log);

        tracing::debug!(
            "Appended batch of {} entries starting at index {} (avg batch size: {:.1})",
            batch_size,
            start_index,
            PROPOSAL_BATCH_SIZE.get_sample_sum() / PROPOSAL_BATCHES_TOTAL.get() as f64
        );

        // 2. Trigger replication immediately (Pipeline)
        self.replication_trigger.notify_waiters();

        // 3. BATCHED WAL persistence (MASSIVE PERFORMANCE WIN!)
        match self.storage.append_raft_entries_batch(&batch_data).await {
            Ok(_offsets) => {
                // All succeeded, respond to all clients
                for (tx, index) in senders {
                    let _ = tx.send(Ok(index));
                }
            }
            Err(e) => {
                // All failed, notify all clients
                for (tx, _) in senders {
                    let _ = tx.send(Err(RaftError::Storage(e.clone())));
                }
            }
        }

        // Record total latency
        let elapsed = batch_start.elapsed();
        PROPOSAL_LATENCY.observe(elapsed.as_secs_f64());

        if batch_size > 1 {
            tracing::info!(
                "Batch processed: {} proposals in {:.2}ms ({:.0} ops/sec)",
                batch_size,
                elapsed.as_secs_f64() * 1000.0,
                batch_size as f64 / elapsed.as_secs_f64()
            );
        }
    }
}
