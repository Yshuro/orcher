//! The outbox processor's reads do not grow with a workflow's backlog.
//!
//! After every batch of a workflow's rows, the processor read every pending
//! row of that workflow, payloads and all, to count them: none pending meant
//! it could forget the workflow's in-memory lock. A workflow whose rows were
//! made faster than they left — a fan-out of N tasks, behind slow workers, a
//! busy database, or dispatches waiting for polls — had its backlog of up to N
//! rows read once per batch, so the reads grew with the square of the
//! fan-out (orcher-io/orcher#21). The lock is now freed by the batches that
//! hold it, and nothing is read for it.
//!
//! The test gives one workflow a backlog of dispatches this processor never
//! claims (the task pollers do), and rows it does handle, one per batch. It
//! counts the rows of `outbox_queue` the processor's connections read. The
//! statistics are reported when those connections end, so the processor gets
//! a pool of its own, closed before the count is read. Measured, the processor
//! reads about five rows per row it handles, the same with a backlog of 0,
//! 2,000 or 4,000. Before, it read at least the backlog once per batch,
//! 80,000 rows here, and over a million once every batch's reads were in.
//!
//! Requires PostgreSQL 15 or later. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test outbox_backlog_reads -- --ignored --test-threads=1
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use orcher_common::error::OrcherResult;
use orcher_core::engine::processors::{OutboxProcessor, OutboxProcessorConfig};
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::queues::OutboxTask;
use orcher_storage::repositories::domains::coordination::{
    CoordinationRepository, OutboxRepository,
};
use sqlx::PgPool;
use uuid::Uuid;

/// Dispatches waiting for a poll: the backlog.
const BACKLOG: usize = 2_000;
/// Rows the processor handles, each in a batch of its own.
const HANDLED: usize = 40;

fn database_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string())
}

async fn pool(max_connections: u32) -> PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&database_url())
        .await
        .expect("connect to the test database")
}

/// Rows of `outbox_queue` read so far, by index and by scan, as the server's
/// statistics have them.
async fn rows_read(pool: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(idx_tup_fetch, 0) + COALESCE(seq_tup_read, 0) \
         FROM pg_stat_user_tables WHERE relname = 'outbox_queue'",
    )
    .fetch_one(pool)
    .await
    .expect("read the outbox statistics")
}

/// [`rows_read`] once it has stopped moving: a backend reports what it read
/// when it ends, a moment after it closes.
async fn settled_rows_read(pool: &PgPool) -> i64 {
    let mut read = rows_read(pool).await;
    let mut steady = 0;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let now = rows_read(pool).await;
        steady = if now == read { steady + 1 } else { 0 };
        read = now;
        if steady == 3 {
            return read;
        }
    }
    panic!("the outbox statistics never settled");
}

/// Insert `count` pending rows of `task_type` for `workflow`, oldest first.
async fn rows(pool: &PgPool, workflow: Uuid, task_type: &str, count: usize) {
    sqlx::query(
        r#"
        INSERT INTO outbox_queue
            (task_id, workflow_id, task_type, payload, idempotency_key, status,
             created_at, attempt_count, max_retries, backoff_multiplier)
        SELECT id, $1, $2,
               jsonb_build_object('task_queue', 'backlog-q', 'namespace', 'default',
                                  'execution_id', id::text, 'padding', repeat('x', 512)),
               id::text, 'pending', NOW() - make_interval(secs => $3 - n), 0, 3, 2.0
        FROM (SELECT gen_random_uuid() AS id, n FROM generate_series(1, $3) AS n) r
        "#,
    )
    .bind(workflow)
    .bind(task_type)
    .bind(count as i32)
    .execute(pool)
    .await
    .expect("seed outbox rows");
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_workflows_backlog_is_not_read_once_per_batch() {
    let setup = pool(2).await;
    orcher_storage::setup::run_migrations(&setup)
        .await
        .expect("apply migrations");
    // Nothing else in the outbox for the processor to take.
    sqlx::query("DELETE FROM outbox_queue")
        .execute(&setup)
        .await
        .expect("empty the outbox");

    let workflow = Uuid::new_v4();
    rows(&setup, workflow, "dispatch_task", BACKLOG).await;
    rows(&setup, workflow, "dispatch_step", HANDLED).await;
    // The planner is told what the outbox now holds. Its statistics were
    // last taken of an outbox an earlier test had emptied, and a claim
    // planned for an empty outbox reads every pending row it holds, on each
    // claim, whatever the processor does: over a million rows here, when
    // autovacuum had not yet looked at the table, and a few thousand when it
    // looked partway through (orcher-io/orcher#61). What is measured is the
    // processor, not how recently the table was analyzed.
    sqlx::query("ANALYZE outbox_queue")
        .execute(&setup)
        .await
        .expect("analyze the outbox");

    // The setup's own reads, its emptying the outbox among them, are
    // reported as its connections end; the count starts once they are in.
    setup.close().await;
    let observer = pool(1).await;
    let before = settled_rows_read(&observer).await;

    let processor_pool = Arc::new(pool(4).await);
    let outbox = OutboxQueue::new(Arc::new(OutboxRepository::new(processor_pool.clone())));
    let processor = Arc::new(OutboxProcessor::new(
        outbox,
        Arc::new(CoordinationRepository::new(processor_pool.clone())),
        OutboxProcessorConfig {
            batch_size: 1,
            polling_interval: Duration::from_millis(5),
            cleanup_interval: Duration::from_secs(3_600),
            ..Default::default()
        },
    ));
    let handled = Arc::new(AtomicUsize::new(0));
    let counter = handled.clone();
    processor
        .clone()
        .start(move |_task: OutboxTask| {
            let counter = counter.clone();
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
                as std::pin::Pin<Box<dyn std::future::Future<Output = OrcherResult<()>> + Send>>
        })
        .await
        .expect("start the processor");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while handled.load(Ordering::SeqCst) < HANDLED {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the processor handled {} of {HANDLED} rows",
            handled.load(Ordering::SeqCst)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Let the last batch finish what follows its row: the mark, the lock.
    tokio::time::sleep(Duration::from_millis(300)).await;
    processor.shutdown().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    processor_pool.close().await;

    // The processor's backends report their reads as they exit.
    let read = settled_rows_read(&observer).await - before;
    assert!(
        read > 0,
        "the statistics never showed the processor's reads"
    );

    // Each handled row costs its claim and its mark, a handful of rows read
    // whatever the backlog. Reading the backlog once per batch would cost
    // HANDLED × BACKLOG = 80,000.
    let bound = (HANDLED * 25) as i64;
    assert!(
        read < bound,
        "the processor read {read} outbox rows to handle {HANDLED} rows beside a \
         backlog of {BACKLOG}; at most {bound} were expected"
    );

    sqlx::query("DELETE FROM outbox_queue WHERE workflow_id = $1")
        .bind(workflow)
        .execute(&observer)
        .await
        .expect("clean up");
}
