//! Retention deletes a run's journal only once the run has ended, long ago.
//!
//! A run replays from its journal. Retention used to delete every journal
//! entry older than its period whatever the run's state, so a run that slept,
//! waited on an event or ran longer than the period lost the start of its
//! history, and its next activation failed as non-deterministic or did the
//! work again (orcher-io/orcher#44). Every journal here is older than the
//! period; what decides is the run it belongs to.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test retention_keeps_open_journals -- --ignored --test-threads=1
//! ```

use orcher_core::jobs::{data_retention, RetentionDays};
use sqlx::PgPool;
use uuid::Uuid;

fn database_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string())
}

/// Days ago, as an interval Postgres subtracts from `NOW()`.
fn days_ago(days: i32) -> String {
    format!("{days} days")
}

/// A run in `status` that started 400 days ago, ended `ended_days_ago` days
/// ago if given, with a retry scheduled if `retry_scheduled`.
async fn run(
    pool: &PgPool,
    status: &str,
    ended_days_ago: Option<i32>,
    retry_scheduled: bool,
) -> String {
    let id = Uuid::new_v4();
    let execution_id = format!("retention-{id}");
    sqlx::query(
        r#"INSERT INTO workflow_executions
             (id, execution_id, workflow_id, workflow_name, workflow_type, workflow_version,
              namespace, task_queue, status, trigger_type, submitted_at, created_at, updated_at,
              completed_at, retry_count, max_retries, next_retry_at)
           VALUES ($1, $2, $1, 'retention', 'retention', '1.0.0', 'default', 'retention-q',
                   $3, 'api', NOW() - INTERVAL '400 days', NOW() - INTERVAL '400 days', NOW(),
                   NOW() - $4::interval, 0, $5,
                   CASE WHEN $6 THEN NOW() + INTERVAL '1 hour' END)"#,
    )
    .bind(id)
    .bind(&execution_id)
    .bind(status)
    .bind(ended_days_ago.map(days_ago))
    .bind(if retry_scheduled { 3 } else { 0 })
    .bind(retry_scheduled)
    .execute(pool)
    .await
    .expect("seed a run");
    journal(pool, &id.to_string(), &execution_id).await;
    execution_id
}

/// Three journal entries, written 400, 399 and 398 days ago.
async fn journal(pool: &PgPool, workflow_id: &str, execution_id: &str) {
    sqlx::query(
        r#"INSERT INTO workflow_execution_journal
             (workflow_id, execution_id, event_id, event_type, payload, timestamp, created_at)
           SELECT $1, $2, n, 'retention_test', '{}'::jsonb,
                  NOW() - make_interval(days => 401 - n), NOW() - make_interval(days => 401 - n)
           FROM generate_series(1, 3) AS n"#,
    )
    .bind(workflow_id)
    .bind(execution_id)
    .execute(pool)
    .await
    .expect("seed a journal");
}

async fn entries(pool: &PgPool, execution_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM workflow_execution_journal WHERE execution_id = $1")
        .bind(execution_id)
        .fetch_one(pool)
        .await
        .expect("count journal entries")
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn retention_keeps_the_journal_of_a_run_that_has_not_ended_long_ago() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    orcher_storage::setup::run_migrations(&pool)
        .await
        .expect("apply migrations");
    let period = RetentionDays::default().journal_entries;
    assert!(
        period < 200,
        "the ages below assume a period under 200 days"
    );

    let running = run(&pool, "running", None, false).await;
    let sleeping = run(&pool, "waiting", None, false).await;
    let retrying = run(&pool, "failed", Some(250), true).await;
    let ended_lately = run(&pool, "completed", Some(period - 10), false).await;
    let completed = run(&pool, "completed", Some(250), false).await;
    let terminated = run(&pool, "terminated", Some(250), false).await;
    let failed = run(&pool, "failed", Some(250), false).await;
    let orphan = format!("retention-orphan-{}", Uuid::new_v4());
    journal(&pool, &Uuid::new_v4().to_string(), &orphan).await;

    data_retention::run(&pool, &RetentionDays::default())
        .await
        .expect("run retention");

    for (name, execution_id) in [
        ("a running run", &running),
        ("a run waiting on an event or a timer", &sleeping),
        ("a failed run with a retry scheduled", &retrying),
        ("a run that ended within the period", &ended_lately),
    ] {
        assert_eq!(
            entries(&pool, execution_id).await,
            3,
            "retention deleted the journal of {name}, which it replays from"
        );
    }
    let retry_kept: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM workflow_executions WHERE execution_id = $1)",
    )
    .bind(&retrying)
    .fetch_one(&pool)
    .await
    .expect("look for the retrying run");
    assert!(
        retry_kept,
        "retention deleted a failed run whose retry is scheduled"
    );
    for (name, execution_id) in [
        ("a run completed before the period", &completed),
        ("a run terminated before the period", &terminated),
        ("a run that failed for good before the period", &failed),
        ("a run that no longer exists", &orphan),
    ] {
        assert_eq!(
            entries(&pool, execution_id).await,
            0,
            "retention kept the journal of {name}"
        );
    }

    for execution_id in [
        &running,
        &sleeping,
        &retrying,
        &ended_lately,
        &completed,
        &terminated,
        &failed,
        &orphan,
    ] {
        sqlx::query("DELETE FROM workflow_execution_journal WHERE execution_id = $1")
            .bind(execution_id)
            .execute(&pool)
            .await
            .expect("clean up the journal");
        sqlx::query("DELETE FROM workflow_executions WHERE execution_id = $1")
            .bind(execution_id)
            .execute(&pool)
            .await
            .expect("clean up the run");
    }
}
