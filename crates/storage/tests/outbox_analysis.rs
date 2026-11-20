//! The outbox is analyzed after a thousand changes, however large it is.
//!
//! The claims are planned from the outbox's statistics. Autovacuum analyzes a
//! table once a tenth of it has changed, so a burst of pending rows on a large
//! outbox was never analyzed: every claim was planned for an outbox with
//! nothing pending and walked the whole burst to claim one row, 12,000 buffers
//! where 10 are enough with 45,000 pending beside 500,000 retained
//! (orcher-io/orcher#63). V089 makes a thousand changes enough at any size.
//!
//! Autovacuum acts on its own schedule, a minute apart by default, so this
//! reads the rule it decides by, the table's options over the server's
//! defaults, rather than waiting on it: a table is analyzed once more rows
//! have changed than `autovacuum_analyze_threshold +
//! autovacuum_analyze_scale_factor × rows`.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-storage --test outbox_analysis -- --ignored --test-threads=1
//! ```

use sqlx::PgPool;

fn database_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string())
}

/// Changed rows after which autovacuum analyzes `table` when it holds `rows`.
async fn changes_before_analysis(pool: &PgPool, table: &str, rows: f64) -> f64 {
    sqlx::query_scalar(
        r#"
        WITH options AS (
            SELECT option_name, option_value
            FROM pg_class c, pg_options_to_table(c.reloptions)
            WHERE c.oid = $1::regclass
        )
        SELECT COALESCE((SELECT option_value FROM options
                         WHERE option_name = 'autovacuum_analyze_threshold'),
                        current_setting('autovacuum_analyze_threshold'))::float8
             + COALESCE((SELECT option_value FROM options
                         WHERE option_name = 'autovacuum_analyze_scale_factor'),
                        current_setting('autovacuum_analyze_scale_factor'))::float8 * $2
        "#,
    )
    .bind(table)
    .bind(rows)
    .fetch_one(pool)
    .await
    .expect("read the analysis threshold")
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_burst_on_a_large_outbox_is_due_for_analysis() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    orcher_storage::setup::run_migrations(&pool)
        .await
        .expect("apply migrations");

    let autovacuumed: bool = sqlx::query_scalar(
        "SELECT COALESCE((SELECT option_value::bool FROM pg_class c, \
         pg_options_to_table(c.reloptions) WHERE c.oid = 'outbox_queue'::regclass \
         AND option_name = 'autovacuum_enabled'), true)",
    )
    .fetch_one(&pool)
    .await
    .expect("read whether autovacuum covers the outbox");
    assert!(autovacuumed, "autovacuum is turned off for the outbox");

    for rows in [0.0, 500_000.0, 10_000_000.0] {
        let due = changes_before_analysis(&pool, "outbox_queue", rows).await;
        assert!(
            due <= 1_000.0,
            "an outbox of {rows} rows is analyzed only after {due} changes; \
             a burst under that is planned for as if it were not there"
        );
    }
}
