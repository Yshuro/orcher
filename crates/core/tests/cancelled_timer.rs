//! A cancelled timer does not fire.
//!
//! CancelTimer used to journal TimerCanceled and nothing else, and the fire
//! checked only whether the run had ended: a workflow that cancelled a
//! one-hour timeout timer after its task won had TimerFired journaled after
//! the cancellation an hour later, and was woken for a replay it did not need
//! (orcher-io/orcher#52). The cancel now cancels the timer's row too, and a
//! fire refuses a timer whose cancellation is journaled, which covers a row
//! the timer processor had already claimed when the cancel came.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test cancelled_timer -- --ignored --test-threads=1
//! ```

use orcher_core::engine::dispatch::PollingManager;
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::queues::TimerTask;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::services::TimerExecutionService;
use orcher_core::workflows::ChildWorkflowManager;
use orcher_core::workflows::WorkflowExecutionService;
use orcher_core::workflows::{CommandProcessor, JournalService};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::{
    command, CancelTimerCommandAttributes, Command, CommandType, StartTimerCommandAttributes,
};
use orcher_storage::repositories::domains::{
    child_workflow::ChildWorkflowRepository, coordination::OutboxRepository,
    journal::JournalRepository, task_execution::TaskExecutionRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use orcher_storage::repositories::PostgresExecutionLogRepository;
use orcher_storage::schema::types::coordination::TimerTaskType;
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

const NAMESPACE: &str = "default";

struct Engine {
    pool: PgPool,
    commands: CommandProcessor,
    timers: TimerExecutionService,
}

impl Engine {
    async fn new() -> Self {
        let database_url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
        });
        let pool = PgPool::connect(&database_url)
            .await
            .expect("connect to the test database");
        orcher_storage::setup::run_migrations(&pool)
            .await
            .expect("apply migrations");

        let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
        let journal_service = Arc::new(JournalService::new(journal_repo.clone()));
        let exec_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let outbox_queue = Arc::new(OutboxQueue::new(Arc::new(OutboxRepository::new(Arc::new(
            pool.clone(),
        )))));
        let event_bus = EventBus::new(EventBusConfig::default());
        let workflow_execution_service = Arc::new(WorkflowExecutionService::new(
            exec_repo.clone(),
            journal_service.clone(),
            outbox_queue,
            event_bus.clone(),
            pool.clone(),
        ));
        let child_workflow_manager = Arc::new(ChildWorkflowManager::new(
            Arc::new(ChildWorkflowRepository::new(pool.clone())),
            journal_service.clone(),
            workflow_execution_service,
        ));
        let coordinator = Arc::new(
            WorkflowCoordinator::new(
                Arc::new(pool.clone()),
                orcher_core::engine::CoordinatorConfig::default(),
            )
            .await
            .expect("create coordinator"),
        );
        let polling_manager = Arc::new(PollingManager::new(
            exec_repo.clone(),
            journal_repo.clone(),
            300,
        ));
        let commands = CommandProcessor::new(
            journal_service.clone(),
            child_workflow_manager,
            event_bus,
            Arc::new(PostgresExecutionLogRepository::new(pool.clone())),
            Arc::new(TaskExecutionRepository::new(pool.clone())),
            exec_repo.clone(),
            Arc::new(pool.clone()),
        )
        .with_coordinator(coordinator)
        .with_polling_manager(polling_manager.clone());
        let timers = TimerExecutionService::new(exec_repo, journal_service, polling_manager);

        Self {
            pool,
            commands,
            timers,
        }
    }

    /// A running run, and its execution id.
    async fn run(&self) -> (Uuid, String) {
        let id = Uuid::new_v4();
        let execution_id = format!("cancelled-timer-{id}");
        sqlx::query(
            r#"INSERT INTO workflow_executions
                 (id, execution_id, workflow_id, workflow_name, workflow_type, workflow_version,
                  namespace, task_queue, status, trigger_type, submitted_at, created_at,
                  updated_at)
               VALUES ($1, $2, $1, 'cancelled-timer', 'cancelled-timer', '1.0.0', $3,
                       'cancelled-timer-q', 'running', 'api', NOW(), NOW(), NOW())"#,
        )
        .bind(id)
        .bind(&execution_id)
        .bind(NAMESPACE)
        .execute(&self.pool)
        .await
        .expect("seed a run");
        (id, execution_id)
    }

    async fn send(&self, id: Uuid, execution_id: &str, command: Command) {
        self.commands
            .process_commands(&id.to_string(), execution_id, NAMESPACE, vec![command])
            .await
            .expect("process the command");
    }

    async fn fire(&self, id: Uuid, timer_id: &str) {
        let task = TimerTask::new(
            id,
            TimerTaskType::FireTimer,
            serde_json::json!({ "timer_name": timer_id }),
            orcher_common::types::time::Timestamptz::now(),
        );
        self.timers.fire_timer(task).await.expect("fire the timer");
    }

    async fn row_status(&self, id: Uuid, timer_id: &str) -> String {
        sqlx::query_scalar(
            "SELECT status FROM timer_queue \
             WHERE workflow_id = $1 AND payload->>'timer_name' = $2",
        )
        .bind(id)
        .bind(timer_id)
        .fetch_one(&self.pool)
        .await
        .expect("read the timer's row")
    }

    async fn journaled(&self, execution_id: &str, event_type: &str, timer_id: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM workflow_execution_journal \
             WHERE execution_id = $1 AND event_type = $2 AND payload->>'timer_id' = $3",
        )
        .bind(execution_id)
        .bind(event_type)
        .bind(timer_id)
        .fetch_one(&self.pool)
        .await
        .expect("count journal entries")
    }

    async fn cleanup(&self, id: Uuid, execution_id: &str) {
        for statement in [
            "DELETE FROM timer_queue WHERE workflow_id = $1",
            "DELETE FROM workflow_executions WHERE id = $1",
        ] {
            sqlx::query(statement)
                .bind(id)
                .execute(&self.pool)
                .await
                .expect("clean up");
        }
        sqlx::query("DELETE FROM workflow_execution_journal WHERE execution_id = $1")
            .bind(execution_id)
            .execute(&self.pool)
            .await
            .expect("clean up the journal");
    }
}

fn start_timer(timer_id: &str, secs: i64) -> Command {
    Command {
        command_type: CommandType::StartTimer as i32,
        attributes: Some(command::Attributes::StartTimer(
            StartTimerCommandAttributes {
                timer_id: timer_id.to_string(),
                start_to_fire_timeout: Some(prost_types::Duration {
                    seconds: secs,
                    nanos: 0,
                }),
            },
        )),
    }
}

fn cancel_timer(timer_id: &str) -> Command {
    Command {
        command_type: CommandType::CancelTimer as i32,
        attributes: Some(command::Attributes::CancelTimer(
            CancelTimerCommandAttributes {
                timer_id: timer_id.to_string(),
            },
        )),
    }
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_cancelled_timer_is_cancelled_where_it_is_scheduled() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.run().await;

    engine
        .send(id, &execution_id, start_timer("timeout", 3_600))
        .await;
    assert_eq!(engine.row_status(id, "timeout").await, "pending");
    engine
        .send(id, &execution_id, cancel_timer("timeout"))
        .await;
    assert_eq!(
        engine.row_status(id, "timeout").await,
        "cancelled",
        "the cancelled timer's row is still waiting to fire"
    );

    engine.cleanup(id, &execution_id).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_fire_after_a_journaled_cancel_records_nothing() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.run().await;

    // The timer processor claimed the row as it fell due, the workflow
    // cancelled the timer, and the claimed fire ran after the cancellation.
    engine
        .send(id, &execution_id, start_timer("raced", 0))
        .await;
    engine.send(id, &execution_id, cancel_timer("raced")).await;
    engine.fire(id, "raced").await;
    assert_eq!(
        engine.journaled(&execution_id, "TimerFired", "raced").await,
        0,
        "a timer the workflow cancelled was journaled as fired"
    );
    assert_eq!(
        engine
            .journaled(&execution_id, "TimerCanceled", "raced")
            .await,
        1
    );

    engine.cleanup(id, &execution_id).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_timer_not_cancelled_fires_once() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.run().await;

    engine
        .send(id, &execution_id, start_timer("sleep", 0))
        .await;
    engine.fire(id, "sleep").await;
    // A fire retried after it was journaled, as when its row's completion
    // was not recorded.
    engine.fire(id, "sleep").await;
    assert_eq!(
        engine.journaled(&execution_id, "TimerFired", "sleep").await,
        1,
        "a timer was not journaled as fired exactly once"
    );

    engine.cleanup(id, &execution_id).await;
}
