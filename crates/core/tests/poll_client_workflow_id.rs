//! A workflow poll names the run by the workflow id its users know it by.
//!
//! A poll's `workflow_id` is the engine's own id for the run, a UUID, which a
//! worker sends back with its completion. A worker had nothing else to name
//! the run by, so its logs and errors (a non-determinism report among them)
//! named a run started as `order-1001` by a UUID nobody had seen
//! (orcher-io/orcher#4). A poll now also carries `client_workflow_id`: the id
//! the run was started with, or, for a run started without one, the id its
//! start answered with, which is its `execution_id`.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test poll_client_workflow_id -- --ignored --test-threads=1
//! ```

use std::sync::Arc;

use orcher_core::engine::dispatch::{PollingManager, TaskPollingManager};
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::ExecutionService;
use orcher_core::workflows::{
    ChildWorkflowManager, CommandProcessor, JournalService, RunStart, RunTimeouts,
    WorkflowExecutionService,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::{PollWorkflowExecutionRequest, PollWorkflowExecutionResponse};
use orcher_storage::repositories::domains::{
    child_workflow::ChildWorkflowRepository, coordination::OutboxRepository,
    journal::JournalRepository, task_execution::TaskExecutionRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use orcher_storage::repositories::{ExecutionLogRepository, PostgresExecutionLogRepository};
use orcher_storage::schema::types::execution::WorkflowExecution;
use sqlx::PgPool;
use tonic::Request;
use uuid::Uuid;

const NAMESPACE: &str = "default";
const WORKER: &str = "client-id-worker";

async fn pool() -> PgPool {
    let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
    });
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the test database");
    static MIGRATED: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
    MIGRATED
        .get_or_init(|| async {
            orcher_storage::setup::run_migrations(&pool)
                .await
                .expect("apply migrations");
        })
        .await;
    pool
}

struct Engine {
    starts: WorkflowExecutionService,
    execution: ExecutionService,
}

async fn engine(pool: &PgPool) -> Engine {
    let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
    let journal_service = Arc::new(JournalService::new(journal_repo.clone()));
    let exec_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    let outbox_queue = Arc::new(OutboxQueue::new(Arc::new(OutboxRepository::new(Arc::new(
        pool.clone(),
    )))));
    let event_bus = EventBus::new(EventBusConfig::default());
    let execution_log_repo = Arc::new(PostgresExecutionLogRepository::new(pool.clone()));
    let task_execution_repo = Arc::new(TaskExecutionRepository::new(pool.clone()));
    let polling_manager = Arc::new(PollingManager::new(
        exec_repo.clone(),
        journal_repo.clone(),
        300,
    ));
    let starts = WorkflowExecutionService::new(
        exec_repo.clone(),
        journal_service.clone(),
        outbox_queue.clone(),
        event_bus.clone(),
        pool.clone(),
    )
    .with_polling_manager(polling_manager.clone());
    let child_workflow_manager = Arc::new(ChildWorkflowManager::new(
        Arc::new(ChildWorkflowRepository::new(pool.clone())),
        journal_service.clone(),
        Arc::new(WorkflowExecutionService::new(
            exec_repo.clone(),
            journal_service.clone(),
            outbox_queue.clone(),
            event_bus.clone(),
            pool.clone(),
        )),
    ));
    let coordinator = Arc::new(
        WorkflowCoordinator::new(
            Arc::new(pool.clone()),
            orcher_core::engine::CoordinatorConfig::default(),
        )
        .await
        .expect("create coordinator"),
    );
    let command_processor = CommandProcessor::new(
        journal_service,
        child_workflow_manager,
        event_bus,
        execution_log_repo.clone(),
        task_execution_repo.clone(),
        exec_repo.clone(),
        Arc::new(pool.clone()),
    )
    .with_coordinator(coordinator)
    .with_polling_manager(polling_manager.clone());
    let execution = ExecutionService::new(
        Arc::new(command_processor),
        polling_manager,
        execution_log_repo as Arc<dyn ExecutionLogRepository>,
        journal_repo.clone(),
        exec_repo.clone(),
        task_execution_repo.clone(),
        pool.clone(),
    )
    .with_task_polling_manager(Arc::new(TaskPollingManager::new(outbox_queue)));
    Engine { starts, execution }
}

async fn activation(engine: &Engine, task_queue: &str) -> PollWorkflowExecutionResponse {
    let activation = engine
        .execution
        .poll_workflow_execution(Request::new(PollWorkflowExecutionRequest {
            task_queue: task_queue.to_string(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            poll_timeout: Some(orcher_proto::prost_types::Duration {
                seconds: 10,
                nanos: 0,
            }),
            ..Default::default()
        }))
        .await
        .expect("poll for the workflow")
        .into_inner();
    assert!(
        !activation.execution_id.is_empty(),
        "the workflow was not handed out"
    );
    activation
}

async fn start(
    engine: &Engine,
    task_queue: &str,
    workflow_id: Option<String>,
) -> WorkflowExecution {
    engine
        .starts
        .start_workflow(
            RunStart {
                workflow_name: "orders".to_string(),
                workflow_version: "1.0".to_string(),
                namespace: NAMESPACE.to_string(),
                task_queue: task_queue.to_string(),
                input: serde_json::Value::Null,
                workflow_id,
                labels: None,
                max_retries: None,
                retry_policy: None,
                timeouts: RunTimeouts::default(),
                annotations: None,
                start_delay: None,
                request_id: None,
            },
            None,
            None,
        )
        .await
        .expect("start a run")
}

async fn cleanup(pool: &PgPool, task_queue: &str) {
    for q in [
        "DELETE FROM workflow_execution_journal WHERE execution_id IN \
         (SELECT execution_id FROM workflow_executions WHERE task_queue = $1)",
        "DELETE FROM workflow_execution_logs WHERE execution_id IN \
         (SELECT execution_id FROM workflow_executions WHERE task_queue = $1)",
        "DELETE FROM execution_log WHERE workflow_id IN \
         (SELECT id FROM workflow_executions WHERE task_queue = $1)",
        "DELETE FROM outbox_queue WHERE workflow_id IN \
         (SELECT id FROM workflow_executions WHERE task_queue = $1)",
        "DELETE FROM workflow_executions WHERE task_queue = $1",
    ] {
        let _ = sqlx::query(q).bind(task_queue).execute(pool).await;
    }
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_poll_names_a_run_by_the_id_it_was_started_with() {
    let pool = pool().await;
    let engine = engine(&pool).await;
    let task_queue = format!("client-id-{}", Uuid::new_v4().simple());
    let named = format!("order-{}", Uuid::new_v4().simple());

    let run = start(&engine, &task_queue, Some(named.clone())).await;
    let polled = activation(&engine, &task_queue).await;

    assert_eq!(polled.execution_id, run.execution_id);
    assert_eq!(
        polled.client_workflow_id, named,
        "the poll names the run by the id it was started with"
    );
    assert_eq!(
        polled.workflow_id,
        run.id.as_uuid().to_string(),
        "workflow_id stays the engine's own id, which completions send back"
    );
    assert_ne!(polled.workflow_id, polled.client_workflow_id);

    cleanup(&pool, &task_queue).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_run_started_without_an_id_is_named_by_its_execution_id() {
    let pool = pool().await;
    let engine = engine(&pool).await;
    let task_queue = format!("client-id-{}", Uuid::new_v4().simple());

    let run = start(&engine, &task_queue, None).await;
    assert!(run.workflow_id.is_none());
    let polled = activation(&engine, &task_queue).await;

    assert_eq!(polled.execution_id, run.execution_id);
    assert_eq!(
        polled.client_workflow_id, run.execution_id,
        "a run started without an id is named by the id its start answered with"
    );

    cleanup(&pool, &task_queue).await;
}
