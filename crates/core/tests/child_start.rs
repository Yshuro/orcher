//! Starting a child workflow: what the parent sees when the start is refused,
//! and what happens when a start was only half recorded.
//!
//! - A child whose input is not JSON used to be started with `{}` in its place.
//!   Refusing it outright made the parent's activation fail on every replay,
//!   so a refused command fails the parent instead, with the reason.
//! - A command type the engine does not know is left to retry, since a newer
//!   engine may know it.
//! - The child's tracking record is written before its execution is created.
//!   If creating the execution failed, every replay found the record and
//!   reported the child started, and the parent waited on nothing. A replay
//!   now creates the missing execution.
//! - A child holds its workflow id in its namespace, as a top-level run does.
//!   It used to hold nothing, so children of two parents ran under one id at
//!   once and a child could take the id of an open top-level run. A child
//!   started under an id another open run holds is refused, and its parent
//!   told so as of a child that failed.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test child_start -- --ignored
//! ```

// Helpers return `tonic::Status`, the error the engine's gRPC services answer with.
#![allow(clippy::result_large_err)]

use orcher_core::engine::dispatch::PollingManager;
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::ExecutionService;
use orcher_core::workflows::ChildWorkflowManager;
use orcher_core::workflows::WorkflowExecutionService;
use orcher_core::workflows::{CommandProcessor, JournalService};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::{
    command, Command, CommandType, CompleteWorkflowExecutionRequest,
    StartChildWorkflowCommandAttributes,
};
use orcher_storage::repositories::domains::{
    child_workflow::ChildWorkflowRepository, coordination::OutboxRepository,
    journal::JournalRepository, task_execution::TaskExecutionRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use orcher_storage::repositories::{ExecutionLogRepository, PostgresExecutionLogRepository};
use orcher_storage::schema::types::child_workflow::{ChildWorkflowExecution, OrphanPolicy};
use sqlx::PgPool;
use std::sync::Arc;
use tonic::Request;
use uuid::Uuid;

const NAMESPACE: &str = "default";
const TASK_QUEUE: &str = "default";
const WORKER: &str = "worker-a";

struct Engine {
    pool: PgPool,
    service: ExecutionService,
}

impl Engine {
    async fn new() -> Self {
        let database_url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
        });
        let pool = PgPool::connect(&database_url)
            .await
            .expect("connect to the test database");
        // Tests in this binary run in parallel and each builds its own engine;
        // the migration runner is not safe to race against itself, so apply
        // the schema once per process.
        static MIGRATED: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
        MIGRATED
            .get_or_init(|| async {
                orcher_storage::setup::run_migrations(&pool)
                    .await
                    .expect("apply migrations");
            })
            .await;

        let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
        let journal_service = Arc::new(JournalService::new(journal_repo.clone()));
        let child_repo = Arc::new(ChildWorkflowRepository::new(pool.clone()));
        let exec_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let outbox_repo = Arc::new(OutboxRepository::new(Arc::new(pool.clone())));
        let outbox_queue = Arc::new(OutboxQueue::new(outbox_repo));
        let event_bus = EventBus::new(EventBusConfig::default());
        let execution_log_repo = Arc::new(PostgresExecutionLogRepository::new(pool.clone()));
        let task_execution_repo = Arc::new(TaskExecutionRepository::new(pool.clone()));

        let workflow_execution_service = Arc::new(WorkflowExecutionService::new(
            exec_repo.clone(),
            journal_service.clone(),
            outbox_queue,
            event_bus.clone(),
            pool.clone(),
        ));
        let child_workflow_manager = Arc::new(ChildWorkflowManager::new(
            child_repo,
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

        // The polling manager is what the in-workflow SendEvent handler uses to
        // release the target's claim and wake pollers; the fix under test
        // routes through it, so it has to be wired exactly as in production.
        let polling_manager = Arc::new(PollingManager::new(
            exec_repo.clone(),
            journal_repo.clone(),
            300,
        ));

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

        let service = ExecutionService::new(
            Arc::new(command_processor),
            polling_manager,
            execution_log_repo as Arc<dyn ExecutionLogRepository>,
            journal_repo,
            exec_repo,
            task_execution_repo,
            pool.clone(),
        );

        Self { pool, service }
    }

    /// A running execution that a worker has claimed — the state every
    /// activation starts from.
    async fn claimed_workflow(&self, name: &str) -> (Uuid, String) {
        let id = Uuid::new_v4();
        let execution_id = Uuid::new_v4().to_string();
        sqlx::query(
            r#"
            INSERT INTO workflow_executions (
                id, execution_id, workflow_name, workflow_version, namespace, task_queue,
                status, parameters, context, environment, progress,
                total_steps, completed_steps, failed_steps, retried_steps,
                claimed_by, claimed_at, claim_timeout_at
            )
            VALUES ($1, $2, $3, '1.0.0', $4, $5, 'running', '{}', '{}', '{}', '{}',
                    0, 0, 0, 0, $6, NOW(), NOW() + INTERVAL '5 minutes')
            "#,
        )
        .bind(id)
        .bind(&execution_id)
        .bind(name)
        .bind(NAMESPACE)
        .bind(TASK_QUEUE)
        .bind(WORKER)
        .execute(&self.pool)
        .await
        .expect("insert claimed workflow");
        (id, execution_id)
    }

    async fn complete(
        &self,
        id: Uuid,
        execution_id: &str,
        commands: Vec<Command>,
    ) -> Result<(), tonic::Status> {
        let request = CompleteWorkflowExecutionRequest {
            workflow_id: id.to_string(),
            execution_id: execution_id.to_string(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            commands,
            ..Default::default()
        };
        self.service
            .complete_workflow_execution(Request::new(request))
            .await
            .map(|_| ())
    }

    /// An open top-level run holding `workflow_id`, as a start writes one.
    async fn open_run_holding(&self, workflow_id: &str) -> (Uuid, String) {
        let id = Uuid::new_v4();
        let execution_id = Uuid::new_v4().to_string();
        sqlx::query(
            r#"
            INSERT INTO workflow_executions (
                id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
                task_queue, status, parameters, context, environment, progress,
                total_steps, completed_steps, failed_steps, retried_steps, holds_workflow_id
            )
            VALUES ($1, $2, $3, 'holder', '1.0.0', $4, $5, 'running', '{}', '{}', '{}', '{}',
                    0, 0, 0, 0, TRUE)
            "#,
        )
        .bind(id)
        .bind(&execution_id)
        .bind(workflow_id)
        .bind(NAMESPACE)
        .bind(TASK_QUEUE)
        .execute(&self.pool)
        .await
        .expect("insert the run holding the id");
        (id, execution_id)
    }

    /// The child run `parent_execution_id` started: its id, status and
    /// whether it holds its workflow id.
    async fn child(&self, parent_execution_id: &str) -> (Uuid, String, String, bool) {
        sqlx::query_as(
            "SELECT id, execution_id, status, holds_workflow_id FROM workflow_executions \
             WHERE parent_execution_id = $1",
        )
        .bind(parent_execution_id)
        .fetch_one(&self.pool)
        .await
        .expect("read the child")
    }

    /// The failure type and reason of the child outcome in the parent's
    /// journal, if one is there.
    async fn child_failure(&self, parent_execution_id: &str) -> Option<(String, String)> {
        sqlx::query_as(
            "SELECT payload->>'failure_type', payload->>'failure_reason' \
             FROM workflow_execution_journal \
             WHERE execution_id = $1 AND event_type = 'ChildWorkflowExecutionFailed'",
        )
        .bind(parent_execution_id)
        .fetch_optional(&self.pool)
        .await
        .expect("read the child outcome")
    }

    async fn status(&self, execution_id: &str) -> String {
        sqlx::query_scalar("SELECT status FROM workflow_executions WHERE execution_id = $1")
            .bind(execution_id)
            .fetch_one(&self.pool)
            .await
            .expect("read status")
    }

    async fn failure_message(&self, id: Uuid, execution_id: &str) -> Option<String> {
        sqlx::query_scalar(
            "SELECT payload->>'failure_reason' FROM workflow_execution_journal \
             WHERE workflow_id = $1 AND execution_id = $2 \
               AND event_type = 'WorkflowExecutionFailed' \
             ORDER BY event_id DESC LIMIT 1",
        )
        .bind(id.to_string())
        .bind(execution_id)
        .fetch_optional(&self.pool)
        .await
        .expect("read failure")
        .flatten()
    }

    async fn children(&self, parent_execution_id: &str) -> Vec<Uuid> {
        sqlx::query_scalar("SELECT id FROM workflow_executions WHERE parent_execution_id = $1")
            .bind(parent_execution_id)
            .fetch_all(&self.pool)
            .await
            .expect("read children")
    }

    async fn cleanup(&self, ids: &[Uuid]) {
        for id in ids {
            let _ =
                sqlx::query("DELETE FROM child_workflow_executions WHERE parent_workflow_id = $1")
                    .bind(id.to_string())
                    .execute(&self.pool)
                    .await;
            let _ = sqlx::query("DELETE FROM workflow_execution_journal WHERE workflow_id = $1")
                .bind(id.to_string())
                .execute(&self.pool)
                .await;
            let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
                .bind(id)
                .execute(&self.pool)
                .await;
        }
    }
}

fn start_child(child_workflow_id: &str, input: &[u8]) -> Command {
    Command {
        command_type: CommandType::StartChildWorkflow as i32,
        attributes: Some(command::Attributes::StartChildWorkflow(
            StartChildWorkflowCommandAttributes {
                workflow_id: child_workflow_id.to_string(),
                workflow_type: "child-type".to_string(),
                task_queue: TASK_QUEUE.to_string(),
                input: input.to_vec(),
                ..Default::default()
            },
        )),
    }
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_child_whose_input_is_not_json_fails_its_parent() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.claimed_workflow("parent-bad-child-input").await;

    engine
        .complete(
            id,
            &execution_id,
            vec![start_child("child_0", b"\x00not json")],
        )
        .await
        .expect("a refused command fails the workflow, not the RPC");

    assert_eq!(engine.status(&execution_id).await, "failed");
    let message = engine
        .failure_message(id, &execution_id)
        .await
        .unwrap_or_default();
    assert!(
        message.contains("not valid JSON") && message.contains("child_0"),
        "the parent's failure must say which child and why: {message:?}"
    );
    assert!(
        engine.children(&execution_id).await.is_empty(),
        "no child may be started on input that was refused"
    );

    engine.cleanup(&[id]).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn an_unknown_command_type_is_left_to_retry() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.claimed_workflow("parent-unknown-command").await;

    let unknown = Command {
        command_type: 9_999,
        attributes: None,
    };
    let refused = engine.complete(id, &execution_id, vec![unknown]).await;

    assert!(refused.is_err(), "an unknown command must not be accepted");
    assert_eq!(
        engine.status(&execution_id).await,
        "running",
        "a newer engine may know the command; the workflow must not be failed for it"
    );

    engine.cleanup(&[id]).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_child_recorded_without_an_execution_is_created_on_replay() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.claimed_workflow("parent-half-started-child").await;

    // The state a failure between the two writes left behind: the tracking
    // record exists, the execution does not.
    ChildWorkflowRepository::new(engine.pool.clone())
        .create(&ChildWorkflowExecution::new(
            id.to_string(),
            execution_id.clone(),
            1,
            "child_0".to_string(),
            Uuid::new_v4().to_string(),
            "child-type".to_string(),
            TASK_QUEUE.to_string(),
            serde_json::json!({ "n": 1 }),
            OrphanPolicy::Abandon,
        ))
        .await
        .expect("seed the tracking record");
    assert!(engine.children(&execution_id).await.is_empty());

    // The parent replays and re-issues the start.
    engine
        .complete(
            id,
            &execution_id,
            vec![start_child("child_0", br#"{"n":1}"#)],
        )
        .await
        .expect("replayed start");

    let children = engine.children(&execution_id).await;
    assert_eq!(children.len(), 1, "the missing child must be created, once");

    // Replaying again finds both and creates nothing more.
    engine
        .complete(
            id,
            &execution_id,
            vec![start_child("child_0", br#"{"n":1}"#)],
        )
        .await
        .expect("second replay");
    assert_eq!(engine.children(&execution_id).await, children);

    engine.cleanup(&[id]).await;
    engine.cleanup(&children).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_child_cannot_take_the_id_an_open_run_holds() {
    let engine = Engine::new().await;
    let workflow_id = format!("order-{}", Uuid::new_v4());
    let (holder, holder_execution_id) = engine.open_run_holding(&workflow_id).await;
    let (parent, parent_execution_id) = engine.claimed_workflow("parent-taken-id").await;

    engine
        .complete(
            parent,
            &parent_execution_id,
            vec![start_child(&workflow_id, b"{}")],
        )
        .await
        .expect("a refused child does not fail the completion");

    let (child, _, status, holds) = engine.child(&parent_execution_id).await;
    assert_eq!(status, "failed", "the child must not run under a held id");
    assert!(!holds, "a refused child holds nothing");
    assert_eq!(engine.status(&holder_execution_id).await, "running");

    let (failure_type, reason) = engine
        .child_failure(&parent_execution_id)
        .await
        .expect("the parent must be told its child was refused, without waiting for the sweep");
    assert_eq!(failure_type, "ChildWorkflowIdInUse");
    assert!(
        reason.contains(&workflow_id) && reason.contains(&holder_execution_id),
        "the reason must name the id and the run holding it: {reason:?}"
    );

    // The id still names the run holding it, not the later child refused
    // for starting under it.
    let found = WorkflowExecutionRepository::new(engine.pool.clone())
        .find_run(
            orcher_storage::repositories::domains::workflow_execution::RunRef {
                organization_id: None,
                namespace: NAMESPACE,
                workflow_id: &workflow_id,
                execution_id: "",
            },
        )
        .await
        .expect("find the run")
        .expect("the id names a run");
    assert_eq!(found.execution_id, holder_execution_id);

    engine.cleanup(&[parent, holder, child]).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn two_parents_cannot_run_children_under_one_id() {
    let engine = Engine::new().await;
    let workflow_id = format!("shared-child-{}", Uuid::new_v4());
    let (first, first_execution_id) = engine.claimed_workflow("parent-first").await;
    let (second, second_execution_id) = engine.claimed_workflow("parent-second").await;

    engine
        .complete(
            first,
            &first_execution_id,
            vec![start_child(&workflow_id, b"{}")],
        )
        .await
        .expect("first parent starts its child");
    let (first_child, first_child_execution_id, status, holds) =
        engine.child(&first_execution_id).await;
    assert_eq!(status, "running");
    assert!(holds, "a child holds its workflow id");

    engine
        .complete(
            second,
            &second_execution_id,
            vec![start_child(&workflow_id, b"{}")],
        )
        .await
        .expect("second parent's completion");
    let (second_child, _, status, _) = engine.child(&second_execution_id).await;
    assert_eq!(
        status, "failed",
        "a second child must not run under an id an open child holds"
    );
    let (failure_type, reason) = engine
        .child_failure(&second_execution_id)
        .await
        .expect("the second parent must be told");
    assert_eq!(failure_type, "ChildWorkflowIdInUse");
    assert!(reason.contains(&first_child_execution_id), "{reason:?}");
    assert!(
        engine.child_failure(&first_execution_id).await.is_none(),
        "the first parent's child is running; it has nothing to be told"
    );

    // Once the first child is over, the id is free to start under again.
    sqlx::query("UPDATE workflow_executions SET status = 'completed' WHERE id = $1")
        .bind(first_child)
        .execute(&engine.pool)
        .await
        .expect("finish the first child");
    let (third, third_execution_id) = engine.claimed_workflow("parent-third").await;
    engine
        .complete(
            third,
            &third_execution_id,
            vec![start_child(&workflow_id, b"{}")],
        )
        .await
        .expect("third parent starts its child");
    let (third_child, _, status, holds) = engine.child(&third_execution_id).await;
    assert_eq!(status, "running", "a closed child no longer holds its id");
    assert!(holds);

    engine
        .cleanup(&[first, second, third, first_child, second_child, third_child])
        .await;
}
