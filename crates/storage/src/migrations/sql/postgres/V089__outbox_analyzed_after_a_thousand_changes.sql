-- V089: the outbox is analyzed after a thousand changes, whatever its size
--
-- The claims read the outbox by the planner's idea of its pending rows, and
-- that idea is the table's statistics. Autovacuum analyzes a table once 10%
-- of it has changed, so an outbox of 500,000 retained rows given a burst of
-- 45,000 pending ones is not analyzed at all: every claim is planned for an
-- outbox with nothing pending, walks the whole burst through an index that
-- does not order it by type, and reads 12,000 buffers to claim one row where
-- 10 are enough. It stays that way until the burst's own churn crosses the
-- 10%, and a smaller burst on a larger outbox can stay under it for good
-- (orcher-io/orcher#63).
--
-- A queue's statistics describe what it held a moment ago, so its analysis
-- is triggered by a fixed number of changes rather than a share of the
-- table: the next autovacuum round after a burst analyzes the outbox, and
-- the claims are planned for the burst once it has. The round vacuums the
-- table first when the burst's inserts call for that too, so on a large
-- outbox the analysis comes when that vacuum ends, not at once (measured:
-- 57 buffers a claim again, after a vacuum of 545,000 rows). An analysis
-- samples a fixed number of rows however large the table is, and
-- autovacuum makes at most one per table per round, so this costs an outbox
-- that is always busy one sample a minute.
--
-- Setting autovacuum options takes a SHARE UPDATE EXCLUSIVE lock, which
-- blocks no read or write of the outbox.
ALTER TABLE outbox_queue SET (
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 1000
);
