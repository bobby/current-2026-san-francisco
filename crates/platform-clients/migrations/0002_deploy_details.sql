-- cicd-sim (issue #6): what a deployment ran with and who triggered it, and
-- where each checkpoint is stored, so a rollback can restore from it.
ALTER TABLE deployments
    ADD COLUMN args text[] NOT NULL DEFAULT '{}',
    ADD COLUMN triggered_by text NOT NULL DEFAULT 'unknown',
    ADD COLUMN failure_reason text;
ALTER TABLE checkpoints ADD COLUMN external_path text;
CREATE INDEX checkpoints_restore ON checkpoints (job_name, ts) WHERE status = 'completed' AND external_path IS NOT NULL;
