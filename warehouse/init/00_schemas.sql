-- Runs once, on first initialization of an empty warehouse volume.
--
-- Only `ops` is created here. The other two schemas belong to the tools that
-- own them and must not be pre-created:
--   raw_pylon   created by dlt on the first ingest
--   analytics   created by Metabase on the first transform run
--
-- Table DDL inside ops belongs to `dq ops-init`, which is idempotent and can
-- evolve without recreating the volume. This file exists only so that the
-- schema and its grants are in place before anything tries to write.

CREATE SCHEMA IF NOT EXISTS ops;

-- Single warehouse role for a laptop-scale stack: dlt, Great Expectations and
-- Metabase all connect as it. Splitting reader/writer roles is the obvious
-- hardening step if this ever leaves a development machine.
DO $$
BEGIN
  EXECUTE format('GRANT ALL ON SCHEMA ops TO %I', current_user);
  EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA ops GRANT ALL ON TABLES TO %I', current_user);
END
$$;

COMMENT ON SCHEMA ops IS
  'Pipeline operations: data-quality results, DAG run history, transform run history. '
  'Written by the dq CLI, read by the Metabase Pipeline Health dashboard.';
