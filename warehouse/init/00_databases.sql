-- Runs once, on first initialization of an empty warehouse volume.
--
-- ClickHouse has no schemas: a "schema" IS a database. The three logical layers
-- are therefore three databases, and Metabase shows them where it would show
-- Postgres schemas.
--
-- All three are created here, which differs from the Postgres arrangement where
-- dlt and Metabase each created their own schema. ClickHouse forces it: a
-- client selects its database as part of connecting, so dlt fails with
-- "Code: 81. Database raw_pylon does not exist" during its pre-run sync, before
-- it has a chance to create anything. The database has to be there first.
--
-- Ownership of the CONTENTS is unchanged: dlt owns the tables in raw_pylon,
-- Metabase transforms own the tables in analytics, and `dq ops-init` owns the
-- tables in ops. This file creates empty containers and nothing else.

CREATE DATABASE IF NOT EXISTS raw_pylon;
CREATE DATABASE IF NOT EXISTS analytics;
CREATE DATABASE IF NOT EXISTS ops;

-- Single warehouse role for a laptop-scale stack: dlt, Great Expectations and
-- Metabase all connect as it. It needs CREATE DATABASE because dlt and Metabase
-- each create their own. Splitting reader/writer roles is the obvious hardening
-- step if this ever leaves a development machine.
GRANT CREATE DATABASE ON *.* TO CURRENT_USER;
GRANT CREATE TABLE, CREATE VIEW, DROP TABLE, DROP VIEW, TRUNCATE, ALTER,
      SELECT, INSERT, OPTIMIZE, SHOW ON *.* TO CURRENT_USER;
