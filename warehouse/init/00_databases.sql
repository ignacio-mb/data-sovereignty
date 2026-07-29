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

-- No GRANTs here, deliberately. CLICKHOUSE_USER makes the entrypoint define the
-- user in users.xml, and that storage is read-only to SQL: any GRANT aborts the
-- whole init with "Code: 495 ... Cannot update user `warehouse` in users_xml
-- because this storage is readonly", leaving the container unhealthy forever.
-- The grants were also redundant — an XML-defined user already has full access,
-- verified against this image for every operation the stack performs: CREATE
-- DATABASE, CREATE/DROP TABLE and VIEW, INSERT, SELECT, ALTER, OPTIMIZE,
-- TRUNCATE, SHOW, and reading system.tables.
--
-- Splitting reader from writer is still the obvious hardening step, but it has
-- to be done in a users.d/*.xml file mounted into the server, not from here.
