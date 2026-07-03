-- Run as the PostgreSQL superuser (e.g. `sudo -u postgres psql -f 01_roles.sql`).
--
-- Two principals:
--   lake_admin : owns the DuckLake catalog, sees and writes everything
--   analyst    : may only see/read the tables listed in lake_table_grants
CREATE ROLE lake_admin LOGIN PASSWORD 'admin_pw';
CREATE ROLE analyst LOGIN PASSWORD 'analyst_pw';
CREATE DATABASE ducklake_catalog OWNER lake_admin;
