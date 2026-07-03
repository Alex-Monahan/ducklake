-- Run as the PostgreSQL superuser against the catalog database:
--   sudo -u postgres psql -d ducklake_catalog -f 03_row_level_security.sql
--
-- Table-level security for DuckLake = row-level security on the DuckLake
-- catalog tables. Every catalog table that carries a table_id column gets a
-- policy that only exposes rows for tables the current role has been granted
-- in lake_table_grants. A DuckLake client connecting as a restricted role
-- then simply does not see ungranted tables: they are absent from SHOW TABLES
-- and unresolvable in queries, and their file paths / stats / schema are
-- invisible too.
--
-- lake_admin owns the catalog tables, and PostgreSQL RLS does not apply to a
-- table's owner (unless FORCE ROW LEVEL SECURITY is set), so the admin keeps
-- full read/write access.

-- 1. The grants mapping: which role may see which DuckLake table.
CREATE TABLE IF NOT EXISTS lake_table_grants (
    role_name TEXT NOT NULL,
    table_id  BIGINT NOT NULL,
    PRIMARY KEY (role_name, table_id)
);
ALTER TABLE lake_table_grants OWNER TO lake_admin;
GRANT SELECT ON lake_table_grants TO PUBLIC;

-- 2. Enable RLS + attach the visibility policy on every catalog table that
--    has a table_id column. Driving this off information_schema keeps the
--    script correct across DuckLake schema versions.
DO $$
DECLARE
    t RECORD;
BEGIN
    FOR t IN
        SELECT c.table_name
        FROM information_schema.columns c
        WHERE c.table_schema = 'public'
          AND c.table_name LIKE 'ducklake\_%'
          AND c.column_name = 'table_id'
    LOOP
        EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY', t.table_name);
        EXECUTE format('DROP POLICY IF EXISTS table_visibility ON public.%I', t.table_name);
        EXECUTE format(
            $p$CREATE POLICY table_visibility ON public.%I FOR SELECT
               USING (table_id IN (SELECT g.table_id
                                   FROM lake_table_grants g
                                   WHERE pg_has_role(current_user, g.role_name, 'MEMBER')))$p$,
            t.table_name);
    END LOOP;
END $$;

-- 3. The restricted role gets plain SELECT on the catalog schema (RLS filters
--    the rows) and nothing else, which also makes it read-only: it cannot
--    write snapshots, so it cannot commit to the lake at all.
GRANT USAGE ON SCHEMA public TO analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analyst;

-- 4. Grant the analyst visibility of public_orders only.
INSERT INTO lake_table_grants
SELECT 'analyst', table_id FROM ducklake_table WHERE table_name = 'public_orders'
ON CONFLICT DO NOTHING;

-- 5. Inlined-data tables (ducklake_inlined_data_<table_id>_<schema_version>)
--    hold actual row data inside the catalog and have no table_id column, so
--    the RLS policies above do not cover them. The bootstrap script disables
--    inlining, but if any inlined tables exist (or you keep inlining on),
--    align their plain GRANTs with lake_table_grants. Re-run this block (or
--    wire it into an event trigger) whenever new inlined tables appear.
DO $$
DECLARE
    t RECORD;
BEGIN
    FOR t IN SELECT i.table_id, i.table_name FROM ducklake_inlined_data_tables i
    LOOP
        IF EXISTS (SELECT 1 FROM lake_table_grants g
                   WHERE g.role_name = 'analyst' AND g.table_id = t.table_id) THEN
            EXECUTE format('GRANT SELECT ON public.%I TO analyst', t.table_name);
        ELSE
            EXECUTE format('REVOKE SELECT ON public.%I FROM analyst', t.table_name);
        END IF;
    END LOOP;
END $$;
