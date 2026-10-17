# Role-Based Access Control (Enterprise)

GizmoSQL Enterprise can manage **users, roles and grants in SQL**, the way you
would in PostgreSQL: create users and roles, grant roles privileges on
catalogs, schemas, tables, views and columns, and GizmoSQL checks every
statement a user sends against those grants.

The checks run **inside DuckDB, on the statement's own plan**, before DuckDB
optimizes it. Each table a statement reads (and each column it uses, in the
select list, filters, joins, sorts and window functions alike), each table it
writes, and each object it creates, drops or alters is checked. What the check
cannot classify is refused. It does not matter how a read is spelled: through a
subquery, a CTE, `EXPLAIN ANALYZE`, a prepared statement, a temporary view or a
DuckLake or PostgreSQL catalog, it is the same check.

!!! info "Requires a license"
    Role-based access control is part of GizmoSQL Enterprise. The server refuses
    to start with `--rbac-store-uri` unless its license includes it (licenses
    with catalog permissions do).

## Turning it on

Users, roles and grants live in a **PostgreSQL database** (the *store*). Start the
server with `--rbac-store-uri` (or the `GIZMOSQL_RBAC_STORE_URI` environment
variable) pointing at it:

```bash
gizmosql_server \
  --database-filename data.duckdb \
  --username admin --password 'a-long-admin-password' \
  --rbac-store-uri 'postgresql://gizmosql:store-password@pg.internal:5432/gizmosql'
```

GizmoSQL creates its tables in a `gizmosql_rbac` schema of that database on first
start. **Every node of a cluster points at the same store**: a change made on one
node reaches the others within about a second, and disabling or dropping a user
(or changing their password) ends that user's open sessions on every node.

## Who is who

- **The administrator** (the server's `--username` / `--password`, or a token
  whose `role` claim is `admin`) is the superuser. Its statements are not checked,
  and it is the only one who manages users, roles and grants.
- **Everyone else is a store user.** A store user logs in with the password set in
  the store (Basic authentication, like the administrator), or with a token whose
  `sub` claim names a store user. A token for a user who is not in the store, or
  who is disabled, is refused when the session starts. A token's `catalog_access`
  claim is ignored once access control is on: access comes only from grants.

## Managing users and roles

Run these as the administrator, from any client:

```sql
CREATE ROLE analyst;
CREATE USER ann WITH PASSWORD 'change-me-1';
GRANT analyst TO ann;

-- One table, some columns of a table, or every table in a schema
GRANT SELECT ON sales.main.orders TO analyst;
GRANT SELECT (id, region) ON sales.main.customers TO analyst;
GRANT SELECT ON SCHEMA sales.reporting TO analyst;

-- Writes and DDL are separate privileges
GRANT INSERT, UPDATE, DELETE ON sales.main.orders TO analyst;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA sales.sandbox TO analyst;

SHOW GRANTS FOR ann;
REVOKE INSERT, UPDATE, DELETE ON sales.main.orders FROM analyst;
ALTER USER ann WITH PASSWORD 'another-password-2';
ALTER USER ann DISABLE;
```

Privileges are granted to **roles**, and roles to users (or to other roles, which
then inherit them). Every user also holds the built-in role `PUBLIC`. Names follow
DuckDB's rules: unqualified table and schema names resolve the way they do in your
session's queries.

| Privilege | Granted on | Allows |
|-----------|------------|--------|
| `SELECT` | table, view (optionally a column list), schema, catalog | reading |
| `INSERT`, `UPDATE`, `DELETE` | table, schema, catalog | writing (a write that also reads, such as `UPDATE ... WHERE`, needs `SELECT` on what it reads) |
| `CREATE TABLE`, `CREATE VIEW` | schema, catalog | creating in it |
| `CREATE SCHEMA` | catalog | creating schemas |
| `DROP TABLE`, `DROP VIEW`, `ALTER TABLE`, `ALTER VIEW` | schema, catalog | dropping or altering objects in it that someone else owns |
| `DROP SCHEMA` | catalog | dropping schemas someone else owns |
| `ALL` | table, view, schema, catalog | `SELECT`, `INSERT`, `UPDATE` and `DELETE` |

A grant on a schema or catalog covers the objects in it, including ones created
later. A store user can see their own grants with `SHOW GRANTS`.

## Owners

Whoever creates a table, view or schema **owns** it, and may read, change and drop
it without further grants. The owner of a schema may also create and drop objects
in it. The administrator can hand an object to a user or role, and take it back
under its own user name:

```sql
ALTER TABLE sales.main.orders OWNER TO analyst;
ALTER TABLE sales.main.orders OWNER TO admin;  -- the server's --username
```

No store user may be named like the administrator.

## Views read with their owner's rights

To read a view, a user needs `SELECT` on **the view** (on the columns their
statement uses), and nothing on what the view reads: that is checked against the
**view's owner**. This is how you publish data without exposing the tables behind
it:

```sql
-- As the administrator: a curated view over a table analysts cannot read
CREATE SCHEMA curated;
CREATE VIEW curated.orders_by_region AS
    SELECT region, sum(amount) AS total FROM sales.main.orders GROUP BY region;
GRANT SELECT ON curated.orders_by_region TO analyst;
```

`ann` can now run `SELECT * FROM curated.orders_by_region`, while
`SELECT * FROM sales.main.orders` is still refused. A view never reaches more than
its owner can: a user who creates a view over a table they cannot read cannot read
it through the view either, and neither can anyone they share it with. The owner's
rights are checked at query time, so revoking the owner's access closes the view.
Anything a statement names *beside* the view is still checked against the user.

A view **without an owner on record** (one that existed before access control was
turned on, or was created by `--init-sql-commands`) reads with the caller's own
rights until the administrator gives it an owner:

```sql
ALTER VIEW curated.orders_by_region OWNER TO analyst;  -- a role, a user, ...
```

!!! warning "A view is a column and object boundary, not a row boundary"
    A view that filters rows (`WHERE region = 'EU'`) hides the other rows from
    ordinary queries, but a user's own predicates can be evaluated before the
    view's filter, and an error message can echo a value it would have hidden.
    Use views to choose which tables and columns users see; do not rely on them
    to hide rows.

## What store users cannot do

Besides what their grants do not cover, store users cannot:

- read files or URLs directly (`read_parquet(...)`, `FROM 'file.csv'`, `COPY ... TO`),
  or run `query()` / `query_table()`. A view the administrator owns may read files.
- create macros or types (not even temporary ones), or use `SET VARIABLE`. Each of
  these could change what someone else's view computes.
- have a view read their temporary tables or views. Temporary tables are fine in
  their own statements.
- turn off the optimizer, change the SQL dialect, attach or detach databases,
  install or load extensions, or run the other [admin-only commands](admin_command_gating.md).

Every refusal says `GizmoSQL blocked ...` and what is missing; a refusal caused by
what a view reads does not name the objects behind the view. The server also logs
each refusal of a statement (`"kind":"access_control"`, `"status":"denied"`),
whether or not queries are logged.

## Limits of this release

- Only the administrator grants and revokes. Owners granting their own objects,
  `WITH GRANT OPTION` and system privileges come later.
- A user's privileges are those of all their roles together (there is no `SET ROLE`).
- Names are not secret: catalog functions such as `duckdb_tables()` and the Flight
  SQL metadata calls list every table and view, including ones a user cannot read.
- Ownership follows an object's name: renaming a table or view needs its owner (or
  `ALTER TABLE` / `ALTER VIEW`), and the renamed object has no owner on record.
- On the [LTS channel](lts_channel.md) (DuckDB 1.4), a view reads with the caller's
  rights: owner's rights rely on a hook that GizmoSQL adds to DuckDB 1.5 and later.
