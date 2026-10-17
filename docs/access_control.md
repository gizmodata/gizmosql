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
(or changing their password) ends that user's open sessions on every node. A user
who loses any privilege also has the statement they are running stopped, on every
node; their next statement is checked against the new grants.

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

## Acting with one role

A user's privileges are those of all their roles together. `SET ROLE` narrows a
session to one of them (plus what that role inherits, and `PUBLIC`), for example
to check what a role can do, or to keep a script from touching more than it
should:

```sql
SET ROLE analyst;
SELECT * FROM sales;    -- analyst may
SELECT * FROM payroll;  -- refused: only hr may
RESET ROLE;             -- all roles again (also SET ROLE ALL or SET ROLE NONE)
```

While a role is active, refusals say so (`This session acts with role analyst
only (SET ROLE)`), `SHOW GRANTS` lists what the session acts with,
`is_role_active()` in row policies sees only the active role, and audit records
carry `active_role`. `SET ROLE` affects only the session that runs it. A user can
set only a role granted to them; a role that does not exist is refused the same
way. A tenant's user may leave out the tenant (`SET ROLE readers`). If the active
role is revoked, the session's statements are refused until it runs `SET ROLE`
or `RESET ROLE` again. The administrator always holds every right, so `SET ROLE`
is for store users.

## Roles from identity-provider groups

When users log in with a token from your identity provider (Okta, Microsoft
Entra ID, ...), the administrator can map the provider's groups to roles:

```sql
CREATE GROUP MAPPING 'okta-analysts' ISSUER 'https://example.okta.com' TO ROLE analyst;
CREATE GROUP MAPPING 'hr-team' TO ROLE hr;  -- from any issuer the server accepts
SHOW GROUP MAPPINGS;
DROP GROUP MAPPING 'hr-team';               -- all of the group's mappings; or add TO ROLE hr
```

```text
group,issuer,role
hr-team,(any),hr
okta-analysts,https://example.okta.com,analyst
```

A user whose login token lists the group in its `groups` claim (set
`GIZMOSQL_TOKEN_GROUPS_CLAIM` for another claim name, such as `roles`) holds the
role for that session, as if it were granted. Group names match exactly. The user
must still exist in the store (`CREATE USER`): a group never creates a user. A
tenant's role counts only for that tenant's users. Groups are read from the login
token when the session starts. Dropping a mapping takes the role away at once and
stops statements that relied on it; a change of groups at the identity provider
applies from the user's next login. Mapped roles can be made active with
`SET ROLE`. Only the administrator manages mappings.

## Owners

Whoever creates a table, view or schema **owns** it, and may read, change and drop
it without further grants. The owner of a schema may also create and drop objects
in it. The administrator can hand an object to a user or role, and take it back
under its own user name (an owner can hand it only to itself or to a role it
holds):

```sql
ALTER TABLE sales.main.orders OWNER TO analyst;
ALTER TABLE sales.main.orders OWNER TO admin;  -- the server's --username
```

No store user may be named like the administrator.

## Delegating grants

Users and roles are the administrator's to create, but grants can be delegated:

- **Owners grant on what they own.** The owner of a table or view (or of the schema
  it is in) may `GRANT` and `REVOKE` privileges on it.
- **`WITH GRANT OPTION`** lets a role's members pass a privilege on, never more
  than they hold: a column grant passes on only its columns.
- **`WITH ADMIN OPTION`** on a role lets its holders grant and revoke that role.

```sql
GRANT SELECT ON sales.main.orders TO analyst WITH GRANT OPTION;  -- as the administrator
GRANT SELECT ON sales.main.orders TO intern;                     -- as ann, an analyst
GRANT analyst TO lead WITH ADMIN OPTION;                         -- leads may hand out analyst
REVOKE SELECT ON sales.main.orders FROM analyst;                 -- also removes ann's grant to intern
```

A user revokes only the grants they made; the administrator's `REVOKE` removes a
privilege whoever granted it. Revoking takes away everything that was granted
through it, down the chain (`CASCADE`, the default); `REVOKE ... RESTRICT` refuses
instead when other grants depend on it. `SHOW GRANTS` lists who granted each one.

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

## Row policies

A **row policy** limits which rows of a table a role sees. As soon as a table has
one, it is *protected*: each store user sees the rows that at least one permissive
policy applying to them allows, minus any a restrictive policy removes, and no
rows at all if no permissive policy applies.

```sql
ALTER USER ann SET ATTRIBUTE region = 'EU';

CREATE ROW POLICY by_region ON sales.main.orders TO analyst
    USING (region = session_attribute('region'));
CREATE ROW POLICY everything ON sales.main.orders TO auditor USING (true);
CREATE ROW POLICY no_test_data ON sales.main.orders AS RESTRICTIVE
    USING (customer <> 'test');

SHOW ROW POLICIES ON sales.main.orders;
DROP ROW POLICY everything ON sales.main.orders;
```

`TO` names the roles a policy applies to (leave it out for every role);
`CREATE OR REPLACE` changes a policy in place. The predicate is a SQL expression
over the table's own columns and these context functions, which GizmoSQL fills in
from the session before DuckDB sees the policy:

| Function | Value |
|----------|-------|
| `current_user`, `session_user`, `gizmosql_user()` | the user's name |
| `is_role_active('role')` | whether the user holds the role |
| `session_attribute('key')` | the user's attribute (`ALTER USER ... SET ATTRIBUTE`), or NULL |

A policy cannot contain subqueries, parameters or variables, and cannot read
session settings: nothing a user can change decides what they see. Policies apply
however a statement reaches the table (subqueries, CTEs, joins, views, prepared
statements, copies into a new table), and in a view they follow the user reading
it, not the view's owner. The administrator is not subject to them.

## Column masking

A **masking policy** replaces a column's value for the users it applies to:

```sql
CREATE MASKING POLICY email_mask AS (val VARCHAR) RETURNS VARCHAR ->
    CASE WHEN is_role_active('support') THEN val
         ELSE regexp_replace(val, '.+@', '****@') END;
ALTER TABLE sales.main.customers MODIFY COLUMN email SET MASKING POLICY email_mask;

CREATE MASKING POLICY hidden AS (val VARCHAR) RETURNS VARCHAR -> '***';
ALTER TABLE sales.main.customers MODIFY COLUMN tax_id SET MASKING POLICY hidden EXEMPT ROLE finance;

SHOW MASKING POLICIES;
ALTER TABLE sales.main.customers MODIFY COLUMN tax_id UNSET MASKING POLICY;
```

The body reads only its parameter and the context functions above. The mask is
applied where the column is read, so filters, joins, grouping, sorting and copies
all see the masked value: `WHERE email = 'ann@example.com'` cannot be used to test
for a real value. Masking a join key makes joins on it stop matching for masked
users. Row policies decide on the real values of masked columns.

Changing a policy, its roles or a user's attributes applies to the next statement
(prepared statements too) and stops statements the change affects that are
running.

## System privileges

Server-wide operations stay the administrator's unless granted to a role as a
**system privilege**, with no `ON` clause:

```sql
CREATE ROLE etl;
GRANT READ FILESYSTEM, WRITE FILESYSTEM TO etl;
GRANT MANAGE CATALOGS TO etl WITH ADMIN OPTION;  -- its members may grant it on
```

| System privilege | Lets its holders run |
|------------------|----------------------|
| `READ FILESYSTEM` | `read_csv`, `read_parquet`, `read_json`, `read_text`, `glob`, ..., `FROM 'file.parquet'`, `COPY ... FROM`: local files and URLs |
| `WRITE FILESYSTEM` | `COPY ... TO` a file or URL (plus `SELECT` on what is copied) |
| `MANAGE CATALOGS` | `ATTACH`, `DETACH` |
| `MANAGE EXTENSIONS` | `INSTALL`, `LOAD`, extension repositories |
| `MANAGE SECRETS` | `CREATE SECRET`, `DROP SECRET`, `duckdb_secrets()` |
| `MANAGE CONFIG` | `SET GLOBAL` and other instance-wide settings |
| `EXPORT DATABASE` | `EXPORT DATABASE` (plus `SELECT` on what is exported) |
| `IMPORT DATABASE` | `IMPORT DATABASE` (each statement it runs is checked as usual) |
| `CHECKPOINT` | `CHECKPOINT`, `FORCE CHECKPOINT` |

A statement runs only if every gated operation in it is covered: holding
`READ FILESYSTEM` does not let an `ATTACH` ride along. The [sensitive-path
guard](security.md#sensitive-path-guard) still applies to everyone. A view reads
with its owner's system privileges, so a view over a file works for readers who
hold none, while its owner holds `READ FILESYSTEM`. `MANAGE EXTENSIONS`,
`MANAGE SECRETS` and `MANAGE CONFIG` reach the whole server: grant them only to
operators you would trust with it.

## Tenants

Several customers (tenants) can share one server, each seeing only its own
catalogs. The administrator creates a tenant and gives it catalogs:

```sql
ATTACH 'acme.duckdb' AS acme_db;        -- put this in --init-sql-commands
CREATE TENANT acme;
ALTER CATALOG acme_db SET TENANT acme;  -- or ALTER DATABASE
ALTER TENANT acme SET DEFAULT CATALOG acme_db;
CREATE USER acme/boss PASSWORD 'boss-password-123';
GRANT acme/tenant_admin TO acme/boss;
```

A tenant's users and roles are named `tenant/name`. `CREATE TENANT` makes two
roles: `acme/PUBLIC`, which every acme user holds instead of `PUBLIC`, and
`acme/tenant_admin`, which holds every privilege on the tenant's catalogs `WITH
GRANT OPTION` and lets its members create and drop the tenant's users and roles.
So the tenant manages itself:

```sql
-- as acme/boss
CREATE USER acme/joe PASSWORD 'joe-password-123';
CREATE ROLE acme/readers;
GRANT SELECT ON acme_db.main.orders TO acme/readers;
GRANT acme/readers TO acme/joe;
```

A tenant's users see their tenant's catalogs (plus `system`, `temp`, and
`_gizmosql_system`, which holds the views JDBC metadata calls use). Everything
else is invisible: a statement that names another tenant's catalog, schema or
table fails exactly like one that names something that does not exist:

```text
GizmoSQL blocked this statement: it names a catalog, schema, table or view that
does not exist, or that user acme/joe cannot see
```

The same goes for `USE`, `SET search_path`, `DESCRIBE` and
`pragma_table_info()`. Metadata (`duckdb_tables()` and the other `duckdb_*()`
functions, `information_schema`, `SHOW TABLES`, the Flight SQL metadata calls
and GizmoSQL's JDBC helper views) lists only the tenant's catalogs, also when read
through a view.

`SHOW TENANTS` lists every tenant for the administrator, and their own tenant for
a tenant's users:

```text
tenant,default_catalog,catalogs,users,roles
acme,acme_db,acme_db,2,3
```

Tenants never mix:

- a tenant's roles hold privileges only on its catalogs, and no system
  privileges;
- roles are granted only to users and roles of the same tenant;
- a tenant admin manages only its own tenant's users and roles;
- only the administrator creates, drops and changes tenants and their catalogs.
  `DROP TENANT` is refused while the tenant still has users or catalogs.

Giving a tenant a catalog or taking one away (`ALTER CATALOG ... UNSET TENANT`,
which also removes the tenant roles' grants on it) ends the tenant's sessions on
every node, so nobody keeps the old view of the catalogs.

## Audit records

Every access-control record in the server log (`"kind":"access_control"`:
refused statements, allowed ones with the objects they read and wrote,
access-control statements, sessions ended by an access change) is part of a hash
chain. Each record carries `audit_chain`, `audit_seq`, `audit_prev` and
`audit_hash`. Editing, removing, repeating or inserting a record breaks the
chain. Write the log as JSON (`--log-format json --log-file ...`) and check it
with:

```bash
gizmosql_server --verify-audit-log server.log
```

```text
chain c09696f7e1e4e4d7: sequence 1 to 9
9 audit records, intact
```

The exit status is 0 for an intact log and 1 otherwise, with a `PROBLEM:` line
for each broken record. Set `GIZMOSQL_AUDIT_CHAIN_KEY` on the server, and again
when checking, to seal records with HMAC-SHA256: without the key nobody can
rebuild a consistent chain after editing the log. Without a key, records are
sealed with plain SHA-256, which shows accidental and partial edits. Each server
start begins a new chain. `--verify-audit-log-partial` accepts a rotated log that
begins in the middle of a chain. The newest records can be cut off a log file
without a trace, so ship the log to another system as it is written.

## What store users cannot do

Besides what their grants and system privileges do not cover, store users cannot:

- run `query()` / `query_table()`, or read files or URLs without `READ FILESYSTEM`.
  A view the administrator owns may read files.
- create macros or types (not even temporary ones), or use `SET VARIABLE`. Each of
  these could change what someone else's view computes.
- have a view read their temporary tables or views. Temporary tables are fine in
  their own statements.
- turn off the optimizer or change the SQL dialect (not even with `MANAGE CONFIG`),
  or run the other [admin-only commands](admin_command_gating.md) that no system
  privilege covers.

Every refusal says `GizmoSQL blocked ...` and what is missing; a refusal caused by
what a view reads does not name the objects behind the view. The server also logs
each refusal of a statement (`"kind":"access_control"`, `"status":"denied"`),
whether or not queries are logged.

## Limits of this release

- Only the administrator (and, within a tenant, its tenant admins) creates and drops
  users and roles, and only the administrator manages row and masking policies.
- Store users cannot `INSERT`, `UPDATE` or `MERGE` into a table with row or
  masking policies yet; `DELETE` removes only rows they can see. Row policies
  cannot read other tables (mapping tables) yet.
- Names are not secret outside tenants: for a user who belongs to no tenant,
  catalog functions such as `duckdb_tables()` and the Flight SQL metadata calls list
  every table and view, including ones the user cannot read.
- A tenant's users cannot read a view in their catalogs that reads another
  tenant's catalog, even one the administrator made.
- Ownership follows an object's name: renaming a table or view needs its owner (or
  `ALTER TABLE` / `ALTER VIEW`), and the renamed object has no owner on record.
- On the [LTS channel](lts_channel.md) (DuckDB 1.4), a view reads with the caller's
  rights: owner's rights rely on a hook that GizmoSQL adds to DuckDB 1.5 and later.
