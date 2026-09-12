# DDL/DML execution and driver compatibility

GizmoSQL ADBC is the recommended ADBC driver. It provides GizmoSQL's OAuth flow,
`gizmosql://` connection scheme and other client conveniences. Generic Arrow
Flight SQL driver compatibility is an additional server interoperability guarantee.

The 1.39.0 candidate executes fully bound DuckDB DDL/DML that has no user result
set when the client requests execution (`GetFlightInfo`). The returned ticket
identifies an **execution**, not a reusable prepared statement. Downloading that
ticket returns the completed result and never repeats the write.

Preparing an INSERT does not execute it. A client can prepare once, then bind and
execute repeatedly. Each execution uses its current parameters and gets a new
ticket; rebinding does not alter old tickets. Normal update RPCs continue to execute
synchronously and return affected-row counts.

Eligible statements are INSERT, UPDATE, DELETE, MERGE, CREATE (including CTAS),
DROP, ALTER, COPY, EXPORT, ATTACH and DETACH, when DuckDB classifies their return
type as changed rows or no result. Statements with RETURNING and result-producing
SELECT/SHOW/PRAGMA/CALL retain their query behavior. Missing parameters do not
cause preparation or GetFlightInfo to execute an unbound write.

Completed results are currently retained for up to five minutes and 1024 entries
per session. A missing, evicted or foreign-session execution ticket fails; it
never falls back to executing SQL. Closing a prepared statement does not discard
its cached completed execution results. A completed ticket cannot cancel a later
query on the same session. Cancelling execution while it is running still uses
the existing timeout, disconnect and cancellation machinery.

This does not deduplicate a new execution RPC sent as an application retry after
a lost response. As with synchronous update RPCs, the caller cannot infer that a
write did not commit merely because the response was lost. Transaction rollback
still rolls back transactional changes; an eager result is not an implicit commit.

This behavior is enabled without a CLI flag. The compatibility suite exercises
both released ADBC and JDBC drivers, including repeated prepared-statement binds.
See the [driver validation guide](https://github.com/gizmodata/gizmosql/blob/main/tests/drivers/README.md)
for reproducible checks and the released JDBC drivers' existing transaction and
batch-count limitations. The GizmoSQL JDBC maintenance update addresses those
client bugs while retaining compatibility with v1.38.x servers.
