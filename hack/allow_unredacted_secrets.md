## Purpose

DuckDB starts with `allow_unredacted_secrets` off. The person who opens the database can turn it on. Then `SET allow_unredacted_secrets = true` lets them print a stored secret. That is fine when one person is looking at their own DuckDB.

GizmoSQL is shared. One process opens the DuckDB, and every client sends SQL to it. A secret stored here is the login or key those clients use. If any of them can turn the setting on, they get that secret back in the query result.

The person who starts GizmoSQL can refuse that `SET`. The refusal is the default. A client who sends it gets the rejection from GizmoSQL.


## Code walk through

- The person starts GizmoSQL. That start stores a yes/no, `block_unredacted_secrets`. This is yes by default, meaning the block is on. The yes/no sits on the server so the later `SET` can see it.

- A client connects and sends SQL. That text arrives in `Create`. The `if` asks two questions.

- Is the block on? (if it's not, the DuckDB rejects the `SET`) If the stored answer is no, the check stops. The text is handed to DuckDB.

- Is this `SET allow_unredacted_secrets = true`? The new function calls `ParseQuery`, which is DuckDB’s parser, already in the process. The parser reads the text and builds the statement. The function then reads the setting name and the value. 

- If the name is `allow_unredacted_secrets` and the value is `true`, the function returns a short label. `Create` sees that label and returns the error. `Prepare` does not run. DuckDB never sees the statement.

- If the value is `false`, or the statement is not this `SET`, the function returns nothing. `Create` continues, and `Prepare` hands the text to DuckDB.

## Implementation details

The reject happens in `Create`, before `Prepare`. `Prepare` is the call that hands the SQL to DuckDB. Returning there means DuckDB never sees `SET allow_unredacted_secrets = true`.

The new function calls DuckDB’s existing parser and reads the setting name and the value. It runs for every client, including an admin. 

## Product Context Considered


## What we looked at - product flow

There are two GizmoSQL servers. Each one opened its own DuckDB. The admin is connected to the first. The rows they want are on the second.

They send `CREATE SECRET`. That statement stores the second server’s username and password in this server’s DuckDB, in memory, until this process stops. Then they `ATTACH`. Then they `SELECT`. The first server logs into the second with that username and password and returns the rows.

That stored password is what `SET allow_unredacted_secrets = true` would print.
