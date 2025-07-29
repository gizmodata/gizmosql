import os
import pyarrow as pa
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions

def main():
    """
    Demonstrates GizmoSQL database operations including connection, data insertion, and querying.
    This function establishes a connection to a GizmoSQL database using gRPC with TLS,
    inserts a sample record into the nation table using PyArrow record batches, and then
    queries the inserted data to verify the operation.
    The function performs the following operations:
    1. Connects to GizmoSQL database with TLS encryption and authentication
    2. Prepares and executes an INSERT statement with parameterized data
    3. Creates a PyArrow record batch with nation data (nationkey, name, regionkey, comment)
    4. Binds the data to the prepared statement and executes the insert
    5. Queries the inserted record using a parameterized SELECT statement
    6. Fetches and displays the results as an Arrow table
    Environment Variables:
        GIZMOSQL_USERNAME: Database username (defaults to "gizmosql_username")
        GIZMOSQL_PASSWORD: Database password (defaults to "gizmosql_password")
    Raises:
        ConnectionError: If unable to connect to the GizmoSQL database
        DatabaseError: If SQL operations fail during execution
    Note:
        Uses TLS_SKIP_VERIFY for testing purposes - should use trusted CA-signed
        certificates in production environments.
    """

    with gizmosql.connect(uri="grpc+tls://localhost:31337",
                        db_kwargs={"username": os.getenv("GIZMOSQL_USERNAME", "gizmosql_username"),
                                    "password": os.getenv("GIZMOSQL_PASSWORD", "gizmosql_password"),
                                    DatabaseOptions.TLS_SKIP_VERIFY.value: "true"  # Not needed if you use a trusted CA-signed TLS cert
                                    }
                        ) as conn:
        with conn.cursor() as cur:
            # Insert data into nation table
            insert_query = "INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment) VALUES (?, ?, ?, ?)"
            cur.adbc_statement.set_sql_query(insert_query)
            cur.adbc_statement.prepare()
            
            # Create PyArrow record batch for parameters
            schema = pa.schema([
                ("n_nationkey", pa.int64()),
                ("n_name", pa.string()),
                ("n_regionkey", pa.int64()),
                ("n_comment", pa.string())
            ])
            batch = pa.record_batch([
                [99],
                ["EXAMPLE_NATION"],
                [1],
                ["Sample nation for testing"]
            ], schema=schema)
            
            cur.adbc_statement.bind(batch)
            rows_affected = cur.adbc_statement.execute_update()
            print(f"Inserted {rows_affected} row(s)")
            
            # Query the data
            cur.execute("SELECT n_nationkey, n_name FROM nation WHERE n_nationkey = ?",
                        parameters=[99]
                        )
            x = cur.fetch_arrow_table()
            print(x)


if __name__ == "__main__":
    main()
