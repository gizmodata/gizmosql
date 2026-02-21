import os
from time import sleep
from adbc_driver_gizmosql import dbapi as gizmosql


# Setup variables
max_attempts: int = 10
sleep_interval: int = 10
gizmosql_password = os.environ["GIZMOSQL_PASSWORD"]

def main():
    for attempt in range(max_attempts):
        try:
            with gizmosql.connect(
                "grpc+tls://localhost:31337",
                username="gizmosql_user",
                password=gizmosql_password,
                tls_skip_verify=True,  # Not needed if you use a trusted CA-signed TLS cert
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT n_nationkey, n_name FROM nation WHERE n_nationkey = ?",
                                parameters=[24]
                                )
                    x = cur.fetch_arrow_table()
                    print(x)
        except Exception as e:
            if attempt == max_attempts - 1:
                raise e
            else:
                print(f"Attempt {attempt + 1} failed: {e}, sleeping for {sleep_interval} seconds")
                sleep(sleep_interval)
        else:
            print("Success!")
            break


if __name__ == "__main__":
    main()
