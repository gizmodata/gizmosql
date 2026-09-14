import os
from pathlib import Path

import click

import duckdb

DIR_PATH = os.path.dirname(os.path.realpath(__file__))


@click.command()
@click.option(
    "--file-name",
    type=str,
    required=True,
    help="The name of the DuckDB database file to create.",
)
@click.option(
    "--file-path",
    type=str,
    default=f"{DIR_PATH}/../data",
    show_default=True,
    help="The target directory path for the DuckDB database file",
)
@click.option(
    "--overwrite-file",
    type=bool,
    default=False,
    required=True,
    help="Overwrite the DuckDB database file if it exists...",
)
@click.option(
    "--scale-factor",
    type=float,
    default=0.01,
    show_default=True,
    required=True,
    help="The TPC-H Scale factor used to create the DuckDB database file.",
)
def main(file_name: str, file_path: str, overwrite_file: bool, scale_factor: float):
    data_dir_path = Path(file_path)
    duckdb_db_file = data_dir_path / file_name

    if os.path.exists(path=duckdb_db_file):
        if overwrite_file:
            os.remove(path=duckdb_db_file)
        else:
            raise (
                Exception(
                    f"DuckDB database file: '{duckdb_db_file.as_posix()}' already exists.  Aborting"
                )
            )

    # establish all connections to database
    con = duckdb.connect(database=duckdb_db_file.as_posix(), read_only=False)

    con.execute(f"CALL dbgen(sf={scale_factor})")
    con.execute("VACUUM ANALYZE")

    # close the connection
    con.close()

    print(
        f"Successfully created DuckDB database file: '{duckdb_db_file.as_posix()}' - with TPC-H Scale Factor: {scale_factor}"
    )


if __name__ == "__main__":
    main()
