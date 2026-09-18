"""
Shared DuckDB connection helper for the visualization scripts in this
folder. Centralizes the Floci/S3 credential + extension setup so each chart
script doesn't repeat it — same spirit as the S3Staging.java helper on the
extraction side.
"""

import os
import duckdb


def get_connection(warehouse_path: str = "warehouse.duckdb", read_only: bool = True) -> duckdb.DuckDBPyConnection:
    """
    Opens a DuckDB connection against the local copy of warehouse.duckdb,
    with the httpfs/delta extensions loaded and a session-scoped S3 secret
    pointing at Floci.

    Reads endpoint/credentials from the same .env variables the rest of the
    project uses. Defaults match Floci's out-of-the-box test credentials, so
    this works with no environment configured at all for local dev.

    The secret is intentionally NOT persistent: it only needs to live for
    the duration of this one script run, and a persisted secret wouldn't be
    reachable from the host anyway (it lives under $HOME/.duckdb, tied to
    whichever process/container created it).
    """
    endpoint = os.getenv("FLOCI_ENDPOINT", "http://localhost:4566") \
        .replace("http://", "").replace("https://", "")
    access_key = os.getenv("FLOCI_ACCESS_KEY", "test")
    secret_key = os.getenv("FLOCI_SECRET_KEY", "test")

    con = duckdb.connect(warehouse_path, read_only=read_only)
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL delta; LOAD delta;")
    con.execute(f"""
        CREATE OR REPLACE SECRET floci_s3 (
            TYPE S3,
            KEY_ID '{access_key}',
            SECRET '{secret_key}',
            ENDPOINT '{endpoint}',
            URL_STYLE 'path',
            USE_SSL false
        );
    """)
    return con