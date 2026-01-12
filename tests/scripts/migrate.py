#!/usr/bin/env python3
"""
Database migration script for Oracle test environments.

Usage:
    # Run migrations (reads from tests/.env or environment)
    python tests/scripts/migrate.py --env 23ai
    python tests/scripts/migrate.py --env 19c

    # Check migration status
    python tests/scripts/migrate.py --env 23ai --status

    # Force re-run a specific migration
    python tests/scripts/migrate.py --env 23ai --force 001_create_sample_datatypes

Environment Variables:
    For 23ai: ORACLE_23AI_HOST, ORACLE_23AI_PORT, ORACLE_23AI_SERVICE,
              ORACLE_23AI_PASSWORD (uses SYS for migration, creates test_user)
    For 19c:  ORACLE_19C_HOST, ORACLE_19C_PORT, ORACLE_19C_SERVICE,
              ORACLE_19C_USERNAME, ORACLE_19C_PASSWORD

Connection Mode:
    - 23ai: Connects as SYS/SYSDBA using ORACLE_PWD
    - 19c: Connects as admin using ORACLE_19C_PASSWORD
"""

import os
import sys
import hashlib
import argparse
from pathlib import Path
from typing import Optional

import oracledb
from dotenv import load_dotenv

# Load .env from tests directory
TESTS_DIR = Path(__file__).parent.parent
load_dotenv(TESTS_DIR / ".env")


def get_connection_params(env: str) -> dict:
    """Get connection parameters for the specified environment."""
    prefix = f"ORACLE_{env.upper()}_"

    if env == "23ai":
        # 23ai: Connect as SYS/SYSDBA for migrations
        params = {
            "host": os.environ.get(f"{prefix}HOST", "localhost"),
            "port": int(os.environ.get(f"{prefix}PORT", 1521)),
            "service_name": os.environ.get(f"{prefix}SERVICE", "freepdb1"),
            "user": "sys",
            "password": os.environ.get("ORACLE_PWD"),
            "mode": oracledb.AUTH_MODE_SYSDBA,
        }
    else:
        # 19c: Connect as admin (RDS master user)
        params = {
            "host": os.environ.get(f"{prefix}HOST"),
            "port": int(os.environ.get(f"{prefix}PORT", 1521)),
            "service_name": os.environ.get(f"{prefix}SERVICE"),
            "user": os.environ.get(f"{prefix}USERNAME", "admin"),
            "password": os.environ.get(f"{prefix}PASSWORD"),
        }

    # Validate required params
    missing = [k for k, v in params.items() if v is None and k not in ("port", "mode")]
    if missing:
        raise ValueError(f"Missing environment variables for {env}: {missing}")

    return params


def connect(env: str) -> oracledb.Connection:
    """Connect to the specified Oracle environment."""
    params = get_connection_params(env)
    mode_str = " (SYSDBA)" if params.get("mode") == oracledb.AUTH_MODE_SYSDBA else ""
    print(f"Connecting to {params['host']}:{params['port']}/{params['service_name']} as {params['user']}{mode_str}...")
    return oracledb.connect(**params)


def ensure_migrations_table(conn: oracledb.Connection):
    """Create the migrations tracking table if it doesn't exist."""
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT 1 FROM "_migrations" WHERE ROWNUM = 1')
        print("  _migrations table exists")
    except oracledb.DatabaseError as e:
        if "ORA-00942" in str(e):  # Table doesn't exist
            print("  Creating _migrations table...")
            cursor.execute("""
                CREATE TABLE "_migrations" (
                    id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    migration_name VARCHAR2(255) NOT NULL UNIQUE,
                    applied_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
                    checksum VARCHAR2(64)
                )
            """)
            conn.commit()
            print("  _migrations table created")
        else:
            raise
    cursor.close()


def is_migration_applied(conn: oracledb.Connection, name: str) -> bool:
    """Check if a migration has already been applied."""
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM "_migrations" WHERE migration_name = :name', {"name": name})
    result = cursor.fetchone() is not None
    cursor.close()
    return result


def record_migration(conn: oracledb.Connection, name: str, checksum: str):
    """Record that a migration has been applied."""
    cursor = conn.cursor()
    cursor.execute(
        'INSERT INTO "_migrations" (migration_name, checksum) VALUES (:name, :checksum)',
        {"name": name, "checksum": checksum}
    )
    conn.commit()
    cursor.close()


def compute_checksum(content: str) -> str:
    """Compute SHA256 checksum of migration content."""
    return hashlib.sha256(content.encode()).hexdigest()[:16]


# ============================================================================
# MIGRATIONS - Add new migrations here
# ============================================================================

MIGRATIONS = [
    "000_create_test_user",
    "001_create_sample_datatypes",
    "002_create_test_data",
]


def migrate_000_create_test_user(conn: oracledb.Connection, env: str):
    """
    Create test_user for Rust integration tests (23ai only).
    On 19c, this is skipped because admin user already exists.
    """
    cursor = conn.cursor()

    # Skip on 19c - admin user already exists and has privileges
    if env == "19c":
        print("    Skipping - using admin user on 19c")
        cursor.close()
        return

    # On 23ai, create test_user
    try:
        cursor.execute("DROP USER test_user CASCADE")
        print("    Dropped existing test_user")
    except oracledb.DatabaseError:
        pass  # User doesn't exist

    password = os.environ.get("ORACLE_23AI_PASSWORD", os.environ.get("ORACLE_PWD"))
    if not password:
        raise ValueError("ORACLE_23AI_PASSWORD or ORACLE_PWD must be set")

    cursor.execute(f"""
        CREATE USER test_user IDENTIFIED BY "{password}"
        DEFAULT TABLESPACE users
        TEMPORARY TABLESPACE temp
        QUOTA UNLIMITED ON users
    """)
    cursor.execute("GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE TO test_user")
    conn.commit()
    print("    Created test_user with privileges")
    cursor.close()


def migrate_001_create_sample_datatypes(conn: oracledb.Connection, env: str):
    """
    Create sample_datatypes_tbl with various Oracle data types.
    Used for testing type parsing.
    """
    cursor = conn.cursor()

    # On 23ai, create in test_user schema
    schema_prefix = "test_user." if env == "23ai" else ""

    # Drop if exists
    try:
        cursor.execute(f"DROP TABLE {schema_prefix}sample_datatypes_tbl PURGE")
        print(f"    Dropped existing {schema_prefix}sample_datatypes_tbl")
    except oracledb.DatabaseError as e:
        if "ORA-00942" not in str(e):
            raise

    # Create table
    cursor.execute(f"""
        CREATE TABLE {schema_prefix}sample_datatypes_tbl (
            id            NUMBER(10) PRIMARY KEY,
            char_col      CHAR(10),
            varchar2_col  VARCHAR2(50),
            nchar_col     NCHAR(10),
            nvarchar2_col NVARCHAR2(50),
            clob_col      CLOB,
            blob_col      BLOB,
            date_col      DATE,
            timestamp_col TIMESTAMP,
            number_col    NUMBER(10, 2),
            float_col     FLOAT(126),
            binary_float_col BINARY_FLOAT,
            binary_double_col BINARY_DOUBLE,
            raw_col       RAW(16)
        )
    """)
    print(f"    Created {schema_prefix}sample_datatypes_tbl")

    # Insert test data
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            1,
            'FixedChar1',
            'VariableChar',
            'NCharFix01',
            'NVarCharValue',
            'This is a large text object stored in a CLOB column.',
            UTL_RAW.CAST_TO_RAW('This is a blob data, very big by the way.'),
            SYSDATE,
            SYSTIMESTAMP,
            12345.67,
            123.45e6,
            1.23e0,
            1.23e0,
            UTL_RAW.CAST_TO_RAW('raw data')
        )
    """)

    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            2,
            'FixedChar2',
            'AnotherVarChar',
            'NCharFix02',
            'AnotherNVarChar',
            'Second CLOB data entry for testing purposes.',
            UTL_RAW.CAST_TO_RAW('Second blob data entry.'),
            SYSDATE + 1,
            SYSTIMESTAMP + INTERVAL '1' HOUR,
            98765.43,
            54.32e3,
            4.56e1,
            7.89e1,
            UTL_RAW.CAST_TO_RAW('more raw data')
        )
    """)

    conn.commit()
    print("    Inserted 2 test rows")
    cursor.close()


def migrate_002_create_test_data(conn: oracledb.Connection, env: str):
    """
    Create TEST_DATA table with 5000 rows for cursor/fetch testing.
    """
    cursor = conn.cursor()

    # On 23ai, create in test_user schema
    schema_prefix = "test_user." if env == "23ai" else ""

    # Drop if exists
    try:
        cursor.execute(f"DROP TABLE {schema_prefix}TEST_DATA PURGE")
        print(f"    Dropped existing {schema_prefix}TEST_DATA")
    except oracledb.DatabaseError as e:
        if "ORA-00942" not in str(e):
            raise

    # Create table
    cursor.execute(f"""
        CREATE TABLE {schema_prefix}TEST_DATA (
            ID NUMBER PRIMARY KEY,
            STR_COL VARCHAR2(100),
            INT_COL NUMBER,
            DEC_COL NUMBER(10,2)
        )
    """)
    print(f"    Created {schema_prefix}TEST_DATA table")

    # Insert rows in batches
    NUM_ROWS = 5000
    BATCH_SIZE = 500
    rows_inserted = 0

    for batch_start in range(1, NUM_ROWS + 1, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, NUM_ROWS + 1)
        batch_data = []

        for i in range(batch_start, batch_end):
            str_val = f"row_{i:05d}"
            int_val = i * 10
            dec_val = i / 100.0
            batch_data.append((i, str_val, int_val, dec_val))

        cursor.executemany(
            f"INSERT INTO {schema_prefix}TEST_DATA (ID, STR_COL, INT_COL, DEC_COL) VALUES (:1, :2, :3, :4)",
            batch_data,
        )
        rows_inserted += len(batch_data)

    conn.commit()
    print(f"    Inserted {rows_inserted} rows")
    cursor.close()


# Migration function dispatch - note: functions now take (conn, env)
MIGRATION_FUNCTIONS = {
    "000_create_test_user": migrate_000_create_test_user,
    "001_create_sample_datatypes": migrate_001_create_sample_datatypes,
    "002_create_test_data": migrate_002_create_test_data,
}


def run_migrations(conn: oracledb.Connection, env: str, force: Optional[str] = None):
    """Run all pending migrations."""
    ensure_migrations_table(conn)

    for name in MIGRATIONS:
        if force and name != force:
            continue

        if is_migration_applied(conn, name) and not force:
            print(f"  [SKIP] {name} (already applied)")
            continue

        print(f"  [RUN] {name}...")
        func = MIGRATION_FUNCTIONS[name]
        func(conn, env)  # Pass env to migration function

        # Get function source for checksum
        import inspect
        source = inspect.getsource(func)
        checksum = compute_checksum(source)

        if force:
            # Delete old record if forcing
            cursor = conn.cursor()
            cursor.execute('DELETE FROM "_migrations" WHERE migration_name = :name', {"name": name})
            conn.commit()
            cursor.close()

        record_migration(conn, name, checksum)
        print(f"  [DONE] {name}")


def show_status(conn: oracledb.Connection):
    """Show migration status."""
    ensure_migrations_table(conn)

    cursor = conn.cursor()
    cursor.execute("""
        SELECT migration_name, applied_at, checksum
        FROM "_migrations"
        ORDER BY id
    """)

    applied = {row[0]: (row[1], row[2]) for row in cursor}
    cursor.close()

    print("\nMigration Status:")
    print("-" * 60)
    for name in MIGRATIONS:
        if name in applied:
            applied_at, checksum = applied[name]
            print(f"  [APPLIED] {name}")
            print(f"            at {applied_at}, checksum: {checksum}")
        else:
            print(f"  [PENDING] {name}")
    print("-" * 60)


def main():
    parser = argparse.ArgumentParser(description="Run database migrations")
    parser.add_argument("--env", required=True, choices=["23ai", "19c"],
                        help="Target environment")
    parser.add_argument("--status", action="store_true",
                        help="Show migration status")
    parser.add_argument("--force", metavar="MIGRATION",
                        help="Force re-run a specific migration")

    args = parser.parse_args()

    try:
        conn = connect(args.env)

        if args.status:
            show_status(conn)
        else:
            run_migrations(conn, args.env, args.force)
            print("\nMigrations complete!")

        conn.close()

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
