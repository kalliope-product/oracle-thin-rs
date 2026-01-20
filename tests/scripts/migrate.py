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
    Create sample_datatypes_tbl with comprehensive Oracle data types.
    Generates 500 rows with varied test data including NULLs and edge cases.

    Supported types (19c compatible):
    - Character: CHAR, VARCHAR2, NCHAR, NVARCHAR2, LONG
    - Numeric: NUMBER, BINARY_FLOAT, BINARY_DOUBLE
    - Date/Time: DATE, TIMESTAMP, TIMESTAMP WITH TIME ZONE,
                 TIMESTAMP WITH LOCAL TIME ZONE, INTERVAL DAY(5) TO SECOND(9),
                 INTERVAL YEAR(4) TO MONTH
    - LOB: CLOB, NCLOB, BLOB
    - Binary: RAW
    """
    import random
    import string

    cursor = conn.cursor()
    schema_prefix = "test_user." if env == "23ai" else ""

    # Drop if exists
    try:
        cursor.execute(f"DROP TABLE {schema_prefix}sample_datatypes_tbl PURGE")
        print(f"    Dropped existing {schema_prefix}sample_datatypes_tbl")
    except oracledb.DatabaseError as e:
        if "ORA-00942" not in str(e):
            raise

    # Create comprehensive table
    cursor.execute(f"""
        CREATE TABLE {schema_prefix}sample_datatypes_tbl (
            -- Primary Key
            id                    NUMBER(10) PRIMARY KEY,

            -- Character Types
            char_col              CHAR(20),
            varchar2_col          VARCHAR2(200),
            nchar_col             NCHAR(20),
            nvarchar2_col         NVARCHAR2(200),
            long_col              LONG,

            -- Numeric Types
            number_col            NUMBER(38,10),
            number_int_col        NUMBER(10),
            number_free_col       NUMBER,            -- No precision/scale (Oracle flexible NUMBER)
            binary_float_col      BINARY_FLOAT,
            binary_double_col     BINARY_DOUBLE,

            -- Date/Time Types
            date_col              DATE,
            timestamp_col         TIMESTAMP(9),
            timestamp_tz_col      TIMESTAMP(9) WITH TIME ZONE,
            timestamp_ltz_col     TIMESTAMP(9) WITH LOCAL TIME ZONE,
            interval_ds_col       INTERVAL DAY(9) TO SECOND(9),
            interval_ym_col       INTERVAL YEAR(9) TO MONTH,

            -- LOB Types
            clob_col              CLOB,
            nclob_col             NCLOB,
            blob_col              BLOB,

            -- Binary Type
            raw_col               RAW(2000)
        )
    """)
    print(f"    Created {schema_prefix}sample_datatypes_tbl with 21 columns")

    # =========================================================================
    # Special Test Rows (1-20): Edge cases, NULLs, extremes
    # =========================================================================

    # Row 1: All NULLs (except ID)
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl (id) VALUES (1)
    """)

    # Row 2: Empty strings and zeros
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            2,
            '                    ',  -- CHAR padded with spaces
            '',                      -- Empty VARCHAR2
            N'                    ', -- NCHAR padded
            N'',                     -- Empty NVARCHAR2
            '',                      -- Empty LONG
            0,                       -- Zero NUMBER
            0,                       -- Zero integer
            0,                       -- Zero NUMBER (free)
            0.0,                     -- Zero BINARY_FLOAT
            0.0,                     -- Zero BINARY_DOUBLE
            DATE '1970-01-01',       -- Unix epoch
            TIMESTAMP '1970-01-01 00:00:00.000000000',
            TIMESTAMP '1970-01-01 00:00:00.000000000 +00:00',
            TIMESTAMP '1970-01-01 00:00:00.000000000',
            INTERVAL '0 00:00:00.000000000' DAY(5) TO SECOND(9),
            INTERVAL '0-0' YEAR(4) TO MONTH,
            EMPTY_CLOB(),
            EMPTY_CLOB(),
            EMPTY_BLOB(),
            HEXTORAW('')
        )
    """)

    # Row 3: Large positive values
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            3,
            'XXXXXXXXXXXXXXXXXXXX',  -- Max CHAR(20)
            RPAD('X', 200, 'X'),     -- Max VARCHAR2(200)
            N'XXXXXXXXXXXXXXXXXXXX', -- Max NCHAR(20)
            RPAD(N'X', 200, N'X'),   -- Max NVARCHAR2(200)
            RPAD('LONG_MAX_', 32000, 'X'),  -- Large LONG
            123456789012345678901234567.1234567890,  -- Large NUMBER(38,10)
            9999999999,              -- Max 10-digit integer
            12345678901234567890.12345678901234567890,  -- Large NUMBER (free) with many decimals
            1.23456E+30,             -- Large BINARY_FLOAT
            1.23456789012345E+100,   -- Large BINARY_DOUBLE
            DATE '9999-12-31',       -- Max DATE
            TIMESTAMP '9999-12-31 23:59:59.999999999',
            TIMESTAMP '9999-12-31 23:59:59.999999999 +14:00',
            TIMESTAMP '9999-12-31 23:59:59.999999999',
            INTERVAL '99999 23:59:59.999999999' DAY(5) TO SECOND(9),
            INTERVAL '9999-11' YEAR(4) TO MONTH,
            TO_CLOB(RPAD('CLOB_MAX_', 4000, 'X')),
            TO_NCLOB(RPAD(N'NCLOB_MAX_', 4000, N'X')),
            UTL_RAW.CAST_TO_RAW(RPAD('BLOB_MAX_', 2000, 'X')),
            UTL_RAW.CAST_TO_RAW(RPAD('R', 2000, 'R'))
        )
    """)

    # Row 4: Minimum/negative values
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            4,
            'A                   ',  -- Min non-empty CHAR
            'A',                     -- Min VARCHAR2
            N'A                   ', -- Min NCHAR
            N'A',                    -- Min NVARCHAR2
            'A',                     -- Min LONG
            -123456789012345678901234567.1234567890,  -- Negative NUMBER
            -9999999999,             -- Min 10-digit integer
            -12345678901234567890.12345678901234567890,  -- Negative NUMBER (free)
            -1.23456E+30,            -- Negative BINARY_FLOAT
            -1.23456789012345E+100,  -- Negative BINARY_DOUBLE
            DATE '0001-01-01',       -- Min DATE (Oracle min)
            TIMESTAMP '0001-01-01 00:00:00.000000000',
            TIMESTAMP '0001-01-01 00:00:00.000000000 -12:00',
            TIMESTAMP '0001-01-01 00:00:00.000000000',
            INTERVAL '-99999 23:59:59.999999999' DAY(5) TO SECOND(9),
            INTERVAL '-9999-11' YEAR(4) TO MONTH,
            TO_CLOB('A'),
            TO_NCLOB(N'A'),
            UTL_RAW.CAST_TO_RAW('A'),
            HEXTORAW('00')
        )
    """)

    # Row 5: Special float values (infinity, very small)
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            5,
            'FLOAT_SPECIAL       ',
            'Float special values',
            N'FLOAT_SPECIAL       ',
            N'Float special values',
            'Float special values row',
            0.0000000001,            -- Very small NUMBER
            1,
            0.000000000000000000000000000001,  -- Very small NUMBER (free)
            1.17549E-38,             -- Min positive BINARY_FLOAT
            2.22507485850720E-308,   -- Min positive BINARY_DOUBLE
            SYSDATE,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            INTERVAL '0 00:00:00.000000001' DAY(5) TO SECOND(9),
            INTERVAL '0-1' YEAR(4) TO MONTH,
            NULL,
            NULL,
            NULL,
            NULL
        )
    """)

    # Row 6: Unicode/special characters
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            6,
            'Unicode: ' || UNISTR('\\00C0\\00C1'),  -- À Á
            'Special: ' || UNISTR('\\00E9\\00E8\\00EA\\00EB') || ' café',  -- éèêë café
            N'CJK: ' || UNISTR('\\4E2D\\6587'),     -- 中文
            N'Emoji: ' || UNISTR('\\D83D\\DE00'),   -- 😀 (may not work on all DBs)
            'Unicode LONG: ' || UNISTR('\\00C0\\00C1\\00C2'),
            123.456,
            42,
            3.14159265358979323846,  -- Pi with many decimals (free NUMBER)
            3.14159,
            3.14159265358979,
            SYSDATE,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            INTERVAL '1 02:03:04.567890000' DAY(5) TO SECOND(9),
            INTERVAL '1-6' YEAR(4) TO MONTH,
            TO_CLOB('CLOB with ' || UNISTR('\\00C0\\00C1\\00C2\\00C3')),
            TO_NCLOB(N'NCLOB with ' || UNISTR('\\4E2D\\6587\\65E5\\672C')),  -- 中文日本
            UTL_RAW.CAST_TO_RAW('Binary data with special bytes'),
            HEXTORAW('DEADBEEF00FF')
        )
    """)

    # Row 7: Typical business data
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            7,
            'ACME_CORP           ',
            'John Smith - Senior Developer',
            N'ACME_CORP           ',
            N'Jean-Pierre Müller',
            'This is a typical long text field containing business information that might span multiple lines and contain various details about a customer or transaction.',
            12345.67,
            1000000,
            999.999999999999,        -- Business decimal (free NUMBER)
            99.99,
            123456.789012,
            DATE '2024-06-15',
            TIMESTAMP '2024-06-15 14:30:45.123456789',
            TIMESTAMP '2024-06-15 14:30:45.123456789 -05:00',
            TIMESTAMP '2024-06-15 14:30:45.123456789',
            INTERVAL '30 08:30:00.000000000' DAY(5) TO SECOND(9),
            INTERVAL '2-6' YEAR(4) TO MONTH,
            TO_CLOB('Customer notes: Important client with premium support subscription. Contact preferred via email.'),
            TO_NCLOB(N'国际客户备注：重要客户，需要多语言支持。'),
            UTL_RAW.CAST_TO_RAW('PDF_SIGNATURE_BYTES_HERE'),
            HEXTORAW('48454C4C4F')  -- 'HELLO' in hex
        )
    """)

    # Row 8: Boundary dates
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            8,
            'DATE_BOUNDARIES     ',
            'Testing date boundaries',
            N'DATE_BOUNDARIES     ',
            N'Testing date boundaries',
            'Date boundary tests',
            99.99,
            2000,
            2000.0101,               -- Y2K as decimal (free NUMBER)
            1.0,
            1.0,
            DATE '2000-01-01',       -- Y2K
            TIMESTAMP '2000-01-01 00:00:00.000000000',
            TIMESTAMP '2000-01-01 00:00:00.000000000 +00:00',
            TIMESTAMP '2000-01-01 00:00:00.000000000',
            INTERVAL '365 00:00:00.000000000' DAY(5) TO SECOND(9),
            INTERVAL '100-0' YEAR(4) TO MONTH,
            TO_CLOB('Y2K test data'),
            TO_NCLOB(N'Y2K test data'),
            UTL_RAW.CAST_TO_RAW('Y2K'),
            HEXTORAW('59324B')
        )
    """)

    # Row 9: Leap year / Feb 29
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            9,
            'LEAP_YEAR           ',
            'February 29th leap year',
            N'LEAP_YEAR           ',
            N'February 29th leap year',
            'Leap year date test',
            29.02,
            2024,
            20240229,                -- Leap date as integer (free NUMBER)
            29.0,
            29.0,
            DATE '2024-02-29',
            TIMESTAMP '2024-02-29 12:00:00.000000000',
            TIMESTAMP '2024-02-29 12:00:00.000000000 +00:00',
            TIMESTAMP '2024-02-29 12:00:00.000000000',
            INTERVAL '366 00:00:00.000000000' DAY(5) TO SECOND(9),
            INTERVAL '4-0' YEAR(4) TO MONTH,
            TO_CLOB('Leap year Feb 29'),
            TO_NCLOB(N'Leap year Feb 29'),
            UTL_RAW.CAST_TO_RAW('LEAP'),
            HEXTORAW('4C454150')
        )
    """)

    # Row 10: Mixed NULL pattern 1
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            10,
            'MIXED_NULL_1        ',
            NULL,
            N'MIXED_NULL_1        ',
            NULL,
            NULL,
            123.45,
            NULL,
            NULL,                    -- NULL free NUMBER
            3.14,
            NULL,
            DATE '2025-01-01',
            NULL,
            TIMESTAMP '2025-01-01 00:00:00.000000000 +00:00',
            NULL,
            INTERVAL '1 00:00:00.000000000' DAY(5) TO SECOND(9),
            NULL,
            TO_CLOB('Not null'),
            NULL,
            UTL_RAW.CAST_TO_RAW('Not null'),
            NULL
        )
    """)

    # Row 11: Mixed NULL pattern 2 (opposite of row 10)
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            11,
            NULL,
            'MIXED_NULL_2',
            NULL,
            N'MIXED_NULL_2',
            'Not null long',
            NULL,
            456,
            2.71828182845904523536,  -- e constant (free NUMBER)
            NULL,
            2.71828,
            NULL,
            TIMESTAMP '2025-06-15 10:30:00.000000000',
            NULL,
            TIMESTAMP '2025-06-15 10:30:00.000000000',
            NULL,
            INTERVAL '5-3' YEAR(4) TO MONTH,
            NULL,
            TO_NCLOB(N'Not null nclob'),
            NULL,
            HEXTORAW('CAFEBABE')
        )
    """)

    # Row 12: Very precise numbers
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            12,
            'PRECISION           ',
            'High precision numbers',
            N'PRECISION           ',
            N'High precision numbers',
            'Testing numeric precision',
            3.1415926535897932384626433832795028841971,
            123456789,
            1.41421356237309504880168872420969807856967,  -- sqrt(2) (free NUMBER)
            3.14159265,
            3.141592653589793238,
            SYSDATE,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            INTERVAL '0 00:00:00.123456789' DAY(5) TO SECOND(9),
            INTERVAL '0-0' YEAR(4) TO MONTH,
            TO_CLOB('Pi approximation test'),
            TO_NCLOB(N'Pi approximation test'),
            UTL_RAW.CAST_TO_RAW('PI'),
            HEXTORAW('5049')
        )
    """)

    # Row 13: Scientific notation numbers
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            13,
            'SCIENTIFIC          ',
            'Scientific notation',
            N'SCIENTIFIC          ',
            N'Scientific notation',
            'Scientific notation tests',
            1.23E+20,
            999999999,
            1.23456789E+50,          -- Scientific notation (free NUMBER)
            1.0E+30,
            1.0E+100,
            SYSDATE,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            INTERVAL '0 00:00:01.000000000' DAY(5) TO SECOND(9),
            INTERVAL '0-1' YEAR(4) TO MONTH,
            TO_CLOB('Scientific: 1.23E+20'),
            TO_NCLOB(N'Scientific: 1.23E+20'),
            UTL_RAW.CAST_TO_RAW('SCI'),
            HEXTORAW('534349')
        )
    """)

    # Row 14: Negative interval values
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            14,
            'NEG_INTERVAL        ',
            'Negative intervals',
            N'NEG_INTERVAL        ',
            N'Negative intervals',
            'Negative interval tests',
            -100.50,
            -500,
            -0.333333333333333333333333333,  -- -1/3 (free NUMBER)
            -1.5,
            -1.5,
            DATE '2020-01-01',
            TIMESTAMP '2020-01-01 00:00:00.000000000',
            TIMESTAMP '2020-01-01 00:00:00.000000000 -08:00',
            TIMESTAMP '2020-01-01 00:00:00.000000000',
            INTERVAL '-5 12:30:45.123456789' DAY(5) TO SECOND(9),
            INTERVAL '-10-6' YEAR(4) TO MONTH,
            TO_CLOB('Negative intervals'),
            TO_NCLOB(N'Negative intervals'),
            UTL_RAW.CAST_TO_RAW('NEG'),
            HEXTORAW('4E4547')
        )
    """)

    # Row 15: All LOBs NULL
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            15,
            'LOB_NULL            ',
            'All LOBs are NULL',
            N'LOB_NULL            ',
            N'All LOBs are NULL',
            'All LOBs are NULL in this row',
            15.0,
            15,
            15,                      -- Integer (free NUMBER)
            15.0,
            15.0,
            SYSDATE,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            INTERVAL '15 00:00:00.000000000' DAY(5) TO SECOND(9),
            INTERVAL '1-3' YEAR(4) TO MONTH,
            NULL,
            NULL,
            NULL,
            HEXTORAW('0F')
        )
    """)

    # Row 16: Large CLOB content
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            16,
            'LARGE_CLOB          ',
            'Row with large CLOB',
            N'LARGE_CLOB          ',
            N'Row with large CLOB',
            'Large CLOB test',
            16.0,
            16,
            32000.5,                 -- Large CLOB size hint (free NUMBER)
            16.0,
            16.0,
            SYSDATE,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            INTERVAL '16 00:00:00.000000000' DAY(5) TO SECOND(9),
            INTERVAL '1-4' YEAR(4) TO MONTH,
            TO_CLOB(RPAD('Large CLOB content. ', 32000, 'This is repeated text to make a large CLOB. ')),
            TO_NCLOB(RPAD(N'Large NCLOB. ', 32000, N'繰り返しテキスト。')),
            UTL_RAW.CAST_TO_RAW(RPAD('BLOB', 2000, 'X')),
            HEXTORAW('10')
        )
    """)

    # Row 17: Whitespace variations
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            17,
            '   spaces   test    ',
            '  leading and trailing  ',
            N'   spaces   test    ',
            N'  leading and trailing  ',
            '   Long with spaces   ',
            17.0,
            17,
            17.17171717171717,       -- Repeating decimal (free NUMBER)
            17.0,
            17.0,
            SYSDATE,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            INTERVAL '17 00:00:00.000000000' DAY(5) TO SECOND(9),
            INTERVAL '1-5' YEAR(4) TO MONTH,
            TO_CLOB('  CLOB with spaces  '),
            TO_NCLOB(N'  NCLOB with spaces  '),
            UTL_RAW.CAST_TO_RAW('  BLOB  '),
            HEXTORAW('2020')
        )
    """)

    # Row 18: Newlines and tabs
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            18,
            'NEWLINES' || CHR(10) || 'TABS' || CHR(9),
            'Line1' || CHR(10) || 'Line2' || CHR(13) || CHR(10) || 'Line3',
            N'NEWLINES' || CHR(10) || 'TAB' || CHR(9),
            N'Line1' || CHR(10) || N'Line2',
            'Long with' || CHR(10) || 'newlines' || CHR(9) || 'and tabs',
            18.0,
            18,
            18.181818181818,         -- Another repeating pattern (free NUMBER)
            18.0,
            18.0,
            SYSDATE,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            INTERVAL '18 00:00:00.000000000' DAY(5) TO SECOND(9),
            INTERVAL '1-6' YEAR(4) TO MONTH,
            TO_CLOB('CLOB' || CHR(10) || 'with' || CHR(10) || 'newlines'),
            TO_NCLOB(N'NCLOB' || CHR(10) || N'改行付き'),
            UTL_RAW.CAST_TO_RAW('BLOB' || CHR(10)),
            HEXTORAW('0A0D09')
        )
    """)

    # Row 19: Single quotes and special SQL chars
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            19,
            'O''Brien test       ',
            'It''s a "test" with ''quotes''',
            N'O''Brien test       ',
            N'It''s a "test"',
            'Long with ''single'' and "double" quotes',
            19.0,
            19,
            19.9999999999999999,     -- Almost 20 (free NUMBER)
            19.0,
            19.0,
            SYSDATE,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            INTERVAL '19 00:00:00.000000000' DAY(5) TO SECOND(9),
            INTERVAL '1-7' YEAR(4) TO MONTH,
            TO_CLOB('CLOB ''quoted'''),
            TO_NCLOB(N'NCLOB ''quoted'''),
            UTL_RAW.CAST_TO_RAW('QUOTE'),
            HEXTORAW('27')
        )
    """)

    # Row 20: Binary patterns
    cursor.execute(f"""
        INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
            20,
            'BINARY_PATTERNS     ',
            'Various binary patterns',
            N'BINARY_PATTERNS     ',
            N'Various binary patterns',
            'Binary pattern tests',
            20.0,
            20,
            3735928559,              -- 0xDEADBEEF as decimal (free NUMBER)
            20.0,
            20.0,
            SYSDATE,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            SYSTIMESTAMP,
            INTERVAL '20 00:00:00.000000000' DAY(5) TO SECOND(9),
            INTERVAL '1-8' YEAR(4) TO MONTH,
            TO_CLOB('Binary test'),
            TO_NCLOB(N'Binary test'),
            HEXTORAW('000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F'),
            HEXTORAW('FFFEFDFCFBFAF9F8F7F6F5F4F3F2F1F0')
        )
    """)

    conn.commit()
    print("    Inserted 20 special test rows (edge cases)")

    # =========================================================================
    # Bulk Test Rows (21-500): Varied data for volume testing
    # =========================================================================

    random.seed(42)  # Reproducible randomness

    def random_string(length, charset=string.ascii_letters + string.digits):
        return ''.join(random.choices(charset, k=length))

    def maybe_null(value, null_probability=0.1):
        """Return NULL with given probability, otherwise return value."""
        return None if random.random() < null_probability else value

    bulk_rows = []
    for i in range(21, 501):
        # Vary the data patterns
        null_prob = 0.1 if i % 10 != 0 else 0.5  # Every 10th row has more NULLs

        # Character columns
        char_val = maybe_null(random_string(random.randint(1, 20)).ljust(20), null_prob)
        varchar2_val = maybe_null(random_string(random.randint(1, 200)), null_prob)
        nchar_val = maybe_null(random_string(random.randint(1, 20)).ljust(20), null_prob)
        nvarchar2_val = maybe_null(random_string(random.randint(1, 200)), null_prob)
        long_val = maybe_null(random_string(random.randint(10, 1000)), null_prob)

        # Numeric columns with varied ranges
        number_val = maybe_null(round(random.uniform(-1e10, 1e10), 10), null_prob)
        number_int_val = maybe_null(random.randint(-999999999, 999999999), null_prob)
        # Free NUMBER with random precision - can have many decimal places
        number_free_val = maybe_null(round(random.uniform(-1e20, 1e20), random.randint(0, 30)), null_prob)
        binary_float_val = maybe_null(random.uniform(-1e30, 1e30), null_prob)
        binary_double_val = maybe_null(random.uniform(-1e100, 1e100), null_prob)

        # Date/time - random dates between 1900 and 2100
        days_offset = random.randint(-36500, 36500)  # ~100 years range

        # Interval values
        interval_ds_days = random.randint(-1000, 1000)
        interval_ds_hours = random.randint(0, 23)
        interval_ds_mins = random.randint(0, 59)
        interval_ds_secs = random.randint(0, 59)
        interval_ds_frac = random.randint(0, 999999999)

        interval_ym_years = random.randint(-100, 100)
        interval_ym_months = random.randint(0, 11)

        # LOB columns - vary sizes
        clob_size = random.choice([0, 10, 100, 1000, 4000])
        clob_val = maybe_null(random_string(clob_size) if clob_size > 0 else '', null_prob)
        nclob_val = maybe_null(random_string(random.choice([0, 10, 100, 500])), null_prob)
        blob_val = maybe_null(random_string(random.choice([0, 10, 100, 500])).encode(), null_prob)

        # RAW column
        raw_size = random.randint(0, 100)
        raw_val = maybe_null(bytes([random.randint(0, 255) for _ in range(raw_size)]), null_prob)

        bulk_rows.append((
            i,
            char_val,
            varchar2_val,
            nchar_val,
            nvarchar2_val,
            long_val,
            number_val,
            number_int_val,
            number_free_val,
            binary_float_val,
            binary_double_val,
            days_offset,  # Will be used with DATE arithmetic
            interval_ds_days, interval_ds_hours, interval_ds_mins, interval_ds_secs, interval_ds_frac,
            interval_ym_years, interval_ym_months,
            clob_val,
            nclob_val,
            blob_val,
            raw_val
        ))

    # Insert in batches
    BATCH_SIZE = 50
    for batch_start in range(0, len(bulk_rows), BATCH_SIZE):
        batch = bulk_rows[batch_start:batch_start + BATCH_SIZE]

        for row in batch:
            (row_id, char_val, varchar2_val, nchar_val, nvarchar2_val, long_val,
             number_val, number_int_val, number_free_val, binary_float_val, binary_double_val,
             days_offset,
             ds_days, ds_hours, ds_mins, ds_secs, ds_frac,
             ym_years, ym_months,
             clob_val, nclob_val, blob_val, raw_val) = row

            # Build the INSERT statement with proper NULL handling
            cursor.execute(f"""
                INSERT INTO {schema_prefix}sample_datatypes_tbl VALUES (
                    :id,
                    :char_val,
                    :varchar2_val,
                    :nchar_val,
                    :nvarchar2_val,
                    :long_val,
                    :number_val,
                    :number_int_val,
                    :number_free_val,
                    :binary_float_val,
                    :binary_double_val,
                    DATE '2000-01-01' + :days_offset,
                    TIMESTAMP '2000-01-01 00:00:00' + NUMTODSINTERVAL(:days_offset, 'DAY'),
                    TIMESTAMP '2000-01-01 00:00:00 +00:00' + NUMTODSINTERVAL(:days_offset, 'DAY'),
                    TIMESTAMP '2000-01-01 00:00:00' + NUMTODSINTERVAL(:days_offset, 'DAY'),
                    NUMTODSINTERVAL(:ds_days * 86400 + :ds_hours * 3600 + :ds_mins * 60 + :ds_secs, 'SECOND') + NUMTODSINTERVAL(:ds_frac / 1000000000, 'SECOND'),
                    NUMTOYMINTERVAL(:ym_years * 12 + :ym_months, 'MONTH'),
                    :clob_val,
                    :nclob_val,
                    :blob_val,
                    :raw_val
                )
            """, {
                'id': row_id,
                'char_val': char_val,
                'varchar2_val': varchar2_val,
                'nchar_val': nchar_val,
                'nvarchar2_val': nvarchar2_val,
                'long_val': long_val,
                'number_val': number_val,
                'number_int_val': number_int_val,
                'number_free_val': number_free_val,
                'binary_float_val': binary_float_val,
                'binary_double_val': binary_double_val,
                'days_offset': days_offset,
                'ds_days': ds_days,
                'ds_hours': ds_hours,
                'ds_mins': ds_mins,
                'ds_secs': ds_secs,
                'ds_frac': ds_frac,
                'ym_years': ym_years,
                'ym_months': ym_months,
                'clob_val': clob_val,
                'nclob_val': nclob_val,
                'blob_val': blob_val,
                'raw_val': raw_val
            })

        conn.commit()

    print(f"    Inserted 480 bulk test rows (21-500)")
    print(f"    Total: 500 rows with comprehensive test data")
    cursor.close()


# Migration function dispatch - note: functions now take (conn, env)
MIGRATION_FUNCTIONS = {
    "000_create_test_user": migrate_000_create_test_user,
    "001_create_sample_datatypes": migrate_001_create_sample_datatypes,
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
