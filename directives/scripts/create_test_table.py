#!/usr/bin/env python3
"""
Create a test table with 5000 rows for Milestone 3 testing.

Table: TEST_DATA
Columns:
  - ID: NUMBER (primary key, 1-5000)
  - STR_COL: VARCHAR2(100) - string values
  - INT_COL: NUMBER - integer values
  - DEC_COL: NUMBER(10,2) - decimal values

Usage:
  python create_test_table.py
"""

import oracledb

# Connection parameters for Oracle 19c RDS
CONN_PARAMS = {
    "user": "admin",
    "password": "ThisIsASecret123",
    "host": "test-oracle-19c.ctgcsik2itm5.ap-southeast-1.rds.amazonaws.com",
    "port": 1521,
    "service_name": "pdb1",
}

NUM_ROWS = 5000


def main():
    print(f"Connecting to Oracle...")
    conn = oracledb.connect(**CONN_PARAMS)
    cursor = conn.cursor()

    # Drop table if exists
    print("Dropping existing table (if any)...")
    try:
        cursor.execute("DROP TABLE TEST_DATA PURGE")
        print("  Dropped existing TEST_DATA table")
    except oracledb.DatabaseError as e:
        if "ORA-00942" in str(e):  # Table doesn't exist
            print("  Table doesn't exist, creating fresh")
        else:
            raise

    # Create table
    print("Creating TEST_DATA table...")
    cursor.execute("""
        CREATE TABLE TEST_DATA (
            ID NUMBER PRIMARY KEY,
            STR_COL VARCHAR2(100),
            INT_COL NUMBER,
            DEC_COL NUMBER(10,2)
        )
    """)
    print("  Table created")

    # Insert rows in batches
    print(f"Inserting {NUM_ROWS} rows...")
    batch_size = 500
    rows_inserted = 0

    for batch_start in range(1, NUM_ROWS + 1, batch_size):
        batch_end = min(batch_start + batch_size, NUM_ROWS + 1)
        batch_data = []

        for i in range(batch_start, batch_end):
            str_val = f"row_{i:05d}"  # e.g., "row_00001"
            int_val = i * 10  # e.g., 10, 20, 30...
            dec_val = i / 100.0  # e.g., 0.01, 0.02, 0.03...
            batch_data.append((i, str_val, int_val, dec_val))

        cursor.executemany(
            "INSERT INTO TEST_DATA (ID, STR_COL, INT_COL, DEC_COL) VALUES (:1, :2, :3, :4)",
            batch_data,
        )
        rows_inserted += len(batch_data)
        print(f"  Inserted {rows_inserted}/{NUM_ROWS} rows...")

    conn.commit()
    print("Committed!")

    # Verify
    cursor.execute("SELECT COUNT(*) FROM TEST_DATA")
    count = cursor.fetchone()[0]
    print(f"\nVerification: TEST_DATA has {count} rows")

    # Show sample rows
    print("\nSample rows:")
    cursor.execute("SELECT * FROM TEST_DATA WHERE ID <= 5 ORDER BY ID")
    for row in cursor:
        print(f"  {row}")

    print("\nLast 3 rows:")
    cursor.execute("SELECT * FROM TEST_DATA WHERE ID > :1 ORDER BY ID", [NUM_ROWS - 3])
    for row in cursor:
        print(f"  {row}")

    cursor.close()
    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
