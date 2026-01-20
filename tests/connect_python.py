import os
os.environ["PYO_DEBUG_PACKETS"] = "1"
from dotenv import load_dotenv
import oracledb
import asyncio

VERSION = "23AI" # "23AI"
# Test connection to Oracle database
async def main():
    # conn = await oracledb.connect_async(
    #     user="read_user",
    #     password="ThisIsASecret123",
    #     dsn="localhost/freepdb1",
    # )
    load_dotenv(".env")
    conn = await oracledb.connect_async(
        user=os.environ[f"ORACLE_{VERSION}_USERNAME"],
        password=os.environ[f"ORACLE_{VERSION}_PASSWORD"],
        dsn=f'{os.environ[f"ORACLE_{VERSION}_HOST"]}:{os.environ[f"ORACLE_{VERSION}_PORT"]}/{os.environ[f"ORACLE_{VERSION}_SERVICE"]}',
        sdu=5081,
    )
    print("Connected to the database.")
    cursor = conn.cursor()
    print("Preparing and executing query...")
    cursor.prepare("SELECT id, CAST(clob_col AS VARCHAR2(4000)) as T1, CAST(clob_col AS VARCHAR2(4000)) as T2 FROM sample_datatypes_tbl WHERE id = 3")
    cursor.arraysize = 100
    cursor.prefetchrows = 100
    print("Fetching data with LOBs...")
    await cursor.execute(None, fetch_lobs=True)
    print("Fetching data ")
    data = await cursor.fetchall()
    print(f"Received data: {len(data)}")
    for row in data:
        print(f"Row ID: {row[0]}, CLOB Data: {row[1]}...")  # Print first 30 chars of CLOB

if __name__ == "__main__":
    asyncio.run(main())