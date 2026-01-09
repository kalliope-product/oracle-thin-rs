#!/usr/bin/env python3
"""
Execute a simple SELECT query with PYO_DEBUG_PACKETS=1 to dump packet bytes.

Usage:
    PYO_DEBUG_PACKETS=1 python query_with_debug.py
"""

import asyncio
import oracledb

# Connection parameters (same as Rust test)
host = "test-oracle-19c.ctgcsik2itm5.ap-southeast-1.rds.amazonaws.com"
port = 1521
service_name = "pdb1"
user = "admin"
password = "ThisIsASecret123"


async def main():
    try:
        print(f"Connecting to {host}:{port}/{service_name}...")
        conn = await oracledb.connect_async(
            user=user,
            password=password,
            host=host,
            port=port,
            service_name=service_name
        )
        print("Connected!")

        cursor = conn.cursor()
        cursor.prefetchrows = 100

        print("\nExecuting: SELECT 'hello' FROM DUAL")
        await cursor.execute("SELECT 'hello' FROM DUAL")

        async for row in cursor:
            print(f"Result: {row}")

        await cursor.close()
        await conn.close()
        print("\nDone!")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    asyncio.run(main())
