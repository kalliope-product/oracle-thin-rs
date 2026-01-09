#!/usr/bin/env python3
"""
Dump the raw bytes of an execute message from python-oracledb.

This script builds an execute message similar to what the Python thin client
would send for a simple SELECT query.

Usage:
    cd python-ref/python-oracledb
    python ../../directives/scripts/dump_execute_message.py
"""

import sys
import os

# Add the python-oracledb src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'python-ref', 'python-oracledb', 'src'))

# Import after path setup - these imports may fail if not built
try:
    import oracledb
    from oracledb.impl.thin import packet as thin_packet
    from oracledb.impl.thin import constants as thin_constants
except ImportError as e:
    print(f"Failed to import oracledb: {e}")
    print("Make sure python-oracledb is installed or built")
    sys.exit(1)


def build_execute_message_manually():
    """
    Build an execute message manually using the known protocol.
    This mirrors what python-oracledb does internally.
    """
    # SQL to execute
    sql = "SELECT 'hello' FROM DUAL"
    sql_bytes = sql.encode('utf-8')

    # Message parameters
    cursor_id = 0  # New cursor
    is_query = True
    prefetch_rows = 100
    ttc_field_version = 12  # Oracle 19c typical

    # Options calculation
    options = 0
    options |= thin_constants.TNS_EXEC_OPTION_PARSE  # New cursor
    options |= thin_constants.TNS_EXEC_OPTION_EXECUTE  # Query
    options |= thin_constants.TNS_EXEC_OPTION_FETCH  # Fetch prefetch
    options |= thin_constants.TNS_EXEC_OPTION_NOT_PLSQL  # Not PL/SQL

    # Exec flags
    exec_flags = 0
    exec_flags |= thin_constants.TNS_EXEC_FLAGS_IMPLICIT_RESULTSET

    buf = bytearray()

    def write_uint8(val):
        buf.append(val)

    def write_ub4(val):
        """Write Oracle UB4 (variable-length unsigned 4-byte int)"""
        if val == 0:
            buf.append(0)
        elif val <= 0x3f:
            buf.append(val)
        elif val <= 0x3fff:
            buf.append((val >> 8) | 0x40)
            buf.append(val & 0xff)
        elif val <= 0x3fffff:
            buf.append((val >> 16) | 0x80)
            buf.append((val >> 8) & 0xff)
            buf.append(val & 0xff)
        else:
            buf.append((val >> 24) | 0xc0)
            buf.append((val >> 16) & 0xff)
            buf.append((val >> 8) & 0xff)
            buf.append(val & 0xff)

    def write_bytes_with_length(data):
        """Write bytes with length prefix"""
        length = len(data)
        if length <= 0xfc:
            buf.append(length)
        else:
            buf.append(0xfe)
            write_ub4(length)
        buf.extend(data)

    # Function header
    write_uint8(thin_constants.TNS_MSG_TYPE_FUNCTION)  # message type (3)
    write_uint8(thin_constants.TNS_FUNC_EXECUTE)  # function code (94)
    write_uint8(1)  # sequence number

    # Options and cursor
    write_ub4(options)
    write_ub4(cursor_id)

    # SQL pointer and length
    write_uint8(1)  # has SQL (pointer)
    write_ub4(len(sql_bytes))  # SQL length

    # Vector pointer and al8i4 length
    write_uint8(1)  # pointer (vector)
    write_ub4(13)  # al8i4 array length

    # Various pointers
    write_uint8(0)  # al8o4 pointer
    write_uint8(0)  # al8o4l pointer

    # Prefetch settings
    write_ub4(0)  # prefetch buffer size
    write_ub4(prefetch_rows)  # prefetch rows
    write_ub4(thin_constants.TNS_MAX_LONG_LENGTH)  # max long size

    # Bind pointers (no binds)
    write_uint8(0)  # binds pointer
    write_ub4(0)  # num binds

    # More pointers
    write_uint8(0)  # al8app
    write_uint8(0)  # al8txn
    write_uint8(0)  # al8txl
    write_uint8(0)  # al8kv
    write_uint8(0)  # al8kvl

    # Define pointers
    write_uint8(0)  # al8doac pointer
    write_ub4(0)  # num defines

    # Registration and more pointers
    write_ub4(0)  # registration id
    write_uint8(0)  # al8objlist pointer
    write_uint8(1)  # al8objlen pointer (must be 1 per Python)
    write_uint8(0)  # al8blv pointer
    write_ub4(0)  # al8blvl
    write_uint8(0)  # al8dnam pointer
    write_ub4(0)  # al8dnaml
    write_ub4(0)  # al8regid_msb

    # DML rowcount pointers
    write_uint8(0)  # al8pidmlrc pointer
    write_ub4(0)  # al8pidmlrcbl
    write_uint8(0)  # al8pidmlrcl pointer

    # 12.2+ fields (ttc_field_version >= 7)
    if ttc_field_version >= 7:  # TNS_CCAP_FIELD_VERSION_12_2
        write_uint8(0)  # al8sqlsig pointer
        write_ub4(0)  # SQL signature length
        write_uint8(0)  # SQL ID pointer
        write_ub4(0)  # SQL ID size
        write_uint8(0)  # SQL ID length pointer

        # 12.2 EXT1 fields (ttc_field_version >= 9)
        if ttc_field_version >= 9:  # TNS_CCAP_FIELD_VERSION_12_2_EXT1
            write_uint8(0)  # chunk ids pointer
            write_ub4(0)  # num chunk ids

    # SQL bytes (for new cursor)
    write_bytes_with_length(sql_bytes)

    # al8i4 array
    write_ub4(1)  # [0] parse flag
    write_ub4(0)  # [1] execution count (0 for new query)
    write_ub4(0)  # [2]
    write_ub4(0)  # [3]
    write_ub4(0)  # [4]
    write_ub4(0)  # [5] SCN part 1
    write_ub4(0)  # [6] SCN part 2
    write_ub4(1 if is_query else 0)  # [7] is_query flag
    write_ub4(0)  # [8]
    write_ub4(exec_flags)  # [9] exec_flags
    write_ub4(0)  # [10] fetch orientation
    write_ub4(0)  # [11] fetch pos
    write_ub4(0)  # [12]

    return bytes(buf)


def main():
    print("Building execute message manually...")
    msg_bytes = build_execute_message_manually()

    print(f"\nMessage length: {len(msg_bytes)} bytes")
    print("\nHex dump:")
    for i in range(0, len(msg_bytes), 16):
        hex_part = ' '.join(f'{b:02X}' for b in msg_bytes[i:i+16])
        ascii_part = ''.join(chr(b) if 32 <= b < 127 else '.' for b in msg_bytes[i:i+16])
        print(f"{i:04X}  {hex_part:<48}  {ascii_part}")

    # Also dump as a single line for easy comparison
    print("\nSingle-line hex:")
    print(' '.join(f'{b:02X}' for b in msg_bytes))

    # Save to file
    with open('/tmp/execute_message_python.bin', 'wb') as f:
        f.write(msg_bytes)
    print("\nSaved to /tmp/execute_message_python.bin")

    # Save hex to file
    with open('/tmp/execute_message_python.hex', 'w') as f:
        f.write(' '.join(f'{b:02X}' for b in msg_bytes))
    print("Saved hex to /tmp/execute_message_python.hex")


if __name__ == '__main__':
    main()
