#!/usr/bin/env python3
"""
Compare execute message bytes between Python and Rust implementations.

Usage:
    python directives/scripts/compare_execute_msg.py
"""

import re
import sys
from pathlib import Path

TMP_DIR = Path(__file__).parent.parent.parent / "tmp"


def extract_python_execute_packet(log_file: Path) -> list[int]:
    """
    Extract the execute message packet from Python debug log.
    Looking for packet with function code 94 (0x5E = TNS_FUNC_EXECUTE).
    """
    content = log_file.read_text()

    # Find packets - format is "Sending packet [op N] on socket N"
    # followed by hex dump lines like "0000 : XX XX XX XX ..."
    packets = []
    current_packet = None

    for line in content.split('\n'):
        if 'Sending packet' in line:
            if current_packet is not None:
                packets.append(current_packet)
            current_packet = {'header': line, 'bytes': []}
        elif current_packet is not None and re.match(r'^\d{4} :', line):
            # Parse hex bytes from line like "0000 : 00 4A 00 00 01 00 00 00 |.J......|"
            parts = line.split('|')[0]  # Remove ASCII part
            hex_part = parts.split(':')[1].strip()
            for byte_str in hex_part.split():
                if len(byte_str) == 2:
                    try:
                        current_packet['bytes'].append(int(byte_str, 16))
                    except ValueError:
                        pass

    if current_packet is not None:
        packets.append(current_packet)

    # Find execute packet - DATA packet (type 6) with function code 94 (0x5E)
    for pkt in packets:
        data = pkt['bytes']
        if len(data) > 10:
            # Check if it's a DATA packet (type at offset 4 is 6)
            # and has function code 94 at payload start
            # Packet header is 8 bytes, then data flags (2 bytes), then message
            if data[4] == 6:  # DATA packet type
                # Payload starts at offset 8 (after 8-byte header)
                # Data flags at offset 8-9, then message type and function code
                payload_start = 8
                if len(data) > payload_start + 4:
                    # Check for message type 3 (TNS_MSG_TYPE_FUNCTION) and func 94
                    # Data flags are 2 bytes, then message type, then function code
                    msg_type = data[payload_start + 2]  # After 2-byte data flags
                    func_code = data[payload_start + 3]
                    if msg_type == 3 and func_code == 94:
                        # Return just the message payload (after header and data flags)
                        return data[payload_start + 2:]  # Skip data flags

    return []


def extract_rust_execute_bytes(log_file: Path) -> list[int]:
    """
    Extract execute message bytes from Rust debug log.
    Looking for lines after "[DEBUG] Execute message hex"
    """
    content = log_file.read_text()
    lines = content.split('\n')

    in_hex_dump = False
    hex_bytes = []

    for line in lines:
        if '[DEBUG] Execute message hex' in line:
            in_hex_dump = True
            continue
        if in_hex_dump:
            # Lines are like "  03 5E 01 61 00 01 18 01"
            stripped = line.strip()
            if stripped and all(c in '0123456789ABCDEFabcdef ' for c in stripped):
                for byte_str in stripped.split():
                    if len(byte_str) == 2:
                        try:
                            hex_bytes.append(int(byte_str, 16))
                        except ValueError:
                            pass
            else:
                # End of hex dump
                if hex_bytes:
                    break

    return hex_bytes


def compare_bytes(py_bytes: list[int], rs_bytes: list[int]) -> list[dict]:
    """Compare two byte arrays and return differences."""
    differences = []
    max_len = max(len(py_bytes), len(rs_bytes))

    for i in range(max_len):
        py_byte = py_bytes[i] if i < len(py_bytes) else None
        rs_byte = rs_bytes[i] if i < len(rs_bytes) else None

        if py_byte != rs_byte:
            differences.append({
                'offset': i,
                'py_byte': f'{py_byte:02X}' if py_byte is not None else 'N/A',
                'rs_byte': f'{rs_byte:02X}' if rs_byte is not None else 'N/A',
            })

    return differences


def main():
    py_log = TMP_DIR / "debug-py.log"
    rs_log = TMP_DIR / "debug-rs.log"

    if not py_log.exists():
        print(f"Python log not found: {py_log}")
        sys.exit(1)
    if not rs_log.exists():
        print(f"Rust log not found: {rs_log}")
        sys.exit(1)

    print("Extracting Python execute message...")
    py_bytes = extract_python_execute_packet(py_log)
    print(f"  Found {len(py_bytes)} bytes")

    print("\nExtracting Rust execute message...")
    rs_bytes = extract_rust_execute_bytes(rs_log)
    print(f"  Found {len(rs_bytes)} bytes")

    if not py_bytes:
        print("\nERROR: Could not find Python execute message")
        print("Looking for DATA packet with function code 94...")
        sys.exit(1)

    if not rs_bytes:
        print("\nERROR: Could not find Rust execute message")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("COMPARISON")
    print("=" * 60)

    print(f"\nPython: {len(py_bytes)} bytes")
    print(f"Rust:   {len(rs_bytes)} bytes")

    # Print both as hex dumps side by side
    print("\nHex dumps:")
    print("-" * 60)

    max_len = max(len(py_bytes), len(rs_bytes))
    for i in range(0, max_len, 8):
        py_chunk = py_bytes[i:i+8]
        rs_chunk = rs_bytes[i:i+8]

        py_hex = ' '.join(f'{b:02X}' for b in py_chunk)
        rs_hex = ' '.join(f'{b:02X}' for b in rs_chunk)

        # Mark differences
        marker = "  "
        if py_chunk != rs_chunk:
            marker = "!!"

        print(f"{i:04d}: {py_hex:<24} | {rs_hex:<24} {marker}")

    # Find differences
    differences = compare_bytes(py_bytes, rs_bytes)

    print("\n" + "=" * 60)
    print(f"DIFFERENCES: {len(differences)} bytes differ")
    print("=" * 60)

    for diff in differences[:20]:  # Show first 20 differences
        print(f"  Offset {diff['offset']:4d}: Python={diff['py_byte']} Rust={diff['rs_byte']}")

    if len(differences) > 20:
        print(f"  ... and {len(differences) - 20} more differences")

    # Save comparison to file
    output_file = TMP_DIR / "execute_comparison.txt"
    with open(output_file, 'w') as f:
        f.write(f"Python bytes ({len(py_bytes)}):\n")
        f.write(' '.join(f'{b:02X}' for b in py_bytes) + '\n\n')
        f.write(f"Rust bytes ({len(rs_bytes)}):\n")
        f.write(' '.join(f'{b:02X}' for b in rs_bytes) + '\n\n')
        f.write(f"Differences ({len(differences)}):\n")
        for diff in differences:
            f.write(f"  {diff['offset']:4d}: py={diff['py_byte']} rs={diff['rs_byte']}\n")
    print(f"\nComparison saved to: {output_file}")


if __name__ == '__main__':
    main()
