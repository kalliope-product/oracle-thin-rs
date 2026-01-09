# Protocol Learnings

## TNS Quirks Discovered
*(Add entries as you discover them)*

### Template
```
## [Short Title]
**Date:** YYYY-MM-DD
**Symptom:** What went wrong
**Root Cause:** Why it happened
**Fix:** What solved it
**Versions Affected:** 19c / 23ai / both
```

---

## TNS Packet Header (8 bytes)
**Date:** 2026-01-06
- Bytes 0-1: Packet length (BE), or 0-3 for large SDU
- Byte 4: Packet type (1=CONNECT, 2=ACCEPT, 4=REFUSE, 6=DATA)
- Byte 5: Packet flags

---

## Connection Flow
**Date:** 2026-01-06
1. TCP connect → 2. CONNECT packet → 3. ACCEPT → 4. Data type exchange → 5. O5LOGON auth → 6. Session ready

---

## Authentication
**Date:** 2026-01-06
- Phase 1: Client info (terminal, program, machine, pid, sid)
- Phase 2: Verifier (11g SHA1 or 12c PBKDF2+SHA512)
- AES-CBC encrypt password with derived combo key

---

## Version Differences (Known)
**Date:** 2026-01-06
- 19c: Typically 12c verifier, may have stricter capability requirements
- 23ai: May require native encryption by default, newer capability flags

---

## UB4 Byte Order is Big-Endian
**Date:** 2026-01-06
**Symptom:** Execute message rejected by server (MARKER packets with BREAK/RESET)
**Root Cause:** write_ub4 was using little-endian byte order, but Oracle TNS uses big-endian
**Fix:** Changed `val.to_le_bytes()` to `val.to_be_bytes()` in write_ub4/write_ub8
**Versions Affected:** both
**Python Location:** `impl/base/buffer.pyx` - `decode_integer()` uses BE order

---

## DESCRIBE_INFO Has Leading Raw Bytes
**Date:** 2026-01-06
**Symptom:** "Invalid UB2 length: 3" error when parsing execute response
**Root Cause:** After DESCRIBE_INFO message type (0x10), server sends raw bytes that must be skipped before parsing column metadata
**Fix:** Call `skip_raw_bytes_chunked()` immediately after reading DESCRIBE_INFO message type
**Versions Affected:** 19c (likely both)
**Python Location:** `impl/thin/messages/base.pyx` line 1248-1249:
```python
elif message_type == TNS_MSG_TYPE_DESCRIBE_INFO:
    buf.skip_raw_bytes_chunked()  # <-- Must skip first!
    self._process_describe_info(buf, self.cursor, self.cursor_impl)
```

---

## Debugging Protocol Issues - Byte Comparison Workflow
**Date:** 2026-01-06
**Process:**
1. Save Python debug output: `PYO_DEBUG_PACKETS=1 python script.py > tmp/debug-py.log 2>&1`
2. Save Rust debug output: `cargo test ... > tmp/debug-rs.log 2>&1`
3. Use comparison script: `python directives/scripts/compare_execute_msg.py`
4. Never load raw bytes directly into context - write scripts to analyze them

---

## TODO: read_ub4 Byte Order
**Status:** Needs investigation
**Issue:** read_ub4 in buffer.rs uses little-endian (val |= b << (i * 8)), but should use big-endian to match Python
**Python Location:** `impl/base/buffer.pyx` line 389+ - uses `decode_integer()` which is BE

---

## FastAuth Auth Parameter Format
**Date:** 2026-01-08
**Symptom:** "Missing AUTH_VFR_DATA" error when connecting to Oracle 23ai
**Root Cause:** Auth parameters have a ub4 indicator before each string that we weren't reading
**Fix:** Add `read_ub4()` before each `read_str_with_length()` in parameter parsing
**Versions Affected:** 23ai (FastAuth path)
**Format:**
```
num_params: ub2
for each param:
    key_indicator: ub4    <-- Was missing!
    key: str_with_length (u8 len + bytes)
    value_indicator: ub4  <-- Was missing!
    value: str_with_length (u8 len + bytes)
    flags/verifier_type: ub4
```
**Python Location:** `impl/thin/messages/auth.pyx` line 212-222 - `read_str_with_length()` internally calls `read_ub4()` first

---

## TNS_MSG_TYPE_ERROR is a Complex Status Structure
**Date:** 2026-01-08
**Symptom:** ORA-00001 error with empty message when connecting to 23ai
**Root Cause:** TNS_MSG_TYPE_ERROR (0x04) is NOT a simple error - it's a ~30 field status structure. The first ub4 is `call_status`, not the error code. The real error number (`info.num`) comes near the end.
**Fix:** Parse full structure or skip to actual_error_num field; only raise error if actual_error_num != 0
**Versions Affected:** both (but FastAuth sends this as completion status)
**Structure (simplified):**
```
call_status (ub4)        <-- NOT the error code!
end_to_end_seq (ub2)
row_number (ub4)
error_num_hint (ub2)     <-- Also not the real error
... 15+ more fields ...
actual_error_num (ub4)   <-- THIS is the real error number
rowcount (ub8)
error_message (str)      <-- Only present if actual_error_num != 0
```
**Python Location:** `impl/thin/messages/base.pyx` line 162-250 - `_process_error_info()`

---

## Server vs Client TTC Field Version for Error Info
**Date:** 2026-01-08
**Symptom:** 19c queries fail with "Buffer too small" after 23ai fix; or 23ai gets wrong message types
**Root Cause:** Error info (TNS_MSG_TYPE_ERROR) has 20c+ fields (sql_type, server_checksum) that are sent based on the SERVER's version, not what we request. Column metadata uses what we REQUEST. Need to track both versions.
**Fix:** Add `server_ttc_field_version` to Capabilities struct:
- `ttc_field_version`: What we request (used for column metadata parsing)
- `server_ttc_field_version`: Server's actual version (used for error info parsing)
**Versions Affected:** 19c vs 23ai difference
**Key Insight:** For FastAuth, we request version 13 but 23ai server (version 25) still sends 20c+ error fields. For 19c (version 12), no 20c+ fields are sent.
```rust
// In parse_error_info, use server version:
if server_ttc_field_version >= TNS_CCAP_FIELD_VERSION_20_1 {
    let _sql_type = buf.read_ub4()?;
    let _server_checksum = buf.read_ub4()?;
}
```