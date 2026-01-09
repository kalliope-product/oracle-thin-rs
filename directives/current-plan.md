# Current Plan: Query Support - COMPLETE

## Completed Work
- [x] Oracle 23ai connection (FastAuth path)
- [x] Oracle 19c connection (standard auth path)
- [x] Fixed FastAuth parameter parsing format
- [x] Fixed TTC field version tracking
- [x] Simple query returns results (both 19c and 23ai)
- [x] Query with NULL values works
- [x] Query from table works
- [x] Column metadata correct
- [x] ORA-01403 "no data found" handled correctly

## Key Implementation Details

### 1. Server vs Client TTC Field Version
Added `server_ttc_field_version` to Capabilities struct to track the server's actual field version separately from what we request:
- `ttc_field_version`: Used for column metadata parsing (what we request)
- `server_ttc_field_version`: Used for error info parsing (what server sends)

### 2. Message Handlers Added
- `TNS_MSG_TYPE_STATUS` (0x09): Simple status message
- `TNS_MSG_TYPE_SERVER_SIDE_PIGGYBACK` (0x17): Server state updates with opcode handling

### 3. 20c+ Error Info Fields
Server versions >= 20.1 send additional fields in TNS_MSG_TYPE_ERROR:
- `sql_type` (ub4)
- `server_checksum` (ub4)

These are only read when `server_ttc_field_version >= TNS_CCAP_FIELD_VERSION_20_1`.

## Files Modified
1. `src/protocol/packet.rs` - Added `server_ttc_field_version` field
2. `src/protocol/response.rs` - Added message handlers, updated parsing
3. `src/protocol/connect.rs` - Track server field version
4. `src/connection.rs` - Pass both field versions to parsing

## Next Steps (Future Work)
- [ ] Cursor-based fetching with pagination
- [ ] DML support (INSERT, UPDATE, DELETE)
- [ ] Bind variables / parameterized queries
- [ ] Transaction support (commit, rollback)
- [ ] More data types (DATE, TIMESTAMP, BLOB, CLOB)
