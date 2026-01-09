# Rust Implementation Patterns

## Buffer Allocation Strategy

**Problem:** Creating intermediate buffers then copying to prepend headers causes excessive allocations:
```rust
// BAD: Multiple allocations
let mut payload = Vec::new();
payload.write_ub4(value)?;
payload.write_bytes(&data)?;

let mut packet = Vec::with_capacity(8 + payload.len());
packet.write_packet_header(payload.len())?;  // header needs payload length
packet.extend_from_slice(&payload);          // copy!
```

**Solution:** Use a builder/struct pattern - construct message structure first, calculate sizes, then serialize once:

```rust
// GOOD: Builder pattern - single allocation

/// Message builder that knows its serialized size before writing
pub struct AuthMessageBuilder {
    function_code: u8,
    user_bytes: Option<Vec<u8>>,
    auth_mode: u32,
    key_values: Vec<(String, String, u32)>,  // key, value, flags
}

impl AuthMessageBuilder {
    /// Calculate total serialized size (excluding packet header)
    fn serialized_size(&self) -> usize {
        let mut size = 0;
        size += 1;  // function code
        size += 1;  // has_user flag
        size += ub4_size(self.user_bytes.as_ref().map_or(0, |b| b.len() as u32));
        size += ub4_size(self.auth_mode);
        // ... etc
        size
    }

    /// Serialize into pre-allocated buffer
    fn write_to(&self, buf: &mut WriteBuffer) -> Result<()> {
        buf.write_ub1(self.function_code)?;
        // ... write all fields
        Ok(())
    }
}

/// Packet wrapper that handles header
pub struct PacketBuilder {
    packet_type: u8,
    packet_flags: u8,
    data_flags: u16,
    payload: Box<dyn MessageBuilder>,
}

impl PacketBuilder {
    pub fn build(self) -> Vec<u8> {
        let payload_size = self.payload.serialized_size();
        let total_size = 8 + payload_size;  // 8-byte header

        let mut buf = Vec::with_capacity(total_size);

        // Write header with known size
        buf.extend_from_slice(&(total_size as u16).to_be_bytes());
        buf.extend_from_slice(&[0, 0]);  // checksum
        buf.push(self.packet_type);
        buf.push(self.packet_flags);
        buf.extend_from_slice(&[0, 0]);  // header checksum

        // Write payload directly
        self.payload.write_to(&mut buf)?;

        buf
    }
}
```

## Alternative: Cursor-based writing with size reservation

For DATA packets where we need data_flags after payload:

```rust
pub struct WriteBuffer {
    data: Vec<u8>,
    packet_start: usize,  // Where current packet header starts
}

impl WriteBuffer {
    /// Start a new packet, reserving header space
    pub fn start_packet(&mut self, packet_type: u8) {
        self.packet_start = self.data.len();
        // Reserve 8 or 10 bytes for header (fill in later)
        self.data.extend_from_slice(&[0u8; 10]);
    }

    /// Finish packet - go back and fill in header with actual length
    pub fn end_packet(&mut self, data_flags: u16) {
        let packet_len = self.data.len() - self.packet_start;

        // Write header at reserved position
        let header = &mut self.data[self.packet_start..];
        header[0..2].copy_from_slice(&(packet_len as u16).to_be_bytes());
        // ... fill rest of header
    }
}
```

## TNS Universal Integer Helpers

Pre-calculate size for capacity planning:

```rust
/// Returns number of bytes needed to encode value in TNS ub4 format
#[inline]
pub fn ub4_size(value: u32) -> usize {
    match value {
        0 => 1,
        1..=0xFF => 2,
        0x100..=0xFFFF => 3,
        _ => 5,
    }
}

/// Returns number of bytes needed for length-prefixed bytes
#[inline]
pub fn bytes_with_length_size(len: usize) -> usize {
    if len <= 252 {
        1 + len  // 1-byte length + data
    } else {
        // Long format: 0xFE + chunked
        1 + ((len + 32766) / 32767) * 5 + len + 4
    }
}
```

## Message Trait Pattern

```rust
pub trait Message {
    /// Calculate serialized size (for pre-allocation)
    fn serialized_size(&self) -> usize;

    /// Write message content to buffer (buffer already has capacity)
    fn write_to(&self, buf: &mut impl Write) -> Result<()>;

    /// Process response from server
    fn process_response(&mut self, buf: &mut ReadBuffer) -> Result<()>;
}

/// Helper to build and send a message
pub fn send_message<M: Message>(transport: &mut Transport, msg: &M) -> Result<()> {
    let size = 8 + msg.serialized_size();  // packet header + payload
    let mut buf = Vec::with_capacity(size);

    // Write packet header
    write_packet_header(&mut buf, TNS_PACKET_TYPE_DATA, size)?;

    // Write message
    msg.write_to(&mut buf)?;

    transport.send(&buf)?;
    Ok(())
}
```

## Shared Immutable Data with Arc

**Problem:** Multiple structs need access to the same immutable data (e.g., rows sharing column metadata).

**Bad:** Clone the data for each struct:
```rust
// BAD: Each row clones all column names
pub struct Row {
    values: Vec<OracleValue>,
    column_names: Vec<String>,  // Cloned for every row!
}
```

**Good:** Use `Arc<T>` for shared ownership:
```rust
// GOOD: All rows share one Arc reference
pub struct ColumnInfo {
    pub columns: Vec<Column>,
}

pub struct Row {
    values: Vec<OracleValue>,
    column_info: Arc<ColumnInfo>,  // Cheap clone (reference count)
}

// Create once, share across all rows
let column_info = Arc::new(ColumnInfo::from_metadata(&metadata));
for row_data in rows {
    rows.push(Row::new(values, column_info.clone()));  // Arc::clone is O(1)
}
```

## Internal vs Public API Types

**Pattern:** Separate wire-format types (internal) from user-facing types (public).

```rust
// Internal: matches wire protocol exactly
pub struct ColumnMetadata {
    pub name: String,
    pub oracle_type: u8,      // Raw type number from wire
    pub precision: i8,
    pub scale: i8,
    pub max_size: u32,
    pub buffer_size: u32,
    pub nullable: bool,
}

// Public: user-friendly with rich types
pub struct Column {
    pub name: String,
    pub nullable: bool,
    pub data_type: OracleType,      // Parsed enum (Result-based construction)
    pub oracle_type_num: u8,        // Keep raw for debugging
}

// Conversion via From trait
impl From<&ColumnMetadata> for Column {
    fn from(meta: &ColumnMetadata) -> Self {
        Column {
            name: meta.name.clone(),
            nullable: meta.nullable,
            data_type: OracleType::from_raw(meta.oracle_type, ...).unwrap_or_default(),
            oracle_type_num: meta.oracle_type,
        }
    }
}
```

## Result Return for Unsupported Features

**Problem:** An enum constructor needs to handle unknown variants.

**Bad:** Return `Option<Self>` - silently swallows the problem:
```rust
// BAD: Caller doesn't know WHY it failed, error gets lost
pub fn from_raw(type_num: u8) -> Option<Self> {
    match type_num {
        1 => Some(OracleType::Varchar2 { max_size }),
        _ => None,  // Error is silently swallowed!
    }
}
```

**Good:** Return `Result<Self>` with explicit error:
```rust
// GOOD: Error propagates with context
pub fn from_raw(type_num: u8, ...) -> Result<Self> {
    match type_num {
        1 => Ok(OracleType::Varchar2 { max_size }),
        2 => Ok(OracleType::Number { precision, scale }),
        _ => Err(Error::UnsupportedType { type_num }),  // Explicit, propagates
    }
}

// Caller can handle or propagate
let oracle_type = OracleType::from_raw(meta.oracle_type, ...)?;  // Propagates error
```

**When to use Option vs Result:**
- `Option`: Value might legitimately not exist (e.g., nullable field, optional config)
- `Result`: Operation can fail and caller needs to know why (e.g., unsupported type, parse error)

## Don't Redefine Constants

**Bad:** Copy constants to multiple files:
```rust
// src/types/oracle_type.rs
const ORA_TYPE_NUM_VARCHAR: u8 = 1;  // BAD: Duplicate!
const ORA_TYPE_NUM_NUMBER: u8 = 2;
```

**Good:** Import from single source of truth:
```rust
// src/types/oracle_type.rs
use crate::protocol::constants::{
    ORA_TYPE_NUM_VARCHAR, ORA_TYPE_NUM_NUMBER, ORA_TYPE_NUM_CHAR,
};
```

Note: Constants in `protocol/constants.rs` are `u16`. Cast when needed: `col.oracle_type as u16`.

## Module Organization for Extensibility

**Pattern:** Group related decode/encode functions by data type for easy extension.

```
src/protocol/
├── types/           # Type definitions (part of protocol)
│   ├── mod.rs
│   ├── oracle_type.rs
│   ├── column.rs
│   ├── row.rs
│   └── value.rs
├── decode/          # Decode functions by type
│   ├── mod.rs
│   ├── number.rs
│   └── date.rs      # (future)
└── encode/          # Encode functions (future)
```

## Summary

1. **Never** create a buffer for payload, then create another buffer to prepend headers
2. **Always** calculate total size first, allocate once, write in order
3. Use builder structs or `start_packet()`/`end_packet()` pattern for size reservation
4. Implement `serialized_size()` methods on message types for capacity planning
5. Use `Arc<T>` for sharing immutable data across structs (e.g., column info across rows)
6. Separate internal wire-format types from public API types, use `From` trait for conversion
7. Return `Result<Self>` with explicit error for unsupported features, NOT `Option<Self>`
8. **Never** duplicate constants - import from single source (`protocol/constants.rs`)
9. Keep protocol-related types inside `protocol/` module
