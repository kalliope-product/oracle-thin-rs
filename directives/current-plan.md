# Current Implementation Plan: CLOB/BLOB Support

## Status: Planning
Last Updated: 2026-01-10

## Overview

Add CLOB (Character Large Object) and BLOB (Binary Large Object) support to oracle-thin-rs. This enables reading large text and binary data from Oracle databases.

## Background

### LOB Types in Oracle
| Type | Description | Wire Type | Data Type |
|------|-------------|-----------|-----------|
| CLOB | Character Large Object | 112 | UTF-8 text |
| NCLOB | National CLOB | 112 | UTF-16 text |
| BLOB | Binary Large Object | 113 | Raw bytes |
| BFILE | External binary file | 114 | File reference |

### Two LOB Access Patterns

**1. Inline/Prefetch (Small LOBs)**
- Data returned directly in query results
- Uses `TNS_DATA_TYPE_DCLOB` (195) / `TNS_DATA_TYPE_DBLOB` (196)
- Simple: decode as String/bytes
- Limited to ~1GB

**2. LOB Locators (Large LOBs)**
- Query returns 40-byte locator (handle)
- Requires separate LOB_OP messages to read data
- Supports chunked/streaming reads
- No size limit

### Python Reference Files
| File | Purpose |
|------|---------|
| `python-ref/.../lob.pyx` | LOB class implementation |
| `python-ref/.../messages/lob_op.pyx` | LOB operation messages |
| `python-ref/.../packet.pyx:490-506` | LOB locator parsing |
| `python-ref/.../constants.pxi` | LOB constants and flags |

---

## Implementation Phases

### Phase 1: Basic Inline LOB Support (MVP)
**Goal:** Read small CLOBs/BLOBs that fit in a single response.

This covers the common case where LOB data is small enough to be prefetched inline with query results.

#### 1.1 Add OracleType variants

**File:** `src/protocol/types/oracle_type.rs`

```rust
pub enum OracleType {
    // ... existing variants
    Clob,
    Nclob,
    Blob,
    // Bfile (Phase 3)
}
```

Update `from_raw()` to handle type numbers:
- `ORA_TYPE_NUM_CLOB` (112) → `Clob`
- `ORA_TYPE_NUM_BLOB` (113) → `Blob`

#### 1.2 Add OracleValue::Bytes variant

**File:** `src/protocol/types/value.rs`

```rust
pub enum OracleValue {
    Null,
    String(String),
    Number(String),
    Date(NaiveDateTime),
    Bytes(Vec<u8>),  // NEW: For BLOB data
}
```

Add accessor methods:
```rust
impl OracleValue {
    pub fn as_bytes(&self) -> Option<&[u8]> { ... }
    pub fn into_bytes(self) -> Option<Vec<u8>> { ... }
}
```

#### 1.3 Update column value parsing

**File:** `src/protocol/response.rs`

In `parse_column_value()`, add cases for LOB types:

```rust
ORA_TYPE_NUM_CLOB | ORA_TYPE_NUM_LONG_NVARCHAR => {
    // CLOB data is UTF-8 encoded
    let s = String::from_utf8_lossy(&bytes).to_string();
    Ok(OracleValue::String(s))
}
ORA_TYPE_NUM_BLOB => {
    // BLOB data is raw bytes
    Ok(OracleValue::Bytes(bytes.to_vec()))
}
```

#### 1.4 Create LOB decoder module

**File:** `src/protocol/decode/lob.rs` (NEW)

```rust
//! LOB (Large Object) decoding utilities

/// Decode inline CLOB data to String
pub fn decode_clob(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}

/// Decode inline BLOB data (passthrough)
pub fn decode_blob(bytes: &[u8]) -> Vec<u8> {
    bytes.to_vec()
}
```

#### 1.5 Add integration tests

**File:** `tests/test_23ai.rs` and `tests/test_19c.rs`

```rust
#[tokio::test]
async fn test_clob_read() {
    let mut conn = connect().await;
    let mut cursor = conn.query("SELECT clob_col FROM sample_datatypes_tbl WHERE id = 1").await.unwrap();
    let row = cursor.fetch_one().await.unwrap().unwrap();

    let clob_value = row.get::<String>(0).unwrap();
    assert!(clob_value.contains("large text object"));
}

#[tokio::test]
async fn test_blob_read() {
    let mut conn = connect().await;
    let mut cursor = conn.query("SELECT blob_col FROM sample_datatypes_tbl WHERE id = 1").await.unwrap();
    let row = cursor.fetch_one().await.unwrap().unwrap();

    let blob_value = row.get::<Vec<u8>>(0).unwrap();
    assert!(!blob_value.is_empty());
}
```

#### 1.6 Files Changed (Phase 1)

| File | Changes |
|------|---------|
| `src/protocol/types/oracle_type.rs` | Add Clob, Nclob, Blob variants |
| `src/protocol/types/value.rs` | Add Bytes variant, accessor methods |
| `src/protocol/types/mod.rs` | Re-export new types |
| `src/protocol/decode/mod.rs` | Add lob module |
| `src/protocol/decode/lob.rs` | NEW: LOB decoding functions |
| `src/protocol/response.rs` | Handle LOB types in parse_column_value |
| `tests/test_23ai.rs` | Add LOB tests |
| `tests/test_19c.rs` | Add LOB tests |

---

### Phase 2: LOB Locators (For Large LOBs)
**Goal:** Support reading LOBs larger than prefetch buffer via locators.

#### 2.1 LOB Locator Structure

**File:** `src/protocol/types/lob.rs` (NEW)

```rust
/// LOB locator - 40-byte handle returned by Oracle for large LOBs
#[derive(Debug, Clone)]
pub struct LobLocator {
    /// Raw locator bytes (40 bytes)
    pub(crate) data: Vec<u8>,
    /// LOB size in bytes (BLOB) or characters (CLOB)
    pub size: u64,
    /// Optimal chunk size for reading
    pub chunk_size: u32,
    /// LOB type
    pub lob_type: LobType,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LobType {
    Clob,
    Nclob,
    Blob,
    Bfile,
}

impl LobLocator {
    /// Check if this is a temporary LOB
    pub fn is_temporary(&self) -> bool { ... }

    /// Check if LOB uses variable-length charset (UTF-16)
    pub fn is_var_length_charset(&self) -> bool { ... }
}
```

#### 2.2 LOB Locator Parsing

**File:** `src/protocol/decode/lob.rs`

```rust
/// Parse LOB locator from wire format
///
/// Format:
/// - 4 bytes: total length (0 = NULL)
/// - 8 bytes: LOB size (except BFILE)
/// - 4 bytes: chunk size (except BFILE)
/// - 40 bytes: locator data
pub fn decode_lob_locator(buf: &mut ReadBuffer, lob_type: LobType) -> Result<Option<LobLocator>> {
    let total_len = buf.read_u32()?;
    if total_len == 0 {
        return Ok(None);  // NULL LOB
    }

    let (size, chunk_size) = if lob_type != LobType::Bfile {
        (buf.read_u64()?, buf.read_u32()?)
    } else {
        (0, 0)
    };

    let data = buf.read_bytes(40)?;

    Ok(Some(LobLocator {
        data: data.to_vec(),
        size,
        chunk_size,
        lob_type,
    }))
}

/// Locator flag positions
const LOB_LOC_FLAGS_BLOB: u8 = 0x01;      // Byte 4, bit 0
const LOB_LOC_FLAGS_VALUE_BASED: u8 = 0x20;  // Byte 4, bit 5
const LOB_LOC_FLAGS_ABSTRACT: u8 = 0x40;     // Byte 4, bit 6 (temp LOB)
const LOB_LOC_FLAGS_VAR_LENGTH_CHARSET: u8 = 0x80;  // Byte 6, bit 7
```

#### 2.3 LOB Operation Message

**File:** `src/protocol/messages/lob_op.rs` (NEW)

```rust
/// LOB operation codes
pub const TNS_LOB_OP_GET_LENGTH: u32 = 0x0001;
pub const TNS_LOB_OP_READ: u32 = 0x0002;
pub const TNS_LOB_OP_TRIM: u32 = 0x0020;
pub const TNS_LOB_OP_WRITE: u32 = 0x0040;
pub const TNS_LOB_OP_GET_CHUNK_SIZE: u32 = 0x4000;
pub const TNS_LOB_OP_CREATE_TEMP: u32 = 0x0110;
pub const TNS_LOB_OP_FREE_TEMP: u32 = 0x0111;
pub const TNS_LOB_OP_OPEN: u32 = 0x8000;
pub const TNS_LOB_OP_CLOSE: u32 = 0x10000;

/// Build a LOB read request message
pub fn build_lob_read_message(
    locator: &LobLocator,
    offset: u64,
    amount: u64,
) -> Vec<u8> { ... }

/// Parse LOB data response
pub fn parse_lob_data_response(buf: &mut ReadBuffer) -> Result<Vec<u8>> { ... }
```

#### 2.4 Cursor LOB Reading Methods

**File:** `src/cursor.rs`

```rust
impl Cursor {
    /// Read CLOB data from a LOB locator
    pub async fn read_clob(&mut self, locator: &LobLocator) -> Result<String> {
        let bytes = self.read_lob_bytes(locator, 0, locator.size).await?;
        Ok(String::from_utf8_lossy(&bytes).to_string())
    }

    /// Read BLOB data from a LOB locator
    pub async fn read_blob(&mut self, locator: &LobLocator) -> Result<Vec<u8>> {
        self.read_lob_bytes(locator, 0, locator.size).await
    }

    /// Read LOB data in chunks
    pub async fn read_lob_bytes(
        &mut self,
        locator: &LobLocator,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>> { ... }
}
```

#### 2.5 OracleValue LOB Variant

**File:** `src/protocol/types/value.rs`

```rust
pub enum OracleValue {
    // ... existing
    Lob(LobLocator),  // For large LOBs that need separate reading
}
```

#### 2.6 Files Changed (Phase 2)

| File | Changes |
|------|---------|
| `src/protocol/types/lob.rs` | NEW: LobLocator, LobType |
| `src/protocol/types/mod.rs` | Export lob module |
| `src/protocol/types/value.rs` | Add Lob variant |
| `src/protocol/decode/lob.rs` | Add locator parsing |
| `src/protocol/messages/mod.rs` | Add lob_op module |
| `src/protocol/messages/lob_op.rs` | NEW: LOB operation messages |
| `src/cursor.rs` | Add read_clob, read_blob methods |

---

### LOB Size Awareness & User Helpers

**Problem:** Users shouldn't have to guess whether a LOB is 1KB or 1GB before reading it.

#### Approach 1: Expose Size Metadata on LobLocator

The LOB locator from Oracle already contains size information:

```rust
/// LOB locator with size metadata
#[derive(Debug, Clone)]
pub struct LobLocator {
    data: Vec<u8>,
    /// LOB size in bytes (BLOB) or characters (CLOB)
    pub size: u64,
    /// Optimal chunk size recommended by Oracle
    pub chunk_size: u32,
    pub lob_type: LobType,
}

impl LobLocator {
    /// Size in bytes (BLOB) or characters (CLOB)
    pub fn size(&self) -> u64 { self.size }

    /// Human-readable size (e.g., "1.5 MB", "32 KB")
    pub fn size_human(&self) -> String {
        const KB: u64 = 1024;
        const MB: u64 = KB * 1024;
        const GB: u64 = MB * 1024;
        match self.size {
            s if s >= GB => format!("{:.1} GB", s as f64 / GB as f64),
            s if s >= MB => format!("{:.1} MB", s as f64 / MB as f64),
            s if s >= KB => format!("{:.1} KB", s as f64 / KB as f64),
            s => format!("{} bytes", s),
        }
    }

    /// Check if LOB exceeds a threshold
    pub fn exceeds(&self, max_bytes: u64) -> bool { self.size > max_bytes }
}
```

#### Approach 2: Smart OracleValue Enum

Differentiate between inline data (small, already loaded) and locators (large, need explicit read):

```rust
pub enum OracleValue {
    // ... existing variants

    /// Small CLOB data (already loaded inline)
    Clob(String),

    /// Small BLOB data (already loaded inline)
    Blob(Vec<u8>),

    /// Large LOB - use locator to read in chunks
    /// Contains size metadata so user can decide how to proceed
    LobRef(LobLocator),
}

impl OracleValue {
    /// Check if this is a LOB reference that needs explicit reading
    pub fn is_lob_ref(&self) -> bool {
        matches!(self, OracleValue::LobRef(_))
    }

    /// Get LOB locator if this is a large LOB
    pub fn as_lob_ref(&self) -> Option<&LobLocator> {
        match self {
            OracleValue::LobRef(loc) => Some(loc),
            _ => None,
        }
    }

    /// Get LOB size if this is a LOB (inline or reference)
    pub fn lob_size(&self) -> Option<u64> {
        match self {
            OracleValue::Clob(s) => Some(s.len() as u64),
            OracleValue::Blob(b) => Some(b.len() as u64),
            OracleValue::LobRef(loc) => Some(loc.size),
            _ => None,
        }
    }
}
```

#### Approach 3: Configurable Prefetch Threshold

Let users control when LOBs are fetched inline vs returned as locators:

```rust
/// Query options for LOB handling
pub struct QueryOptions {
    /// Max LOB size to prefetch inline (default: 1MB)
    /// LOBs larger than this return as LobRef
    pub lob_prefetch_size: u64,

    /// Whether to fetch LOB data at all (default: true)
    /// If false, all LOBs return as LobRef regardless of size
    pub fetch_lobs: bool,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            lob_prefetch_size: 1024 * 1024, // 1MB
            fetch_lobs: true,
        }
    }
}

// Usage:
let opts = QueryOptions { lob_prefetch_size: 100_000, ..Default::default() };
let cursor = conn.query_with_options("SELECT clob_col FROM t", opts).await?;
```

#### Approach 4: Safe Read Methods with Size Limits

Prevent accidental memory exhaustion:

```rust
impl Cursor {
    /// Read CLOB with a maximum size limit
    /// Returns Err if LOB exceeds max_size
    pub async fn read_clob_limited(
        &mut self,
        locator: &LobLocator,
        max_size: u64,
    ) -> Result<String> {
        if locator.size > max_size {
            return Err(Error::LobTooLarge {
                actual: locator.size,
                limit: max_size,
            });
        }
        self.read_clob(locator).await
    }

    /// Read first N bytes/chars of a LOB (preview)
    pub async fn read_lob_preview(
        &mut self,
        locator: &LobLocator,
        max_len: u64,
    ) -> Result<Vec<u8>> {
        let len = std::cmp::min(locator.size, max_len);
        self.read_lob_bytes(locator, 0, len).await
    }
}
```

#### Recommended API Pattern

Combine all approaches for a complete user experience:

```rust
// Example: Safe LOB handling with size awareness
let row = cursor.fetch_one().await?.unwrap();
let value = row.get_value(0);

match value {
    OracleValue::Clob(s) => {
        // Small CLOB, already loaded - safe to use
        println!("Content: {}", s);
    }
    OracleValue::LobRef(locator) => {
        // Large LOB - check size before reading
        println!("LOB size: {}", locator.size_human());

        if locator.exceeds(10 * 1024 * 1024) { // 10MB
            // Stream it instead of loading all at once
            let mut stream = cursor.stream_lob(&locator);
            while let Some(chunk) = stream.next().await {
                process_chunk(chunk?);
            }
        } else {
            // Safe to load entirely
            let data = cursor.read_clob(&locator).await?;
            println!("Content: {}", data);
        }
    }
    _ => { /* not a LOB */ }
}
```

#### Decision: Which Approaches to Implement?

| Approach | Phase | Priority | Rationale |
|----------|-------|----------|-----------|
| 1. Size metadata on LobLocator | 2 | **Must have** | Core functionality, no extra work |
| 2. Smart OracleValue enum | 2 | **Must have** | Clear API distinction |
| 3. Configurable prefetch | 2 | Nice to have | Adds complexity, can defer |
| 4. Safe read with limits | 2 | **Should have** | Prevents footguns |

**Minimum viable:** Approaches 1 + 2 + 4 give users full visibility and safety.

---

### Phase 3: Advanced Features (Future)
**Goal:** Full LOB functionality including streaming and writes.

#### 3.1 Streaming LOB Reads
```rust
/// Stream LOB data in chunks
pub fn stream_lob(&mut self, locator: &LobLocator) -> impl Stream<Item = Result<Vec<u8>>> { ... }
```

#### 3.2 LOB Write Operations
- Create temporary LOBs
- Write data to LOBs
- Append to LOBs
- Truncate LOBs

#### 3.3 BFILE Support
- Read external file references
- File existence checking
- Directory alias handling

#### 3.4 NCLOB Encoding
- Handle UTF-16 encoding for NCLOBs
- Detect charset from locator flags

---

## Testing Strategy

### Test Data (Already in Migrations)
The `sample_datatypes_tbl` already has CLOB and BLOB columns:
```sql
clob_col CLOB  -- 'This is a large text object...'
blob_col BLOB  -- UTL_RAW.CAST_TO_RAW('This is a blob data...')
```

### Test Cases

| Test | Phase | Description |
|------|-------|-------------|
| `test_clob_read` | 1 | Read small CLOB as String |
| `test_blob_read` | 1 | Read small BLOB as Vec<u8> |
| `test_null_clob` | 1 | Handle NULL CLOB |
| `test_null_blob` | 1 | Handle NULL BLOB |
| `test_large_clob` | 2 | Read CLOB via locator |
| `test_large_blob` | 2 | Read BLOB via locator |
| `test_clob_chunked` | 2 | Read CLOB in chunks |

### Add Large LOB Test Data

Add migration for large LOB testing:
```sql
-- Create table with large LOBs
CREATE TABLE large_lob_test (
    id NUMBER PRIMARY KEY,
    large_clob CLOB,
    large_blob BLOB
);

-- Insert 10MB CLOB
INSERT INTO large_lob_test (id, large_clob)
VALUES (1, RPAD('X', 10000000, 'X'));

-- Insert 10MB BLOB
INSERT INTO large_lob_test (id, large_blob)
VALUES (2, UTL_RAW.CAST_TO_RAW(RPAD('Y', 10000000, 'Y')));
```

---

## Protocol Details

### LOB Capability Flags (Already Defined)
```rust
// From src/protocol/constants.rs
TNS_CCAP_LOB_UB8_SIZE       // 8-byte size support
TNS_CCAP_LOB_ENCS           // Encoded LOBs
TNS_CCAP_LOB_PREFETCH_DATA  // Prefetch LOB data
TNS_CCAP_LOB_TEMP_SIZE
TNS_CCAP_LOB_PREFETCH_LENGTH
TNS_CCAP_LOB_12C
TNS_CCAP_LOB2_QUASI
TNS_CCAP_LOB2_2GB_PREFETCH
```

### Wire Format: LOB Locator (40 bytes)
```
Offset 0-3:   Header
Offset 4:     Flags byte 1 (BLOB=0x01, VALUE_BASED=0x20, ABSTRACT=0x40)
Offset 5:     Flags byte 2
Offset 6:     Flags byte 3 (VAR_LENGTH_CHARSET=0x80)
Offset 7:     Flags byte 4 (TEMP=0x01)
Offset 8-15:  Locator ID
Offset 16-39: Variable data
```

### Wire Format: LOB Read Response
```
Message type: TNS_MSG_TYPE_LOB_DATA (0x09)
Data format:
  - Length prefix (4 bytes)
  - Raw data (length bytes)
```

---

## Implementation Checklist

### Phase 1 (MVP)
- [ ] Add `Clob`, `Nclob`, `Blob` to OracleType enum
- [ ] Update `OracleType::from_raw()` for type 112/113
- [ ] Add `Bytes(Vec<u8>)` variant to OracleValue
- [ ] Add `as_bytes()`, `into_bytes()` to OracleValue
- [ ] Create `src/protocol/decode/lob.rs`
- [ ] Update `parse_column_value()` for LOB types
- [ ] Add CLOB/BLOB integration tests
- [ ] Test on both 23ai and 19c
- [ ] Run `cargo clippy`

### Phase 2 (Locators + Size Helpers)
- [ ] Create `LobLocator` struct with size metadata
- [ ] Add `size()`, `size_human()`, `exceeds()` helpers
- [ ] Create `LobType` enum
- [ ] Implement locator parsing
- [ ] Create `lob_op.rs` message module
- [ ] Add `read_clob()` to Cursor
- [ ] Add `read_blob()` to Cursor
- [ ] Add `read_clob_limited()` with size guard
- [ ] Add `read_lob_preview()` for partial reads
- [ ] Update `OracleValue` with `Clob`, `Blob`, `LobRef` variants
- [ ] Add `is_lob_ref()`, `as_lob_ref()`, `lob_size()` helpers
- [ ] Add `LobTooLarge` error variant
- [ ] Add large LOB test data
- [ ] Add large LOB integration tests
- [ ] Test size helper methods

### Phase 3 (Future)
- [ ] Streaming reads
- [ ] Write operations
- [ ] BFILE support
- [ ] NCLOB charset handling

---

## Version Compatibility

| Feature | 19c | 23ai |
|---------|-----|------|
| Inline CLOB/BLOB | Yes | Yes |
| LOB Locators | Yes | Yes |
| 8-byte LOB size | Yes | Yes |
| LOB prefetch | Yes | Yes |

No version-specific differences expected for basic LOB support.

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Large LOB memory pressure | Medium | High | Implement chunked reading in Phase 2 |
| Encoding issues with CLOB | Low | Medium | Use UTF-8, test with various chars |
| LOB locator expiry | Low | Medium | Document connection lifetime |
| Temporary LOB cleanup | Low | Low | Phase 3 - implement free_temp |

---

## References

- Python reference: `python-ref/python-oracledb/src/oracledb/impl/thin/lob.pyx`
- LOB operations: `python-ref/python-oracledb/src/oracledb/impl/thin/messages/lob_op.pyx`
- Constants: `python-ref/python-oracledb/src/oracledb/impl/thin/constants.pxi`
