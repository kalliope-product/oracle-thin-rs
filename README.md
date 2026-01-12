# Oracle Thin Client for Rust

A pure Rust Oracle Database thin client - no Oracle Instant Client dependency required.

## Why?
Sometime I just wish I have some simple way to get and update data in Oracle Database from Rust without the whole "Finding the right Oracle Instant Client version, setting up LD_LIBRARY_PATH, dealing with incompatible versions, etc." hassle. This library aims to provide a straightforward, easy-to-use Oracle thin client for Rust developers, and if needed be able to run in constrained environments where installing Oracle Instant Client is not feasible.

## Status

**Work in Progress** - This library is under active development.

## Features

### Implemented
- **Connection**: TCP connection with O5LOGON authentication (11g SHA1 and 12c PBKDF2+SHA512 verifiers)
- **Query Execution**: SELECT statements with automatic prefetch
- **Cursor-based Fetching**: Trait-based cursor API for streaming large result sets
- **Stream Support**: `futures::Stream` integration with combinators
- **Data Types**: VARCHAR2, NUMBER, CHAR, DATE, LONG, BINARY_INTEGER, NULL values

### Planned
- TIMESTAMP types (TIMESTAMP, TIMESTAMP WITH TZ, etc.)
- RAW/BLOB/CLOB types
- DML operations (INSERT, UPDATE, DELETE)
- Bind variables
- Connection pooling

## Compatibility

| Oracle Version | Status |
|---------------|--------|
| 19c (AWS RDS) | Tested |
| 23ai (Docker) | Tested |

## Quick Start

```rust
use oracle_thin_rs::{Connection, Cursor, CursorStreamExt};
use futures::stream::TryStreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to Oracle
    let mut conn = Connection::connect(
        "localhost:1521/FREEPDB1",
        "username",
        "password"
    ).await?;

    // Simple query - all results at once
    let result = conn.query("SELECT * FROM employees WHERE rownum < 10").await?;

    println!("Columns: {:?}", result.column_names());
    for row in &result {
        println!("{:?}", row);
    }

    // Cursor-based fetching for large result sets
    let mut cursor = conn.open_cursor("SELECT * FROM large_table").await?;
    while let Some(row) = cursor.next().await? {
        // Process row
    }

    // Stream-based processing with combinators
    let mut cursor = conn.open_cursor("SELECT * FROM large_table").await?;
    let rows: Vec<_> = cursor.into_stream().try_collect().await?;
    println!("Fetched {} rows", rows.len());

    conn.close().await?;
    Ok(())
}
```

## Development

```bash
# Run tests (requires Oracle database)
cargo test

# Run integration tests with output
cargo test --test integration_test -- --nocapture

# Start local Oracle 23ai for testing
cd tests && docker-compose up -d
```

## License

MIT
