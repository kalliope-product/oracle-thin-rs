# Oracle Thin Client for Rust - Agent Guidelines

This document provides instructions for agentic coding agents working on the `oracle-thin-rs` project.
It defines build commands, code style, and architectural patterns to ensure consistency.

## 1. Environment & Commands

### Build & Test
- **Build**: `cargo build`
- **Lint**: `cargo clippy --all-targets --all-features -- -D warnings` (Must be clean before PR)
- **Format**: `cargo fmt`
- **Run All Tests**: `cargo test` (Runs both unit and integration tests)

### Running Specific Tests
To run integration tests (which require Docker/RDS environment):
- **Run 23ai Integration Tests**: `cargo test --test test_23ai`
- **Run 19c Integration Tests**: `cargo test --test test_19c`
- **Run a Single Test Case**: `cargo test --test test_23ai -- test_name_here`
- **Debug Output**: Add `-- --nocapture` to see stdout/stderr (useful for protocol debugging).
  Example: `cargo test --test test_23ai -- connect_success --nocapture`

### Environment Setup
- Tests require a `.env` file in `tests/`.
- Docker containers (for 23ai) must be running: `cd tests && docker compose up -d`.
- Migrations must be applied: `python tests/scripts/migrate.py --env 23ai`.

## 2. Code Style & Conventions

### General
- **Pure Rust**: No C dependencies or FFI (unless absolutely necessary and justified).
- **Async-First**: Use `tokio` for all I/O. Do not use blocking I/O in async functions.
- **Safety**: No `unsafe` blocks without explicit justification and peer review.
- **Documentation**: All public items (pub structs, enums, functions) must have `///` doc comments.

### Imports
Group imports in the following order, separated by a blank line:
1. Standard library (`std::...`)
2. External crates (`tokio`, `bytes`, `thiserror`, etc.)
3. Crate modules (`crate::...`)

```rust
use std::io;
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use tokio::net::TcpStream;

use crate::error::{Error, Result};
use crate::protocol::packet::Packet;
```

### Naming
- **Types/Traits**: `UpperCamelCase` (e.g., `ConnectParams`, `PacketStream`)
- **Functions/Variables**: `snake_case` (e.g., `connect`, `read_packet`)
- **Constants**: `SCREAMING_SNAKE_CASE` (e.g., `TNS_PACKET_TYPE_DATA`)
- **Files**: `snake_case.rs`

### Error Handling
- Use `thiserror` for the library's `Error` enum in `src/error.rs`.
- Return `crate::error::Result<T>` (alias for `Result<T, Error>`).
- Propagate errors with `?`.
- Map external errors (like I/O) to library errors using `From`/`Into` or explicit mapping if context is needed.

```rust
// Example
pub fn parse(input: &str) -> Result<Self> {
    if input.is_empty() {
        return Err(Error::InvalidConnectString { 
            message: "Empty string".to_string() 
        });
    }
    // ...
}
```

## 3. Architecture & Patterns

### Protocol Implementation
- **Source of Truth**: The `python-oracledb` thin client implementation is the authoritative reference.
  Path: `python-ref/python-oracledb/src/oracledb/impl/thin/`
- **Separation**: Keep protocol logic (packet parsing, TNS constants) in `src/protocol/` separate from the high-level API in `src/connection.rs`.
- **Packet Handling**: Use `PacketStream` for reading/writing TNS packets.
- **Buffers**: Use `ReadBuffer` and `WriteBuffer` (wrappers around `bytes`) for parsing/serializing data types.

### Connection State
- The `Connection` struct owns the `PacketStream` and `SessionData`.
- State transitions (Connect -> Auth -> Connected) should be explicit.

### Testing Strategy
- **Unit Tests**: Place in the same file as the code in a `#[cfg(test)] mod tests { ... }` block.
- **Integration Tests**: Place in `tests/` directory. These test the full `Connection` flow against a real database.
- **Mocking**: Minimize mocking. Prefer testing against the real database (23ai Docker) for protocol correctness.

## 4. Workflow

1.  **Understand**: Read the relevant Python reference code first.
2.  **Plan**: Map the Python logic to Rust struct/enums. *Create or update `directives/current_plan.md` with the implementation details.*
3.  **Implement**: Write code + Unit tests.
4.  **Verify**: Run integration tests against 23ai (and 19c if available).
5.  **Refactor**: Run `clippy` and fix warnings.
6.  **Capture**: Update `directives/protocol-learnings.md` with any new protocol insights or bug fixes (whether fixed by you or the user).

## 5. Important Rules (from CLAUDE.md)
- **Protocol Behavior**: Must match `python-oracledb` exactly.
- **Compatibility**: Must work with Oracle 19c and 23ai.
- **Directives**: Check `directives/` for accumulated knowledge before starting complex tasks.
- **Planning**: Current plan stored in directive/current_plan.md.

## 6. Directory Structure
- `src/protocol/`: Low-level TNS protocol, packet types, auth logic.
- `src/protocol/messages/`: Structs for specific TNS messages (Connect, Auth, Execute).
- `src/protocol/types/`: Oracle data type implementations (Varchar, Number, Date).
- `src/connection.rs`: High-level public API.
- `tests/`: Integration tests.

---
*Generated for coding agents. Adhere to these guidelines to maintain codebase integrity.*
