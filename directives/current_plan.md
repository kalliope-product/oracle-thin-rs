# Refactor Integration Tests to use `sample_datatypes_tbl`

## Context
The integration tests (`test_19c.rs` and `test_23ai.rs`) are inconsistent. A new migration `001_create_sample_datatypes` creates a comprehensive table `sample_datatypes_tbl` with 500 rows and various data types. `test_19c.rs` relies on an obsolete `TEST_DATA` table.

## Goal
Standardize both test files to use `sample_datatypes_tbl` for validating protocol behavior, data type parsing, and cursor fetching.

## Test Strategy
The tests will be organized into two main categories:

1.  **Data Types & Protocol Correctness**:
    *   Verify parsing of all supported types (Char, Number, Date, LOBs).
    *   Verify handling of NULL values for all types.
    *   Verify edge cases (max values, special characters).

2.  **Cursor & Fetching Behavior**:
    *   `fetch_one` (implicit via iterating `next()`).
    *   `fetch_all` (collecting all results).
    *   Pagination/Prefetching (verifying multiple network roundtrips).
    *   Stream interface integration.

## Plan

### 1. Refactor `test_19c.rs`
- [x] Remove all references to `TEST_DATA`.
- [x] Update `test_query_table` to query `sample_datatypes_tbl` (e.g., fetch specific rows with known values).
- [ ] **Refine**: Ensure `test_query_null_values` covers the new table's NULL capabilities or explicitly casts NULLs as before.
- [x] Update cursor tests (`test_cursor_fetch`, `test_fetch_all`, `test_cursor_stream_basic`) to query `sample_datatypes_tbl`.
- [ ] Add LOB prefetch tests (copy/adapt from `test_23ai.rs`).

### 2. Refactor `test_23ai.rs`
- [ ] Expand coverage to include more columns from `sample_datatypes_tbl` (Numbers, RAW, etc.).
- [ ] Ensure cursor tests run against the 500-row table to verify pagination/prefetching effectively.

### 3. Verification
- [ ] Run `cargo test --test test_23ai`
- [ ] Run `cargo test --test test_19c` (if environment available, otherwise rely on code consistency).

## Schema Reference
Table: `sample_datatypes_tbl`
Rows: 500 (Ids 1-20 are special cases, 21-500 are bulk)
Columns: `id`, `char_col`, `varchar2_col`, `number_col`, `date_col`, `clob_col`, `blob_col`, etc.
