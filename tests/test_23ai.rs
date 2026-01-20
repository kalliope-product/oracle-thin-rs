//! Integration tests for Oracle 23ai (local Docker).
//!
//! Start the database with: cd tests && docker-compose up -d
//! Run with: cargo test --test test_23ai

use chrono::Datelike;
use oracle_thin_rs::{Connection, Cursor, OracleValue};
use std::env;

/// Load environment variables from tests/.env file.
fn load_env() {
    let _ = dotenvy::from_path("tests/.env");
}

/// Get connection string from environment variables.
fn get_conn_str() -> String {
    load_env();
    let host = env::var("ORACLE_23AI_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = env::var("ORACLE_23AI_PORT").unwrap_or_else(|_| "1521".to_string());
    let service = env::var("ORACLE_23AI_SERVICE").unwrap_or_else(|_| "freepdb1".to_string());
    format!("{}:{}/{}", host, port, service)
}

/// Get username from environment variables.
fn get_username() -> String {
    load_env();
    env::var("ORACLE_23AI_USERNAME").unwrap_or_else(|_| "test_user".to_string())
}

/// Get password from environment variables.
fn get_password() -> String {
    load_env();
    env::var("ORACLE_23AI_PASSWORD")
        .expect("ORACLE_23AI_PASSWORD must be set in tests/.env or environment")
}

/// Helper macro to handle connection errors gracefully.
/// If Oracle is not reachable, skip the test instead of failing.
macro_rules! connect_or_skip {
    ($conn_result:expr) => {
        match $conn_result {
            Ok(conn) => conn,
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("Connection refused") || err_str.contains("I/O error") {
                    eprintln!("Skipping test - Oracle 23ai not reachable: {}", e);
                    eprintln!("Start with: cd tests && docker-compose up -d");
                    return;
                }
                panic!("Unexpected connection error: {}", e);
            }
        }
    };
}

#[tokio::test]
async fn test_connect() {
    let conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    println!("Connected successfully!");
    println!("Protocol version: {}", conn.protocol_version());
    println!("SDU: {}", conn.sdu());

    if let Some(version) = conn.server_version() {
        println!(
            "Server version: {}.{}.{}.{}.{}",
            version.0, version.1, version.2, version.3, version.4
        );
        // Verify it's 23.x
        assert_eq!(version.0, 23, "Expected Oracle 23ai");
    }

    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_query_string() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    let result = conn.query("SELECT 'hello' FROM DUAL").await.unwrap();

    assert_eq!(result.len(), 1, "Expected 1 row");
    println!("Columns: {:?}", result.column_names());

    let row = &result.rows[0];
    if let Some(OracleValue::String(s)) = row.get(0) {
        assert_eq!(s, "hello");
    } else {
        panic!("Expected String value");
    }

    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_query_numbers() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    let result = conn
        .query("SELECT 42 AS INT_VAL, 123.456 AS DEC_VAL, -100 AS NEG_VAL FROM DUAL")
        .await
        .unwrap();

    assert_eq!(result.len(), 1, "Expected 1 row");

    let row = &result.rows[0];

    // Integer
    if let Some(OracleValue::Number(s)) = row.get(0) {
        assert_eq!(s, "42");
    } else {
        panic!("Expected Number for INT_VAL");
    }

    // Decimal
    if let Some(OracleValue::Number(s)) = row.get(1) {
        let val: f64 = s.parse().unwrap();
        assert!(
            (val - 123.456).abs() < 0.001,
            "Expected ~123.456, got {}",
            val
        );
    } else {
        panic!("Expected Number for DEC_VAL");
    }

    // Negative
    if let Some(OracleValue::Number(s)) = row.get(2) {
        assert_eq!(s, "-100");
    } else {
        panic!("Expected Number for NEG_VAL");
    }

    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_query_null_values() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    let result = conn
        .query(
            "SELECT CAST(NULL AS VARCHAR2(10)) AS NULL_STR, \
                    CAST(NULL AS NUMBER) AS NULL_NUM, \
                    'text' AS NON_NULL \
             FROM DUAL",
        )
        .await
        .unwrap();

    assert_eq!(result.len(), 1, "Expected 1 row");

    let row = &result.rows[0];

    // NULL values
    assert!(row.get(0).unwrap().is_null(), "First column should be NULL");
    assert!(
        row.get(1).unwrap().is_null(),
        "Second column should be NULL"
    );

    // Non-NULL
    if let Some(OracleValue::String(s)) = row.get(2) {
        assert_eq!(s, "text");
    } else {
        panic!("Expected String for third column");
    }

    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_query_multiple_rows() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    // Generate multiple rows using CONNECT BY
    let result = conn
        .query("SELECT LEVEL AS ROW_NUM FROM DUAL CONNECT BY LEVEL <= 5")
        .await
        .unwrap();

    assert_eq!(result.len(), 5, "Expected 5 rows");

    for (i, row) in result.rows.iter().enumerate() {
        if let Some(OracleValue::Number(s)) = row.get(0) {
            let val: i32 = s.parse().unwrap();
            assert_eq!(val, (i + 1) as i32, "Row {} should have value {}", i, i + 1);
        } else {
            panic!("Expected Number for row {}", i);
        }
    }

    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_sql_syntax_error() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    let result = conn.query("SELEKT * FROM DUAL").await;

    assert!(result.is_err(), "Expected SQL syntax error");
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("ORA-") || err_str.contains("Oracle error"),
        "Expected Oracle error, got: {}",
        err_str
    );

    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_table_not_found_error() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    let result = conn.query("SELECT * FROM NON_EXISTENT_TABLE_12345").await;

    assert!(result.is_err(), "Expected table not found error");
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("ORA-00942") || err_str.contains("does not exist"),
        "Expected 'table does not exist' error, got: {}",
        err_str
    );

    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_query_date() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    // Query SYSDATE as a simple DATE test
    let result = conn
        .query("SELECT SYSDATE AS CURRENT_DATE FROM DUAL")
        .await
        .unwrap();

    assert_eq!(result.len(), 1, "Expected 1 row");

    let row = &result.rows[0];
    if let Some(OracleValue::Date(dt)) = row.get(0) {
        println!("Got DATE value: {}", dt);
        // Basic sanity checks
        assert!(dt.year() >= 2020, "Year should be >= 2020");
        assert!(dt.year() <= 2100, "Year should be <= 2100");
        assert!(dt.month() >= 1 && dt.month() <= 12);
        assert!(dt.day() >= 1 && dt.day() <= 31);
    } else {
        panic!("Expected Date value, got {:?}", row.get(0));
    }

    // Query from the test table
    let result = conn
        .query("SELECT id, date_col FROM sample_datatypes_tbl ORDER BY id")
        .await
        .unwrap();

    assert!(
        result.len() >= 1,
        "Expected at least 1 row from sample_datatypes_tbl"
    );

    // First row should have a DATE value
    let row = &result.rows[0];
    if let Some(OracleValue::Date(dt)) = row.get(1) {
        println!("Got date_col: {}", dt);
    } else {
        panic!("Expected Date value for date_col");
    }

    conn.close().await.unwrap();
}

// ============================================================================
// Cursor Tests (Phase 1)
// ============================================================================

#[tokio::test]
async fn test_cursor_basic_iteration() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    let mut cursor = conn
        .open_cursor(
            "SELECT LEVEL AS id, 'Row ' || LEVEL AS name FROM DUAL CONNECT BY LEVEL <= 100",
        )
        .await
        .unwrap();

    let mut count = 0;
    while let Some(_) = cursor.next().await.unwrap() {
        count += 1;
    }
    assert_eq!(count, 100);
    assert!(cursor.is_closed());
}

#[tokio::test]
async fn test_cursor_with_fetch_size() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    // Use open_row_cursor for explicit fetch size
    let mut cursor = conn
        .open_row_cursor("SELECT LEVEL FROM DUAL CONNECT BY LEVEL <= 50", 10)
        .await
        .unwrap();

    assert_eq!(cursor.fetch_size(), 10);

    cursor.set_fetch_size(5);
    assert_eq!(cursor.fetch_size(), 5);

    let rows = cursor.fetch_all().await.unwrap();
    assert_eq!(rows.len(), 50);
    assert_eq!(cursor.rowcount(), 50);
}

#[tokio::test]
async fn test_cursor_close_explicitly() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    let mut cursor = conn.open_cursor("SELECT 1 FROM DUAL").await.unwrap();
    assert!(!cursor.is_closed());

    cursor.close().await.unwrap();
    assert!(cursor.is_closed());
}

#[tokio::test]
async fn test_cursor_trait_generic() {
    use oracle_thin_rs::Row;

    // Demonstrate generic cursor usage with a simple query
    async fn process_cursor<C: Cursor<Item = Row>>(cursor: &mut C) -> u64 {
        let mut count = 0;
        while let Some(_) = cursor.next().await.unwrap() {
            count += 1;
        }
        count
    }

    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );
    // Use a simpler query without CONNECT BY to avoid protocol edge cases
    let mut cursor = conn
        .open_cursor("SELECT 1 FROM DUAL UNION ALL SELECT 2 FROM DUAL UNION ALL SELECT 3 FROM DUAL")
        .await
        .unwrap();

    let count = process_cursor(&mut cursor).await;
    assert_eq!(count, 3);
}

#[tokio::test]
async fn test_cursor_fetch_all() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    let mut cursor = conn
        .open_row_cursor(
            "SELECT LEVEL FROM DUAL CONNECT BY LEVEL <= 25",
            5, // Small fetch size to test multiple roundtrips
        )
        .await
        .unwrap();

    let rows = cursor.fetch_all().await.unwrap();
    assert_eq!(rows.len(), 25);
    assert_eq!(cursor.rowcount(), 25);
    assert!(cursor.is_closed());
}

#[tokio::test]
async fn test_cursor_has_more() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    let mut cursor = conn
        .open_cursor("SELECT LEVEL FROM DUAL CONNECT BY LEVEL <= 10")
        .await
        .unwrap();

    // Should have more initially
    assert!(cursor.has_more());

    // Consume all rows
    while let Some(_) = cursor.next().await.unwrap() {}

    // No more after exhaustion
    assert!(!cursor.has_more());
    assert!(cursor.is_closed());
}

// ============================================================================
// Stream Tests (Phase 2)
// ============================================================================

#[tokio::test]
async fn test_cursor_stream_basic() {
    use futures::stream::TryStreamExt;
    use oracle_thin_rs::CursorStreamExt;

    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    let cursor = conn
        .open_cursor("SELECT LEVEL FROM DUAL CONNECT BY LEVEL <= 10")
        .await
        .unwrap();

    let count = cursor
        .into_stream()
        .map_ok(|_| 1usize)
        .try_fold(0, |acc, x| async move { Ok(acc + x) })
        .await
        .unwrap();

    assert_eq!(count, 10);
}

#[tokio::test]
async fn test_cursor_stream_collect() {
    use futures::stream::TryStreamExt;
    use oracle_thin_rs::CursorStreamExt;

    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    let cursor = conn
        .open_cursor("SELECT LEVEL FROM DUAL CONNECT BY LEVEL <= 5")
        .await
        .unwrap();

    let rows: Vec<_> = cursor.into_stream().try_collect().await.unwrap();

    assert_eq!(rows.len(), 5);
}

#[tokio::test]
async fn test_cursor_stream_take() {
    use futures::stream::{StreamExt, TryStreamExt};
    use oracle_thin_rs::CursorStreamExt;

    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    let cursor = conn
        .open_cursor("SELECT LEVEL FROM DUAL CONNECT BY LEVEL <= 100")
        .await
        .unwrap();

    // Take only 5 rows from potentially many
    let count = cursor
        .into_stream()
        .take(5)
        .map_ok(|_| 1usize)
        .try_fold(0, |acc, x| async move { Ok(acc + x) })
        .await
        .unwrap();

    assert_eq!(count, 5);
}

// ============================================================================
// LOB Prefetch Tests (Phase 2)
// ============================================================================

#[tokio::test]
async fn test_clob_prefetch() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    // Query CLOB column from test table - data should be prefetched since it's small
    let result = conn
        .query("SELECT id, clob_col FROM sample_datatypes_tbl ORDER BY id")
        .await
        .unwrap();

    assert!(result.len() >= 2, "Expected at least 2 rows");

    // First row CLOB
    let row = &result.rows[0];
    let id = row.get(0).unwrap();
    let clob_val = row.get(1).unwrap();

    println!("Row 1 - id: {}, clob type: {:?}", id, clob_val);

    match clob_val {
        OracleValue::Clob(lob) => {
            // Should have prefetched data since the CLOB is small
            assert!(lob.has_data(), "Expected CLOB data to be prefetched");
            let text = lob.as_string().expect("CLOB should have string data");
            println!("CLOB content: {}", text);
            assert!(
                text.contains("CLOB"),
                "Expected CLOB content to contain 'CLOB'"
            );
        }
        OracleValue::String(s) => {
            // May be returned as string if not using LOB prefetch flow
            println!("Got String instead of Clob: {}", s);
        }
        _ => panic!(
            "Expected Clob or String value for clob_col, got {:?}",
            clob_val
        ),
    }

    // Second row CLOB
    let row = &result.rows[1];
    let clob_val = row.get(1).unwrap();
    println!("Row 2 - clob type: {:?}", clob_val);

    match clob_val {
        OracleValue::Clob(lob) => {
            assert!(lob.has_data(), "Expected CLOB data to be prefetched");
            let text = lob.as_string().expect("CLOB should have string data");
            println!("CLOB content: {}", text);
        }
        OracleValue::String(s) => {
            println!("Got String instead of Clob: {}", s);
        }
        _ => panic!("Expected Clob or String value"),
    }

    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_blob_prefetch() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    // Query BLOB column from test table - data should be prefetched since it's small
    let result = conn
        .query("SELECT id, blob_col FROM sample_datatypes_tbl ORDER BY id")
        .await
        .unwrap();

    assert!(result.len() >= 2, "Expected at least 2 rows");

    // First row BLOB
    let row = &result.rows[0];
    let id = row.get(0).unwrap();
    let blob_val = row.get(1).unwrap();

    println!("Row 1 - id: {}, blob type: {:?}", id, blob_val);

    match blob_val {
        OracleValue::Blob(lob) => {
            // Should have prefetched data since the BLOB is small
            assert!(lob.has_data(), "Expected BLOB data to be prefetched");
            let bytes = lob.as_bytes().expect("BLOB should have binary data");
            println!("BLOB size: {} bytes", bytes.len());
            // Convert to string for verification (our test data is text stored as blob)
            let text = String::from_utf8_lossy(bytes);
            println!("BLOB as text: {}", text);
            assert!(
                text.contains("blob"),
                "Expected BLOB content to contain 'blob'"
            );
        }
        OracleValue::Raw(bytes) => {
            println!("Got Raw instead of Blob: {} bytes", bytes.len());
        }
        _ => panic!(
            "Expected Blob or Raw value for blob_col, got {:?}",
            blob_val
        ),
    }

    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_null_lob() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    // Query NULL CLOB and BLOB
    let result = conn
        .query("SELECT CAST(NULL AS CLOB) AS null_clob, CAST(NULL AS BLOB) AS null_blob FROM DUAL")
        .await
        .unwrap();

    assert_eq!(result.len(), 1, "Expected 1 row");

    let row = &result.rows[0];

    // NULL CLOB
    let null_clob = row.get(0).unwrap();
    println!("NULL CLOB: {:?}", null_clob);
    assert!(null_clob.is_null(), "Expected NULL for CLOB");

    // NULL BLOB
    let null_blob = row.get(1).unwrap();
    println!("NULL BLOB: {:?}", null_blob);
    assert!(null_blob.is_null(), "Expected NULL for BLOB");

    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_clob_inline_literal() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    // Create a CLOB from inline literal using TO_CLOB
    let result = conn
        .query("SELECT TO_CLOB('Hello from CLOB literal!') AS clob_literal FROM DUAL")
        .await
        .unwrap();

    assert_eq!(result.len(), 1, "Expected 1 row");

    let row = &result.rows[0];
    let clob_val = row.get(0).unwrap();

    println!("CLOB literal result: {:?}", clob_val);

    match clob_val {
        OracleValue::Clob(lob) => {
            let text = lob.as_string().unwrap_or_default();
            assert_eq!(text, "Hello from CLOB literal!");
        }
        OracleValue::String(s) => {
            assert_eq!(s, "Hello from CLOB literal!");
        }
        _ => panic!("Expected Clob or String, got {:?}", clob_val),
    }

    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_mixed_columns_with_lob() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    // Query multiple column types including LOBs
    let result = conn
        .query(
            "SELECT id, varchar2_col, number_col, clob_col, blob_col, date_col \
             FROM sample_datatypes_tbl ORDER BY id",
        )
        .await
        .unwrap();

    assert!(result.len() >= 1, "Expected at least 1 row");

    let row = &result.rows[0];

    // Verify each column type
    let id = row.get(0).unwrap();
    println!("id: {}", id);
    assert!(matches!(id, OracleValue::Number(_)));

    let varchar = row.get(1).unwrap();
    println!("varchar2_col: {}", varchar);
    assert!(matches!(varchar, OracleValue::String(_)));

    let number = row.get(2).unwrap();
    println!("number_col: {}", number);
    assert!(matches!(number, OracleValue::Number(_)));

    let clob = row.get(3).unwrap();
    println!("clob_col: {:?}", clob);
    assert!(
        matches!(clob, OracleValue::Clob(_) | OracleValue::String(_)),
        "Expected Clob or String for clob_col"
    );

    let blob = row.get(4).unwrap();
    println!("blob_col: {:?}", blob);
    assert!(
        matches!(blob, OracleValue::Blob(_) | OracleValue::Raw(_)),
        "Expected Blob or Raw for blob_col"
    );

    let date = row.get(5).unwrap();
    println!("date_col: {}", date);
    assert!(matches!(date, OracleValue::Date(_)));

    conn.close().await.unwrap();
}
