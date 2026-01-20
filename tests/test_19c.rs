//! Integration tests for Oracle 19c (AWS RDS).
//!
//! Run with: cargo test --test test_19c

use chrono::Datelike;
use oracle_thin_rs::{ConnectParams, Connection, Cursor, OracleValue};
use std::{env, time::Duration};

/// Load environment variables from tests/.env file.
fn load_env() {
    let _ = dotenvy::from_path("tests/.env");
}

/// Get connection string from environment variables.
fn get_conn_str() -> String {
    load_env();
    let host = env::var("ORACLE_19C_HOST").expect("ORACLE_19C_HOST must be set");
    let port = env::var("ORACLE_19C_PORT").unwrap_or_else(|_| "1521".to_string());
    let service = env::var("ORACLE_19C_SERVICE").unwrap_or_else(|_| "pdb1".to_string());
    format!("{}:{}/{}", host, port, service)
}

/// Get username from environment variables.
fn get_username() -> String {
    load_env();
    env::var("ORACLE_19C_USERNAME").unwrap_or_else(|_| "admin".to_string())
}

/// Get password from environment variables.
fn get_password() -> String {
    load_env();
    env::var("ORACLE_19C_PASSWORD")
        .expect("ORACLE_19C_PASSWORD must be set in tests/.env or environment")
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
                    eprintln!("Skipping test - Oracle 19c not reachable: {}", e);
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
        // Verify it's 19.x
        assert_eq!(version.0, 19, "Expected Oracle 19c");
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
async fn test_query_table() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    // Query a specific row (Row 7) known to have populated data
    let result = conn
        .query(
            "SELECT id, varchar2_col, number_col, date_col 
             FROM sample_datatypes_tbl 
             WHERE id = 7",
        )
        .await
        .unwrap();

    assert_eq!(result.len(), 1, "Expected 1 row");

    let row = &result.rows[0];

    // Check ID
    if let Some(OracleValue::Number(s)) = row.get(0) {
        assert_eq!(s, "7");
    } else {
        panic!("Expected Number for ID");
    }

    // Check VARCHAR2
    if let Some(OracleValue::String(s)) = row.get(1) {
        assert_eq!(s, "John Smith - Senior Developer");
    } else {
        panic!("Expected String for varchar2_col");
    }

    // Check NUMBER
    if let Some(OracleValue::Number(s)) = row.get(2) {
        // 12345.67
        let val: f64 = s.parse().unwrap();
        assert!((val - 12345.67).abs() < 0.001);
    } else {
        panic!("Expected Number for number_col");
    }

    // Check DATE
    if let Some(OracleValue::Date(dt)) = row.get(3) {
        // 2024-06-15
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 6);
        assert_eq!(dt.day(), 15);
    } else {
        panic!("Expected Date for date_col");
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

    // Check column metadata
    let columns = &result.columns;
    assert_eq!(
        columns[0].oracle_type, 1,
        "NULL_STR should be VARCHAR2 type (1)"
    );
    assert_eq!(
        columns[1].oracle_type, 2,
        "NULL_NUM should be NUMBER type (2)"
    );

    let row = &result.rows[0];

    // NULL VARCHAR2
    let val0 = row.get(0).expect("Should have column 0");
    assert!(val0.is_null(), "First column should be NULL");

    // NULL NUMBER
    let val1 = row.get(1).expect("Should have column 1");
    assert!(val1.is_null(), "Second column should be NULL");

    // Non-NULL string
    let val2 = row.get(2).expect("Should have column 2");
    assert!(!val2.is_null(), "Third column should NOT be NULL");
    if let OracleValue::String(s) = val2 {
        assert_eq!(s, "text");
    } else {
        panic!("Expected String for third column");
    }

    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_cursor_fetch() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    // Open cursor with small fetch size to force multiple fetches
    // Use the bulk data rows (21-500)
    let mut cursor = conn
        .open_row_cursor(
            "SELECT id FROM sample_datatypes_tbl WHERE id > 20 ORDER BY id",
            100,
        )
        .await
        .unwrap();

    let column_names: Vec<_> = cursor.columns().iter().map(|c| c.name.as_str()).collect();
    println!("Cursor opened, columns: {:?}", column_names);

    // Count all rows
    let mut row_count = 0;
    let mut last_id = 20i64;

    while let Some(row) = cursor.next().await.unwrap() {
        row_count += 1;

        if let Some(OracleValue::Number(id_str)) = row.get(0) {
            let id: i64 = id_str.parse().unwrap();
            assert!(id > last_id, "IDs should be ordered: {} > {}", id, last_id);
            last_id = id;
        }
    }

    println!("Total rows: {}", row_count);
    // Rows 21 to 500 = 480 rows
    assert_eq!(row_count, 480, "Should have 480 bulk rows");
    assert!(!cursor.has_more(), "Cursor should be exhausted");
    assert!(cursor.is_closed(), "Cursor should be closed");

    drop(cursor);
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_fetch_all() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    // Select the first 200 bulk rows (IDs 21 to 220)
    let mut cursor = conn
        .open_row_cursor(
            "SELECT id FROM sample_datatypes_tbl WHERE id > 20 AND id <= 220 ORDER BY id",
            50,
        )
        .await
        .unwrap();

    let rows = cursor.fetch_all().await.unwrap();

    println!("Collected {} rows", rows.len());
    assert_eq!(rows.len(), 200, "Should collect 200 rows");

    // Verify first and last rows
    if let Some(OracleValue::Number(first_id)) = rows[0].get(0) {
        assert_eq!(first_id, "21");
    }
    if let Some(OracleValue::Number(last_id)) = rows[199].get(0) {
        assert_eq!(last_id, "220");
    }

    assert!(
        cursor.is_closed(),
        "Cursor should be closed after fetch_all"
    );

    drop(cursor);
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_sql_syntax_error() {
    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    // Invalid SQL statement
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
    // ORA-00942: table or view does not exist
    assert!(
        err_str.contains("ORA-00942") || err_str.contains("does not exist"),
        "Expected 'table does not exist' error, got: {}",
        err_str
    );

    conn.close().await.unwrap();
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
        .open_cursor("SELECT id FROM sample_datatypes_tbl WHERE id <= 30 ORDER BY id")
        .await
        .unwrap();

    let count = cursor
        .into_stream()
        .map_ok(|_| 1usize)
        .try_fold(0, |acc, x| async move { Ok(acc + x) })
        .await
        .unwrap();

    assert_eq!(count, 30);
}

#[tokio::test]
async fn test_cursor_stream_collect() {
    use futures::stream::TryStreamExt;
    use oracle_thin_rs::CursorStreamExt;

    let mut conn = connect_or_skip!(
        Connection::connect(&get_conn_str(), &get_username(), &get_password()).await
    );

    let cursor = conn
        .open_cursor("SELECT id FROM sample_datatypes_tbl WHERE id <= 5 ORDER BY id")
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
        .open_cursor("SELECT id FROM sample_datatypes_tbl ORDER BY id")
        .await
        .unwrap();

    // Take only 5 rows from 500
    let count = cursor
        .into_stream()
        .take(5)
        .map_ok(|_| 1usize)
        .try_fold(0, |acc, x| async move { Ok(acc + x) })
        .await
        .unwrap();

    assert_eq!(count, 5);
}

#[tokio::test]
async fn test_clob_prefetch() {
    load_env();
    let mut conn = connect_or_skip!(
        Connection::connect_with_params(
            &ConnectParams {
                host: env::var("ORACLE_19C_HOST").expect("ORACLE_19C_HOST must be set"),
                port: env::var("ORACLE_19C_PORT")
                    .unwrap_or_else(|_| "1521".to_string())
                    .parse()
                    .expect("Invalid port number"),
                service_name: env::var("ORACLE_19C_SERVICE").unwrap_or_else(|_| "pdb1".to_string()),
                sdu: 8192,
                connect_timeout: Duration::from_secs(10)
            },
            &get_username(),
            &get_password()
        )
        .await
    );

    // Query CLOB column from test table - data should be prefetched since it's small
    let result = conn
        .query("SELECT id, clob_col FROM sample_datatypes_tbl WHERE id = 3")
        .await
        .unwrap();

    assert!(result.len() >= 1, "Expected at least 1 row");

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
            println!("CLOB content: {:?}", text);
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
