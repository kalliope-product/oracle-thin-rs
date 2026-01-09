//! Integration tests for Oracle 23ai (local Docker).
//!
//! Start the database with: cd tests && docker-compose up -d
//! Run with: cargo test --test test_23ai

use oracle_thin_rs::{Connection, OracleValue};

/// Connection string for Oracle 23ai local Docker.
const CONN_STR: &str = "localhost:1521/freepdb1";
const USERNAME: &str = "read_user";
const PASSWORD: &str = "ThisIsASecret123";

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
    let conn = connect_or_skip!(Connection::connect(CONN_STR, USERNAME, PASSWORD).await);

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
    let mut conn = connect_or_skip!(Connection::connect(CONN_STR, USERNAME, PASSWORD).await);

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
    let mut conn = connect_or_skip!(Connection::connect(CONN_STR, USERNAME, PASSWORD).await);

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
        assert!((val - 123.456).abs() < 0.001, "Expected ~123.456, got {}", val);
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
    let mut conn = connect_or_skip!(Connection::connect(CONN_STR, USERNAME, PASSWORD).await);

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
    assert!(row.get(1).unwrap().is_null(), "Second column should be NULL");

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
    let mut conn = connect_or_skip!(Connection::connect(CONN_STR, USERNAME, PASSWORD).await);

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
    let mut conn = connect_or_skip!(Connection::connect(CONN_STR, USERNAME, PASSWORD).await);

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
    let mut conn = connect_or_skip!(Connection::connect(CONN_STR, USERNAME, PASSWORD).await);

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
