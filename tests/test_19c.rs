//! Integration tests for Oracle 19c (AWS RDS).
//!
//! Run with: cargo test --test test_19c

use oracle_thin_rs::{Connection, OracleValue};

/// Connection string for Oracle 19c RDS.
const CONN_STR: &str = "test-oracle-19c.ctgcsik2itm5.ap-southeast-1.rds.amazonaws.com:1521/pdb1";
const USERNAME: &str = "admin";
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
    let conn = connect_or_skip!(Connection::connect(CONN_STR, USERNAME, PASSWORD).await);

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
async fn test_query_table() {
    let mut conn = connect_or_skip!(Connection::connect(CONN_STR, USERNAME, PASSWORD).await);

    let result = conn
        .query("SELECT ID, STR_COL, INT_COL, DEC_COL FROM TEST_DATA WHERE ROWNUM < 2")
        .await
        .unwrap();

    assert_eq!(result.len(), 1, "Expected 1 row");
    println!("Columns: {:?}", result.column_names());

    let row = &result.rows[0];

    // Check ID column
    if let Some(OracleValue::Number(s)) = row.get(0) {
        let id: i64 = s.parse().expect("ID should be parseable");
        println!("ID: {}", id);
        assert!((1..=5000).contains(&id), "ID should be between 1 and 5000");
    } else {
        panic!("Expected Number for ID");
    }

    // Check STR_COL
    if let Some(OracleValue::String(s)) = row.get(1) {
        println!("STR_COL: {}", s);
        assert!(s.starts_with("row_"), "STR_COL should start with 'row_'");
    } else {
        panic!("Expected String for STR_COL");
    }

    // Check INT_COL
    if let Some(OracleValue::Number(s)) = row.get(2) {
        let int_col: i64 = s.parse().expect("INT_COL should be parseable");
        println!("INT_COL: {}", int_col);
        assert!(
            (10..=50000).contains(&int_col),
            "INT_COL should be between 10 and 50000"
        );
    } else {
        panic!("Expected Number for INT_COL");
    }

    // Check DEC_COL
    if let Some(OracleValue::Number(s)) = row.get(3) {
        let dec_col: f64 = s.parse().expect("DEC_COL should be parseable");
        println!("DEC_COL: {}", dec_col);
        assert!(
            (0.01..=50.0).contains(&dec_col),
            "DEC_COL should be between 0.01 and 50.0"
        );
    } else {
        panic!("Expected Number for DEC_COL");
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

    // Check column metadata
    let columns = &result.columns;
    assert_eq!(columns[0].oracle_type, 1, "NULL_STR should be VARCHAR2 type (1)");
    assert_eq!(columns[1].oracle_type, 2, "NULL_NUM should be NUMBER type (2)");

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
    let mut conn = connect_or_skip!(Connection::connect(CONN_STR, USERNAME, PASSWORD).await);

    // Open cursor with small fetch size to force multiple fetches
    let mut cursor = conn
        .open_cursor_with_fetch_size(
            "SELECT ID, STR_COL, INT_COL, DEC_COL FROM TEST_DATA ORDER BY ID",
            100,
        )
        .await
        .unwrap();

    println!("Cursor opened, columns: {:?}", cursor.column_names());
    assert_eq!(cursor.buffered_count(), 100, "Should have 100 prefetched rows");

    // Count all rows
    let mut row_count = 0;
    let mut last_id = 0i64;

    while let Some(row) = conn.next_row(&mut cursor).await.unwrap() {
        row_count += 1;

        if let Some(OracleValue::Number(id_str)) = row.get(0) {
            let id: i64 = id_str.parse().unwrap();
            assert!(id > last_id, "IDs should be ordered: {} > {}", id, last_id);
            last_id = id;
        }
    }

    println!("Total rows: {}", row_count);
    assert_eq!(row_count, 5000, "Should have 5000 rows");
    assert!(cursor.is_exhausted(), "Cursor should be exhausted");

    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_fetch_all() {
    let mut conn = connect_or_skip!(Connection::connect(CONN_STR, USERNAME, PASSWORD).await);

    let mut cursor = conn
        .open_cursor_with_fetch_size(
            "SELECT ID, STR_COL FROM TEST_DATA WHERE ID <= 500 ORDER BY ID",
            50,
        )
        .await
        .unwrap();

    let collected = conn.fetch_all(&mut cursor).await.unwrap();

    println!("Collected {} rows", collected.len());
    assert_eq!(collected.len(), 500, "Should collect 500 rows");

    // Verify first and last rows
    if let Some(OracleValue::Number(first_id)) = collected.rows[0].get(0) {
        assert_eq!(first_id, "1", "First row should have ID=1");
    }
    if let Some(OracleValue::Number(last_id)) = collected.rows[499].get(0) {
        assert_eq!(last_id, "500", "Last row should have ID=500");
    }

    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_sql_syntax_error() {
    let mut conn = connect_or_skip!(Connection::connect(CONN_STR, USERNAME, PASSWORD).await);

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
    let mut conn = connect_or_skip!(Connection::connect(CONN_STR, USERNAME, PASSWORD).await);

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
