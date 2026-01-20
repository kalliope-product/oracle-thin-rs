//! Debug test for LOB support

use oracle_thin_rs::{Connection, Cursor};

#[tokio::main]
async fn main() {
    dotenvy::from_path("tests/.env").ok();

    let host = std::env::var("ORACLE_23AI_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port = std::env::var("ORACLE_23AI_PORT").unwrap_or_else(|_| "1521".to_string());
    let service = std::env::var("ORACLE_23AI_SERVICE").unwrap_or_else(|_| "freepdb1".to_string());
    let username =
        std::env::var("ORACLE_23AI_USERNAME").unwrap_or_else(|_| "test_user".to_string());
    let password = std::env::var("ORACLE_23AI_PASSWORD").expect("ORACLE_23AI_PASSWORD required");

    let conn_str = format!("{}:{}/{}", host, port, service);

    println!("Connecting to {} as {}...", conn_str, username);
    let mut conn = Connection::connect(&conn_str, &username, &password)
        .await
        .unwrap();
    println!("Connected!");

    // Test using cursor (with explicit FETCH)
    println!("\n--- Test: CLOB query using cursor ---");
    let mut cursor = conn
        .open_cursor("SELECT id, clob_col FROM sample_datatypes_tbl WHERE id = 1")
        .await
        .unwrap();

    println!("Cursor opened, columns:");
    for col in cursor.columns() {
        println!("  Column: {} (type={})", col.name, col.oracle_type);
    }

    println!("\nFetching rows...");
    let mut count = 0;
    while let Some(row) = cursor.next().await.unwrap() {
        count += 1;
        println!("Row {}: id={:?}, clob={:?}", count, row.get(0), row.get(1));
    }
    println!("Total rows: {}", count);
    drop(cursor);
    conn.close().await.unwrap();
    println!("\nDone!");
}
