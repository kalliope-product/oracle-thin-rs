//! Integration tests for connection timeout and DNS error handling.

use oracle_thin_rs::{Connection, ConnectParams, Error};
use std::time::{Duration, Instant};

#[tokio::test]
async fn test_connection_timeout_unreachable_host() {
    // 192.0.2.1 is a TEST-NET address that should be unreachable (RFC 5737)
    let params = ConnectParams::new("192.0.2.1", 1521, "ORCL")
        .with_connect_timeout(Duration::from_secs(2));

    let start = Instant::now();
    let result = Connection::connect_with_params(&params, "user", "pass").await;
    let elapsed = start.elapsed();

    assert!(matches!(result, Err(Error::ConnectionTimeout { .. })));
    // Should timeout within 5 seconds (with some margin for OS scheduling)
    assert!(
        elapsed < Duration::from_secs(5),
        "Timeout took too long: {:?}",
        elapsed
    );

    // Verify error message format
    if let Err(Error::ConnectionTimeout {
        host,
        port,
        timeout,
    }) = result
    {
        assert_eq!(host, "192.0.2.1");
        assert_eq!(port, 1521);
        assert_eq!(timeout, Duration::from_secs(2));
    }
}

#[tokio::test]
async fn test_dns_resolution_failure() {
    let params = ConnectParams::new(
        "this-hostname-definitely-does-not-exist-12345.invalid",
        1521,
        "ORCL",
    );

    let result = Connection::connect_with_params(&params, "user", "pass").await;
    assert!(matches!(result, Err(Error::DnsResolutionFailed { .. })));

    // Verify error message format
    if let Err(Error::DnsResolutionFailed { hostname, message }) = result {
        assert_eq!(
            hostname,
            "this-hostname-definitely-does-not-exist-12345.invalid"
        );
        assert!(!message.is_empty());
    }
}

#[tokio::test]
async fn test_connection_timeout_with_blackhole_ip() {
    // Using 198.51.100.1 (another TEST-NET address)
    let params = ConnectParams::new("198.51.100.1", 1521, "ORCL")
        .with_connect_timeout(Duration::from_secs(1));

    let start = Instant::now();
    let result = Connection::connect_with_params(&params, "user", "pass").await;
    let elapsed = start.elapsed();

    assert!(matches!(result, Err(Error::ConnectionTimeout { .. })));
    assert!(
        elapsed < Duration::from_secs(3),
        "Timeout took too long: {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_default_timeout_is_20_seconds() {
    let params = ConnectParams::new("localhost", 1521, "ORCL");
    assert_eq!(params.connect_timeout, Duration::from_secs(20));
}

#[tokio::test]
async fn test_custom_timeout_via_builder() {
    let params = ConnectParams::new("localhost", 1521, "ORCL")
        .with_connect_timeout(Duration::from_secs(10));
    assert_eq!(params.connect_timeout, Duration::from_secs(10));
}
