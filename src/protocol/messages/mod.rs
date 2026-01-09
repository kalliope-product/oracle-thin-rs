//! TNS message definitions using the builder pattern.
//!
//! Each message implements the `Message` trait for zero-copy serialization.

pub mod auth;
pub mod connect;
pub mod data_types;
pub mod execute;
pub mod fetch;

pub use auth::{AuthPhaseOneMessage, AuthPhaseTwoMessage, FastAuthMessage};
pub use connect::{
    ConnectMessage, MarkerMessage, ProtocolMessage, TNS_MARKER_TYPE_BREAK, TNS_MARKER_TYPE_RESET,
};
pub use data_types::DataTypesMessage;
pub use execute::ExecuteMessage;
pub use fetch::FetchMessage;
