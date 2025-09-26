use thiserror::Error;

/// Represents all possible errors that can occur when using Zerobus.
#[derive(Error, Debug, Clone)]
pub enum ZerobusError {
    /// Returned when the client failed to open a gRPC channel to the Zerobus endpoint.
    #[error("Failed to open a channel: {0}.")]
    ChannelCreationError(String),
    /// Returned when the client failed to create a stream.
    #[error("Failed to create stream: {0}.")]
    CreateStreamError(tonic::Status),
    /// Returned when TLS handshake failed during connection setup.
    #[error("Failed to establish TLS connection.")]
    FailedToEstablishTlsConnectionError,
    /// Returned when the specified Zerobus endpoint is in invalid format.
    #[error("The specified Zerobus endpoint is in invalid format: {0}.")]
    InvalidZerobusEndpointError(String),
    /// Returned when the specified Unity Catalog table is invalid.
    #[error("Specified UC table is invalid: {0}.")]
    InvalidTableName(String),
    /// Returned when the specified Unity Catalog endpoint is in invalid format.
    #[error("Specified UC endpoint is in invalid format: {0}.")]
    InvalidUCEndpointError(String),
    /// Returned when the specified Unity Catalog token is invalid.
    #[error("Specified UC token is in invalid format: {0}.")]
    InvalidUCTokenError(String),
    /// Returned when the stream is closed.
    #[error("Stream is closed: {0}")]
    StreamClosedError(tonic::Status),
    /// Returned when the client provided an invalid argument.
    #[error("Invalid argument: {0}.")]
    InvalidArgument(String),
    /// Returned when the server returned an unexpected response.
    #[error("Unexpected response from server. Response: {0}")]
    UnexpectedStreamResponseError(String),
    /// Returned when the stream is in an invalid state for a requested operation.
    #[error("Stream is in invalid state: {0}")]
    InvalidStateError(String),
}

/// List of gRPC status codes that indicate unretriable errors.
const UNRETRIABLE_STATUS_CODES: &[tonic::Code] = &[
    tonic::Code::InvalidArgument,
    tonic::Code::Unauthenticated,
    tonic::Code::PermissionDenied,
    tonic::Code::OutOfRange,
    tonic::Code::Unimplemented,
    tonic::Code::NotFound,
];

impl ZerobusError {
    /// Returns true if this error should trigger recovery.
    pub fn is_retryable(&self) -> bool {
        match self {
            ZerobusError::InvalidArgument(_) => false,
            ZerobusError::StreamClosedError(status) => {
                !UNRETRIABLE_STATUS_CODES.contains(&status.code())
            }
            ZerobusError::CreateStreamError(status) => {
                !UNRETRIABLE_STATUS_CODES.contains(&status.code())
            }
            ZerobusError::ChannelCreationError(_) => true,
            ZerobusError::FailedToEstablishTlsConnectionError => true,
            ZerobusError::InvalidZerobusEndpointError(_) => false,
            ZerobusError::InvalidTableName(_) => false,
            ZerobusError::InvalidUCEndpointError(_) => false,
            ZerobusError::InvalidUCTokenError(_) => false,
            ZerobusError::UnexpectedStreamResponseError(_) => true,
            ZerobusError::InvalidStateError(_) => false,
        }
    }
}
