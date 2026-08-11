use thiserror::Error;
#[cfg(feature = "arrow-flight")]
use tonic_types::StatusExt;

/// The `google.rpc.ErrorInfo.reason` the Zerobus server sets on a schema
/// validation failure. Presence of this reason (in the gRPC status details) is
/// what distinguishes a schema mismatch from any other `InvalidArgument`.
#[cfg(feature = "arrow-flight")]
const SCHEMA_VALIDATION_REASON: &str = "SCHEMA_VALIDATION_FAILED";

/// The `google.rpc.ErrorInfo.domain` the Zerobus server scopes its schema
/// validation detail under. Requiring the domain (in addition to the reason)
/// before specializing keeps an unrelated status that happens to reuse the same
/// reason token from being misclassified as a schema mismatch. An empty domain
/// is accepted for forward-compatibility with servers that omit it.
#[cfg(feature = "arrow-flight")]
const SCHEMA_VALIDATION_DOMAIN: &str = "zerobus.databricks.com";

/// A machine-readable cause of a schema-validation rejection, reported by the
/// server in the gRPC `ErrorInfo` metadata (as stable string tokens) and
/// decoded here into typed variants.
///
/// A single rejection can carry several distinct causes at once (see
/// [`ZerobusError::InvalidSchema`]). The SDK reports the raw causes and does
/// not interpret them: whether a given mismatch is recoverable — e.g. by
/// re-resolving the table schema and rebuilding the stream — is a caller-side
/// policy decision.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SchemaValidationCause {
    /// The client sent a field that does not exist in the Delta table schema
    /// (e.g. a column was dropped from the table).
    FieldNotInTable,
    /// The client omitted a non-nullable Delta column (e.g. a required column
    /// was added to the table).
    MissingRequiredColumn,
    /// The client's fields do not follow the Delta schema column order.
    FieldOutOfOrder,
    /// A client field's type is incompatible with the Delta column type.
    TypeIncompatible,
    /// The client field count does not match the Delta schema.
    FieldCountMismatch,
    /// A cause token the server sent that this SDK version does not recognize.
    /// Carries the raw wire token so newer server causes are surfaced rather
    /// than silently dropped.
    Unknown(String),
}

impl SchemaValidationCause {
    /// Decode a wire token (as sent in the server's `ErrorInfo` metadata) into
    /// a typed cause. Unrecognized tokens map to [`SchemaValidationCause::Unknown`].
    #[cfg(feature = "arrow-flight")]
    fn from_wire(token: &str) -> Self {
        match token {
            "FIELD_NOT_IN_TABLE" => SchemaValidationCause::FieldNotInTable,
            "MISSING_REQUIRED_COLUMN" => SchemaValidationCause::MissingRequiredColumn,
            "FIELD_OUT_OF_ORDER" => SchemaValidationCause::FieldOutOfOrder,
            "TYPE_INCOMPATIBLE" => SchemaValidationCause::TypeIncompatible,
            "FIELD_COUNT_MISMATCH" => SchemaValidationCause::FieldCountMismatch,
            other => SchemaValidationCause::Unknown(other.to_string()),
        }
    }
}

/// Represents all possible errors that can occur when using Zerobus.
#[derive(Error, Debug, Clone)]
#[non_exhaustive]
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
    /// Returned when the specified Unity Catalog table name is invalid.
    #[error("Specified UC table name is invalid: {0}.")]
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
    /// Returned when the server rejected the stream because the client's Arrow
    /// schema does not match the target Delta table (e.g. a column was added to
    /// or dropped from the table). Distinguished from the generic
    /// [`ZerobusError::CreateStreamError`] via the structured `ErrorInfo` the
    /// server attaches to the gRPC status.
    ///
    /// `causes` carries the raw, machine-readable causes the server reported
    /// (e.g. [`SchemaValidationCause::FieldNotInTable`]); a single rejection can
    /// list several. The SDK deliberately does not interpret them: whether a
    /// mismatch is recoverable — e.g. by re-resolving the table schema and
    /// rebuilding the stream — is a caller-side policy decision. Per-field
    /// detail (offending column names) is carried in `message` for diagnostics,
    /// not structurally. `error_code` is the server's numeric Shinkansen code
    /// (e.g. `"8001"`) when present, useful for telemetry correlation. This
    /// error is not SDK-retryable, since the SDK holds a fixed schema and its
    /// recovery loop would re-send the same rejected schema.
    ///
    /// `#[non_exhaustive]` so additional server-reported detail (e.g. the gRPC
    /// code or domain) can be added later without a breaking change.
    #[error("Arrow schema does not match the target table: {message}.")]
    #[non_exhaustive]
    InvalidSchema {
        message: String,
        causes: Vec<SchemaValidationCause>,
        error_code: Option<String>,
    },
    /// Returned when the server returned an unexpected response.
    #[error("Unexpected response from server. Response: {0}")]
    UnexpectedStreamResponseError(String),
    /// Returned when the stream is in an invalid state for a requested operation.
    #[error("Stream is in invalid state: {0}")]
    InvalidStateError(String),
    /// Returned when a connection or setup operation times out.
    #[error("Connection timeout: {0}")]
    ConnectionTimeout(String),
    /// Returned when OAuth token fetching fails due to network or server errors.
    #[error("Token fetch failed: {0}")]
    TokenFetchError(String),
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
    /// Classify a `tonic::Status` returned by the server during stream setup.
    ///
    /// If the server attached a schema-validation `ErrorInfo` detail (reason
    /// `SCHEMA_VALIDATION_FAILED` scoped to the `zerobus.databricks.com`
    /// domain), returns [`ZerobusError::InvalidSchema`] carrying the structured
    /// `causes` the server reported.
    /// Otherwise falls back to [`ZerobusError::CreateStreamError`], preserving
    /// the original status (and its gRPC code) for retry/auth classification.
    #[cfg(feature = "arrow-flight")]
    pub(crate) fn from_setup_status(status: tonic::Status) -> Self {
        if let Some(info) = status.get_details_error_info() {
            // Require both the reason and the server's domain (empty domain
            // accepted for forward-compat) so an unrelated status reusing the
            // reason token is not misread as a schema mismatch.
            let domain_ok = info.domain == SCHEMA_VALIDATION_DOMAIN || info.domain.is_empty();
            if info.reason == SCHEMA_VALIDATION_REASON && domain_ok {
                // Comma-separated tokens; an absent key means no causes.
                let causes = info
                    .metadata
                    .get("causes")
                    .map(|v| {
                        v.split(',')
                            .filter(|s| !s.is_empty())
                            .map(SchemaValidationCause::from_wire)
                            .collect()
                    })
                    .unwrap_or_default();
                return ZerobusError::InvalidSchema {
                    message: status.message().to_string(),
                    causes,
                    error_code: info.metadata.get("error_code").cloned(),
                };
            }
        }
        ZerobusError::CreateStreamError(status)
    }

    /// Determines whether this error can be automatically recovered through stream recovery.
    ///
    /// Retryable errors typically indicate transient issues like network failures or
    /// temporary server problems. Non-retryable errors indicate permanent issues like
    /// authentication failures or invalid configurations that require manual intervention.
    ///
    /// # Returns
    ///
    /// `true` if the SDK should attempt automatic recovery, `false` otherwise.
    pub fn is_retryable(&self) -> bool {
        match self {
            ZerobusError::InvalidArgument(_) => false,
            ZerobusError::InvalidSchema { .. } => false,
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
            ZerobusError::ConnectionTimeout(_) => true,
            ZerobusError::TokenFetchError(_) => true,
        }
    }

    /// Reports whether this is a server-side authentication/authorization
    /// rejection (as opposed to a transient or unrelated failure). Used to
    /// decide when to invalidate cached credentials so the next attempt
    /// re-derives them.
    pub(crate) fn is_auth_rejection(&self) -> bool {
        matches!(
            self,
            ZerobusError::CreateStreamError(status) | ZerobusError::StreamClosedError(status)
                if matches!(
                    status.code(),
                    tonic::Code::Unauthenticated | tonic::Code::PermissionDenied
                )
        )
    }
}

/// Applies the initial-connection retry policy without changing global error
/// classification. An auth rejection may use one retry from the recovery budget
/// so a stale credential can be refreshed; subsequent auth rejections remain terminal.
///
/// Shared by both transports (proto `stream/grpc` and Arrow Flight) so their
/// initial-setup credential-refresh behavior stays identical. It applies only to
/// initial setup; reconnect paths keep the plain `recovery && is_retryable()` rule.
pub(crate) fn should_retry_initial_connection(
    error: &ZerobusError,
    recovery_enabled: bool,
    auth_retry_available: &mut bool,
) -> bool {
    if !recovery_enabled {
        return false;
    }

    if error.is_retryable() {
        return true;
    }

    if error.is_auth_rejection() && *auth_retry_available {
        *auth_retry_available = false;
        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_connection_auth_retry_is_one_shot() {
        let unauthenticated =
            ZerobusError::CreateStreamError(tonic::Status::unauthenticated("stale token"));
        let permission_denied =
            ZerobusError::CreateStreamError(tonic::Status::permission_denied("stale token"));
        let mut auth_retry_available = true;

        assert!(should_retry_initial_connection(
            &unauthenticated,
            true,
            &mut auth_retry_available
        ));
        assert!(!auth_retry_available);
        assert!(!should_retry_initial_connection(
            &permission_denied,
            true,
            &mut auth_retry_available
        ));
        assert!(!unauthenticated.is_retryable());
        assert!(!permission_denied.is_retryable());
    }

    #[test]
    fn initial_connection_auth_retry_requires_recovery_and_budget() {
        let auth_error =
            ZerobusError::CreateStreamError(tonic::Status::unauthenticated("stale token"));
        let permanent_error =
            ZerobusError::CreateStreamError(tonic::Status::invalid_argument("bad schema"));

        let mut auth_retry_available = true;
        assert!(!should_retry_initial_connection(
            &auth_error,
            false,
            &mut auth_retry_available
        ));
        assert!(auth_retry_available);

        let mut no_retry_budget = false;
        assert!(!should_retry_initial_connection(
            &auth_error,
            true,
            &mut no_retry_budget
        ));

        let mut auth_retry_available = true;
        assert!(!should_retry_initial_connection(
            &permanent_error,
            true,
            &mut auth_retry_available
        ));
        assert!(auth_retry_available);
    }

    #[test]
    fn auth_rejection_classification() {
        assert!(
            ZerobusError::CreateStreamError(tonic::Status::unauthenticated("x"))
                .is_auth_rejection()
        );
        assert!(
            ZerobusError::CreateStreamError(tonic::Status::permission_denied("x"))
                .is_auth_rejection()
        );
        assert!(
            ZerobusError::StreamClosedError(tonic::Status::unauthenticated("x"))
                .is_auth_rejection()
        );
        // Non-auth gRPC codes are not rejections.
        assert!(!ZerobusError::CreateStreamError(tonic::Status::internal("x")).is_auth_rejection());
        assert!(
            !ZerobusError::CreateStreamError(tonic::Status::unavailable("x")).is_auth_rejection()
        );
        // Other variants are never auth rejections.
        assert!(!ZerobusError::TokenFetchError("x".to_string()).is_auth_rejection());
    }

    /// Pins the cross-crate invariant the Arrow path relies on: `FlightError ->
    /// tonic::Status` via `From` preserves the inner gRPC code (unlike
    /// `Status::from_error`, which flattens it to `Unknown`). A future
    /// `arrow-flight` change to that `From` impl fails here instead of silently
    /// disabling Arrow auth-rejection detection.
    #[cfg(feature = "arrow-flight")]
    #[test]
    fn auth_rejection_survives_flight_error_conversion() {
        use arrow_flight::error::FlightError;

        let auth: tonic::Status =
            FlightError::Tonic(Box::new(tonic::Status::permission_denied("denied"))).into();
        assert!(ZerobusError::CreateStreamError(auth).is_auth_rejection());

        let non_auth: tonic::Status =
            FlightError::Tonic(Box::new(tonic::Status::unavailable("blip"))).into();
        assert!(!ZerobusError::CreateStreamError(non_auth).is_auth_rejection());
    }

    /// Pins the invariant the whole `InvalidSchema` feature rests on: the
    /// server's `ErrorInfo` detail survives the `FlightError::Tonic -> tonic::Status`
    /// conversion. Production never sees the raw `Status` — the Arrow setup and
    /// reconnect paths (`stream/arrow/connection.rs`) call
    /// `from_setup_status(flight_error.into())`, so the details must round-trip through
    /// this `From` for schema-mismatch
    /// classification to work at all. Building a `Status` directly (as the other
    /// tests do) would skip this conversion and mask a regression here.
    #[cfg(feature = "arrow-flight")]
    #[test]
    fn schema_error_info_survives_flight_error_conversion() {
        use arrow_flight::error::FlightError;

        let status = schema_validation_status("FIELD_NOT_IN_TABLE,TYPE_INCOMPATIBLE");
        // Mirror the production path: server error arrives as a FlightError and
        // is converted to a Status before from_setup_status classifies it.
        let converted: tonic::Status = FlightError::Tonic(Box::new(status)).into();
        match ZerobusError::from_setup_status(converted) {
            ZerobusError::InvalidSchema { causes, .. } => {
                assert_eq!(
                    causes,
                    vec![
                        SchemaValidationCause::FieldNotInTable,
                        SchemaValidationCause::TypeIncompatible,
                    ]
                );
            }
            other => panic!("expected InvalidSchema after FlightError conversion, got {other:?}"),
        }
    }

    /// Build an InvalidArgument status carrying the server's schema-validation
    /// ErrorInfo, mirroring what Shinkansen sends on a schema mismatch.
    #[cfg(feature = "arrow-flight")]
    fn schema_validation_status(causes: &str) -> tonic::Status {
        use std::collections::HashMap;
        use tonic_types::ErrorDetails;

        let mut metadata = HashMap::new();
        metadata.insert("error_code".to_string(), "8001".to_string());
        metadata.insert("causes".to_string(), causes.to_string());
        tonic::Status::with_error_details(
            tonic::Code::InvalidArgument,
            "Arrow Flight schema validation failed: ...",
            ErrorDetails::with_error_info(
                "SCHEMA_VALIDATION_FAILED",
                "zerobus.databricks.com",
                metadata,
            ),
        )
    }

    #[cfg(feature = "arrow-flight")]
    #[test]
    fn setup_status_with_schema_error_info_classifies_as_invalid_schema() {
        let status = schema_validation_status("FIELD_NOT_IN_TABLE,TYPE_INCOMPATIBLE");
        let err = ZerobusError::from_setup_status(status);
        match err {
            ZerobusError::InvalidSchema {
                causes, error_code, ..
            } => {
                assert_eq!(
                    causes,
                    vec![
                        SchemaValidationCause::FieldNotInTable,
                        SchemaValidationCause::TypeIncompatible,
                    ]
                );
                // The server's numeric code is carried for telemetry.
                assert_eq!(error_code.as_deref(), Some("8001"));
            }
            other => panic!("expected InvalidSchema, got {other:?}"),
        }
    }

    /// A cause token this SDK version does not recognize is surfaced as
    /// `Unknown` rather than dropped, so newer server causes still reach callers.
    #[cfg(feature = "arrow-flight")]
    #[test]
    fn setup_status_maps_unknown_cause_token() {
        let status = schema_validation_status("FIELD_NOT_IN_TABLE,BRAND_NEW_CAUSE");
        match ZerobusError::from_setup_status(status) {
            ZerobusError::InvalidSchema { causes, .. } => {
                assert_eq!(
                    causes,
                    vec![
                        SchemaValidationCause::FieldNotInTable,
                        SchemaValidationCause::Unknown("BRAND_NEW_CAUSE".to_string()),
                    ]
                );
            }
            other => panic!("expected InvalidSchema, got {other:?}"),
        }
    }

    #[cfg(feature = "arrow-flight")]
    #[test]
    fn invalid_schema_is_not_retryable() {
        let err = ZerobusError::from_setup_status(schema_validation_status("FIELD_NOT_IN_TABLE"));
        assert!(!err.is_retryable());
    }

    #[cfg(feature = "arrow-flight")]
    #[test]
    fn setup_status_without_error_info_falls_back_to_create_stream_error() {
        // A plain InvalidArgument with no schema ErrorInfo stays a CreateStreamError.
        let status = tonic::Status::invalid_argument("some other bad argument");
        let err = ZerobusError::from_setup_status(status);
        assert!(matches!(err, ZerobusError::CreateStreamError(_)));
    }

    /// A status carrying the schema reason but a *different* domain is not a
    /// Zerobus schema mismatch and must stay a `CreateStreamError`, so an
    /// unrelated service reusing the reason token can't be misclassified.
    #[cfg(feature = "arrow-flight")]
    #[test]
    fn setup_status_with_wrong_domain_falls_back_to_create_stream_error() {
        use std::collections::HashMap;
        use tonic_types::ErrorDetails;

        let mut metadata = HashMap::new();
        metadata.insert("causes".to_string(), "FIELD_NOT_IN_TABLE".to_string());
        let status = tonic::Status::with_error_details(
            tonic::Code::InvalidArgument,
            "schema mismatch from some other service",
            ErrorDetails::with_error_info(
                "SCHEMA_VALIDATION_FAILED",
                "someone.else.example.com",
                metadata,
            ),
        );
        let err = ZerobusError::from_setup_status(status);
        assert!(matches!(err, ZerobusError::CreateStreamError(_)));
    }

    #[cfg(feature = "arrow-flight")]
    #[test]
    fn setup_status_preserves_grpc_code_on_fallback() {
        // Non-schema setup failures keep their original code for retry/auth
        // classification (e.g. Unauthenticated remains an auth rejection).
        let err = ZerobusError::from_setup_status(tonic::Status::unauthenticated("denied"));
        assert!(err.is_auth_rejection());
    }

    /// Absent metadata keys yield empty lists rather than panicking.
    #[cfg(feature = "arrow-flight")]
    #[test]
    fn invalid_schema_tolerates_missing_metadata() {
        use tonic_types::ErrorDetails;

        let status = tonic::Status::with_error_details(
            tonic::Code::InvalidArgument,
            "schema mismatch",
            ErrorDetails::with_error_info(
                "SCHEMA_VALIDATION_FAILED",
                "zerobus.databricks.com",
                std::collections::HashMap::new(),
            ),
        );
        match ZerobusError::from_setup_status(status) {
            ZerobusError::InvalidSchema {
                causes, error_code, ..
            } => {
                assert!(causes.is_empty());
                assert!(error_code.is_none());
            }
            other => panic!("expected InvalidSchema, got {other:?}"),
        }
    }
}
