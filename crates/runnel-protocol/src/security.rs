use std::fmt;

use base64::Engine as _;
use zeroize::Zeroize;

use crate::Request;

/// The fixed authorization roles accepted by the application protocol.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SecurityRole {
    /// Message operations and consumer-policy inspection across all streams.
    Application,
    /// All current application-protocol operations.
    Operator,
}

impl fmt::Debug for SecurityRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Application => "Application",
            Self::Operator => "Operator",
        })
    }
}

/// An opaque bearer token held as a redacted, zeroized secret.
pub struct BearerToken(String);

impl BearerToken {
    /// Parse the canonical unpadded base64url representation of 256 random bits.
    pub fn parse(value: String) -> Result<Self, TokenFormatError> {
        let mut decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(value.as_bytes())
            .map_err(|_| TokenFormatError)?;
        let mut canonical = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&decoded);
        let valid = decoded.len() == 32 && canonical == value;
        decoded.zeroize();
        canonical.zeroize();
        if !valid {
            return Err(TokenFormatError);
        }
        Ok(Self(value))
    }

    /// Hold a token received from the bounded protocol control frame.
    ///
    /// The server intentionally authenticates malformed values generically;
    /// format checking therefore belongs to policy authentication rather than
    /// frame decoding.
    #[doc(hidden)]
    pub fn from_wire(value: String) -> Self {
        Self(value)
    }

    /// Expose the token only for protocol authentication or transmission.
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for BearerToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BearerToken([REDACTED])")
    }
}

impl Drop for BearerToken {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

/// The supplied token is not canonical unpadded base64url for 256 bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenFormatError;

impl fmt::Display for TokenFormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bearer token has an invalid format")
    }
}

impl std::error::Error for TokenFormatError {}

impl Request {
    /// Return the minimum role required to dispatch this operation.
    pub const fn required_role(&self) -> SecurityRole {
        match self {
            Self::CreateStream { .. } | Self::ConfigureConsumer { .. } | Self::Health => {
                SecurityRole::Operator
            }
            Self::Publish { .. }
            | Self::PublishBytes { .. }
            | Self::PublishBatch { .. }
            | Self::Poll { .. }
            | Self::PollBatch { .. }
            | Self::Replay { .. }
            | Self::PollGroup { .. }
            | Self::PollGroupBatch { .. }
            | Self::InspectConsumer { .. }
            | Self::Ack { .. }
            | Self::AckBatch { .. }
            | Self::AckGroup { .. }
            | Self::AckGroupBatch { .. } => SecurityRole::Application,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BearerToken, SecurityRole};
    use crate::{BatchDeliveryReceipt, BinaryPayload, PublishBatchRecord, Request};

    #[test]
    fn every_current_operation_has_the_accepted_fixed_role() {
        let application_requests = [
            Request::Publish {
                stream: "s".to_owned(),
                key: None,
                payload: String::new(),
                request_id: None,
            },
            Request::PublishBytes {
                stream: "s".to_owned(),
                key: None,
                payload: BinaryPayload::new(Vec::new()),
                request_id: None,
            },
            Request::PublishBatch {
                stream: "s".to_owned(),
                records: vec![PublishBatchRecord {
                    key: None,
                    payload: BinaryPayload::new(Vec::new()),
                    request_id: None,
                }],
            },
            Request::Poll {
                stream: "s".to_owned(),
                consumer: "c".to_owned(),
            },
            Request::PollBatch {
                stream: "s".to_owned(),
                consumer: "c".to_owned(),
                max_records: 1,
                max_bytes: 1,
                max_wait_ms: 0,
            },
            Request::Replay {
                stream: "s".to_owned(),
                consumer: "c".to_owned(),
                offset: 0,
            },
            Request::PollGroup {
                stream: "s".to_owned(),
                consumer: "c".to_owned(),
                member: "m".to_owned(),
            },
            Request::PollGroupBatch {
                stream: "s".to_owned(),
                consumer: "c".to_owned(),
                member: "m".to_owned(),
                max_records: 1,
                max_bytes: 1,
                max_wait_ms: 0,
            },
            Request::InspectConsumer {
                stream: "s".to_owned(),
                consumer: "c".to_owned(),
            },
            Request::Ack {
                stream: "s".to_owned(),
                consumer: "c".to_owned(),
                offset: 0,
            },
            Request::AckBatch {
                stream: "s".to_owned(),
                consumer: "c".to_owned(),
                receipts: vec![BatchDeliveryReceipt {
                    offset: 0,
                    delivery_token: "token".to_owned(),
                }],
            },
            Request::AckGroup {
                stream: "s".to_owned(),
                consumer: "c".to_owned(),
                member: "m".to_owned(),
                offset: 0,
                delivery_token: "token".to_owned(),
            },
            Request::AckGroupBatch {
                stream: "s".to_owned(),
                consumer: "c".to_owned(),
                member: "m".to_owned(),
                receipts: vec![BatchDeliveryReceipt {
                    offset: 0,
                    delivery_token: "token".to_owned(),
                }],
            },
        ];
        assert!(application_requests
            .iter()
            .all(|request| request.required_role() == SecurityRole::Application));

        let operator_requests = [
            Request::CreateStream {
                stream: "s".to_owned(),
            },
            Request::ConfigureConsumer {
                stream: "s".to_owned(),
                consumer: "c".to_owned(),
                ack_timeout_ms: 100,
                max_delivery_attempts: None,
            },
            Request::Health,
        ];
        assert!(operator_requests
            .iter()
            .all(|request| request.required_role() == SecurityRole::Operator));
    }

    #[test]
    fn token_debug_is_redacted_and_canonical_form_is_enforced() {
        let token_value = "A".repeat(43);
        let token = BearerToken::parse(token_value.clone()).unwrap();
        assert_eq!(token.expose_secret(), token_value);
        assert!(!format!("{token:?}").contains(&token_value));
        assert!(BearerToken::parse(format!("{token_value}=")).is_err());
        assert!(BearerToken::parse("not-a-token".to_owned()).is_err());
    }
}
