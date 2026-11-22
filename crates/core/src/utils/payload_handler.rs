//! Server-side Payload envelope detection and unwrapping.

use crate::error::{CoreError, CoreResult};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use serde_json::Value;
use tracing::{debug, trace};

/// The value to store for a workflow input, which arrives either as plain
/// JSON or inside a Payload envelope.
///
/// - Plain JSON is returned as it is.
/// - An envelope whose encoding is JSON is unwrapped to the JSON it carries, so
///   that the stored input is readable and a worker receives the input itself.
/// - An envelope with any other encoding (compressed or encrypted by the
///   client, for example) is returned as it is. The engine holds no key and
///   has no business reading such data: it stores the envelope and hands it
///   back unchanged, and the worker that shares the client's codec decodes it.
///
/// # Errors
///
/// Returns an error if an envelope that claims JSON does not carry JSON.
///
/// # Examples
///
/// ```rust,ignore
/// use orcher_core::utils::payload_handler::decode_input_payload;
/// use serde_json::json;
///
/// // Plain JSON passes through
/// let plain = json!({"key": "value"});
/// assert_eq!(decode_input_payload(&plain)?, plain);
///
/// // A JSON envelope is unwrapped
/// let envelope = json!({
///     "metadata": {"encoding": "json"},
///     "data": [123, 34, 107, 101, 121, 34, 58, 34, 118, 97, 108, 117, 101, 34, 125]
/// });
/// assert_eq!(decode_input_payload(&envelope)?, json!({"key": "value"}));
///
/// // An encrypted envelope is kept whole
/// let encrypted = json!({
///     "metadata": {"encoding": "binary/encrypted"},
///     "data": "3q2+7w=="
/// });
/// assert_eq!(decode_input_payload(&encrypted)?, encrypted);
/// # Ok::<(), orcher_core::error::Error>(())
/// ```
pub fn decode_input_payload(input: &Value) -> CoreResult<Value> {
    if !is_payload_envelope(input) {
        trace!("Input is plain JSON, passing through");
        return Ok(input.clone());
    }
    match envelope_encoding(input) {
        Some(encoding) if !encoding.contains("json") => {
            debug!(%encoding, "Payload envelope is not JSON; storing it unread");
            Ok(input.clone())
        }
        _ => unwrap_payload_envelope(input),
    }
}

/// The `encoding` an envelope declares, if it declares one.
///
/// SDKs write a metadata value either as a string or as the bytes of one
/// (an array of numbers), so both are read.
fn envelope_encoding(envelope: &Value) -> Option<String> {
    match envelope.get("metadata")?.get("encoding")? {
        Value::String(s) => Some(s.clone()),
        bytes @ Value::Array(_) => String::from_utf8(extract_bytes_from_data(bytes).ok()?).ok(),
        _ => None,
    }
}

/// Check if a JSON value is a Payload envelope.
///
/// A Payload envelope has the structure:
/// ```json
/// {
///   "metadata": { ... },
///   "data": [...]  // or "data": "base64string"
/// }
/// ```
///
/// # Arguments
///
/// * `value` - The JSON value to check
///
/// # Returns
///
/// `true` if the value appears to be a Payload envelope, `false` otherwise.
///
/// # Examples
///
/// ```rust
/// use orcher_core::utils::payload_handler::is_payload_envelope;
/// use serde_json::json;
///
/// // Payload envelope
/// let envelope = json!({
///     "metadata": {"encoding": "json"},
///     "data": [1, 2, 3]
/// });
/// assert!(is_payload_envelope(&envelope));
///
/// // Plain JSON
/// let plain = json!({"key": "value"});
/// assert!(!is_payload_envelope(&plain));
///
/// // Object with metadata but no data
/// let partial = json!({"metadata": {"encoding": "json"}});
/// assert!(!is_payload_envelope(&partial));
/// ```
pub fn is_payload_envelope(value: &Value) -> bool {
    if let Some(obj) = value.as_object() {
        // Must have both "metadata" and "data" fields
        let has_metadata = obj.contains_key("metadata");
        let has_data = obj.contains_key("data");

        if has_metadata && has_data {
            // Metadata should be an object
            let metadata_is_object = obj.get("metadata").map(|v| v.is_object()).unwrap_or(false);

            // Data should be an array (bytes) or string (base64)
            let data_is_valid = obj
                .get("data")
                .map(|v| v.is_array() || v.is_string())
                .unwrap_or(false);

            return metadata_is_object && data_is_valid;
        }
    }
    false
}

/// Unwrap a Payload envelope whose data is JSON, as bytes or as base64.
///
/// # Errors
///
/// Returns an error if the envelope is malformed or its data is not JSON.
pub fn unwrap_payload_envelope(envelope: &Value) -> CoreResult<Value> {
    let obj = envelope.as_object().ok_or_else(|| {
        CoreError::ValidationFailed("Payload envelope must be an object".to_string())
    })?;

    if !obj.get("metadata").is_some_and(Value::is_object) {
        return Err(CoreError::ValidationFailed(
            "Payload envelope must have metadata object".to_string(),
        ));
    }

    let data = obj.get("data").ok_or_else(|| {
        CoreError::ValidationFailed("Payload envelope must have data field".to_string())
    })?;

    decode_json_payload_data(data)
}

/// Parse JSON from an envelope's data field (a byte array or a base64 string).
fn decode_json_payload_data(data: &Value) -> CoreResult<Value> {
    let bytes = extract_bytes_from_data(data)?;

    serde_json::from_slice(&bytes)
        .map_err(|e| CoreError::Internal(format!("Failed to parse JSON from payload data: {}", e)))
}

/// Extract bytes from the data field of a Payload.
///
/// The data field can be:
/// - An array of numbers (bytes)
/// - A base64-encoded string
///
/// # Arguments
///
/// * `data` - The data field value
///
/// # Returns
///
/// The extracted bytes.
///
/// # Errors
///
/// Returns an error if the data format is invalid.
fn extract_bytes_from_data(data: &Value) -> CoreResult<Vec<u8>> {
    match data {
        // Data as byte array: [123, 34, 107, 101, 121, ...]
        Value::Array(arr) => {
            let bytes: CoreResult<Vec<u8>> = arr
                .iter()
                .map(|v| {
                    v.as_u64()
                        .and_then(|n| u8::try_from(n).ok())
                        .ok_or_else(|| {
                            CoreError::ValidationFailed(format!("Invalid byte value: {}", v))
                        })
                })
                .collect();
            bytes
        }
        // Data as base64 string
        Value::String(s) => BASE64
            .decode(s)
            .map_err(|e| CoreError::Internal(format!("Failed to decode base64 data: {}", e))),
        _ => Err(CoreError::ValidationFailed(
            "Payload data must be an array or string".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_is_payload_envelope_with_valid_envelope() {
        let envelope = json!({
            "metadata": {"encoding": "json"},
            "data": [123, 34, 107, 101, 121, 34, 58, 34, 118, 97, 108, 117, 101, 34, 125]
        });
        assert!(is_payload_envelope(&envelope));
    }

    #[test]
    fn test_is_payload_envelope_with_base64_data() {
        let envelope = json!({
            "metadata": {"encoding": "json"},
            "data": "eyJrZXkiOiJ2YWx1ZSJ9"  // base64 encoded
        });
        assert!(is_payload_envelope(&envelope));
    }

    #[test]
    fn test_is_payload_envelope_with_plain_json() {
        let plain = json!({"key": "value"});
        assert!(!is_payload_envelope(&plain));
    }

    #[test]
    fn test_is_payload_envelope_missing_metadata() {
        let partial = json!({"data": [1, 2, 3]});
        assert!(!is_payload_envelope(&partial));
    }

    #[test]
    fn test_is_payload_envelope_missing_data() {
        let partial = json!({"metadata": {"encoding": "json"}});
        assert!(!is_payload_envelope(&partial));
    }

    #[test]
    fn test_is_payload_envelope_invalid_metadata_type() {
        let invalid = json!({
            "metadata": "not an object",
            "data": [1, 2, 3]
        });
        assert!(!is_payload_envelope(&invalid));
    }

    #[test]
    fn test_decode_input_payload_plain_json() {
        let plain = json!({"amount": 99.99, "currency": "USD"});
        let result = decode_input_payload(&plain).unwrap();
        assert_eq!(result, plain);
    }

    #[test]
    fn test_decode_input_payload_with_byte_array() {
        // {"key":"value"} as bytes
        let bytes = vec![
            123, 34, 107, 101, 121, 34, 58, 34, 118, 97, 108, 117, 101, 34, 125,
        ];
        let envelope = json!({
            "metadata": {"encoding": "json", "content-type": "application/json"},
            "data": bytes
        });

        let result = decode_input_payload(&envelope).unwrap();
        assert_eq!(result, json!({"key": "value"}));
    }

    #[test]
    fn test_decode_input_payload_with_base64() {
        // {"key":"value"} as base64
        let base64_data = "eyJrZXkiOiJ2YWx1ZSJ9";
        let envelope = json!({
            "metadata": {"encoding": "json"},
            "data": base64_data
        });

        let result = decode_input_payload(&envelope).unwrap();
        assert_eq!(result, json!({"key": "value"}));
    }

    #[test]
    fn test_decode_input_payload_complex_object() {
        // Complex object: {"order_id": "123", "items": ["a", "b"], "total": 99.99}
        let json_str = r#"{"order_id":"123","items":["a","b"],"total":99.99}"#;
        let bytes: Vec<u8> = json_str.as_bytes().to_vec();

        let envelope = json!({
            "metadata": {"encoding": "json"},
            "data": bytes
        });

        let result = decode_input_payload(&envelope).unwrap();
        let expected = json!({
            "order_id": "123",
            "items": ["a", "b"],
            "total": 99.99
        });
        assert_eq!(result, expected);
    }

    #[test]
    fn test_unwrap_payload_envelope_missing_metadata() {
        let invalid = json!({"data": [1, 2, 3]});
        let result = unwrap_payload_envelope(&invalid);
        assert!(result.is_err());
    }

    #[test]
    fn test_unwrap_payload_envelope_missing_data() {
        let invalid = json!({"metadata": {"encoding": "json"}});
        let result = unwrap_payload_envelope(&invalid);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_bytes_from_byte_array() {
        let data = json!([72, 101, 108, 108, 111]); // "Hello"
        let bytes = extract_bytes_from_data(&data).unwrap();
        assert_eq!(bytes, b"Hello");
    }

    #[test]
    fn test_extract_bytes_from_base64() {
        let data = json!("SGVsbG8="); // "Hello" in base64
        let bytes = extract_bytes_from_data(&data).unwrap();
        assert_eq!(bytes, b"Hello");
    }

    #[test]
    fn test_extract_bytes_invalid_type() {
        let data = json!(123);
        let result = extract_bytes_from_data(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_bytes_invalid_byte_value() {
        let data = json!([256]); // Out of u8 range
        let result = extract_bytes_from_data(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_json_payload_data() {
        let bytes = br#"{"test":true}"#.to_vec();
        let data = json!(bytes);
        let result = decode_json_payload_data(&data).unwrap();
        assert_eq!(result, json!({"test": true}));
    }

    #[test]
    fn an_encrypted_envelope_is_kept_whole() {
        // What a client that encrypts sends: the data is ciphertext, which
        // is not JSON and must not be parsed as it.
        let envelope = json!({
            "metadata": {"encoding": "binary/encrypted", "key-id": "k1"},
            "data": "3q2+7wABAgM="
        });
        assert_eq!(decode_input_payload(&envelope).unwrap(), envelope);
    }

    #[test]
    fn an_encoding_written_as_bytes_is_read() {
        // serde writes a `Vec<u8>` metadata value as an array of numbers.
        let encoding: Vec<u8> = b"binary/encrypted".to_vec();
        let envelope = json!({
            "metadata": {"encoding": encoding},
            "data": [222, 173, 190, 239]
        });
        assert_eq!(decode_input_payload(&envelope).unwrap(), envelope);

        let json_encoding: Vec<u8> = b"json".to_vec();
        let envelope = json!({
            "metadata": {"encoding": json_encoding},
            "data": br#"{"k":1}"#.to_vec()
        });
        assert_eq!(decode_input_payload(&envelope).unwrap(), json!({"k": 1}));
    }

    #[test]
    fn a_compressed_or_binary_envelope_is_kept_whole() {
        for encoding in [
            "binary/gzip",
            "binary/protobuf",
            "binary",
            "application/x-thing",
        ] {
            let envelope = json!({"metadata": {"encoding": encoding}, "data": [1, 2, 3]});
            assert_eq!(
                decode_input_payload(&envelope).unwrap(),
                envelope,
                "{encoding}"
            );
        }
    }

    #[test]
    fn a_json_envelope_that_does_not_carry_json_is_refused() {
        let envelope = json!({"metadata": {"encoding": "json"}, "data": [222, 173]});
        assert!(decode_input_payload(&envelope).is_err());
    }

    #[test]
    fn test_typescript_sdk_style_envelope() {
        // This is what the TypeScript SDK sends
        let typescript_envelope = json!({
            "metadata": {
                "encoding": "json/plain",
                "messageType": "order"
            },
            "data": [123, 34, 97, 109, 111, 117, 110, 116, 34, 58, 57, 57, 46, 57, 57, 125]
        });

        let result = decode_input_payload(&typescript_envelope).unwrap();
        assert_eq!(result, json!({"amount": 99.99}));
    }

    #[test]
    fn test_nested_json_object() {
        let json_str = r#"{"user":{"id":1,"name":"Alice"},"settings":{"theme":"dark"}}"#;
        let bytes: Vec<u8> = json_str.as_bytes().to_vec();

        let envelope = json!({
            "metadata": {"encoding": "json"},
            "data": bytes
        });

        let result = decode_input_payload(&envelope).unwrap();
        assert_eq!(
            result,
            json!({
                "user": {"id": 1, "name": "Alice"},
                "settings": {"theme": "dark"}
            })
        );
    }

    #[test]
    fn test_unicode_content() {
        let json_str = r#"{"message":"Hello 世界 🌍"}"#;
        let bytes: Vec<u8> = json_str.as_bytes().to_vec();

        let envelope = json!({
            "metadata": {"encoding": "json"},
            "data": bytes
        });

        let result = decode_input_payload(&envelope).unwrap();
        assert_eq!(result, json!({"message": "Hello 世界 🌍"}));
    }
}
