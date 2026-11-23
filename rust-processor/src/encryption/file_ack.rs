//! Public FILEACK shape only. Not a cache COMMIT/byte/AEAD proof or authority
//! to reset Signal. Native pending proof/RPC scheduling are separate stages.
use super::{
    contract::{ContractError, MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES},
    epoch_contract, epoch_rpc, recovery_contract,
};
use crate::proto::e2ee_v2 as v2;
use prost::Message;
use std::collections::BTreeSet;

pub const MAX_FILE_ACKNOWLEDGEMENTS: usize = 32;
fn invalid() -> ContractError {
    ContractError::Invalid("file acknowledgement binding")
}
pub fn validate_request(r: &v2::AcknowledgeFilesRequest) -> Result<(), ContractError> {
    epoch_rpc::validate_context(r.context.as_ref())?;
    if r.files.is_empty()
        || r.files.len() > MAX_FILE_ACKNOWLEDGEMENTS
        || r.encoded_len() > MAX_REQUEST_BYTES
    {
        return Err(invalid());
    }
    let generation = &r.context.as_ref().unwrap().device_generation_id;
    let mut ids = BTreeSet::new();
    let mut recipient = None;
    for a in &r.files {
        let b = a.binding.as_ref().ok_or_else(invalid)?;
        recovery_contract::validate_send_binding(b)?;
        epoch_contract::validate_blob_ref(a.blob.as_ref().ok_or_else(invalid)?)?;
        let own = b.recipient.as_ref().unwrap();
        if b.protocol_version != 3
            || &own.generation_id != generation
            || recipient.is_some_and(|user| user != &own.user_id)
            || !ids.insert(&b.message_id)
        {
            return Err(invalid());
        }
        recipient = Some(&own.user_id);
    }
    Ok(())
}
pub fn validate_receipts(
    r: &v2::AcknowledgeFilesRequest,
    response: &v2::FileAcknowledgements,
) -> Result<(), ContractError> {
    validate_request(r)?;
    if response.files.len() != r.files.len() || response.encoded_len() > MAX_RESPONSE_BYTES {
        return Err(invalid());
    }
    for (expected, receipt) in r.files.iter().zip(&response.files) {
        if receipt.acknowledged_at_unix_ms <= 0
            || receipt.acknowledgement.as_ref() != Some(expected)
        {
            return Err(invalid());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn vector() -> (v2::AcknowledgeFilesRequest, v2::FileAcknowledgements) {
        let a = include_bytes!("../../../proto/fixtures/file-ack-v1/request.bin");
        let b = include_bytes!("../../../proto/fixtures/file-ack-v1/response.bin");
        let r = v2::AcknowledgeFilesRequest::decode(a.as_slice()).unwrap();
        let s = v2::FileAcknowledgements::decode(b.as_slice()).unwrap();
        assert_eq!(r.encode_to_vec(), a);
        assert_eq!(s.encode_to_vec(), b);
        (r, s)
    }
    #[test]
    fn public_file_ack_vector_matches_go_and_preserves_large_epoch() {
        let (mut r, mut s) = vector();
        validate_receipts(&r, &s).unwrap();
        assert_eq!(r.files[0].binding.as_ref().unwrap().epoch, (1 << 53) + 17);
        r.files[0].binding.as_mut().unwrap().epoch = 0;
        s.files[0]
            .acknowledgement
            .as_mut()
            .unwrap()
            .binding
            .as_mut()
            .unwrap()
            .epoch = 0;
        validate_receipts(&r, &s).unwrap();
    }
    #[test]
    fn file_ack_request_rejects_missing_mixed_duplicate_and_wrong_scope() {
        let (original, _) = vector();
        for case in 0..12 {
            let mut r = original.clone();
            match case {
                0 => r.context = None,
                1 => r.files.clear(),
                2 => r.files[0].binding = None,
                3 => r.files[0].blob = None,
                4 => r.files[0].binding.as_mut().unwrap().protocol_version = 2,
                5 => r.files[0].binding.as_mut().unwrap().epoch = u64::MAX,
                6 => r.context.as_mut().unwrap().protocol_version = 3,
                7 => {
                    r.context.as_mut().unwrap().device_generation_id =
                        uuid::Uuid::new_v4().to_string()
                }
                8 => r.files.push(r.files[0].clone()),
                9 => {
                    let mut other = r.files[0].clone();
                    let b = other.binding.as_mut().unwrap();
                    b.message_id = uuid::Uuid::new_v4().to_string();
                    b.recipient.as_mut().unwrap().user_id = uuid::Uuid::new_v4().to_string();
                    r.files.push(other);
                }
                10 => r.files[0].blob.as_mut().unwrap().format_version = 2,
                _ => {
                    r.files[0].blob.as_mut().unwrap().sha256.pop().unwrap();
                }
            }
            assert!(validate_request(&r).is_err(), "invalid case {case}");
        }
    }
    #[test]
    fn file_ack_batch_bound_exact_receipts_and_no_local_clock_assumption() {
        let (mut r, mut s) = vector();
        let item = r.files.remove(0);
        r.files.clear();
        s.files.clear();
        for _ in 0..MAX_FILE_ACKNOWLEDGEMENTS {
            let mut a = item.clone();
            a.binding.as_mut().unwrap().message_id = uuid::Uuid::new_v4().to_string();
            s.files.push(v2::FileAcknowledgementReceipt {
                acknowledgement: Some(a.clone()),
                acknowledged_at_unix_ms: 1,
            });
            r.files.push(a);
        }
        validate_receipts(&r, &s).unwrap(); // Retained first ACK time, not local TTL.
        r.files.push(item);
        assert!(validate_request(&r).is_err());
        r.files.pop();
        s.files.swap(0, 1);
        assert!(validate_receipts(&r, &s).is_err());
        s.files.swap(0, 1);
        s.files[0].acknowledged_at_unix_ms = 0;
        assert!(validate_receipts(&r, &s).is_err());
        s.files[0].acknowledged_at_unix_ms = 1;
        s.files[0]
            .acknowledgement
            .as_mut()
            .unwrap()
            .blob
            .as_mut()
            .unwrap()
            .sha256[0] ^= 1;
        assert!(validate_receipts(&r, &s).is_err());
    }
}
