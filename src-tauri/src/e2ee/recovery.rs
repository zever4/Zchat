//! Explicit manual recovery, not device replacement. Renderer selects only a
//! known chat, the observed epoch/reference and a confirmed action. Signing,
//! durable freeze, transport retries and activation stay with the native owner.
use super::{DesktopE2ee, Reason};
use serde::{Deserialize, Serialize};
use tauri::ipc::Response;

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    Accept,
    Decline,
    Cancel,
}
fn required_reference<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    Option::<String>::deserialize(d)
}
#[derive(Deserialize, Serialize)]
#[serde(
    tag = "action",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum RecoveryRequest {
    Get {
        conversation_id: String,
    },
    Propose {
        conversation_id: String,
        expected_epoch: String,
        #[serde(deserialize_with = "required_reference")]
        expected_own_proposal_id: Option<String>,
        confirmed: bool,
    },
    Decide {
        conversation_id: String,
        expected_epoch: String,
        proposal_id: String,
        decision: Decision,
        confirmed: bool,
    },
}
pub(super) fn epoch(value: &str) -> Result<u64, Reason> {
    if value.is_empty() || value.len() > 19 {
        return Err(Reason::Invalid);
    }
    let number = value.parse::<u64>().map_err(|_| Reason::Invalid)?;
    if number > i64::MAX as u64 || number.to_string() != value {
        return Err(Reason::Invalid);
    }
    Ok(number)
}
impl RecoveryRequest {
    pub fn validate(&self) -> Result<(), Reason> {
        let valid = super::admission::valid_id;
        match self {
            Self::Get { conversation_id } if valid(conversation_id) => Ok(()),
            Self::Propose {
                conversation_id,
                expected_epoch,
                expected_own_proposal_id,
                confirmed: true,
            } if valid(conversation_id)
                && expected_own_proposal_id.as_deref().is_none_or(valid) =>
            {
                epoch(expected_epoch).map(|_| ())
            }
            Self::Decide {
                conversation_id,
                expected_epoch,
                proposal_id,
                confirmed: true,
                ..
            } if valid(conversation_id) && valid(proposal_id) => epoch(expected_epoch).map(|_| ()),
            _ => Err(Reason::Invalid),
        }
    }
}
#[cfg(feature = "experimental-e2ee")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Proposal {
    proposal_id: String,
    state: Option<String>,
    own_proposal: bool,
    decision: Option<Decision>,
    sending_frozen: bool,
}
#[cfg(feature = "experimental-e2ee")]
#[derive(Serialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum RecoveryReply {
    State {
        conversation_id: String,
        crypto_epoch: Option<String>,
        receive_only: bool,
        sending_frozen: bool,
        proposals: Vec<Proposal>,
    },
    // Only durable LOCAL acceptance of the signed intent, not peer consent,
    // server ACK, successful cancellation or completed epoch activation.
    Queued {
        conversation_id: String,
        crypto_epoch: String,
        operation_id: String,
    },
}
// Disabled builds retain the rejecting command/input contract only.
#[cfg(not(feature = "experimental-e2ee"))]
#[derive(Serialize)]
pub struct RecoveryReply;
#[derive(Serialize)]
struct Reply {
    context: u64,
    recovery: RecoveryReply,
}
pub(crate) async fn execute(
    state: &DesktopE2ee,
    context: u64,
    request_id: String,
    operation: RecoveryRequest,
    check_page: impl Fn() -> Result<(), Reason>,
) -> Result<Response, Reason> {
    check_page()?;
    operation.validate()?;
    state.manager.ensure_dialog_ready(context)?;
    let guard = state.admission.begin(context, request_id)?;
    let recovery = tokio::select! {
        biased;
        _ = guard.cancel.cancelled() => return Err(Reason::Stale),
        result = tokio::time::timeout(std::time::Duration::from_secs(30), state.manager.recovery(context, operation)) => result.map_err(|_| Reason::NativeWait)??,
    };
    check_page()?;
    state.manager.ensure_dialog_ready(context)?;
    guard.deliver(context, &Reply { context, recovery })
}
#[cfg(feature = "experimental-e2ee")]
pub(super) async fn request(
    client: &rust_processor::encryption::account_owner::AccountClient,
    operation: RecoveryRequest,
) -> Result<RecoveryReply, Reason> {
    use rust_processor::{
        encryption::signal_state::EpochRecoveryDecision as D, proto::e2ee::RecoveryState as S,
    };
    let convert_decision = |decision| match decision {
        D::Accept => Decision::Accept,
        D::Decline => Decision::Decline,
        D::Cancel => Decision::Cancel,
    };
    match operation {
        RecoveryRequest::Get { conversation_id } => {
            let view = client
                .recovery_overview(&conversation_id)
                .await
                .map_err(super::factory::account_error)?;
            if view.proposals.len() > 2 {
                return Err(Reason::Runtime);
            }
            let proposals = view
                .proposals
                .into_iter()
                .map(|p| {
                    let state = p
                        .state()
                        .map(|s| match s {
                            S::Pending => Ok("pending"),
                            S::Accepted => Ok("accepted"),
                            S::Declined => Ok("declined"),
                            S::Cancelled => Ok("cancelled"),
                            S::Expired => Ok("expired"),
                            S::Superseded => Ok("superseded"),
                            _ => Err(Reason::Runtime),
                        })
                        .transpose()?
                        .map(str::to_owned);
                    Ok(Proposal {
                        proposal_id: p.proposal_id().into(),
                        state,
                        own_proposal: p.own_proposal(),
                        decision: p.decision().map(convert_decision),
                        sending_frozen: p.sending_frozen(),
                    })
                })
                .collect::<Result<Vec<_>, Reason>>()?;
            Ok(RecoveryReply::State {
                conversation_id,
                crypto_epoch: view.crypto_epoch.map(|e| e.to_string()),
                receive_only: view.receive_only,
                sending_frozen: view.sending_frozen,
                proposals,
            })
        }
        RecoveryRequest::Propose {
            conversation_id,
            expected_epoch,
            expected_own_proposal_id,
            confirmed: true,
        } => {
            let operation_id = client
                .propose_recovery_at(
                    &conversation_id,
                    epoch(&expected_epoch)?,
                    expected_own_proposal_id.as_deref(),
                )
                .await
                .map_err(super::factory::account_error)?;
            Ok(RecoveryReply::Queued {
                conversation_id,
                crypto_epoch: expected_epoch,
                operation_id,
            })
        }
        RecoveryRequest::Decide {
            conversation_id,
            expected_epoch,
            proposal_id,
            decision,
            confirmed: true,
        } => {
            let decision = match decision {
                Decision::Accept => D::Accept,
                Decision::Decline => D::Decline,
                Decision::Cancel => D::Cancel,
            };
            let operation_id = client
                .decide_recovery_at(
                    &conversation_id,
                    epoch(&expected_epoch)?,
                    &proposal_id,
                    decision,
                )
                .await
                .map_err(super::factory::account_error)?;
            Ok(RecoveryReply::Queued {
                conversation_id,
                crypto_epoch: expected_epoch,
                operation_id,
            })
        }
        _ => Err(Reason::Invalid),
    }
}
#[cfg(test)]
#[path = "recovery_tests.rs"]
mod tests;
