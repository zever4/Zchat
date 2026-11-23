//! A native-owned offer is tied to the current operation context. Rendering its
//! public generation does not let JS choose a different CAS or journal request.
use super::{Reason, ReplacementView};
use rust_processor::encryption::provisioning::{DeviceProvisioning, Reconciliation};
use zeroize::Zeroizing;

#[derive(Clone, PartialEq, Eq)]
pub(super) struct ReplacementTarget {
    pub expected: String,
    pub pending: Option<String>,
    pub continuing: bool,
}
pub(super) struct ReplacementInput {
    pub target: ReplacementTarget,
    pub password: Zeroizing<String>,
}
impl ReplacementTarget {
    pub fn view(&self) -> ReplacementView {
        ReplacementView {
            expected_generation: self.expected.clone(),
            continuing: self.continuing,
            retargeting: !self.continuing && self.pending.is_some(),
        }
    }
}
pub(super) fn offer(
    observed: &Reconciliation,
    local: &mut DeviceProvisioning,
    state: &rust_processor::proto::e2ee::OwnDeviceState,
) -> Result<Option<ReplacementTarget>, Reason> {
    let active = match observed {
        Reconciliation::ReplacementRequired { active_device } => Some(active_device),
        Reconciliation::DeviceUnavailable { active_device, .. } => active_device.as_ref(),
        Reconciliation::ReplacementPending {
            request_id,
            expected_generation_id,
            active_device,
        } => {
            let Some(active) = active_device else {
                return Ok(None);
            };
            let attempt = local.replacement_attempt().map_err(|_| Reason::Storage)?;
            if attempt.request().registration_request_id != *request_id
                || attempt.expected_generation_id() != expected_generation_id
            {
                return Err(Reason::Storage);
            }
            // A lost Replace reply may already have installed our candidate.
            // Its public identity/registration are authenticated by GetOwn.
            let candidate_active = state.active_device.as_ref().is_some_and(|d| {
                d.reference.as_ref() == Some(active)
                    && d.identity_key == attempt.request().identity_key
                    && d.registration_id == attempt.request().registration_id
            });
            let continuing = active.generation_id == *expected_generation_id || candidate_active;
            return Ok(Some(ReplacementTarget {
                expected: if continuing {
                    expected_generation_id.clone()
                } else {
                    active.generation_id.clone()
                },
                pending: Some(request_id.clone()),
                continuing,
            }));
        }
        _ => return Ok(None),
    };
    Ok(active.map(|active| ReplacementTarget {
        expected: active.generation_id.clone(),
        pending: None,
        continuing: false,
    }))
}
