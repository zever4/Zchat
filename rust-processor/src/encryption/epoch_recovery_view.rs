//! Bounded current-epoch presentation snapshot. It never creates a missing root,
//! signs consent, advances an epoch or exposes signatures/private state.
use super::*;

pub struct EpochRecoveryOverview {
    pub crypto_epoch: Option<u64>,
    pub receive_only: bool,
    pub sending_frozen: bool,
    // At most the current own and peer references, not retained journal history.
    pub proposals: Vec<EpochRecoverySnapshot>,
}
impl Engine {
    pub(super) fn epoch_recovery_overview(
        &mut self,
        chat: &str,
    ) -> CryptoResult<EpochRecoveryOverview> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            // require_ready can seed a genuinely empty index for explicit
            // mutations. A presentation read must never take that path.
            if !recovery_indexes::ready(tx, own)? {
                return Err(CryptoError::RecoveryIndexesNotReady);
            }
            recovery_indexes::require_ready(tx, own)?;
            let c = conversation_lifecycle::load(tx, own, chat)?
                .ok_or(CryptoError::ConversationUnavailable)?;
            let (public, registration) = identity(tx, own)?;
            let Some(l) = load(tx, own, &c, public, registration)? else {
                return Ok(EpochRecoveryOverview {
                    crypto_epoch: None,
                    receive_only: c.state == common::ConversationState::Closed as i32,
                    sending_frozen: false,
                    proposals: Vec::new(),
                });
            };
            let s = scope(&l, &c, store)?;
            let pins = pins(&c, public)?;
            let (head, _) = read_head(tx, own, &l.root, &pins)?;
            let proposals = [&head.own, &head.peer]
                .into_iter()
                .flatten()
                .map(|r| {
                    let j = load_journal(tx, own, &pins, &r.id)?.ok_or(CryptoError::Invalid)?;
                    bound(&l.root, &j)?;
                    snapshot(&j, &l.root, None)
                })
                .collect::<CryptoResult<Vec<_>>>()?;
            Ok(EpochRecoveryOverview {
                crypto_epoch: Some(s.epoch),
                receive_only: s.receive_only,
                sending_frozen: s.send_frozen,
                proposals,
            })
        })
    }
}
