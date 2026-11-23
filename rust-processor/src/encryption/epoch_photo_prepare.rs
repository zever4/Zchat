//! One-shot, account-instance-bound input admission. Contains no DB lease or
//! private key: a codec may finish after shutdown without holding the store open.
use super::*;
#[cfg(test)]
use crate::encryption::photo_preparation::PreparedPhotoAttachment;

pub(in crate::encryption) struct EpochAttachmentInput {
    instance: uuid::Uuid,
    scope: EpochScope,
    message: String,
    permit: OwnedSemaphorePermit,
}

impl Engine {
    pub(super) fn new_attachment_input(
        &mut self,
        chat: &str,
        message: &str,
        expected_epoch: Option<u64>,
    ) -> CryptoResult<Option<EpochScope>> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            // Check retained metadata, including promoted/removed files and
            // text outbox entries, BEFORE any native image codec is started.
            if load_retained_journal(tx, own, message)?.is_some()
                || tx.get(&outgoing::link_key(own, message)?)?.is_some()
            {
                return Err(CryptoError::Conflict);
            }
            tx.require_empty_attachment_slot(&key(own, message)?)?;
            let scope = current(tx, own, &store, chat)?;
            if scope
                .as_ref()
                .is_some_and(|s| expected_epoch.is_some_and(|epoch| s.epoch != epoch))
            {
                return Err(CryptoError::Conflict);
            }
            Ok(scope)
        })
    }
}

impl CryptoCoordinator {
    pub(in crate::encryption) async fn new_epoch_attachment_input(
        &self,
        chat: String,
        message: String,
        expected_epoch: Option<u64>,
    ) -> CryptoResult<Option<EpochAttachmentInput>> {
        if !epoch_rpc::uuid4(&chat) || !epoch_rpc::uuid4(&message) {
            return Err(CryptoError::Invalid);
        }
        let permit = self.attachment_permit()?;
        let scope = self
            .request(|reply| {
                Command::Epoch(EpochCommand::Attachment(AttachmentCommand::NewInput(
                    chat,
                    message.clone(),
                    expected_epoch,
                    reply,
                )))
            })
            .await?;
        Ok(scope.map(|scope| EpochAttachmentInput {
            instance: self.view_instance,
            scope,
            message,
            permit,
        }))
    }

    pub(in crate::encryption) async fn finish_epoch_attachment_input(
        &self,
        input: EpochAttachmentInput,
        draft: AttachmentDraft,
        plaintext: Zeroizing<Vec<u8>>,
    ) -> CryptoResult<EpochAttachmentPreparation> {
        if input.instance != self.view_instance {
            return Err(CryptoError::Conflict);
        }
        let input = Input {
            chat: input.scope.conversation.clone(),
            message: input.message,
            expected: Some(input.scope),
            draft,
            plaintext,
            _permit: input.permit,
        };
        // The same file-memory permit crosses preflight, codec and atomic
        // preparation. Do not acquire a second slot while holding these bytes.
        self.request(|reply| {
            Command::Epoch(EpochCommand::Attachment(AttachmentCommand::Prepare(
                Box::new(input),
                reply,
            )))
        })
        .await
    }

    #[cfg(test)]
    pub(in crate::encryption) async fn new_epoch_photo_input(
        &self,
        chat: String,
        message: String,
    ) -> CryptoResult<Option<EpochAttachmentInput>> {
        self.new_epoch_attachment_input(chat, message, None).await
    }

    #[cfg(test)]
    pub(in crate::encryption) async fn finish_epoch_photo_input(
        &self,
        input: EpochAttachmentInput,
        prepared: PreparedPhotoAttachment,
    ) -> CryptoResult<EpochAttachmentPreparation> {
        let (draft, plaintext) = prepared.into_input();
        self.finish_epoch_attachment_input(input, draft, plaintext)
            .await
    }
}
