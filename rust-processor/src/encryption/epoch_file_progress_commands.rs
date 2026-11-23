use super::*;

#[derive(Clone)]
pub struct EpochFileStatusCursor {
    store: StoreScope,
    own: DeviceRef,
    after: String,
}
/// Only accepted attachment plans, independently of message ACK/status. A full
/// page may have an empty continuation; it is not global absence evidence.
pub struct EpochFileStatusPage {
    pub files: Vec<EpochOutgoingPlan>,
    pub next: Option<EpochFileStatusCursor>,
}
pub struct FileStatusSeedProgress {
    pub scanned: usize,
    pub ready: bool,
}
pub(in crate::encryption::signal_state) enum FileProgressCommand {
    Seed(Reply<FileStatusSeedProgress>),
    Pending(Option<EpochFileStatusCursor>, Reply<EpochFileStatusPage>),
    Get(String, Reply<EpochFileProgress>),
    Accept(Box<EpochFileStatuses>, Reply<Vec<EpochFileProgress>>),
}
impl Engine {
    pub(in crate::encryption::signal_state) fn handle_file_progress(
        &mut self,
        c: FileProgressCommand,
    ) {
        match c {
            FileProgressCommand::Seed(r) if !r.is_closed() => {
                let _ = r.send(self.seed_file_statuses());
            }
            FileProgressCommand::Pending(c, r) if !r.is_closed() => {
                let _ = r.send(self.pending_file_statuses(c));
            }
            FileProgressCommand::Get(uuid, r) if !r.is_closed() => {
                let _ = r.send(self.file_progress(&uuid));
            }
            FileProgressCommand::Accept(b, r) if !r.is_closed() => {
                let _ = r.send(self.accept_file_statuses(*b));
            }
            _ => (),
        }
    }
    fn seed_file_statuses(&mut self) -> CryptoResult<FileStatusSeedProgress> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            current_owner(tx, own)?;
            let mut s = index(tx, own)?;
            if s.ready == Some(true) {
                return Ok(FileStatusSeedProgress {
                    scanned: 0,
                    ready: true,
                });
            }
            // Explicit migration only: <=32 source outbox rows, including those
            // removed from message pending by descriptor ACK. No history scan on open.
            let rows = tx.record_page(
                &own.generation_id,
                RecordKind::Outbox,
                "v2:",
                (!s.after.is_empty()).then_some(s.after.as_str()),
            )?;
            for (id, bytes) in &rows {
                let p = plan(tx, own, &store, id)?;
                check_projections(tx, own, id, p.needs_status_poll())?;
                if data::decode_envelope(bytes).map_err(|_| CryptoError::Invalid)?
                    != *p.request.envelope.as_ref().unwrap()
                {
                    return Err(CryptoError::Conflict);
                }
                if let Some(receipt) = p.receipt() {
                    track_receipt(tx, own, id, p.request(), receipt, false)?;
                }
            }
            if let Some((last, _)) = rows.last() {
                s.after = last.clone();
            }
            s.ready = Some(rows.len() < contract::MAX_FILE_STATUSES);
            save_index(tx, own, &mut s)?;
            Ok(FileStatusSeedProgress {
                scanned: rows.len(),
                ready: s.ready == Some(true),
            })
        })
    }
    fn pending_file_statuses(
        &mut self,
        c: Option<EpochFileStatusCursor>,
    ) -> CryptoResult<EpochFileStatusPage> {
        let own = &self.own;
        let store = self.store.scope().clone();
        if c.as_ref()
            .is_some_and(|c| c.store != store || c.own != *own)
        {
            return Err(CryptoError::Conflict);
        }
        self.store.transaction_with(|tx| {
            require_ready(tx, own)?;
            // Protected small pending rows use the existing PK, not a scan over
            // completed files or retained history. Reload full cohort before RPC.
            let rows = tx.record_page_limited(
                &own.generation_id,
                RecordKind::Control,
                PENDING,
                c.as_ref().map(|c| c.after.as_str()),
                false,
                (contract::MAX_FILE_STATUSES, 128),
            )?;
            let next = (rows.len() == contract::MAX_FILE_STATUSES).then(|| EpochFileStatusCursor {
                store: store.clone(),
                own: own.clone(),
                after: rows.last().unwrap().0.clone(),
            });
            let mut files = Vec::with_capacity(rows.len());
            for (k, _) in rows {
                let id = k.strip_prefix(PENDING).ok_or(CryptoError::Invalid)?;
                let p = plan(tx, own, &store, id)?;
                check_projections(tx, own, id, p.needs_status_poll())?;
                let j = load(
                    tx,
                    own,
                    id,
                    p.request(),
                    p.receipt().ok_or(CryptoError::Invalid)?,
                )?
                .ok_or(CryptoError::Invalid)?;
                if !EpochFileProgress(j).needs_poll() || p.sealed.is_some() {
                    return Err(CryptoError::Conflict);
                }
                files.push(p);
            }
            Ok(EpochFileStatusPage { files, next })
        })
    }
    fn file_progress(&mut self, uuid: &str) -> CryptoResult<EpochFileProgress> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            require_ready(tx, own)?;
            let link = read_link(tx, own, uuid)?;
            let p = plan(tx, own, &store, &link.id)?;
            check_projections(tx, own, &link.id, p.needs_status_poll())?;
            Ok(EpochFileProgress(
                load(
                    tx,
                    own,
                    &link.id,
                    p.request(),
                    p.receipt().ok_or(CryptoError::MessageUnavailable)?,
                )?
                .ok_or(CryptoError::MessageUnavailable)?,
            ))
        })
    }
    fn accept_file_statuses(
        &mut self,
        b: EpochFileStatuses,
    ) -> CryptoResult<Vec<EpochFileProgress>> {
        let plans = b.request();
        if b.store_scope() != self.store.scope() {
            return Err(CryptoError::Conflict);
        }
        let mut refs = Vec::with_capacity(plans.len());
        for p in plans {
            self.check_epoch_owner(&p.scope)?;
            refs.push(reference(p.request())?);
        }
        contract::validate_response(&request(&self.own, refs), b.response())
            .map_err(|_| CryptoError::Invalid)?;
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            require_ready(tx, own)?;
            let mut results = Vec::with_capacity(plans.len());
            for (p, fact) in plans.iter().zip(&b.response().files) {
                let id = record_id(&p.scope, &p.request.envelope.as_ref().unwrap().message_id)?;
                let saved = plan(tx, own, &store, &id)?;
                check_projections(tx, own, &id, saved.needs_status_poll())?;
                if saved.request != p.request
                    || saved.sealed.is_some()
                    || saved.receipt.is_none()
                    || saved.receipt != p.receipt
                    || saved.receipt != fact.receipt
                {
                    return Err(CryptoError::Conflict);
                }
                let mut j = load(tx, own, &id, saved.request(), saved.receipt().unwrap())?
                    .ok_or(CryptoError::Invalid)?;
                let previous = j.clone();
                for (old, new) in [
                    (&mut j.acknowledged_at, fact.acknowledged_at_unix_ms),
                    (&mut j.replaced_at, fact.recipient_replaced_at_unix_ms),
                ] {
                    if let Some(new) = new {
                        if old.is_some_and(|old| old != new) {
                            return Err(CryptoError::Conflict);
                        }
                        *old = Some(new);
                    }
                }
                j.observed_at = Some(j.observed_at.unwrap_or(0).max(fact.observed_at_unix_ms));
                if j != previous {
                    save(tx, own, &id, &mut j)?;
                }
                results.push(EpochFileProgress(j));
            }
            Ok(results)
        })
    }
}
impl CryptoCoordinator {
    pub async fn seed_epoch_file_status_index(&self) -> CryptoResult<FileStatusSeedProgress> {
        self.request(|r| {
            Command::Epoch(EpochCommand::Outgoing(OutgoingCommand::FileProgress(
                FileProgressCommand::Seed(r),
            )))
        })
        .await
    }
    pub async fn pending_epoch_file_statuses(
        &self,
        cursor: Option<EpochFileStatusCursor>,
    ) -> CryptoResult<EpochFileStatusPage> {
        self.request(|r| {
            Command::Epoch(EpochCommand::Outgoing(OutgoingCommand::FileProgress(
                FileProgressCommand::Pending(cursor, r),
            )))
        })
        .await
    }
    pub async fn epoch_file_progress(&self, uuid: String) -> CryptoResult<EpochFileProgress> {
        if !epoch_rpc::uuid4(&uuid) {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| {
            Command::Epoch(EpochCommand::Outgoing(OutgoingCommand::FileProgress(
                FileProgressCommand::Get(uuid, r),
            )))
        })
        .await
    }
    pub async fn accept_epoch_file_statuses(
        &self,
        b: EpochFileStatuses,
    ) -> CryptoResult<Vec<EpochFileProgress>> {
        if b.request().is_empty()
            || b.request().len() > contract::MAX_FILE_STATUSES
            || b.response().encoded_len() > crate::encryption::contract::MAX_RESPONSE_BYTES
        {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| {
            Command::Epoch(EpochCommand::Outgoing(OutgoingCommand::FileProgress(
                FileProgressCommand::Accept(Box::new(b), r),
            )))
        })
        .await
    }
}
