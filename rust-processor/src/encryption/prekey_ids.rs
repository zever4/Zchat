//! Durable, monotonic prekey ID reservations. No key generation or RPC here.
//! EC, signed EC and PQ (including last-resort) have independent namespaces.
use super::*;

const LIMIT: u64 = u32::MAX as u64 + 1;
const STATE: &str = "prekey-ids:state:v1";
const ANCHOR: &str = "prekey-ids:anchor:v1";
const RESERVATIONS: &str = "prekey-ids:reservation:";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreKeyIdNamespace {
    Ec,
    Pq,
    Signed,
}
impl PreKeyIdNamespace {
    fn index(self) -> usize {
        match self {
            Self::Ec => 0,
            Self::Pq => 1,
            Self::Signed => 2,
        }
    }
    fn kind(self) -> RecordKind {
        match self {
            Self::Ec => RecordKind::EcPreKey,
            Self::Pq => RecordKind::PqPreKey,
            Self::Signed => RecordKind::SignedPreKey,
        }
    }
    fn from_kind(kind: RecordKind) -> CryptoResult<Self> {
        match kind {
            RecordKind::EcPreKey => Ok(Self::Ec),
            RecordKind::PqPreKey => Ok(Self::Pq),
            RecordKind::SignedPreKey => Ok(Self::Signed),
            _ => Err(CryptoError::Invalid),
        }
    }
}
const NAMESPACES: [PreKeyIdNamespace; 3] = [
    PreKeyIdNamespace::Ec,
    PreKeyIdNamespace::Pq,
    PreKeyIdNamespace::Signed,
];

/// Public IDs only; never a capability to install arbitrary private material.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreKeyIdReservation {
    pub operation_id: String,
    pub namespace: PreKeyIdNamespace,
    pub first: u32,
    pub count: u32,
}
pub struct PreKeyIdSeedPage {
    pub scanned: usize,
    pub ready: bool,
}

#[derive(Message)]
struct State {
    #[prost(uint32, tag = "1")]
    format: u32,
    // u64 represents the exhausted sentinel 2^32 without wraparound.
    #[prost(uint64, repeated, tag = "2")]
    next: Vec<u64>,
    #[prost(uint32, tag = "3")]
    phase: u32,
    #[prost(string, tag = "4")]
    after: String,
}
fn key(generation: &str, id: &str) -> Result<RecordKey, StoreError> {
    RecordKey::new(generation, RecordKind::Control, id)
}
fn numeric(id: &str) -> CryptoResult<u32> {
    let value: u32 = id.parse().map_err(|_| CryptoError::Invalid)?;
    if value.to_string() != id {
        return Err(CryptoError::Invalid);
    }
    Ok(value)
}
fn load(tx: &mut ProtectedTransaction<'_>, generation: &str) -> CryptoResult<Option<State>> {
    let state = tx.get(&key(generation, STATE)?)?;
    let anchor = tx.get(&key(generation, ANCHOR)?)?;
    match (state, anchor) {
        (None, None) => Ok(None),
        (Some(bytes), Some(anchor)) if anchor.as_slice() == [1] => {
            let state = State::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
            if state.format != 1
                || state.next.len() != 3
                || state.next.iter().any(|n| *n > LIMIT)
                || state.phase > 3
                || (state.phase == 3 && !state.after.is_empty())
            {
                return Err(CryptoError::Invalid);
            }
            if !state.after.is_empty() {
                let id = numeric(&state.after)?;
                if u64::from(id) >= state.next[state.phase as usize] {
                    return Err(CryptoError::Invalid);
                }
            }
            Ok(Some(state))
        }
        _ => Err(CryptoError::Invalid), // Never silently repair/reset a lost counter.
    }
}
fn save(tx: &mut ProtectedTransaction<'_>, generation: &str, state: &State) -> CryptoResult<()> {
    tx.put(&key(generation, STATE)?, &state.encode_to_vec())?;
    Ok(())
}

// Called only for a genuinely new private record, in its installation TX.
// Interleaving an install with paged seeding cannot hide a newly inserted ID
// behind the persisted lexicographic cursor. Existing exact saves don't advance.
pub(super) fn observe_new(
    tx: &mut ProtectedTransaction<'_>,
    generation: &str,
    kind: RecordKind,
    id: u32,
) -> CryptoResult<()> {
    if let Some(mut state) = load(tx, generation)? {
        let index = PreKeyIdNamespace::from_kind(kind)?.index();
        if u64::from(id) < state.next[index] {
            return Err(CryptoError::Conflict);
        }
        state.next[index] = u64::from(id) + 1;
        save(tx, generation, &state)?;
    }
    Ok(())
}

impl Engine {
    pub(super) fn seed_prekey_ids(&mut self) -> CryptoResult<PreKeyIdSeedPage> {
        let generation = &self.own.generation_id;
        self.store.transaction_with(|tx| {
            let mut state = match load(tx, generation)? {
                Some(state) => state,
                None => {
                    // Only an installed generation can initialize. A surviving
                    // journal without counters is damaged state, not a fresh DB.
                    let identity = tx
                        .get(&RecordKey::new(generation, RecordKind::Identity, "local")?)?
                        .ok_or(CryptoError::Invalid)?;
                    if identity.len() < 6 || identity[0] != 1 {
                        return Err(CryptoError::Invalid);
                    }
                    IdentityKeyPair::try_from(&identity[5..])?;
                    let credential = tx
                        .get(&RecordKey::new(
                            generation,
                            RecordKind::DeviceCredential,
                            "local",
                        )?)?
                        .ok_or(CryptoError::Invalid)?;
                    if credential.len() != 32
                        || !tx
                            .record_page(generation, RecordKind::Control, RESERVATIONS, None)?
                            .is_empty()
                    {
                        return Err(CryptoError::Invalid);
                    }
                    tx.put(&key(generation, ANCHOR)?, &[1])?;
                    State {
                        format: 1,
                        next: vec![0; 3],
                        phase: 0,
                        after: String::new(),
                    }
                }
            };
            if state.phase == 3 {
                return Ok(PreKeyIdSeedPage {
                    scanned: 0,
                    ready: true,
                });
            }
            let namespace = NAMESPACES[state.phase as usize];
            let rows = tx.record_page(
                generation,
                namespace.kind(),
                "",
                (!state.after.is_empty()).then_some(state.after.as_str()),
            )?;
            for (id, bytes) in &rows {
                let id = numeric(id)?;
                prekey_state::decode(namespace.kind(), id, bytes)?;
                // SQL order is textual ("10" < "9"); max must be numeric.
                state.next[namespace.index()] =
                    state.next[namespace.index()].max(u64::from(id) + 1);
            }
            if rows.len() == 32 {
                state.after = rows.last().expect("nonempty page").0.clone();
            } else {
                state.phase += 1;
                state.after.clear();
            }
            save(tx, generation, &state)?;
            Ok(PreKeyIdSeedPage {
                scanned: rows.len(),
                ready: state.phase == 3,
            })
        })
    }

    pub(super) fn reserve_prekey_ids(
        &mut self,
        operation: &str,
        namespace: PreKeyIdNamespace,
        count: u32,
    ) -> CryptoResult<PreKeyIdReservation> {
        let generation = &self.own.generation_id;
        self.store
            .transaction_with(|tx| reserve_in_tx(tx, generation, operation, namespace, count))
    }
}

// The refill parent journal and child reservations share one COMMIT. Private to
// the owner modules; not an additional store/renderer API.
pub(super) fn reserve_in_tx(
    tx: &mut ProtectedTransaction<'_>,
    generation: &str,
    operation: &str,
    namespace: PreKeyIdNamespace,
    count: u32,
) -> CryptoResult<PreKeyIdReservation> {
    RecordKey::new(operation, RecordKind::Control, "operation")?;
    if !(1..=100).contains(&count) {
        return Err(CryptoError::Invalid);
    }
    let mut state = load(tx, generation)?.ok_or(CryptoError::PreKeyIdsNotReady)?;
    if state.phase != 3 {
        return Err(CryptoError::PreKeyIdsNotReady);
    }
    let journal = key(generation, &format!("{RESERVATIONS}{operation}"))?;
    let index = namespace.index();
    if let Some(saved) = reservation(tx, &journal, operation, namespace, count, state.next[index])?
    {
        return Ok(saved);
    }
    let end = state.next[index] + u64::from(count);
    if end > LIMIT {
        return Err(CryptoError::PreKeyIdsExhausted);
    }
    let first: u32 = state.next[index]
        .try_into()
        .map_err(|_| CryptoError::PreKeyIdsExhausted)?;
    state.next[index] = end;
    let bytes = [
        &[1, index as u8],
        first.to_be_bytes().as_slice(),
        count.to_be_bytes().as_slice(),
    ]
    .concat();
    tx.put(&journal, &bytes)?;
    save(tx, generation, &state)?;
    Ok(PreKeyIdReservation {
        operation_id: operation.into(),
        namespace,
        first,
        count,
    })
}

fn reservation(
    tx: &mut ProtectedTransaction<'_>,
    journal: &RecordKey,
    operation: &str,
    namespace: PreKeyIdNamespace,
    count: u32,
    next: u64,
) -> CryptoResult<Option<PreKeyIdReservation>> {
    let Some(bytes) = tx.get(journal)? else {
        return Ok(None);
    };
    let index = namespace.index();
    // Fixed frame: version, namespace, first(u32 BE), count(u32 BE).
    if bytes.len() != 10 || bytes[0] != 1 || bytes[1] > 2 {
        return Err(CryptoError::Invalid);
    }
    let first = u32::from_be_bytes(bytes[2..6].try_into().map_err(|_| CryptoError::Invalid)?);
    let saved_count =
        u32::from_be_bytes(bytes[6..10].try_into().map_err(|_| CryptoError::Invalid)?);
    if bytes[1] != index as u8 || saved_count != count {
        return Err(CryptoError::Conflict);
    }
    let end = u64::from(first) + u64::from(count);
    if end > LIMIT || end > next {
        return Err(CryptoError::Invalid);
    }
    Ok(Some(PreKeyIdReservation {
        operation_id: operation.into(),
        namespace,
        first,
        count,
    }))
}

pub(super) fn reserved_in_tx(
    tx: &mut ProtectedTransaction<'_>,
    generation: &str,
    operation: &str,
    namespace: PreKeyIdNamespace,
    count: u32,
) -> CryptoResult<Option<PreKeyIdReservation>> {
    RecordKey::new(operation, RecordKind::Control, "operation")?;
    if !(1..=100).contains(&count) {
        return Err(CryptoError::Invalid);
    }
    let state = load(tx, generation)?.ok_or(CryptoError::PreKeyIdsNotReady)?;
    if state.phase != 3 {
        return Err(CryptoError::PreKeyIdsNotReady);
    }
    reservation(
        tx,
        &key(generation, &format!("{RESERVATIONS}{operation}"))?,
        operation,
        namespace,
        count,
        state.next[namespace.index()],
    )
}
