// Shared by the k6 receiver and deterministic tests of delayed callbacks.
export function expireDeliveries(state, now, deadline = 5000) {
  let missed = 0;
  for (const [key, start] of state.pending) {
    if (now - start > deadline) { state.pending.delete(key); missed++; }
  }
  return missed;
}

// Caller filters to messages issued by this session before calling this helper.
export function receiveDelivery(state, key, now, deadline = 5000) {
  if (state.seen.has(key)) return { kind: 'duplicate' };
  state.seen.add(key);
  if (state.seen.size > 1000) state.seen.delete(state.seen.values().next().value);
  const start = state.pending.get(key);
  // The expiration timer already counted this missing delivery.
  if (start === undefined) return { kind: 'late', newlyMissed: false };
  state.pending.delete(key);
  const latency = now - start;
  return latency > deadline
    ? { kind: 'late', newlyMissed: true, latency }
    : { kind: 'received', latency };
}
