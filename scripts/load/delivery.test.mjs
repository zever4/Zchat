import test from 'node:test';
import assert from 'node:assert/strict';
import { expireDeliveries, receiveDelivery } from './delivery.mjs';

const session = () => ({ pending: new Map([['message', 1000]]), seen: new Set() });
test('delivery exactly at the deadline is accepted once', () => {
  const state = session();
  assert.deepEqual(receiveDelivery(state, 'message', 6000), { kind: 'received', latency: 5000 });
  assert.deepEqual(receiveDelivery(state, 'message', 6001), { kind: 'duplicate' });
  assert.equal(expireDeliveries(state, 10000), 0);
});
test('late delivery fails even when its callback runs before the timeout callback', () => {
  const state = session();
  assert.deepEqual(receiveDelivery(state, 'message', 6001), { kind: 'late', newlyMissed: true, latency: 5001 });
  assert.equal(expireDeliveries(state, 10000), 0);
});
test('timeout callback first: late delivery does not count the miss twice', () => {
  const state = session();
  assert.equal(expireDeliveries(state, 6001), 1);
  assert.deepEqual(receiveDelivery(state, 'message', 7000), { kind: 'late', newlyMissed: false });
  assert.deepEqual(receiveDelivery(state, 'message', 8000), { kind: 'duplicate' });
  assert.equal(expireDeliveries(state, 10000), 0);
});
test('expiration leaves newer outstanding messages eligible for delivery', () => {
  const state = session(); state.pending.set('new', 5500);
  assert.equal(expireDeliveries(state, 6500), 1);
  assert.deepEqual(receiveDelivery(state, 'new', 6500), { kind: 'received', latency: 1000 });
});
