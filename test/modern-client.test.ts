import assert from 'node:assert/strict'
import test from 'node:test'
import { installModernClientBinding } from '../lib/shared/modern-client.js'

const flush = () => new Promise(resolve => setImmediate(resolve))

test('modern client ties reverse RPC and cleanup to the native generation without replacing forward RPC', async t => {
  const saved = Object.fromEntries(['WebSocket', 'location', 'fetch'].map(key => [key, Object.getOwnPropertyDescriptor(globalThis, key)]))
  t.after(() => { for (const [key, descriptor] of Object.entries(saved)) descriptor ? Object.defineProperty(globalThis, key, descriptor) : delete globalThis[key] })
  const sockets = []
  class Socket extends EventTarget {
    readyState = 1
    sent = []
    constructor(url, protocol) { super(); this.url = url.href; this.protocol = protocol; sockets.push(this) }
    send(data) { this.sent.push(JSON.parse(data)) }
    close() { if (this.readyState === 3) return; this.readyState = 3; this.dispatchEvent(new Event('close')) }
  }
  Object.defineProperty(globalThis, 'WebSocket', { configurable: true, value: Socket })
  Object.defineProperty(globalThis, 'location', { configurable: true, value: { origin: 'https://dsh.test' } })
  let opens = 0
  globalThis.fetch = async () => Response.json({ id: `peer-${++opens}` })
  let snapshot
  let inspect
  let dispose
  const forward = () => {}
  const connection = { rpc: { call: forward }, generation: { getSnapshot: () => snapshot, subscribe(listener) { inspect = listener; return () => { inspect = undefined } } } }
  installModernClientBinding({ get: () => connection, effect(factory) { dispose = factory() } })
  assert.equal(opens, 0)
  connection.rpc.intercept('/api', endpoint => endpoint === 'echo', async (_, value) => ({ ok: true, value }))
  snapshot = { id: 1 }; inspect(); await flush()
  assert.equal(sockets.length, 2)
  assert.equal(connection.rpc.call, forward)
  assert(sockets.every(socket => socket.url.startsWith('wss://')))
  const host = sockets.find(socket => socket.url.endsWith('.host'))
  host.dispatchEvent(new MessageEvent('message', { data: JSON.stringify({ type: 'server-request', rpcId: 'call-1', method: 'connection.rpc', payload: { channel: '/api', endpoint: 'echo', payload: 'hello' } }) }))
  await flush()
  assert.deepEqual(host.sent, [{ type: 'client-response', rpcId: 'call-1', result: { ok: true, value: 'hello' } }])
  snapshot = { id: 2 }; inspect(); await flush()
  assert.equal(opens, 2)
  assert(sockets.slice(0, 2).every(socket => socket.readyState === 3))
  dispose(); await flush()
  assert(sockets.every(socket => socket.readyState === 3))
  assert.equal(connection.rpc.intercept, undefined)
  assert.equal(inspect, undefined)
})
