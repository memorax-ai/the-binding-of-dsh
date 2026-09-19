import assert from 'node:assert/strict'
import test from 'node:test'
import { BrowserPeerClient } from '../lib/browser-peer-client.js'

const tick = () => new Promise(resolve => setTimeout(resolve, 10))
const until = async predicate => { for (let i = 0; i < 100 && !predicate(); i++) await tick(); assert.ok(predicate()) }

function fixture(options = {}) {
  const sockets = [], replies = [], states = []
  let serial = 0
  class Socket extends EventTarget {
    readyState = 0
    bufferedAmount = 0
    sent = []
    constructor(url) {
      super(); this.url = url; sockets.push(this)
      queueMicrotask(() => { if (this.readyState === 3) return; this.readyState = 1; this.dispatchEvent(new Event('open')) })
    }
    send(data) {
      const frame = JSON.parse(data); this.sent.push(frame)
      if (frame.type === 'open' && frame.endpoint === '$events') {
        this.streamId = frame.streamId
        if (!options.noReady) queueMicrotask(() => this.event({ type: 'ready', clientId: 'client-' + serial, host: { home: '/test' } }))
      }
      if (frame.method === 'connection.rpc') {
        replies.push(frame.payload)
        queueMicrotask(() => this.message({ type: 'server-response', rpcId: frame.rpcId, result: { ok: true, value: null } }))
      }
    }
    message(value) { this.dispatchEvent(new MessageEvent('message', { data: JSON.stringify(value) })) }
    event(value) { this.message({ type: 'item', streamId: this.streamId, value }) }
    close() { if (this.readyState === 3) return; this.readyState = 3; this.dispatchEvent(new Event('close')) }
  }
  const client = new BrowserPeerClient({
    baseUrl: 'http://localhost:3000', contribution: { package: 'event-test', descriptors: [] },
    reconnectDelayMs: options.reconnectDelayMs,
    fetch: async (_, init) => {
      assert.equal(JSON.parse(init.body).eventTransport, 'gateway-v1')
      return Response.json({ id: 'peer-' + ++serial, eventTransport: 'gateway-v1' })
    },
    createWebSocket: url => new Socket(url),
  })
  client.onState(value => states.push(value))
  return { client, sockets, replies, states, mux: () => sockets.findLast(socket => socket.url.endsWith('/api/remote.mux')) }
}

test('Gateway notifications, waterfall result/next/rejection and multiple independent streams', async t => {
  const f = fixture(); t.after(() => f.client.close())
  const received = []
  f.client.subscribe('api-session/status', (...args) => { received.push(args) })
  f.client.handleEvent('approval/request', async ({ request, agentId }) => {
    assert.equal(agentId, 'agent-1'); return { allowed: request.allowed }
  })
  f.client.handleEvent('throws', () => { throw new TypeError('bad input') })
  await f.client.connect()
  assert.equal(f.client.eventTransport, 'gateway-v1')
  f.mux().event({ type: 'emit', event: 'api-session/status', args: ['s1', true] })
  for (const [eventId, event] of [['1', 'approval/request'], ['2', 'unhandled'], ['3', 'throws']]) {
    f.mux().event({ type: 'waterfall', eventId, event, agentId: 'agent-1', request: { allowed: true } })
  }
  await until(() => f.replies.length === 3)
  assert.deepEqual(received, [['s1', true]])
  const results = Object.fromEntries(f.replies.map(reply => [reply.payload.args.eventId, reply.payload.args]))
  assert.equal(results['1'].clientId, 'client-1')
  assert.deepEqual(results['1'].outcome, { kind: 'result', value: { allowed: true } })
  assert.deepEqual(results['2'].outcome, { kind: 'next' })
  assert.deepEqual(results['3'].outcome, { kind: 'rejected', error: { name: 'TypeError', message: 'bad input' } })
  assert.ok(f.replies.every(reply => reply.endpoint === '$events/result'))
  const abort = new AbortController()
  const stream = f.client.stream('session/watch', { args: { id: 's1' } }, abort.signal)[Symbol.asyncIterator]()
  const next = stream.next()
  const opened = f.mux().sent.find(frame => frame.endpoint === 'session/watch')
  f.mux().message({ type: 'item', streamId: opened.streamId, value: { seq: 42 } })
  assert.deepEqual(await next, { done: false, value: { seq: 42 } })
  abort.abort(new Error('stop stream'))
  assert.ok(f.mux().sent.some(frame => frame.type === 'cancel' && frame.streamId === opened.streamId))
  // Cancellation must reach Host even while the consumer is paused at a yield.
  await assert.rejects(stream.next(), /stop stream/)
  assert.equal(f.client.connected, true)
})

test('cancel and disconnect abort requests and suppress late results across reconnect', async t => {
  const f = fixture({ reconnectDelayMs: 10 }); t.after(() => f.client.close())
  const requests = [], releases = []
  f.client.handleEvent('approval/request', request => {
    requests.push(request)
    return new Promise(resolve => releases.push(resolve))
  })
  await f.client.connect()
  const first = f.mux()
  first.event({ type: 'waterfall', event: 'approval/request', eventId: 'cancelled', agentId: 'a', request: {} })
  await until(() => requests.length === 1)
  first.event({ type: 'cancel', eventId: 'cancelled' })
  await until(() => requests[0].signal.aborted)
  releases[0](true)
  first.event({ type: 'waterfall', event: 'approval/request', eventId: 'disconnected', agentId: 'a', request: {} })
  await until(() => requests.length === 2)
  first.close()
  assert.equal(requests[1].signal.aborted, true)
  await until(() => f.client.connected && f.mux() !== first)
  releases[1](true)
  await tick()
  assert.equal(f.replies.length, 0)
  assert.deepEqual(f.states, [false, true, false, true])
  await f.client.close()
  const count = f.sockets.length
  await tick(); await tick()
  assert.equal(f.sockets.length, count)
  assert.ok(f.sockets.every(socket => socket.readyState === 3))
})

test('ready handshake is mandatory and opening can be cancelled', async t => {
  const f = fixture({ noReady: true }); t.after(() => f.client.close())
  const controller = new AbortController()
  const connecting = f.client.connect(controller.signal)
  await until(() => f.mux()?.streamId !== undefined)
  assert.equal(f.client.connected, false)
  controller.abort(new Error('cancel opening'))
  await assert.rejects(connecting, /cancel opening/)
  assert.ok(f.sockets.every(socket => socket.readyState === 3))
})

test('malformed event and terminal event stream failure close the generation', async t => {
  const f = fixture(); t.after(() => f.client.close())
  await f.client.connect()
  f.mux().event({ type: 'waterfall', event: 'bad', eventId: 'x', agentId: 'a', request: { signal: {} } })
  await until(() => !f.client.connected)
  await f.client.connect()
  f.mux().message({ type: 'error', streamId: f.mux().streamId, error: { code: 'gateway/internal', message: 'closed', details: {} } })
  await until(() => !f.client.connected)
})
