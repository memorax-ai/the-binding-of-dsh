import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { mkdtempSync, mkdirSync, symlinkSync, writeFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import WebSocket from 'ws'
import { NodePeerClient } from '../lib/node-peer-client.js'
import { BrowserPeerClient } from '../lib/browser-peer-client.js'

const root = fileURLToPath(new URL('../', import.meta.url))
const upstream = resolve(process.argv[2])
const harmony = resolve(process.argv[3] ?? '../dsh-harmony/lib/bin.js')
const home = mkdtempSync(join(tmpdir(), 'binding-upstream-'))
const profile = join(home, 'profiles/web')
const modules = join(profile, 'node_modules')
mkdirSync(modules, { recursive: true })
symlinkSync(root, join(modules, 'the-binding-of-dsh'), process.platform === 'win32' ? 'junction' : 'dir')
const fixture = join(modules, 'binding-probe')
mkdirSync(fixture)
writeFileSync(join(fixture, 'package.json'), JSON.stringify({ name: 'binding-probe', version: '1.0.0', type: 'module', main: 'index.js', dsh: { bundle: { patch: './patch.yml' } } }))
writeFileSync(join(fixture, 'patch.yml'), '- insert:\n    - id: binding-probe\n      name: binding-probe\n      inject: [connection]\n')
writeFileSync(join(fixture, 'index.js'), `import { scopeTarget } from ${JSON.stringify(pathToFileURL(join(upstream, 'node_modules/@deepseek-ai/dsh-scope/lib/index.js')).href)};
export function apply(ctx) {
  let pending;
  for (const action of ['echo', 'reverse', 'peers', 'event', 'waterfall', 'cancel']) ctx.connection.fetch.register({
    path: '/api/binding-test/' + action, methods: ['POST'], requestBody: 'buffered',
    async fetch(request) {
      const input = await request.json();
      if (action === 'event') { await ctx.parallel('api-session/status', input); return Response.json(true); }
      if (action === 'cancel') { pending?.abort(new Error('probe cancelled')); return Response.json(true); }
      if (action === 'waterfall') {
        const agent = { id: 'binding-probe-agent', ctx };
        pending = new AbortController();
        try { return Response.json({ value: await ctx.waterfall(scopeTarget(agent, agent), 'approval/request', { agent, signal: pending.signal, mode: input.mode }, () => 'host-fallback') }); }
        catch (error) { return Response.json({ error: error.message }); }
      }
      if (action === 'peers') return Response.json(ctx.connection.peers.list().length);
      if (action === 'reverse') return Response.json(await ctx.connection.peers.list()[0].call('/api', 'reverse/echo', input));
      return Response.json({type:'server-response', rpcId:input.rpcId, result:{ok:true,value:'host:' + input.payload.args.value}});
    }
  });
}`)
writeFileSync(join(profile, 'package.json'), JSON.stringify({ name: 'binding-test-profile', private: true, dependencies: { 'the-binding-of-dsh': '*', 'binding-probe': '*' }, dsh: { profile: { bundles: ['@deepseek-ai/dsh-base', '@deepseek-ai/dsh-web-app', 'the-binding-of-dsh', 'binding-probe'] } } }))
const child = spawn(process.execPath, [harmony, 'web', '--port', '0', '--no-open'], { env: { ...process.env, DSH_HOME: home, DSH_HARMONY_DSH_ENTRY: join(upstream, 'node_modules/@deepseek-ai/dsh/lib/bin.js') }, stdio: ['ignore', 'pipe', 'pipe'] })
const exited = once(child, 'exit')
let output = ''
let client
let browser
const timeout = setTimeout(() => child.kill(), 45000)
try {
  const url = await new Promise((resolve, reject) => {
    const read = chunk => { output += chunk; const match = output.match(/dsh web: (http:\/\/127\.0\.0\.1:\d+\/\?token=[^\s]+)/); if (match) resolve(match[1]) }
    child.stdout.on('data', read); child.stderr.on('data', read)
    child.once('exit', () => reject(new Error('Host exited before startup: ' + output.replace(/token=[^\s]+/g, 'token=REDACTED'))))
  })
  const origin = new URL(url).origin
  const login = await fetch(url, { redirect: 'manual' })
  const cookie = login.headers.getSetCookie().map(value => value.split(';')[0]).join('; ')
  assert(cookie, 'launch-token exchange must issue an authenticated cookie')
  const authenticatedFetch = (url, init = {}) => fetch(url, { ...init, headers: { ...Object.fromEntries(new Headers(init.headers)), cookie } })
  assert.equal((await fetch(origin + '/api/connection.open', { method: 'POST' })).status, 401)
  await new Promise((resolve, reject) => {
    const socket = new WebSocket(origin.replace('http:', 'ws:') + '/api/events.host', 'invalid')
    socket.on('unexpected-response', (_, response) => { assert.equal(response.statusCode, 401); response.resume(); socket.terminate(); resolve() })
    socket.on('open', () => { socket.close(); reject(new Error('Unauthenticated WebSocket accepted')) })
    socket.on('error', () => {})
  })
  assert.equal((await authenticatedFetch(origin + '/api/connection.open', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ nativeEvents: true }) })).status, 409)
  let materializations = 0
  const codec = { schema: { parse() { throw new Error('lazy factory must take precedence') } }, mode: 'strict', typeSymbol: 'string', create: () => { materializations++; return { parse: value => { assert.equal(typeof value, 'string'); return value } } } }
  client = new NodePeerClient({ baseUrl: origin, fetch: authenticatedFetch, createWebSocket: (url, protocol) => new WebSocket(url, protocol, { headers: { cookie } }), contribution: { package: 'binding-upstream-probe', descriptors: [{ id: 'probe#echo', service: 'probe', namespace: 'binding-test', method: 'echo', invocation: { kind: 'direct' }, parameters: [{ name: 'value', wire: 'value', source: 'json', codec }], result: codec }] } })
  client.intercept('/api', endpoint => endpoint === 'reverse/echo', async (_, payload) => ({ ok: true, value: 'client:' + payload.value }))
  await client.connect()
  assert.deepEqual(await client.remote['binding-test'].echo('hello'), { ok: true, value: 'host:hello' })
  assert.equal(materializations, 1)
  const post = async (action, value) => (await authenticatedFetch(origin + '/api/binding-test/' + action, { method: 'POST', body: JSON.stringify(value) })).json()
  assert.deepEqual(await post('reverse', { value: 'hello' }), { ok: true, value: 'client:hello' })
  assert.equal(await post('peers', {}), 1)
  await client.close()
  // Wait for the remote close notification, bounded by the test timeout.
  for (let i = 0; i < 30 && await post('peers', {}) !== 0; i++) await new Promise(resolve => setTimeout(resolve, 20))
  assert.equal(await post('peers', {}), 0)
  browser = new BrowserPeerClient({ baseUrl: origin, fetch: authenticatedFetch, createWebSocket: (url, protocol) => new WebSocket(url, protocol, { headers: { cookie } }), contribution: { package: 'browser-probe', descriptors: [] } })
  const notifications = [], requests = []
  browser.subscribe('api-session/status', (...args) => notifications.push(args))
  let lateResult
  browser.handleEvent('approval/request', request => {
    requests.push(request)
    if (request.request.mode === 'next') return undefined
    if (request.request.mode === 'reject') throw new Error('browser rejection')
    if (request.request.mode === 'cancel') return new Promise(resolve => { lateResult = resolve })
    return 'allowed-once'
  })
  const until = async predicate => { for (let i = 0; i < 100 && !predicate(); i++) await new Promise(resolve => setTimeout(resolve, 20)); assert.ok(predicate()) }
  await browser.connect()
  assert.equal(browser.eventTransport, 'gateway-v1')
  await post('event', { sessionId: 'probe', running: true })
  await until(() => notifications.length === 1)
  assert.deepEqual(notifications[0], [{ sessionId: 'probe', running: true }])
  assert.deepEqual(await post('waterfall', { mode: 'result' }), { value: 'allowed-once' })
  assert.deepEqual(await post('waterfall', { mode: 'next' }), { value: 'host-fallback' })
  assert.match((await post('waterfall', { mode: 'reject' })).error, /browser rejection/)
  const cancelled = post('waterfall', { mode: 'cancel' })
  await until(() => lateResult !== undefined)
  await post('cancel', {})
  await until(() => requests.at(-1).signal.aborted)
  assert.ok((await cancelled).error)
  lateResult('allowed-once')
  await browser.close()
  await browser.connect()
  await post('event', { sessionId: 'reconnected' })
  await until(() => notifications.length === 2)
  assert.deepEqual(notifications[1], [{ sessionId: 'reconnected' }])
  await browser.close()
  console.log('PASS: authenticated modern host; forward/reverse RPC; lazy codecs; Gateway notification/result/next/rejection/cancel/reconnect; unauthenticated HTTP/WS rejected; disconnect removes peer')
} finally {
  clearTimeout(timeout)
  await client?.close()
  await browser?.close()
  child.kill()
  await exited
  rmSync(home, { recursive: true, force: true })
}
