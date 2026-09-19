# The Binding of DSH

[![Powered by Harmony](https://memorax-ai.github.io/dsh-harmony/harmony-powered.svg)](https://memorax-ai.github.io/dsh-harmony/)

[English](README.md) | [简体中文](README.zh-CN.md)

A DSH plugin that enables bidirectional RPC between Host and Client through the
native DSH Connection and Typert Gateway.

## Features

- Targeted Host-to-Client calls for connected browser or Node peers.
- Symmetric Typert Gateway calls in both directions.
- Request correlation, cancellation, and connection lifecycle handling.
- Legacy hosts share the native `events.host` WebSocket for RPC and events.
- Modern hosts use Binding sockets for bidirectional RPC and native Gateway streams for events.

Connection owns peer addressing and lifecycle; Typert Gateway owns service
descriptors, codecs, invocation, and errors.

## Installation

```sh
npm install the-binding-of-dsh
```

The package declares its DSH client entrypoint and Harmony patches in
`package.json`, so it can be enabled as a regular DSH plugin. Harmony 0.8.6 or
newer derives the browser module dependency introduced by the Connection patch
and orders the Binding module automatically.

## Development

Requires Node.js 22.22.3 or newer.

```sh
npm install
npm run check
```

## License

MIT

## Browser events across DSH versions

`BrowserPeerClient` negotiates the event transport at connection opening. Legacy
hosts retain `onEvent()` and its `{ channel, envelope: { rpcId, payload } }` wire
shape. The legacy decoder is bundled separately; modern installations do not
need `dsh-host-apiproxy`. Domain payload types can be supplied to `onEvent<T>()`
or `BrowserPeerEvent<T>` using the consumer's legacy DSH types.

On DSH 0.1.5/0.1.6, `eventTransport` is `gateway-v1`. Use `subscribe(event,
listener)` for native notifications and `handleEvent(event, handler)` for
waterfall requests. These preserve upstream event names and arguments;
`onEvent()` does not synthesize legacy frames on modern hosts.

```ts
peer.subscribe('api-session/status', (...args) => refreshSessionStatus(args))
peer.handleEvent('approval/request', async ({ agentId, request, signal }) => {
  return await showApproval(agentId, request, signal)
})
await peer.connect()
```

Handlers receive `eventId`, `agentId`, the JSON `request`, and an `AbortSignal`.
Return a result matching the upstream event contract; return `undefined` to
continue to the next handler/Host, or throw to reject. Binding correlates and
sends the response automatically. Host cancellation and disconnection abort
the signal and suppress late results. Agent contexts are not reconstructed.

`stream(endpoint, payload, signal)` opens another native Gateway stream and
returns an async iterable. Payloads use native wire arguments (`{ args: ... }`).
Abort the signal or finish iteration to cancel that subscription.

`connect()` resolves only after the Gateway ready handshake. After a disconnect,
call `connect()` again, or opt in to automatic retries with `reconnectDelayMs`.
Event registrations survive reconnect; `onState(true)` should refetch application
state and reopen business streams. Events are not replayed by Binding. `close()`
stops retries, closes sockets, cancels requests and withdraws the remote contribution.
Older clients requesting only legacy events still receive HTTP 409 from modern hosts.

Validation (2026-09-19): 31 tests pass with the legacy development baseline and
Harmony 0.8.11. Isolated DSH 0.1.5-rc.2 and 0.1.6-alpha.2 Host tests pass
forward/reverse RPC, lazy codecs, authentication refusal, Gateway notifications,
waterfall result/delegation/rejection/cancellation and reconnect. Studio must
consume these modern APIs before its event-driven UI can claim compatibility.

Run `node test/upstream.e2e.mjs <isolated-dsh-root> <harmony-lib-bin-path>` after building to repeat the modern Host test. It creates and removes an isolated profile, without model calls.
