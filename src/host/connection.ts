import { randomUUID } from 'node:crypto'
import type {} from '@deepseek-ai/dsh-client-connection'
import {
  CONNECTION_CANCEL_METHOD,
  CONNECTION_OPEN_PATH,
  CONNECTION_RPC_METHOD,
  MAX_PENDING_CALLS,
  MAX_SEND_QUEUE_BYTES,
  internalFailure,
  parseClientControlFrame,
  parseServerControlFrame,
  type ClientRequest,
  type RpcResult,
  type ServerControlFrame,
} from '../shared/protocol.js'

export interface ConnectionPeer {
  readonly id: string
  readonly kind: 'browser' | 'node'
  call(
    channel: string,
    endpoint: string,
    payload: unknown,
    signal?: AbortSignal,
  ): Promise<RpcResult<unknown>>
}

export type PeerChange =
  | { type: 'added'; peer: ConnectionPeer }
  | { type: 'removed'; peer: ConnectionPeer }

export interface HostConnectionPeers {
  get(id: string): ConnectionPeer | undefined
  list(): readonly ConnectionPeer[]
  subscribe(listener: (change: PeerChange) => void): () => void
}

interface SocketLike {
  readonly readyState: number
  send(data: string, callback: (error?: Error | null) => void): void
  close(code?: number, reason?: string): void
  once(event: 'close' | 'error', listener: () => void): void
  on(event: 'message', listener: (data: unknown) => void): void
}

interface PendingCall {
  resolve(result: RpcResult<unknown>): void
  reject(error: unknown): void
  dispose(): void
}

interface Generation {
  id: string
  kind: 'browser' | 'node'
  failed: boolean
  mux?: SocketLike
  host?: SocketLike
  headers?: Record<string, unknown>
  peer?: ConnectionPeer
  outgoing: Map<string, PendingCall>
  incoming: Map<string, AbortController>
}

export type HostConnectionDispatcher = (
  request: ClientRequest & { method: typeof CONNECTION_RPC_METHOD },
  headers: Record<string, unknown>,
  signal: AbortSignal,
) => Promise<RpcResult<unknown>>

interface FetchHandler {
  fetch(request: Request): Response | Promise<Response>
}

interface SendQueue {
  tail: Promise<void>
  bytes: number
}

const sendQueues = new WeakMap<object, SendQueue>()

export function sendConnectionMessage(socket: SocketLike, message: unknown): Promise<void> {
  const data = JSON.stringify(message)
  if (data === undefined) return Promise.reject(new Error('Connection message is not serializable'))
  const bytes = Buffer.byteLength(data)
  const queue = sendQueues.get(socket) ?? { tail: Promise.resolve(), bytes: 0 }
  if (queue.bytes + bytes > MAX_SEND_QUEUE_BYTES) {
    return Promise.reject(new Error('Connection WebSocket send queue exceeds limit'))
  }
  queue.bytes += bytes
  const next = queue.tail.catch(() => undefined).then(() => new Promise<void>((resolve, reject) => {
    if (socket.readyState !== 1) return reject(new Error('WebSocket connection is closed'))
    socket.send(data, error => error == null ? resolve() : reject(error))
  })).finally(() => {
    queue.bytes -= bytes
  })
  queue.tail = next
  sendQueues.set(socket, queue)
  return next
}

export function createHostFetchDispatcher(fetchHandler: FetchHandler): HostConnectionDispatcher {
  return async (message, headers, signal) => {
    const requestHeaders = new Headers()
    for (const [name, value] of Object.entries(headers)) {
      if (typeof value === 'string') requestHeaders.set(name, value)
      else if (Array.isArray(value)) requestHeaders.set(name, value.join(', '))
    }
    requestHeaders.set('content-type', 'application/json')
    const channel = message.payload.channel.endsWith('/')
      ? message.payload.channel.slice(0, -1)
      : message.payload.channel
    const endpoint = message.payload.endpoint.startsWith('/')
      ? message.payload.endpoint.slice(1)
      : message.payload.endpoint
    const response = await fetchHandler.fetch(new Request(`http://dsh.internal${channel}/${endpoint}`, {
      method: 'POST',
      headers: requestHeaders,
      body: JSON.stringify({
        type: 'client-request',
        rpcId: message.rpcId,
        method: endpoint,
        payload: message.payload.payload,
      }),
      signal,
    }))
    if (!response.ok) throw new Error(`Connection dispatcher failed with HTTP ${response.status}`)
    const frame = parseServerControlFrame(await response.json())
    if (frame?.type !== 'server-response' || frame.rpcId !== message.rpcId) {
      throw new Error('Connection dispatcher returned a malformed response')
    }
    return frame.result
  }
}

export interface ReverseConnectionHost {
  readonly peers: HostConnectionPeers
  fetch(request: Request): Promise<Response> | undefined
  setDispatcher(dispatcher: HostConnectionDispatcher): void
  attach(kind: 'mux' | 'host', request: { headers: Record<string, unknown> }, socket: SocketLike): boolean
  dispose(): void
}

export class HostConnectionBinding implements ReverseConnectionHost {
  constructor(private readonly nativeEvents = true) {}
  private readonly generations = new Map<string, Generation>()
  private readonly published = new Map<string, ConnectionPeer>()
  private readonly listeners = new Set<(change: PeerChange) => void>()
  private dispatcher: HostConnectionDispatcher | undefined

  readonly peers: HostConnectionPeers = {
    get: id => this.published.get(id),
    list: () => [...this.published.values()],
    subscribe: listener => {
      this.listeners.add(listener)
      return () => this.listeners.delete(listener)
    },
  }

  setDispatcher(dispatcher: HostConnectionDispatcher): void {
    this.dispatcher = dispatcher
  }

  fetch(request: Request): Promise<Response> | undefined {
    return new URL(request.url).pathname === CONNECTION_OPEN_PATH ? this.open(request) : undefined
  }

  attach(
    kind: 'mux' | 'host',
    request: { headers: Record<string, unknown> },
    socket: SocketLike,
  ): boolean {
    const header = request.headers['sec-websocket-protocol']
    const id = typeof header === 'string' ? header.split(',', 1)[0]?.trim() : undefined
    const generation = id === undefined ? undefined : this.generations.get(id)
    if (generation === undefined || generation.failed || generation[kind] !== undefined) {
      socket.close(1008, 'invalid connection peer')
      return false
    }
    generation[kind] = socket
    if (kind === 'host') {
      generation.headers = request.headers
      socket.on('message', data => this.receive(generation, data))
    }
    const failed = (): void => this.fail(generation, new Error('Connection generation closed'))
    socket.once('close', failed)
    socket.once('error', failed)
    if (generation.mux !== undefined && generation.host !== undefined) this.publish(generation)
    return true
  }

  dispose(): void {
    for (const generation of [...this.generations.values()]) {
      this.fail(generation, new Error('Connection service disposed'))
    }
    this.listeners.clear()
  }

  private async open(request: Request): Promise<Response> {
    if (request.method !== 'POST') return new Response('method not allowed', { status: 405 })
    const options = request.headers.get('content-type')?.startsWith('application/json') === true
      ? await request.json() as { kind?: unknown; nativeEvents?: unknown; eventTransport?: unknown }
      : {}
    if (!this.nativeEvents && options.nativeEvents === true && options.eventTransport !== 'gateway-v1') {
      return new Response('This host uses Gateway event streams; legacy browser event subscriptions require migration.', { status: 409 })
    }
    const id = randomUUID()
    this.generations.set(id, {
      id,
      kind: options.kind === 'node'
        ? 'node'
        : 'browser',
      failed: false,
      outgoing: new Map(),
      incoming: new Map(),
    })
    return Response.json({ id, ...(!this.nativeEvents ? { eventTransport: 'gateway-v1' } : {}) })
  }

  private receive(generation: Generation, data: unknown): void {
    if (generation.failed) return
    let value: unknown
    try {
      const text = socketText(data)
      value = JSON.parse(text)
    } catch (error) {
      this.fail(generation, error)
      return
    }
    const frame = parseClientControlFrame(value)
    if (frame === undefined) {
      this.fail(generation, new Error('Malformed Connection control frame'))
      return
    }
    if (frame.type === 'client-response') {
      const pending = generation.outgoing.get(frame.rpcId)
      if (pending === undefined) return
      generation.outgoing.delete(frame.rpcId)
      pending.dispose()
      pending.resolve(frame.result)
      return
    }
    if (frame.method === CONNECTION_CANCEL_METHOD) {
      generation.incoming.get(frame.rpcId)?.abort(new Error('Remote call cancelled'))
      generation.incoming.delete(frame.rpcId)
      return
    }
    if (generation.outgoing.size + generation.incoming.size >= MAX_PENDING_CALLS
      || generation.incoming.has(frame.rpcId)) {
      this.fail(generation, new Error('Connection generation RPC limit reached'))
      return
    }
    void this.dispatch(generation, frame)
  }

  private async dispatch(
    generation: Generation,
    request: ClientRequest & { method: typeof CONNECTION_RPC_METHOD },
  ): Promise<void> {
    const controller = new AbortController()
    generation.incoming.set(request.rpcId, controller)
    let result: RpcResult<unknown>
    try {
      result = this.dispatcher === undefined
        ? internalFailure('Host Connection dispatcher is not installed')
        : await this.dispatcher(request, generation.headers ?? {}, controller.signal)
    } catch (error) {
      result = internalFailure(error)
    }
    if (generation.incoming.get(request.rpcId) !== controller) return
    generation.incoming.delete(request.rpcId)
    if (generation.host === undefined || generation.failed) return
    void sendConnectionMessage(generation.host, {
      type: 'server-response',
      rpcId: request.rpcId,
      result,
    } satisfies ServerControlFrame).catch(error => this.fail(generation, error))
  }

  private publish(generation: Generation): void {
    if (generation.peer !== undefined || generation.failed) return
    const peer: ConnectionPeer = {
      id: generation.id,
      kind: generation.kind,
      call: (channel, endpoint, payload, signal) => this.call(generation, channel, endpoint, payload, signal),
    }
    generation.peer = peer
    this.published.set(peer.id, peer)
    this.emit({ type: 'added', peer })
  }

  private call(
    generation: Generation,
    channel: string,
    endpoint: string,
    payload: unknown,
    signal?: AbortSignal,
  ): Promise<RpcResult<unknown>> {
    if (generation.failed || generation.host === undefined || generation.peer === undefined) {
      return Promise.reject(new Error('Connection peer is not active'))
    }
    if (signal?.aborted === true) return Promise.reject(signal.reason)
    if (generation.outgoing.size + generation.incoming.size >= MAX_PENDING_CALLS) {
      return Promise.reject(new Error('Connection generation RPC limit reached'))
    }
    const rpcId = randomUUID()
    return new Promise((resolve, reject) => {
      const aborted = (): void => {
        const pending = generation.outgoing.get(rpcId)
        if (pending === undefined) return
        generation.outgoing.delete(rpcId)
        pending.dispose()
        reject(signal?.reason)
        void sendConnectionMessage(generation.host!, {
          type: 'server-request',
          rpcId,
          method: CONNECTION_CANCEL_METHOD,
          payload: null,
        } satisfies ServerControlFrame).catch(error => this.fail(generation, error))
      }
      const pending: PendingCall = {
        resolve,
        reject,
        dispose: () => signal?.removeEventListener('abort', aborted),
      }
      generation.outgoing.set(rpcId, pending)
      signal?.addEventListener('abort', aborted, { once: true })
      void sendConnectionMessage(generation.host!, {
        type: 'server-request',
        rpcId,
        method: CONNECTION_RPC_METHOD,
        payload: { channel, endpoint, payload },
      } satisfies ServerControlFrame).catch(error => this.fail(generation, error))
    })
  }

  private fail(generation: Generation, error: unknown): void {
    if (generation.failed) return
    generation.failed = true
    this.generations.delete(generation.id)
    if (generation.peer !== undefined) {
      this.published.delete(generation.id)
      this.emit({ type: 'removed', peer: generation.peer })
    }
    for (const pending of generation.outgoing.values()) {
      pending.dispose()
      pending.reject(error)
    }
    generation.outgoing.clear()
    for (const controller of generation.incoming.values()) controller.abort(error)
    generation.incoming.clear()
    for (const socket of [generation.mux, generation.host]) {
      if (socket !== undefined && (socket.readyState === 0 || socket.readyState === 1)) {
        socket.close(1008, 'connection generation closed')
      }
    }
  }

  private emit(change: PeerChange): void {
    for (const listener of [...this.listeners]) listener(change)
  }
}

function socketText(data: unknown): string {
  if (typeof data === 'string') return data
  if (data instanceof ArrayBuffer) return Buffer.from(data).toString()
  if (ArrayBuffer.isView(data)) return Buffer.from(data.buffer, data.byteOffset, data.byteLength).toString()
  if (Array.isArray(data) && data.every(value => ArrayBuffer.isView(value))) {
    return Buffer.concat(data.map(value => Buffer.from(value.buffer, value.byteOffset, value.byteLength))).toString()
  }
  throw new TypeError('Unsupported WebSocket frame')
}

declare module '@deepseek-ai/dsh-client-connection' {
  interface HostConnectionHandle {
    readonly peers: HostConnectionPeers
  }
}
