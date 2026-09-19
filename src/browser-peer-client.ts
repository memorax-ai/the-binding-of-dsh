import { Context } from '@deepseek-ai/cordis'
import TypertRegistry from '@deepseek-ai/dsh-typert-registry'
import type { TypertDisposer, TypertRemoteContribution } from '@deepseek-ai/dsh-typert-protocol'
import { createClientConnectionBinding, type ClientConnectionGeneration } from './shared/client-connection.js'
import { PeerRemoteProjector, type PeerRemoteApi, type TypertRemoteCaller } from './shared/peer-remote.js'
import { type RpcResult } from './shared/protocol.js'
import { BrowserRemoteEvents, type RemoteEventHandler, type RemoteEventListener } from './shared/browser-events.js'
export type { RemoteEventRequest, RemoteEventHandler, RemoteEventListener } from './shared/browser-events.js'

const MUX_PATH = '/api/events.mux'
const HOST_PATH = '/api/events.host'
const INTERNAL_BASE = 'http://dsh.internal'

export type BrowserPeerChannel = 'mux' | 'host'

/** Legacy event envelope. Domain payload types belong to the caller's DSH version. */
export interface BrowserPeerEvent<T = { type: string; [key: string]: unknown }> {
  channel: BrowserPeerChannel
  envelope: { rpcId: string; payload: T }
}

interface Generation {
  readonly connection: ClientConnectionGeneration
  readonly mux: WebSocket
  readonly host: WebSocket
  readonly abort: AbortController
  active: boolean
  events?: BrowserRemoteEvents
}

export interface BrowserPeerClientOptions {
  readonly baseUrl?: string | URL
  readonly contribution: TypertRemoteContribution
  readonly fetch?: typeof globalThis.fetch
  readonly createWebSocket?: (url: string, protocol?: string) => WebSocket
  /** Opt in to automatic reconnection; consumers must refetch state after onState(true). */
  readonly reconnectDelayMs?: number
}

/** Standalone browser owner for one native DSH Connection and both event streams. */
export class BrowserPeerClient {
  private readonly ctx = new Context()
  private readonly fetch: typeof globalThis.fetch
  private readonly baseUrl: URL
  private readonly contribution: TypertRemoteContribution
  private readonly createWebSocket: (url: string, protocol?: string) => WebSocket
  private readonly connection
  private readonly projector: PeerRemoteProjector
  private readonly caller: TypertRemoteCaller
  private readonly eventListeners = new Set<(event: BrowserPeerEvent) => void>()
  private readonly stateListeners = new Set<(connected: boolean) => void>()
  private readonly remoteListeners = new Map<string, Set<RemoteEventListener>>()
  private readonly remoteHandlers = new Map<string, Set<RemoteEventHandler>>()
  private legacySchemas?: typeof import('./shared/legacy-events.js')
  private readonly reconnectDelayMs?: number
  private reconnectTimer?: ReturnType<typeof setTimeout>
  private stopped = true
  private generation: Generation | undefined
  private mounting: Promise<TypertDisposer> | undefined
  private disposeRemote: TypertDisposer | undefined
  private connecting: Promise<void> | undefined
  private opening: AbortController | undefined

  readonly remote: PeerRemoteApi

  constructor(options: BrowserPeerClientOptions) {
    new TypertRegistry(this.ctx)
    this.fetch = options.fetch ?? globalThis.fetch
    const origin = globalThis.location?.origin
    this.baseUrl = new URL(options.baseUrl ?? (origin !== undefined && origin !== 'null' ? origin : INTERNAL_BASE))
    this.contribution = options.contribution
    this.reconnectDelayMs = options.reconnectDelayMs
    this.createWebSocket = options.createWebSocket ?? ((url, protocol) => new WebSocket(url, protocol))
    this.connection = createClientConnectionBinding({
      nativeEvents: true,
      eventTransport: 'gateway-v1',
      fetch: this.fetch,
      baseUrl: () => this.baseUrl.href,
    })
    this.projector = new PeerRemoteProjector(this.ctx)
    this.caller = { call: (channel, endpoint, payload, signal) => this.call(channel, endpoint, payload, signal) }
    this.remote = this.projector.bind(this.caller, this.ctx)
  }

  get connected(): boolean {
    return this.generation?.active === true
  }

  get eventTransport(): 'legacy' | 'gateway-v1' | undefined {
    return this.generation === undefined ? undefined : this.generation.connection.eventTransport ?? 'legacy'
  }

  /** Native Gateway notification; event names and arguments are forwarded unchanged. */
  subscribe(event: string, listener: RemoteEventListener): () => void {
    const listeners = this.remoteListeners.get(event) ?? new Set()
    this.remoteListeners.set(event, listeners)
    listeners.add(listener)
    return () => { listeners.delete(listener); if (!listeners.size) this.remoteListeners.delete(event) }
  }

  /** Native Gateway waterfall, with generation-scoped cancellation and automatic replies. */
  handleEvent(event: string, handler: RemoteEventHandler): () => void {
    const handlers = this.remoteHandlers.get(event) ?? new Set()
    this.remoteHandlers.set(event, handlers)
    handlers.add(handler)
    return () => { handlers.delete(handler); if (!handlers.size) this.remoteHandlers.delete(event) }
  }

  /** Open a Gateway stream, e.g. a session subscription; reconnect requires opening it again. */
  stream(endpoint: string, payload: unknown, signal?: AbortSignal): AsyncIterable<unknown> {
    if (!this.connected || this.generation?.events === undefined) throw new Error('Gateway events are not connected')
    return this.generation.events.stream(endpoint, payload, signal)
  }

  connect(signal?: AbortSignal): Promise<void> {
    this.stopped = false
    clearTimeout(this.reconnectTimer)
    if (this.connected) return Promise.resolve()
    if (this.connecting !== undefined) return this.connecting
    const opening = new AbortController()
    this.opening = opening
    const combined = signal === undefined ? opening.signal : AbortSignal.any([opening.signal, signal])
    this.connecting = this.open(combined).finally(() => {
      if (this.opening === opening) this.opening = undefined
      this.connecting = undefined
    })
    return this.connecting
  }

  onEvent<T = BrowserPeerEvent['envelope']['payload']>(listener: (event: BrowserPeerEvent<T>) => void): () => void {
    const untyped = listener as (event: BrowserPeerEvent) => void
    this.eventListeners.add(untyped)
    return () => this.eventListeners.delete(untyped)
  }

  onState(listener: (connected: boolean) => void): () => void {
    this.stateListeners.add(listener)
    listener(this.connected)
    return () => this.stateListeners.delete(listener)
  }

  async close(): Promise<void> {
    this.stopped = true
    clearTimeout(this.reconnectTimer)
    this.opening?.abort(new Error('Browser peer client closed'))
    await this.connecting?.catch(() => undefined)
    this.dropGeneration(new Error('Browser peer client closed'))
    const disposeRemote = this.disposeRemote
    this.disposeRemote = undefined
    this.mounting = undefined
    await disposeRemote?.()
  }

  private async open(signal: AbortSignal): Promise<void> {
    this.dropGeneration(new Error('Browser peer connection replaced'))
    this.mounting ??= this.projector.mount(this.ctx, this.contribution)
    this.disposeRemote = await this.mounting
    const connection = await this.connection.open(signal)
    if (signal.aborted) { this.connection.release(connection); throw signal.reason }
    if (connection.eventTransport !== 'gateway-v1') {
      // The bundled decoder has no runtime dependency on the retired API package.
      try {
        this.legacySchemas = await import('./shared/legacy-events.js')
      } catch (error) { this.connection.release(connection); throw error }
    }
    let mux: WebSocket | undefined
    let host: WebSocket
    try {
      mux = this.createWebSocket(webSocketUrl(this.baseUrl, MUX_PATH), connection.id)
      host = this.createWebSocket(webSocketUrl(this.baseUrl, HOST_PATH), connection.id)
    } catch (error) {
      mux?.close()
      this.connection.release(connection)
      throw error
    }
    const generation: Generation = {
      connection,
      mux,
      host,
      abort: new AbortController(),
      active: false,
    }
    this.generation = generation
    this.connection.attach(connection, 'host', generation.host)
    this.attach(generation, 'mux', generation.mux)
    this.attach(generation, 'host', generation.host)
    try {
      await Promise.all([
        waitForOpen(generation.mux, AbortSignal.any([signal, generation.abort.signal])),
        waitForOpen(generation.host, AbortSignal.any([signal, generation.abort.signal])),
      ])
      if (connection.eventTransport === 'gateway-v1') {
        generation.events = new BrowserRemoteEvents(
          this.createWebSocket(webSocketUrl(this.baseUrl, '/api/remote.mux')),
          (endpoint, payload, requestSignal) => this.connection.call('/api', endpoint, payload, requestSignal),
          this.remoteListeners, this.remoteHandlers, error => this.fail(generation, error),
        )
        await generation.events.start(AbortSignal.any([signal, generation.abort.signal]))
      }
      if (this.generation !== generation || generation.abort.signal.aborted) throw generation.abort.signal.reason
      generation.active = true
      this.emitState(true)
    } catch (error) {
      this.dropGeneration(new Error('Browser peer connection failed'))
      throw error
    }
  }

  private attach(generation: Generation, channel: BrowserPeerChannel, socket: WebSocket): void {
    socket.addEventListener('message', event => {
      if (this.generation !== generation || generation.abort.signal.aborted) return
      try {
        if (typeof event.data !== 'string') throw new Error('binary WebSocket frame')
        const raw = JSON.parse(event.data)
        if (channel === 'host' && this.connection.handle(raw, generation.connection)) return
        const schemas = this.legacySchemas
        if (generation.connection.eventTransport === 'gateway-v1' || schemas === undefined) throw new Error('Unexpected legacy event')
        const full = schemas.serverRequestSchema.parse(raw)
        const peerEvent: BrowserPeerEvent = channel === 'mux'
          ? { channel, envelope: { rpcId: full.rpcId, payload: schemas.muxFrameSchema.parse(full.payload) } }
          : { channel, envelope: { rpcId: full.rpcId, payload: schemas.hostFrameSchema.parse(full.payload) } }
        for (const listener of this.eventListeners) listener(peerEvent)
      } catch (error) {
        console.error(`[the-binding-of-dsh] dropping malformed ${channel} frame:`, error)
      }
    })
    const failed = (): void => this.fail(generation, new Error(`Browser peer ${channel} stream closed`))
    socket.addEventListener('close', failed, { once: true })
    socket.addEventListener('error', failed, { once: true })
  }

  private fail(generation: Generation, reason: Error): void {
    if (this.generation !== generation || generation.abort.signal.aborted) return
    this.dropGeneration(reason)
    this.scheduleReconnect()
  }

  private scheduleReconnect(): void {
    if (this.stopped || this.reconnectDelayMs === undefined) return
    clearTimeout(this.reconnectTimer)
    this.reconnectTimer = setTimeout(() => { void this.connect().catch(() => this.scheduleReconnect()) }, Math.max(10, this.reconnectDelayMs))
  }

  private dropGeneration(reason: Error): void {
    const generation = this.generation
    this.generation = undefined
    if (generation === undefined) return
    const wasActive = generation.active
    generation.active = false
    generation.abort.abort(reason)
    generation.events?.dispose(reason)
    this.connection.release(generation.connection)
    generation.mux.close()
    generation.host.close()
    if (wasActive) this.emitState(false)
  }

  private emitState(connected: boolean): void {
    for (const listener of this.stateListeners) listener(connected)
  }

  private async call(
    channel: string,
    endpoint: string,
    payload: unknown,
    signal?: AbortSignal,
  ): Promise<RpcResult<unknown>> {
    const generation = this.generation
    if (generation?.active !== true) throw new Error('Browser peer client is not connected')
    const requestSignal = signal === undefined
      ? generation.abort.signal
      : AbortSignal.any([generation.abort.signal, signal])
    return this.connection.call(channel, endpoint, payload, requestSignal)
  }
}

function webSocketUrl(baseUrl: URL, path: string): string {
  const url = new URL(path, baseUrl)
  url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'
  return url.href
}

function waitForOpen(socket: WebSocket, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.reject(signal.reason)
  if (socket.readyState === 1) return Promise.resolve()
  if (socket.readyState > 1) return Promise.reject(new Error('WebSocket already closed'))
  return new Promise((resolve, reject) => {
    const opened = (): void => finish(resolve)
    const failed = (): void => finish(() => reject(new Error('WebSocket failed before opening')))
    const aborted = (): void => finish(() => reject(signal.reason))
    const finish = (settle: () => void): void => {
      socket.removeEventListener('open', opened)
      socket.removeEventListener('close', failed)
      socket.removeEventListener('error', failed)
      signal.removeEventListener('abort', aborted)
      settle()
    }
    socket.addEventListener('open', opened, { once: true })
    socket.addEventListener('close', failed, { once: true })
    socket.addEventListener('error', failed, { once: true })
    signal.addEventListener('abort', aborted, { once: true })
    if (signal.aborted) aborted()
  })
}
