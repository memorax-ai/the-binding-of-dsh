import type { Context } from '@deepseek-ai/cordis'
import * as Protocol from '@deepseek-ai/dsh-typert-protocol'
import type {
  InvocationDescriptor,
  RemoteResult,
  TypertDisposer,
  TypertRemoteContribution,
  TypertRemoteNamespaceMap,
} from '@deepseek-ai/dsh-typert-protocol'
import type { RpcResult } from './protocol.js'
import { parseGatewayValue } from './gateway-dispatcher.js'

interface MountToken {
  active: boolean
  readonly abort: AbortController
}

interface Projection {
  readonly context: string
  readonly wire: string
  readonly codec: InvocationDescriptor['result']
  readonly parameterIndex?: number
}

interface InstalledMethod {
  readonly descriptor: InvocationDescriptor
  readonly projection?: Projection
  readonly token: MountToken
}

interface MethodVariants {
  direct?: InstalledMethod
  scoped?: InstalledMethod
}

interface NamespaceState {
  readonly value: Record<string, unknown>
  readonly methods: Map<string, MethodVariants>
}

export interface PeerRemoteApi extends TypertRemoteNamespaceMap {}

export interface TypertRemoteCaller {
  call(
    channel: string,
    endpoint: string,
    payload: unknown,
    signal?: AbortSignal,
  ): Promise<RpcResult<unknown>>
}

export interface HostPeerRemote {
  for(peer: TypertRemoteCaller): PeerRemoteApi
  $mount(contribution: TypertRemoteContribution): Promise<TypertDisposer>
}

export class PeerRemoteProjector {
  private readonly methods = new Map<string, MethodVariants>()
  private readonly bound = new Set<BoundPeerRemote>()

  constructor(private readonly ownerCtx: Context) {}

  bind(peer: TypertRemoteCaller, callerCtx: Context): PeerRemoteApi {
    const bound = new BoundPeerRemote(this.ownerCtx, callerCtx, peer)
    for (const [endpoint, variants] of this.methods) bound.install(endpoint, variants)
    this.bound.add(bound)
    return bound.api
  }

  async mount(
    callerCtx: Context,
    contribution: TypertRemoteContribution,
  ): Promise<TypertDisposer> {
    validateContribution(contribution, this.methods)
    const owned = callerCtx.effect(async () => {
      const disposeRemote = callerCtx.typert.remotes.register(contribution)
      const token: MountToken = { active: true, abort: new AbortController() }
      const endpoints: string[] = []
      try {
        for (const descriptor of contribution.descriptors) {
          const endpoint = endpointOf(descriptor)
          const variants: MethodVariants = {}
          if (descriptor.invocation.kind === 'direct') {
            variants.direct = { descriptor, token }
          }
          const projection = scopedProjection(descriptor)
          if (projection !== undefined) {
            variants.scoped = { descriptor, projection, token }
          }
          this.methods.set(endpoint, variants)
          endpoints.push(endpoint)
          for (const remote of this.bound) remote.install(endpoint, variants)
        }
      } catch (error) {
        token.active = false
        token.abort.abort()
        for (const endpoint of endpoints.reverse()) this.withdraw(endpoint, token)
        await disposeRemote()
        throw error
      }
      return async () => {
        if (!token.active) return
        token.active = false
        token.abort.abort()
        for (const endpoint of endpoints.reverse()) this.withdraw(endpoint, token)
        await disposeRemote()
      }
    }, `bidirectional-gateway.remote.$mount(${JSON.stringify(contribution.package)})`)
    await owned
    return async () => {
      await owned()
    }
  }

  private withdraw(endpoint: string, token: MountToken): void {
    const variants = this.methods.get(endpoint)
    if (variants?.direct?.token !== token && variants?.scoped?.token !== token) return
    this.methods.delete(endpoint)
    for (const remote of this.bound) remote.remove(endpoint, token)
  }
}

class BoundPeerRemote {
  readonly api: PeerRemoteApi = Object.create(null) as PeerRemoteApi
  private readonly namespaces = new Map<string, NamespaceState>()

  constructor(
    private readonly ownerCtx: Context,
    private readonly callerCtx: Context,
    private readonly peer: TypertRemoteCaller,
  ) {}

  install(endpoint: string, variants: MethodVariants): void {
    const [namespaceName, method] = splitEndpoint(endpoint)
    let namespace = this.namespaces.get(namespaceName)
    if (namespace === undefined) {
      namespace = { value: Object.create(null) as Record<string, unknown>, methods: new Map() }
      this.namespaces.set(namespaceName, namespace)
      Object.defineProperty(this.api, namespaceName, {
        configurable: true,
        enumerable: true,
        value: namespace.value,
      })
    }
    namespace.methods.set(method, variants)
    Object.defineProperty(namespace.value, method, {
      configurable: true,
      enumerable: true,
      value: (...args: unknown[]) => this.invoke(endpoint, variants, args),
    })
  }

  remove(endpoint: string, token: MountToken): void {
    const [namespaceName, method] = splitEndpoint(endpoint)
    const namespace = this.namespaces.get(namespaceName)
    const variants = namespace?.methods.get(method)
    if (namespace === undefined || variants === undefined) return
    if (variants.direct?.token !== token && variants.scoped?.token !== token) return
    namespace.methods.delete(method)
    Reflect.deleteProperty(namespace.value, method)
    if (namespace.methods.size > 0) return
    this.namespaces.delete(namespaceName)
    Reflect.deleteProperty(this.api, namespaceName)
  }

  private invoke(
    endpoint: string,
    variants: MethodVariants,
    values: unknown[],
  ): Promise<RemoteResult<unknown>> {
    const scoped = variants.scoped
    if (scoped !== undefined) {
      const identity = this.ownerCtx.typert.contexts
        .getClient(scoped.projection!.context)?.identity(this.callerCtx)
      if (identity !== undefined) {
        return this.invokeMethod(endpoint, scoped, values, { value: identity })
      }
    }
    if (variants.direct !== undefined) {
      return this.invokeMethod(endpoint, variants.direct, values)
    }
    if (scoped !== undefined) return this.invokeMethod(endpoint, scoped, values)
    throw new Error('host api: Remote method is no longer mounted')
  }

  private async invokeMethod(
    endpoint: string,
    installed: InstalledMethod,
    values: unknown[],
    boundIdentity?: { readonly value: unknown },
  ): Promise<RemoteResult<unknown>> {
    const { descriptor, projection, token } = installed
    if (!token.active) return withdrawn(endpoint)
    const expected = descriptor.parameters.length - (projection?.parameterIndex === undefined ? 0 : 1)
    const hasCallerSignal = descriptor.cancellation !== undefined && values.length === expected + 1
    if (values.length !== expected && !hasCallerSignal) {
      const contract = descriptor.cancellation === undefined
        ? `${String(expected)} argument(s)`
        : `${String(expected)} business argument(s) plus an optional AbortSignal`
      throw new Error(`host api: ${endpoint} expected ${contract}, got ${String(values.length)}`)
    }
    const args: Record<string, unknown> = Object.create(null) as Record<string, unknown>
    if (projection !== undefined) {
      const binder = boundIdentity === undefined
        ? this.ownerCtx.typert.contexts.getClient(projection.context)
        : undefined
      if (boundIdentity === undefined && binder === undefined) {
        throw new Error(
          `host api: ${endpoint} has no Client Context binder for ${JSON.stringify(projection.context)}`,
        )
      }
      const identity = boundIdentity?.value ?? binder?.identity(this.callerCtx)
      if (identity === undefined) {
        throw new Error(`host api: ${endpoint} requires a ${JSON.stringify(projection.context)} Context`)
      }
      args[projection.wire] = parseGatewayValue(
        projection.codec,
        identity,
        endpoint,
        projection.wire,
      )
    }
    let valueIndex = 0
    descriptor.parameters.forEach((parameter, parameterIndex) => {
      if (parameterIndex === projection?.parameterIndex) return
      const value = parseGatewayValue(parameter.codec, values[valueIndex], endpoint, parameter.wire)
      if (value !== undefined) args[parameter.wire] = value
      valueIndex += 1
    })
    const callerSignal = hasCallerSignal ? values[expected] as AbortSignal : undefined
    const signal = callerSignal === undefined
      ? token.abort.signal
      : AbortSignal.any([token.abort.signal, callerSignal])
    try {
      const result = await this.peer.call('/api', endpoint, { args }, signal)
      if (!token.active) return withdrawn(endpoint)
      if (!result.ok) {
        return remoteFailure({
            code: result.error.code,
            message: result.error.message,
            details: isObject(result.error.details) ? result.error.details : {},
        })
      }
      return {
        ok: true,
        value: parseGatewayValue(descriptor.result, result.value, endpoint, 'result'),
      }
    } catch (error) {
      return internalFailure(
        `host api: ${endpoint} failed: ${error instanceof Error ? error.message : String(error)}`,
      )
    }
  }
}

function validateContribution(
  contribution: TypertRemoteContribution,
  mounted: ReadonlyMap<string, MethodVariants>,
): void {
  const endpoints = new Set<string>()
  for (const descriptor of contribution.descriptors) {
    const endpoint = endpointOf(descriptor)
    requireStrictDescriptor(descriptor, endpoint)
    if (endpoints.has(endpoint)) {
      throw new Error(`host api: contribution repeats Remote method ${endpoint}`)
    }
    if (mounted.has(endpoint)) throw new Error(`host api: Remote method ${endpoint} is already mounted`)
    endpoints.add(endpoint)
  }
}

function requireStrictDescriptor(descriptor: InvocationDescriptor, endpoint: string): void {
  if (descriptor.result.mode !== 'strict') {
    throw new Error(`host api: generated Remote ${endpoint} result has no strict codec`)
  }
  for (const parameter of descriptor.parameters) {
    if (parameter.codec.mode !== 'strict') {
      throw new Error(
        `host api: generated Remote ${endpoint} field ${JSON.stringify(parameter.wire)} has no strict codec`,
      )
    }
  }
  if (descriptor.invocation.kind === 'context' && descriptor.invocation.codec.mode !== 'strict') {
    throw new Error(
      `host api: generated Remote ${endpoint} field ${JSON.stringify(descriptor.invocation.wire)} has no strict codec`,
    )
  }
}

function scopedProjection(descriptor: InvocationDescriptor): Projection | undefined {
  if (descriptor.invocation.kind === 'context') {
    return {
      context: descriptor.invocation.context,
      wire: descriptor.invocation.wire,
      codec: descriptor.invocation.codec,
    }
  }
  if (descriptor.scope === undefined) return undefined
  const lookupParameters = descriptor.parameters
    .map((parameter, index) => ({ parameter, index }))
    .filter(candidate => candidate.parameter.source === 'lookup')
  const selected = lookupParameters.length === 1 ? lookupParameters[0] : undefined
  if (selected === undefined
    || selected.parameter.wire !== descriptor.scope.wire
    || selected.parameter.lookup !== descriptor.scope.context) {
    throw new Error(
      `host api: generated Remote ${endpointOf(descriptor)} scope must select its only lookup parameter`,
    )
  }
  return {
    context: descriptor.scope.context,
    wire: descriptor.scope.wire,
    codec: selected.parameter.codec,
    parameterIndex: selected.index,
  }
}

function endpointOf(descriptor: InvocationDescriptor): string {
  return `${descriptor.namespace}/${descriptor.method}`
}

function splitEndpoint(endpoint: string): [string, string] {
  const slash = endpoint.indexOf('/')
  return [endpoint.slice(0, slash), endpoint.slice(slash + 1)]
}

function withdrawn(endpoint: string): RemoteResult<never> {
  return internalFailure(`host api: Remote method ${endpoint} is no longer mounted`)
}

function internalFailure(message: string): RemoteResult<never> {
  return remoteFailure({ code: 'internal', message, details: {} })
}

function remoteFailure(failure: { code: string; message: string; details: object }): RemoteResult<never> {
  const Constructor = (Protocol as unknown as {
    RemoteError?: new (code: string, message: string, details: object) => Error
  }).RemoteError
  const error = Constructor === undefined ? failure : new Constructor(
    failure.code === 'internal' ? 'gateway/internal' : failure.code,
    failure.message,
    failure.details,
  )
  // The Remote error-code registry is extensible; wire codes are owned by the remote service.
  return { ok: false, error } as RemoteResult<never>
}

function isObject(value: unknown): value is object {
  return typeof value === 'object' && value !== null
}
