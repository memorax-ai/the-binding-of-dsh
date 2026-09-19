import type { Context } from '@deepseek-ai/cordis'
import type {
  InvocationDescriptor,
  InvocationParameterDescriptor,
  RemoteMethodMarker,
  TypertCodec,
} from '@deepseek-ai/dsh-typert-protocol'
import type { RpcError, RpcResult } from './protocol.js'

const NEVER_ABORTED_SIGNAL = new AbortController().signal
const CORDIS_ORIGINAL = Symbol.for('cordis.original')

export type GatewayErrorCode =
  | 'ambiguous-endpoint'
  | 'arguments-invalid'
  | 'binding-invalid'
  | 'context-failed'
  | 'context-not-found'
  | 'context-unavailable'
  | 'definition-unavailable'
  | 'input-invalid'
  | 'invocation-unavailable'
  | 'lookup-failed'
  | 'lookup-not-found'
  | 'lookup-unavailable'
  | 'method-unavailable'
  | 'provider-mismatch'
  | 'result-invalid'
  | 'service-unavailable'
  | 'signature-invalid'

export interface GatewayErrorOptions {
  readonly cause?: unknown
  readonly field?: string
}

export class GatewayDispatchError extends Error {
  readonly field: string | undefined

  constructor(
    readonly code: GatewayErrorCode,
    readonly endpoint: string,
    message: string,
    options: GatewayErrorOptions = {},
  ) {
    super(
      `typert gateway: ${endpoint}: ${message}`,
      options.cause === undefined ? undefined : { cause: options.cause },
    )
    this.name = 'TypertGatewayError'
    this.field = options.field
  }
}

class RemoteInvocationCancelled extends Error {
  constructor(endpoint: string, cause: unknown) {
    super(`Remote invocation "${endpoint}" was aborted`, { cause })
    this.name = 'RemoteInvocationCancelled'
  }
}

interface RemoteBinding {
  readonly service: object
  readonly serviceKey: string
  readonly namespace: string
}

interface GatewayRuntime {
  readonly createError?: (
    code: GatewayErrorCode,
    endpoint: string,
    message: string,
    options?: GatewayErrorOptions,
  ) => Error
  readonly remoteMethods?: (service: object) => readonly RemoteMethodMarker[]
  readonly lookupFailure?: (error: unknown) => RpcError | undefined
}

export interface GatewayRequest {
  readonly namespace: string
  readonly method: string
  readonly args: Readonly<Record<string, unknown>>
  readonly signal?: AbortSignal
}

export interface GatewayDispatcher {
  claimsEndpoint(endpoint: string): boolean
  invoke(request: GatewayRequest): Promise<unknown>
  invokeRpc(endpoint: string, payload: unknown, signal: AbortSignal): Promise<RpcResult<unknown>>
}

export function createGatewayDispatcher(
  ctx: Context,
  runtime: GatewayRuntime = {},
): GatewayDispatcher {
  const lookupFailure = (cause: unknown): RpcError | undefined => {
    const configured = runtime.lookupFailure?.(cause)
    if (configured !== undefined) return configured
    if (!isObject(cause) || Reflect.get(cause, 'name') !== 'TypertLookupFailure') return undefined
    const failure = Reflect.get(cause, 'failure')
    if (!isObject(failure)) return undefined
    if (typeof Reflect.get(failure, 'code') !== 'string'
      || typeof Reflect.get(failure, 'message') !== 'string'
      || !Object.hasOwn(failure, 'details')) return undefined
    return failure as unknown as RpcError
  }
  const error = (
    code: GatewayErrorCode,
    endpoint: string,
    message: string,
    options?: GatewayErrorOptions,
  ): Error => runtime.createError?.(code, endpoint, message, options)
    ?? new GatewayDispatchError(code, endpoint, message, options)
  let srcClaims: Set<string> | undefined

  ctx.on('internal/service', () => {
    srcClaims = undefined
  })

  const collectSrcClaims = (): Set<string> => {
    const claims = new Set<string>()
    if (runtime.remoteMethods === undefined) return claims
    for (const [serviceKey, definition] of Object.entries(ctx.reflect.props)) {
      if (definition.type !== 'service') continue
      const receiver: unknown = ctx.get(serviceKey)
      if (!isObject(receiver)) continue
      const original = originalOf(receiver)
      const binding = Reflect.get(original, 'typertRemote')
      if (!isObject(binding) || typeof Reflect.get(binding, 'namespace') !== 'string') continue
      const namespace = Reflect.get(binding, 'namespace') as string
      for (const candidate of runtime.remoteMethods(original)) {
        claims.add(endpointOf(namespace, candidate.exportName ?? candidate.method))
      }
    }
    return claims
  }

  const claimsEndpoint = (endpoint: string): boolean => {
    const segments = endpoint.split('/')
    if (segments.length !== 2 || segments[0] === '' || segments[1] === '') return false
    if (ctx.typert.local.get(endpoint) !== undefined || ctx.typert.local.hasSeen(endpoint)) return true
    srcClaims ??= collectSrcClaims()
    return srcClaims.has(endpoint)
  }

  const readBinding = (
    value: unknown,
    original: object,
    serviceKey: string,
    endpoint: string,
    namespace?: string,
  ): RemoteBinding => {
    if (!isObject(value)
      || Reflect.get(value, 'service') !== original
      || Reflect.get(value, 'serviceKey') !== serviceKey
      || typeof Reflect.get(value, 'namespace') !== 'string'
      || (namespace !== undefined && Reflect.get(value, 'namespace') !== namespace)) {
      throw error(
        'binding-invalid',
        endpoint,
        `Service ${JSON.stringify(serviceKey)} has an inconsistent typertRemote binding`,
      )
    }
    return value as unknown as RemoteBinding
  }

  const srcDescriptor = (
    binding: RemoteBinding,
    marker: RemoteMethodMarker,
    method: string,
    endpoint: string,
  ): InvocationDescriptor => {
    const names = methodParameterNames(binding.service, marker.method, endpoint, error)
    const signalIndex = names.indexOf('signal')
    if (signalIndex >= 0 && signalIndex !== names.length - 1) {
      throw error(
        'signature-invalid',
        endpoint,
        'SRC cancellation parameter signal must be the final parameter',
        { field: 'signal' },
      )
    }
    const businessNames = signalIndex < 0 ? names : names.slice(0, -1)
    const parameters: InvocationParameterDescriptor[] = []
    const wires = new Set<string>()
    for (const name of businessNames) {
      const matches = ctx.typert.lookups.definitions()
        .filter(definition => definition.parameter === name)
      if (matches.length > 1) {
        throw error(
          'signature-invalid',
          endpoint,
          `parameter ${JSON.stringify(name)} matches multiple lookup providers`,
          { field: name },
        )
      }
      const match = matches[0]
      const parameter: InvocationParameterDescriptor = match === undefined
        ? { name, wire: name, source: 'json', codec: { mode: 'src-json' } }
        : {
            name,
            wire: match.wire,
            source: 'lookup',
            lookup: match.key,
            codec: { mode: 'src-json' },
          }
      if (wires.has(parameter.wire)) {
        throw error(
          'signature-invalid',
          endpoint,
          `multiple parameters use wire field ${JSON.stringify(parameter.wire)}`,
          { field: parameter.wire },
        )
      }
      wires.add(parameter.wire)
      parameters.push(parameter)
    }

    let invocation: InvocationDescriptor['invocation'] = { kind: 'direct' }
    if (marker.invocation.kind === 'context') {
      const provider = ctx.typert.contexts.getHost(marker.invocation.context)
      if (provider === undefined) {
        throw error(
          'context-unavailable',
          endpoint,
          `Context provider ${JSON.stringify(marker.invocation.context)} is unavailable`,
        )
      }
      if (wires.has(provider.wire)) {
        throw error(
          'signature-invalid',
          endpoint,
          `Context identity conflicts with wire field ${JSON.stringify(provider.wire)}`,
          { field: provider.wire },
        )
      }
      invocation = {
        kind: 'context',
        context: marker.invocation.context,
        wire: provider.wire,
        codec: { mode: 'src-json' },
      }
    }

    return {
      id: `src:${binding.serviceKey}#${endpoint}`,
      service: binding.serviceKey,
      namespace: binding.namespace,
      method,
      ...(marker.method === method ? {} : { implementation: marker.method }),
      invocation,
      parameters,
      ...(signalIndex < 0 ? {} : { cancellation: { parameter: 'signal' } }),
      result: { mode: 'src-json' },
    }
  }

  const resolveDescriptor = (
    namespace: string,
    method: string,
    endpoint: string,
  ): InvocationDescriptor => {
    const strict = ctx.typert.local.get(endpoint)
    if (strict !== undefined) return strict
    if (ctx.typert.local.hasSeen(endpoint)) {
      throw error(
        'definition-unavailable',
        endpoint,
        'its strict definition was withdrawn and SRC fallback is forbidden',
      )
    }
    if (runtime.remoteMethods === undefined) {
      throw error(
        'invocation-unavailable',
        endpoint,
        'no active Remote method exports this endpoint',
      )
    }

    const candidates: InvocationDescriptor[] = []
    for (const [serviceKey, definition] of Object.entries(ctx.reflect.props)) {
      if (definition.type !== 'service') continue
      const receiver: unknown = ctx.get(serviceKey)
      if (!isObject(receiver)) continue
      const original = originalOf(receiver)
      const value = Reflect.get(original, 'typertRemote')
      if (value === undefined) continue
      const binding = readBinding(value, original, serviceKey, endpoint)
      if (binding.namespace !== namespace) continue
      const marker = runtime.remoteMethods(original)
        .find(candidate => (candidate.exportName ?? candidate.method) === method)
      if (marker !== undefined) candidates.push(srcDescriptor(binding, marker, method, endpoint))
    }
    if (candidates.length === 0) {
      throw error(
        'invocation-unavailable',
        endpoint,
        'no active Remote method exports this endpoint',
      )
    }
    if (candidates.length > 1) {
      throw error(
        'ambiguous-endpoint',
        endpoint,
        `multiple active Services export this endpoint: ${candidates.map(candidate => candidate.service).sort().join(', ')}`,
      )
    }
    return candidates[0]!
  }

  const resolveReceiverContext = async (
    descriptor: InvocationDescriptor,
    args: Readonly<Record<string, unknown>>,
    endpoint: string,
  ): Promise<Context> => {
    if (descriptor.invocation.kind === 'direct') return ctx
    const invocation = descriptor.invocation
    const provider = ctx.typert.contexts.getHost(invocation.context)
    if (provider === undefined) {
      throw error(
        'context-unavailable',
        endpoint,
        `Context provider ${JSON.stringify(invocation.context)} is unavailable`,
      )
    }
    if (provider.wire !== invocation.wire
      || (invocation.codec.mode === 'strict'
        && provider.wireTypeSymbol !== invocation.codec.typeSymbol)) {
      throw error(
        'provider-mismatch',
        endpoint,
        `Context provider ${JSON.stringify(invocation.context)} does not match its strict definition`,
        { field: invocation.wire },
      )
    }
    const identity = decode(
      invocation.codec,
      args[invocation.wire],
      'input-invalid',
      endpoint,
      invocation.wire,
      error,
    )
    let receiverContext: Context | undefined
    try {
      receiverContext = await provider.resolve(identity)
    } catch (cause) {
      const failure = lookupFailure(cause)
      if (failure !== undefined) throw cause
      throw error(
        'context-failed',
        endpoint,
        `Context provider ${JSON.stringify(invocation.context)} failed`,
        { cause, field: invocation.wire },
      )
    }
    if (receiverContext === undefined) {
      throw error(
        'context-not-found',
        endpoint,
        `Context provider ${JSON.stringify(invocation.context)} did not resolve the requested identity`,
        { field: invocation.wire },
      )
    }
    return receiverContext
  }

  const resolveParameter = async (
    parameter: InvocationParameterDescriptor,
    args: Readonly<Record<string, unknown>>,
    endpoint: string,
  ): Promise<unknown> => {
    if (!Object.hasOwn(args, parameter.wire)) return undefined
    const value = decode(
      parameter.codec,
      args[parameter.wire],
      'input-invalid',
      endpoint,
      parameter.wire,
      error,
    )
    if (parameter.source === 'json') return value
    const key = parameter.lookup
    if (key === undefined) {
      throw error(
        'lookup-unavailable',
        endpoint,
        `lookup parameter ${JSON.stringify(parameter.name)} has no provider key`,
        { field: parameter.wire },
      )
    }
    const provider = ctx.typert.lookups.get(key)
    if (provider === undefined) {
      throw error(
        'lookup-unavailable',
        endpoint,
        `lookup provider ${JSON.stringify(key)} is unavailable`,
        { field: parameter.wire },
      )
    }
    if (provider.wire !== parameter.wire
      || (parameter.codec.mode === 'strict'
        && provider.wireTypeSymbol !== parameter.codec.typeSymbol)) {
      throw error(
        'provider-mismatch',
        endpoint,
        `lookup provider ${JSON.stringify(key)} does not match its strict definition`,
        { field: parameter.wire },
      )
    }
    let resolved: unknown
    try {
      resolved = await provider.resolve(value)
    } catch (cause) {
      const failure = lookupFailure(cause)
      if (failure !== undefined) throw cause
      throw error(
        'lookup-failed',
        endpoint,
        `lookup provider ${JSON.stringify(key)} failed`,
        { cause, field: parameter.wire },
      )
    }
    if (resolved === undefined) {
      throw error(
        'lookup-not-found',
        endpoint,
        `lookup provider ${JSON.stringify(key)} did not resolve the requested identity`,
        { field: parameter.wire },
      )
    }
    return resolved
  }

  const invoke = async (request: GatewayRequest): Promise<unknown> => {
    const endpoint = endpointOf(request.namespace, request.method)
    const descriptor = resolveDescriptor(request.namespace, request.method, endpoint)
    assertExactArguments(request.args, descriptor, endpoint, error)
    const receiverContext = await resolveReceiverContext(descriptor, request.args, endpoint)
    const receiver: unknown = receiverContext.get(descriptor.service)
    if (!isObject(receiver)) {
      throw error(
        'service-unavailable',
        endpoint,
        `active Service ${JSON.stringify(descriptor.service)} is unavailable`,
      )
    }
    const original = originalOf(receiver)
    readBinding(
      Reflect.get(original, 'typertRemote'),
      original,
      descriptor.service,
      endpoint,
      descriptor.namespace,
    )
    const args = await Promise.all(
      descriptor.parameters.map(parameter => resolveParameter(parameter, request.args, endpoint)),
    )
    if (descriptor.cancellation !== undefined) args.push(request.signal ?? NEVER_ABORTED_SIGNAL)
    const implementation = descriptor.implementation ?? descriptor.method
    const methodValue = Reflect.get(receiver, implementation)
    if (typeof methodValue !== 'function') {
      throw error(
        'method-unavailable',
        endpoint,
        `active Service ${JSON.stringify(descriptor.service)} has no callable method ${JSON.stringify(implementation)}`,
      )
    }
    let result: unknown
    try {
      result = await Reflect.apply(methodValue, receiver, args)
    } catch (cause) {
      if (request.signal?.aborted === true) throw new RemoteInvocationCancelled(endpoint, cause)
      throw cause
    }
    if (result === undefined && descriptor.result.mode !== 'strict') return result
    return decode(descriptor.result, result, 'result-invalid', endpoint, 'result', error)
  }

  const invokeRpc = async (
    endpoint: string,
    payload: unknown,
    signal: AbortSignal,
  ): Promise<RpcResult<unknown>> => {
    try {
      const segments = endpoint.split('/')
      if (segments.length !== 2 || segments[0] === '' || segments[1] === '') {
        throw new Error(`invalid Remote endpoint ${JSON.stringify(endpoint)}`)
      }
      if (!isObject(payload)
        || !isPlainObject(payload)
        || Reflect.ownKeys(payload).length !== 1
        || !Object.hasOwn(payload, 'args')
        || !isObject(payload.args)
        || !isPlainObject(payload.args)) {
        throw new Error('Remote payload must contain exactly one plain-object args field')
      }
      return {
        ok: true,
        value: await invoke({
          namespace: segments[0]!,
          method: segments[1]!,
          args: payload.args as Record<string, unknown>,
          signal,
        }),
      }
    } catch (cause) {
      if (cause instanceof RemoteInvocationCancelled) {
        return { ok: false, error: { code: 'cancelled', message: cause.message, details: {} } }
      }
      const failure = lookupFailure(cause)
      if (failure !== undefined) return { ok: false, error: failure }
      return {
        ok: false,
        error: {
          code: 'internal',
          message: cause instanceof Error ? cause.message : String(cause),
          details: {},
        },
      }
    }
  }

  return { claimsEndpoint, invoke, invokeRpc }
}

function endpointOf(namespace: string, method: string): string {
  return `${namespace}/${method}`
}

function originalOf(receiver: object): object {
  const original = Reflect.get(receiver, CORDIS_ORIGINAL)
  return isObject(original) ? original : receiver
}

function methodParameterNames(
  service: object,
  method: string,
  endpoint: string,
  error: NonNullable<GatewayRuntime['createError']>,
): string[] {
  let prototype = Object.getPrototypeOf(service) as object | null
  let implementation: Function | undefined
  while (prototype !== null) {
    const descriptor = Object.getOwnPropertyDescriptor(prototype, method)
    if (descriptor !== undefined) {
      if ('value' in descriptor && typeof descriptor.value === 'function') {
        implementation = descriptor.value as Function
      }
      break
    }
    prototype = Object.getPrototypeOf(prototype) as object | null
  }
  if (implementation === undefined) {
    throw error(
      'method-unavailable',
      endpoint,
      `Remote marker has no prototype method ${JSON.stringify(method)}`,
    )
  }
  const source = Function.prototype.toString.call(implementation)
  const open = source.indexOf('(')
  const close = source.indexOf(')', open + 1)
  if (open < 0 || close < 0) return invalidSignature(endpoint, method, error)
  const body = source.slice(open + 1, close).trim()
  if (body.length === 0) return []
  const parts = body.split(',').map(part => part.trim())
  const names = new Set<string>()
  for (const part of parts) {
    if (!/^[$A-Z_a-z][$\w]*$/u.test(part) || names.has(part)) {
      return invalidSignature(endpoint, method, error)
    }
    names.add(part)
  }
  return [...names]
}

function invalidSignature(
  endpoint: string,
  method: string,
  error: NonNullable<GatewayRuntime['createError']>,
): never {
  throw error(
    'signature-invalid',
    endpoint,
    `SRC method ${JSON.stringify(method)} must use unique identifier parameters without destructuring, defaults, or rest`,
  )
}

function assertExactArguments(
  args: Readonly<Record<string, unknown>>,
  descriptor: InvocationDescriptor,
  endpoint: string,
  error: NonNullable<GatewayRuntime['createError']>,
): void {
  if (!isPlainObject(args)) throw error('arguments-invalid', endpoint, 'args must be a plain object')
  const expected = new Set(descriptor.parameters.map(parameter => parameter.wire))
  if (descriptor.invocation.kind === 'context') expected.add(descriptor.invocation.wire)
  const actual = Reflect.ownKeys(args)
  const extra = actual.filter(key => typeof key !== 'string' || !expected.has(key))
  const acceptsMissing = new Set(
    descriptor.parameters
      .filter(parameter => parameter.source === 'json'
        && (parameter.acceptsUndefined === true || parameter.codec.mode === 'src-json'))
      .map(parameter => parameter.wire),
  )
  const missing = [...expected]
    .filter(key => !Object.hasOwn(args, key) && !acceptsMissing.has(key))
  if (extra.length === 0 && missing.length === 0) return
  const clauses: string[] = []
  if (missing.length > 0) {
    clauses.push(`missing ${missing.map(key => JSON.stringify(key)).join(', ')}`)
  }
  if (extra.length > 0) {
    clauses.push(`unexpected ${extra.map(key => JSON.stringify(String(key))).join(', ')}`)
  }
  throw error(
    'arguments-invalid',
    endpoint,
    `args fields do not match the descriptor: ${clauses.join('; ')}`,
  )
}

export function parseGatewayValue(
  codec: TypertCodec,
  value: unknown,
  endpoint: string,
  field: string,
): unknown {
  if (codec.mode !== 'strict') {
    throw new Error(`client api: generated Remote ${endpoint} field ${JSON.stringify(field)} has no strict codec`)
  }
  try {
    return schemaFor(codec).parse(value)
  } catch (cause) {
    throw new Error(`client api: ${endpoint} rejected ${JSON.stringify(field)}`, { cause })
  }
}

function decode(
  codec: TypertCodec,
  value: unknown,
  code: 'input-invalid' | 'result-invalid',
  endpoint: string,
  field: string,
  error: NonNullable<GatewayRuntime['createError']>,
): unknown {
  try {
    if (codec.mode === 'strict') {
      value = schemaFor(codec).parse(value)
      if (value === undefined) return value
    }
    assertJsonValue(value, new Set())
    return value
  } catch (cause) {
    throw error(
      code,
      endpoint,
      code === 'input-invalid'
        ? `wire field ${JSON.stringify(field)} failed boundary validation`
        : 'business result failed boundary validation',
      { cause, field },
    )
  }
}

function assertJsonValue(value: unknown, ancestors: Set<object>): void {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return
  if (typeof value === 'number') {
    if (Number.isFinite(value)) return
    throw new TypeError('non-finite number is not JSON-safe')
  }
  if (!isObject(value)) throw new TypeError(`${typeof value} is not JSON-safe`)
  if (ancestors.has(value)) throw new TypeError('cyclic value is not JSON-safe')
  ancestors.add(value)
  try {
    if (Array.isArray(value)) {
      if (Object.getOwnPropertySymbols(value).length > 0 || Object.keys(value).length !== value.length) {
        throw new TypeError('sparse or decorated array is not JSON-safe')
      }
      for (let index = 0; index < value.length; index += 1) {
        if (!Object.hasOwn(value, index)) throw new TypeError('sparse array is not JSON-safe')
        assertJsonValue(value[index], ancestors)
      }
      return
    }
    if (!isPlainObject(value)) throw new TypeError('non-plain object is not JSON-safe')
    if (Object.getOwnPropertySymbols(value).length > 0) {
      throw new TypeError('symbol property is not JSON-safe')
    }
    for (const key of Reflect.ownKeys(value)) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key)
      if (descriptor === undefined || !descriptor.enumerable || !('value' in descriptor)) {
        throw new TypeError('non-data property is not JSON-safe')
      }
      assertJsonValue(descriptor.value, ancestors)
    }
  } finally {
    ancestors.delete(value)
  }
}

function isPlainObject(value: object): boolean {
  if (Array.isArray(value)) return false
  const prototype = Object.getPrototypeOf(value)
  return prototype === null || prototype === Object.prototype
}

function isObject(value: unknown): value is object & Record<PropertyKey, unknown> {
  return (typeof value === 'object' && value !== null) || typeof value === 'function'
}

// Codecs are immutable generated descriptors. Materialize lazy schemas once per realm.
const schemas = new WeakMap<object, { parse(value: unknown): unknown }>()
function schemaFor(codec: object): { parse(value: unknown): unknown } {
  const cached = schemas.get(codec)
  if (cached !== undefined) return cached
  const candidate = codec as { create?: () => { parse(value: unknown): unknown }; schema?: { parse(value: unknown): unknown } }
  const schema = typeof candidate.create === 'function' ? candidate.create() : candidate.schema
  if (schema === undefined || typeof schema.parse !== 'function') throw new TypeError('Strict codec has no schema factory or parser')
  schemas.set(codec, schema)
  return schema
}
