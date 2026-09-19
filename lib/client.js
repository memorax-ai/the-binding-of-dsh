window.__ModuleLoader__.load({
  id: "the-binding-of-dsh",
  factory: (require) => {
    const module = { exports: {} };
    const exports = module.exports;
Object.defineProperty(exports, Symbol.toStringTag, { value: "Module" });
//#region src/shared/gateway-dispatcher.ts
const NEVER_ABORTED_SIGNAL = new AbortController().signal;
const CORDIS_ORIGINAL = Symbol.for("cordis.original");
var GatewayDispatchError = class extends Error {
	code;
	endpoint;
	field;
	constructor(code, endpoint, message, options = {}) {
		super(`typert gateway: ${endpoint}: ${message}`, options.cause === void 0 ? void 0 : { cause: options.cause });
		this.code = code;
		this.endpoint = endpoint;
		this.name = "TypertGatewayError";
		this.field = options.field;
	}
};
var RemoteInvocationCancelled = class extends Error {
	constructor(endpoint, cause) {
		super(`Remote invocation "${endpoint}" was aborted`, { cause });
		this.name = "RemoteInvocationCancelled";
	}
};
function createGatewayDispatcher(ctx, runtime = {}) {
	const lookupFailure = (cause) => {
		const configured = runtime.lookupFailure?.(cause);
		if (configured !== void 0) return configured;
		if (!isObject(cause) || Reflect.get(cause, "name") !== "TypertLookupFailure") return void 0;
		const failure = Reflect.get(cause, "failure");
		if (!isObject(failure)) return void 0;
		if (typeof Reflect.get(failure, "code") !== "string" || typeof Reflect.get(failure, "message") !== "string" || !Object.hasOwn(failure, "details")) return void 0;
		return failure;
	};
	const error = (code, endpoint, message, options) => runtime.createError?.(code, endpoint, message, options) ?? new GatewayDispatchError(code, endpoint, message, options);
	let srcClaims;
	ctx.on("internal/service", () => {
		srcClaims = void 0;
	});
	const collectSrcClaims = () => {
		const claims = /* @__PURE__ */ new Set();
		if (runtime.remoteMethods === void 0) return claims;
		for (const [serviceKey, definition] of Object.entries(ctx.reflect.props)) {
			if (definition.type !== "service") continue;
			const receiver = ctx.get(serviceKey);
			if (!isObject(receiver)) continue;
			const original = originalOf(receiver);
			const binding = Reflect.get(original, "typertRemote");
			if (!isObject(binding) || typeof Reflect.get(binding, "namespace") !== "string") continue;
			const namespace = Reflect.get(binding, "namespace");
			for (const candidate of runtime.remoteMethods(original)) claims.add(endpointOf(namespace, candidate.exportName ?? candidate.method));
		}
		return claims;
	};
	const claimsEndpoint = (endpoint) => {
		const segments = endpoint.split("/");
		if (segments.length !== 2 || segments[0] === "" || segments[1] === "") return false;
		if (ctx.typert.local.get(endpoint) !== void 0 || ctx.typert.local.hasSeen(endpoint)) return true;
		srcClaims ??= collectSrcClaims();
		return srcClaims.has(endpoint);
	};
	const readBinding = (value, original, serviceKey, endpoint, namespace) => {
		if (!isObject(value) || Reflect.get(value, "service") !== original || Reflect.get(value, "serviceKey") !== serviceKey || typeof Reflect.get(value, "namespace") !== "string" || namespace !== void 0 && Reflect.get(value, "namespace") !== namespace) throw error("binding-invalid", endpoint, `Service ${JSON.stringify(serviceKey)} has an inconsistent typertRemote binding`);
		return value;
	};
	const srcDescriptor = (binding, marker, method, endpoint) => {
		const names = methodParameterNames(binding.service, marker.method, endpoint, error);
		const signalIndex = names.indexOf("signal");
		if (signalIndex >= 0 && signalIndex !== names.length - 1) throw error("signature-invalid", endpoint, "SRC cancellation parameter signal must be the final parameter", { field: "signal" });
		const businessNames = signalIndex < 0 ? names : names.slice(0, -1);
		const parameters = [];
		const wires = /* @__PURE__ */ new Set();
		for (const name of businessNames) {
			const matches = ctx.typert.lookups.definitions().filter((definition) => definition.parameter === name);
			if (matches.length > 1) throw error("signature-invalid", endpoint, `parameter ${JSON.stringify(name)} matches multiple lookup providers`, { field: name });
			const match = matches[0];
			const parameter = match === void 0 ? {
				name,
				wire: name,
				source: "json",
				codec: { mode: "src-json" }
			} : {
				name,
				wire: match.wire,
				source: "lookup",
				lookup: match.key,
				codec: { mode: "src-json" }
			};
			if (wires.has(parameter.wire)) throw error("signature-invalid", endpoint, `multiple parameters use wire field ${JSON.stringify(parameter.wire)}`, { field: parameter.wire });
			wires.add(parameter.wire);
			parameters.push(parameter);
		}
		let invocation = { kind: "direct" };
		if (marker.invocation.kind === "context") {
			const provider = ctx.typert.contexts.getHost(marker.invocation.context);
			if (provider === void 0) throw error("context-unavailable", endpoint, `Context provider ${JSON.stringify(marker.invocation.context)} is unavailable`);
			if (wires.has(provider.wire)) throw error("signature-invalid", endpoint, `Context identity conflicts with wire field ${JSON.stringify(provider.wire)}`, { field: provider.wire });
			invocation = {
				kind: "context",
				context: marker.invocation.context,
				wire: provider.wire,
				codec: { mode: "src-json" }
			};
		}
		return {
			id: `src:${binding.serviceKey}#${endpoint}`,
			service: binding.serviceKey,
			namespace: binding.namespace,
			method,
			...marker.method === method ? {} : { implementation: marker.method },
			invocation,
			parameters,
			...signalIndex < 0 ? {} : { cancellation: { parameter: "signal" } },
			result: { mode: "src-json" }
		};
	};
	const resolveDescriptor = (namespace, method, endpoint) => {
		const strict = ctx.typert.local.get(endpoint);
		if (strict !== void 0) return strict;
		if (ctx.typert.local.hasSeen(endpoint)) throw error("definition-unavailable", endpoint, "its strict definition was withdrawn and SRC fallback is forbidden");
		if (runtime.remoteMethods === void 0) throw error("invocation-unavailable", endpoint, "no active Remote method exports this endpoint");
		const candidates = [];
		for (const [serviceKey, definition] of Object.entries(ctx.reflect.props)) {
			if (definition.type !== "service") continue;
			const receiver = ctx.get(serviceKey);
			if (!isObject(receiver)) continue;
			const original = originalOf(receiver);
			const value = Reflect.get(original, "typertRemote");
			if (value === void 0) continue;
			const binding = readBinding(value, original, serviceKey, endpoint);
			if (binding.namespace !== namespace) continue;
			const marker = runtime.remoteMethods(original).find((candidate) => (candidate.exportName ?? candidate.method) === method);
			if (marker !== void 0) candidates.push(srcDescriptor(binding, marker, method, endpoint));
		}
		if (candidates.length === 0) throw error("invocation-unavailable", endpoint, "no active Remote method exports this endpoint");
		if (candidates.length > 1) throw error("ambiguous-endpoint", endpoint, `multiple active Services export this endpoint: ${candidates.map((candidate) => candidate.service).sort().join(", ")}`);
		return candidates[0];
	};
	const resolveReceiverContext = async (descriptor, args, endpoint) => {
		if (descriptor.invocation.kind === "direct") return ctx;
		const invocation = descriptor.invocation;
		const provider = ctx.typert.contexts.getHost(invocation.context);
		if (provider === void 0) throw error("context-unavailable", endpoint, `Context provider ${JSON.stringify(invocation.context)} is unavailable`);
		if (provider.wire !== invocation.wire || invocation.codec.mode === "strict" && provider.wireTypeSymbol !== invocation.codec.typeSymbol) throw error("provider-mismatch", endpoint, `Context provider ${JSON.stringify(invocation.context)} does not match its strict definition`, { field: invocation.wire });
		const identity = decode(invocation.codec, args[invocation.wire], "input-invalid", endpoint, invocation.wire, error);
		let receiverContext;
		try {
			receiverContext = await provider.resolve(identity);
		} catch (cause) {
			if (lookupFailure(cause) !== void 0) throw cause;
			throw error("context-failed", endpoint, `Context provider ${JSON.stringify(invocation.context)} failed`, {
				cause,
				field: invocation.wire
			});
		}
		if (receiverContext === void 0) throw error("context-not-found", endpoint, `Context provider ${JSON.stringify(invocation.context)} did not resolve the requested identity`, { field: invocation.wire });
		return receiverContext;
	};
	const resolveParameter = async (parameter, args, endpoint) => {
		if (!Object.hasOwn(args, parameter.wire)) return void 0;
		const value = decode(parameter.codec, args[parameter.wire], "input-invalid", endpoint, parameter.wire, error);
		if (parameter.source === "json") return value;
		const key = parameter.lookup;
		if (key === void 0) throw error("lookup-unavailable", endpoint, `lookup parameter ${JSON.stringify(parameter.name)} has no provider key`, { field: parameter.wire });
		const provider = ctx.typert.lookups.get(key);
		if (provider === void 0) throw error("lookup-unavailable", endpoint, `lookup provider ${JSON.stringify(key)} is unavailable`, { field: parameter.wire });
		if (provider.wire !== parameter.wire || parameter.codec.mode === "strict" && provider.wireTypeSymbol !== parameter.codec.typeSymbol) throw error("provider-mismatch", endpoint, `lookup provider ${JSON.stringify(key)} does not match its strict definition`, { field: parameter.wire });
		let resolved;
		try {
			resolved = await provider.resolve(value);
		} catch (cause) {
			if (lookupFailure(cause) !== void 0) throw cause;
			throw error("lookup-failed", endpoint, `lookup provider ${JSON.stringify(key)} failed`, {
				cause,
				field: parameter.wire
			});
		}
		if (resolved === void 0) throw error("lookup-not-found", endpoint, `lookup provider ${JSON.stringify(key)} did not resolve the requested identity`, { field: parameter.wire });
		return resolved;
	};
	const invoke = async (request) => {
		const endpoint = endpointOf(request.namespace, request.method);
		const descriptor = resolveDescriptor(request.namespace, request.method, endpoint);
		assertExactArguments(request.args, descriptor, endpoint, error);
		const receiver = (await resolveReceiverContext(descriptor, request.args, endpoint)).get(descriptor.service);
		if (!isObject(receiver)) throw error("service-unavailable", endpoint, `active Service ${JSON.stringify(descriptor.service)} is unavailable`);
		const original = originalOf(receiver);
		readBinding(Reflect.get(original, "typertRemote"), original, descriptor.service, endpoint, descriptor.namespace);
		const args = await Promise.all(descriptor.parameters.map((parameter) => resolveParameter(parameter, request.args, endpoint)));
		if (descriptor.cancellation !== void 0) args.push(request.signal ?? NEVER_ABORTED_SIGNAL);
		const implementation = descriptor.implementation ?? descriptor.method;
		const methodValue = Reflect.get(receiver, implementation);
		if (typeof methodValue !== "function") throw error("method-unavailable", endpoint, `active Service ${JSON.stringify(descriptor.service)} has no callable method ${JSON.stringify(implementation)}`);
		let result;
		try {
			result = await Reflect.apply(methodValue, receiver, args);
		} catch (cause) {
			if (request.signal?.aborted === true) throw new RemoteInvocationCancelled(endpoint, cause);
			throw cause;
		}
		if (result === void 0 && descriptor.result.mode !== "strict") return result;
		return decode(descriptor.result, result, "result-invalid", endpoint, "result", error);
	};
	const invokeRpc = async (endpoint, payload, signal) => {
		try {
			const segments = endpoint.split("/");
			if (segments.length !== 2 || segments[0] === "" || segments[1] === "") throw new Error(`invalid Remote endpoint ${JSON.stringify(endpoint)}`);
			if (!isObject(payload) || !isPlainObject(payload) || Reflect.ownKeys(payload).length !== 1 || !Object.hasOwn(payload, "args") || !isObject(payload.args) || !isPlainObject(payload.args)) throw new Error("Remote payload must contain exactly one plain-object args field");
			return {
				ok: true,
				value: await invoke({
					namespace: segments[0],
					method: segments[1],
					args: payload.args,
					signal
				})
			};
		} catch (cause) {
			if (cause instanceof RemoteInvocationCancelled) return {
				ok: false,
				error: {
					code: "cancelled",
					message: cause.message,
					details: {}
				}
			};
			const failure = lookupFailure(cause);
			if (failure !== void 0) return {
				ok: false,
				error: failure
			};
			return {
				ok: false,
				error: {
					code: "internal",
					message: cause instanceof Error ? cause.message : String(cause),
					details: {}
				}
			};
		}
	};
	return {
		claimsEndpoint,
		invoke,
		invokeRpc
	};
}
function endpointOf(namespace, method) {
	return `${namespace}/${method}`;
}
function originalOf(receiver) {
	const original = Reflect.get(receiver, CORDIS_ORIGINAL);
	return isObject(original) ? original : receiver;
}
function methodParameterNames(service, method, endpoint, error) {
	let prototype = Object.getPrototypeOf(service);
	let implementation;
	while (prototype !== null) {
		const descriptor = Object.getOwnPropertyDescriptor(prototype, method);
		if (descriptor !== void 0) {
			if ("value" in descriptor && typeof descriptor.value === "function") implementation = descriptor.value;
			break;
		}
		prototype = Object.getPrototypeOf(prototype);
	}
	if (implementation === void 0) throw error("method-unavailable", endpoint, `Remote marker has no prototype method ${JSON.stringify(method)}`);
	const source = Function.prototype.toString.call(implementation);
	const open = source.indexOf("(");
	const close = source.indexOf(")", open + 1);
	if (open < 0 || close < 0) return invalidSignature(endpoint, method, error);
	const body = source.slice(open + 1, close).trim();
	if (body.length === 0) return [];
	const parts = body.split(",").map((part) => part.trim());
	const names = /* @__PURE__ */ new Set();
	for (const part of parts) {
		if (!/^[$A-Z_a-z][$\w]*$/u.test(part) || names.has(part)) return invalidSignature(endpoint, method, error);
		names.add(part);
	}
	return [...names];
}
function invalidSignature(endpoint, method, error) {
	throw error("signature-invalid", endpoint, `SRC method ${JSON.stringify(method)} must use unique identifier parameters without destructuring, defaults, or rest`);
}
function assertExactArguments(args, descriptor, endpoint, error) {
	if (!isPlainObject(args)) throw error("arguments-invalid", endpoint, "args must be a plain object");
	const expected = new Set(descriptor.parameters.map((parameter) => parameter.wire));
	if (descriptor.invocation.kind === "context") expected.add(descriptor.invocation.wire);
	const extra = Reflect.ownKeys(args).filter((key) => typeof key !== "string" || !expected.has(key));
	const acceptsMissing = new Set(descriptor.parameters.filter((parameter) => parameter.source === "json" && (parameter.acceptsUndefined === true || parameter.codec.mode === "src-json")).map((parameter) => parameter.wire));
	const missing = [...expected].filter((key) => !Object.hasOwn(args, key) && !acceptsMissing.has(key));
	if (extra.length === 0 && missing.length === 0) return;
	const clauses = [];
	if (missing.length > 0) clauses.push(`missing ${missing.map((key) => JSON.stringify(key)).join(", ")}`);
	if (extra.length > 0) clauses.push(`unexpected ${extra.map((key) => JSON.stringify(String(key))).join(", ")}`);
	throw error("arguments-invalid", endpoint, `args fields do not match the descriptor: ${clauses.join("; ")}`);
}
function decode(codec, value, code, endpoint, field, error) {
	try {
		if (codec.mode === "strict") {
			value = schemaFor(codec).parse(value);
			if (value === void 0) return value;
		}
		assertJsonValue(value, /* @__PURE__ */ new Set());
		return value;
	} catch (cause) {
		throw error(code, endpoint, code === "input-invalid" ? `wire field ${JSON.stringify(field)} failed boundary validation` : "business result failed boundary validation", {
			cause,
			field
		});
	}
}
function assertJsonValue(value, ancestors) {
	if (value === null || typeof value === "string" || typeof value === "boolean") return;
	if (typeof value === "number") {
		if (Number.isFinite(value)) return;
		throw new TypeError("non-finite number is not JSON-safe");
	}
	if (!isObject(value)) throw new TypeError(`${typeof value} is not JSON-safe`);
	if (ancestors.has(value)) throw new TypeError("cyclic value is not JSON-safe");
	ancestors.add(value);
	try {
		if (Array.isArray(value)) {
			if (Object.getOwnPropertySymbols(value).length > 0 || Object.keys(value).length !== value.length) throw new TypeError("sparse or decorated array is not JSON-safe");
			for (let index = 0; index < value.length; index += 1) {
				if (!Object.hasOwn(value, index)) throw new TypeError("sparse array is not JSON-safe");
				assertJsonValue(value[index], ancestors);
			}
			return;
		}
		if (!isPlainObject(value)) throw new TypeError("non-plain object is not JSON-safe");
		if (Object.getOwnPropertySymbols(value).length > 0) throw new TypeError("symbol property is not JSON-safe");
		for (const key of Reflect.ownKeys(value)) {
			const descriptor = Object.getOwnPropertyDescriptor(value, key);
			if (descriptor === void 0 || !descriptor.enumerable || !("value" in descriptor)) throw new TypeError("non-data property is not JSON-safe");
			assertJsonValue(descriptor.value, ancestors);
		}
	} finally {
		ancestors.delete(value);
	}
}
function isPlainObject(value) {
	if (Array.isArray(value)) return false;
	const prototype = Object.getPrototypeOf(value);
	return prototype === null || prototype === Object.prototype;
}
function isObject(value) {
	return typeof value === "object" && value !== null || typeof value === "function";
}
const schemas = /* @__PURE__ */ new WeakMap();
function schemaFor(codec) {
	const cached = schemas.get(codec);
	if (cached !== void 0) return cached;
	const candidate = codec;
	const schema = typeof candidate.create === "function" ? candidate.create() : candidate.schema;
	if (schema === void 0 || typeof schema.parse !== "function") throw new TypeError("Strict codec has no schema factory or parser");
	schemas.set(codec, schema);
	return schema;
}
//#endregion
//#region src/shared/client-gateway.ts
function installClientGateway(ctx) {
	const connection = ctx.get("connection");
	if (typeof connection.rpc.intercept !== "function") return;
	const dispatcher = createGatewayDispatcher(ctx);
	const dispose = connection.rpc.intercept("/api", (endpoint) => dispatcher.claimsEndpoint(endpoint), (endpoint, payload, signal) => dispatcher.invokeRpc(endpoint, payload, signal));
	ctx.effect(() => dispose, "bidirectional-gateway.client.local");
}
//#endregion
//#region src/shared/protocol.ts
const CONNECTION_OPEN_PATH = "/api/connection.open";
const CONNECTION_RPC_METHOD = "connection.rpc";
const CONNECTION_CANCEL_METHOD = "connection.cancel";
function record(value) {
	return typeof value === "object" && value !== null && !Array.isArray(value) ? value : void 0;
}
function rpcId(value) {
	return typeof value === "string" && value !== "" ? value : void 0;
}
function isReverseRpcPayload(value) {
	const candidate = record(value);
	return candidate !== void 0 && typeof candidate.channel === "string" && candidate.channel !== "" && typeof candidate.endpoint === "string" && candidate.endpoint !== "" && Object.hasOwn(candidate, "payload");
}
function parseRpcResult(value) {
	const candidate = record(value);
	if (candidate?.ok === true) return {
		ok: true,
		value: candidate.value
	};
	if (candidate?.ok !== false) return void 0;
	const error = record(candidate.error);
	if (error === void 0 || typeof error.code !== "string" || typeof error.message !== "string" || !Object.hasOwn(error, "details")) return void 0;
	return {
		ok: false,
		error: {
			code: error.code,
			message: error.message,
			details: error.details
		}
	};
}
function parseServerControlFrame(value) {
	const candidate = record(value);
	const id = rpcId(candidate?.rpcId);
	if (candidate === void 0 || id === void 0) return void 0;
	if (candidate.type === "server-response") {
		const result = parseRpcResult(candidate.result);
		return result === void 0 ? void 0 : {
			type: "server-response",
			rpcId: id,
			result
		};
	}
	if (candidate.type !== "server-request") return void 0;
	if (candidate.method === "connection.cancel" && candidate.payload === null) return {
		type: "server-request",
		rpcId: id,
		method: CONNECTION_CANCEL_METHOD,
		payload: null
	};
	if (candidate.method === "connection.rpc" && isReverseRpcPayload(candidate.payload)) return {
		type: "server-request",
		rpcId: id,
		method: CONNECTION_RPC_METHOD,
		payload: candidate.payload
	};
}
function isTbodServerFrame(value) {
	const candidate = record(value);
	return candidate?.type === "server-response" || candidate?.type === "server-request" && (candidate.method === "connection.rpc" || candidate.method === "connection.cancel");
}
function internalFailure(error) {
	return {
		ok: false,
		error: {
			code: "internal",
			message: error instanceof Error ? error.message : String(error),
			details: {}
		}
	};
}
//#endregion
//#region src/shared/client-connection.ts
const INTERNAL_BASE = "http://dsh.internal";
function createClientConnectionBinding(options = {}) {
	const fetch = options.fetch ?? globalThis.fetch;
	const kind = options.kind;
	const baseUrl = options.baseUrl ?? (() => {
		const location = globalThis.location;
		return location?.origin !== void 0 && location.origin !== "null" ? location.origin : INTERNAL_BASE;
	});
	const registrations = /* @__PURE__ */ new Set();
	const generationWaiters = /* @__PURE__ */ new Set();
	let current;
	let opening;
	const close = (generation, reason) => {
		if (generation.closed) return;
		generation.closed = true;
		if (current === generation) current = void 0;
		for (const pending of generation.outgoing.values()) {
			pending.dispose();
			pending.reject(reason);
		}
		generation.outgoing.clear();
		for (const waiter of generation.readyWaiters) {
			waiter.dispose();
			waiter.reject(reason);
		}
		generation.readyWaiters.clear();
		for (const controller of generation.incoming.values()) controller.abort(reason);
		generation.incoming.clear();
		if (generation.host !== void 0 && (generation.host.readyState === 0 || generation.host.readyState === 1)) generation.host.close(1008, "connection generation closed");
	};
	const send = (generation, frame) => {
		const socket = generation.host;
		if (generation.closed || socket?.readyState !== 1) throw new Error("Connection host WebSocket is not open");
		const data = JSON.stringify(frame);
		const size = byteLength(data);
		if ((socket.bufferedAmount ?? 0) + size > 8388608) throw new Error("Connection WebSocket send queue exceeds limit");
		socket.send(data);
	};
	const ready = (generation, signal) => {
		if (generation.host?.readyState === 1) return Promise.resolve();
		if (generation.closed) return Promise.reject(/* @__PURE__ */ new Error("Connection generation closed"));
		if (signal?.aborted === true) return Promise.reject(signal.reason);
		return new Promise((resolve, reject) => {
			const aborted = () => {
				generation.readyWaiters.delete(waiter);
				waiter.dispose();
				reject(signal?.reason);
			};
			const waiter = {
				resolve,
				reject,
				dispose: () => signal?.removeEventListener("abort", aborted)
			};
			generation.readyWaiters.add(waiter);
			signal?.addEventListener("abort", aborted, { once: true });
		});
	};
	const activeGeneration = (signal) => {
		if (current !== void 0 && !current.closed) return Promise.resolve(current);
		if (signal?.aborted === true) return Promise.reject(signal.reason);
		return new Promise((resolve, reject) => {
			const aborted = () => {
				generationWaiters.delete(waiter);
				waiter.dispose();
				reject(signal?.reason);
			};
			const waiter = {
				resolve,
				reject,
				dispose: () => signal?.removeEventListener("abort", aborted)
			};
			generationWaiters.add(waiter);
			signal?.addEventListener("abort", aborted, { once: true });
		});
	};
	const publishGeneration = (generation) => {
		current = generation;
		for (const waiter of generationWaiters) {
			waiter.dispose();
			waiter.resolve(generation);
		}
		generationWaiters.clear();
	};
	const rejectGenerationWaiters = (error) => {
		for (const waiter of generationWaiters) {
			waiter.dispose();
			waiter.reject(error);
		}
		generationWaiters.clear();
	};
	const activate = (generation) => {
		if (generation.closed || generation.host?.readyState !== 1) return;
		for (const waiter of generation.readyWaiters) {
			waiter.dispose();
			waiter.resolve();
		}
		generation.readyWaiters.clear();
	};
	const dispatch = async (generation, message) => {
		const request = message.payload;
		const registration = [...registrations].find((candidate) => candidate.channel === request.channel && candidate.matches(request.endpoint));
		const controller = new AbortController();
		generation.incoming.set(message.rpcId, controller);
		registration?.active.add(controller);
		let result;
		try {
			result = registration === void 0 ? internalFailure(`No Client Connection handler owns ${request.channel}/${request.endpoint}`) : await registration.handler(request.endpoint, request.payload, controller.signal);
		} catch (error) {
			result = internalFailure(error);
		} finally {
			registration?.active.delete(controller);
		}
		if (generation.incoming.get(message.rpcId) !== controller) return;
		generation.incoming.delete(message.rpcId);
		try {
			const response = {
				type: "client-response",
				rpcId: message.rpcId,
				result
			};
			send(generation, response);
		} catch (error) {
			close(generation, error);
		}
	};
	return {
		intercept(channel, matches, handler) {
			const registration = {
				channel,
				matches,
				handler,
				active: /* @__PURE__ */ new Set()
			};
			registrations.add(registration);
			return () => {
				registrations.delete(registration);
				for (const controller of registration.active) controller.abort(/* @__PURE__ */ new Error("Client Connection handler disposed"));
				registration.active.clear();
			};
		},
		async call(channel, endpoint, payload, signal) {
			const generation = await activeGeneration(signal);
			await ready(generation, signal);
			if (signal?.aborted === true) throw signal.reason;
			if (current !== generation || generation.closed || generation.host?.readyState !== 1) throw new Error("Connection peer is not active");
			if (generation.outgoing.size + generation.incoming.size >= 256) throw new Error("Connection generation RPC limit reached");
			const rpcId = globalThis.crypto.randomUUID();
			return new Promise((resolve, reject) => {
				const aborted = () => {
					const pending = generation.outgoing.get(rpcId);
					if (pending === void 0) return;
					generation.outgoing.delete(rpcId);
					pending.dispose();
					reject(signal?.reason);
					try {
						send(generation, {
							type: "client-request",
							rpcId,
							method: CONNECTION_CANCEL_METHOD,
							payload: null
						});
					} catch (error) {
						close(generation, error);
					}
				};
				const pending = {
					resolve,
					reject,
					dispose: () => signal?.removeEventListener("abort", aborted)
				};
				generation.outgoing.set(rpcId, pending);
				signal?.addEventListener("abort", aborted, { once: true });
				try {
					send(generation, {
						type: "client-request",
						rpcId,
						method: CONNECTION_RPC_METHOD,
						payload: {
							channel,
							endpoint,
							payload
						}
					});
				} catch (error) {
					generation.outgoing.delete(rpcId);
					pending.dispose();
					reject(error);
					close(generation, error);
				}
			});
		},
		async open(signal) {
			if (current !== void 0 && !current.closed) return current;
			opening ??= (async () => {
				const response = await fetch(new URL(CONNECTION_OPEN_PATH, baseUrl()), {
					method: "POST",
					...kind === void 0 && options.nativeEvents !== true ? {} : {
						headers: { "content-type": "application/json" },
						body: JSON.stringify({
							kind,
							eventTransport: options.eventTransport,
							...options.nativeEvents ? { nativeEvents: true } : {}
						})
					},
					signal
				});
				if (!response.ok) throw new Error(`Connection open failed with HTTP ${response.status}: ${(await response.text()).slice(0, 300)}`);
				const body = await response.json();
				if (typeof body.id !== "string" || body.id === "") throw new Error("Connection open returned no peer id");
				const generation = {
					id: body.id,
					...body.eventTransport === "gateway-v1" ? { eventTransport: "gateway-v1" } : {},
					closed: false,
					readyWaiters: /* @__PURE__ */ new Set(),
					outgoing: /* @__PURE__ */ new Map(),
					incoming: /* @__PURE__ */ new Map()
				};
				publishGeneration(generation);
				return generation;
			})().catch((error) => {
				rejectGenerationWaiters(error);
				throw error;
			}).finally(() => {
				opening = void 0;
			});
			return opening;
		},
		attach(generation, kind, socket) {
			const active = current;
			if (kind !== "host" || generation === void 0 || active !== generation || active.closed) return;
			if (active.host !== void 0 && active.host !== socket) {
				close(active, /* @__PURE__ */ new Error("Connection generation has duplicate host WebSockets"));
				return;
			}
			active.host = socket;
			socket.addEventListener("open", () => activate(active), { once: true });
			activate(active);
		},
		release(generation) {
			if (generation !== void 0 && current === generation) close(current, /* @__PURE__ */ new Error("Connection generation closed"));
		},
		handle(message, generation) {
			const active = current;
			if (generation === void 0 || active === void 0 || active !== generation || active.closed) return false;
			const frame = parseServerControlFrame(message);
			if (frame === void 0) {
				if (!isTbodServerFrame(message)) return false;
				close(active, /* @__PURE__ */ new Error("Malformed Connection control frame"));
				return true;
			}
			if (frame.type === "server-response") {
				const pending = active.outgoing.get(frame.rpcId);
				if (pending === void 0) return true;
				active.outgoing.delete(frame.rpcId);
				pending.dispose();
				pending.resolve(frame.result);
				return true;
			}
			if (frame.method === "connection.cancel") {
				active.incoming.get(frame.rpcId)?.abort(/* @__PURE__ */ new Error("Remote call cancelled"));
				active.incoming.delete(frame.rpcId);
				return true;
			}
			if (active.outgoing.size + active.incoming.size >= 256 || active.incoming.has(frame.rpcId)) {
				close(active, /* @__PURE__ */ new Error("Connection generation RPC limit reached"));
				return true;
			}
			dispatch(active, frame);
			return true;
		}
	};
}
function byteLength(value) {
	return new TextEncoder().encode(value).byteLength;
}
//#endregion
//#region src/shared/modern-client.ts
/** Native RPC keeps its carrier; only reverse calls use Binding sockets. */
function installModernClientBinding(ctx) {
	const connection = ctx.get("connection");
	if (connection.rpc.intercept !== void 0 || connection.generation === void 0) return;
	const binding = createClientConnectionBinding();
	connection.rpc.intercept = binding.intercept;
	ctx.effect(() => {
		let disposed = false;
		let abort;
		let retry;
		let closeSockets;
		const stop = () => {
			clearTimeout(retry);
			abort?.abort(/* @__PURE__ */ new Error("Native connection generation changed"));
			closeSockets?.();
			closeSockets = void 0;
		};
		const open = async () => {
			stop();
			if (disposed || connection.generation?.getSnapshot() === void 0) return;
			const controller = new AbortController();
			abort = controller;
			try {
				const generation = await binding.open(controller.signal);
				if (controller.signal.aborted) {
					binding.release(generation);
					return;
				}
				const sockets = [];
				closeSockets = () => {
					binding.release(generation);
					for (const socket of sockets) socket.close();
				};
				const failed = () => {
					if (controller.signal.aborted || disposed) return;
					stop();
					retry = setTimeout(() => {
						open();
					}, 1e3);
				};
				for (const kind of ["mux", "host"]) {
					const url = new URL(`/api/events.${kind}`, globalThis.location.origin);
					url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
					const socket = new WebSocket(url, generation.id);
					sockets.push(socket);
					if (kind === "host") {
						binding.attach(generation, kind, socket);
						socket.addEventListener("message", (event) => {
							if (controller.signal.aborted) return;
							try {
								if (!binding.handle(JSON.parse(event.data), generation)) failed();
							} catch {
								failed();
							}
						});
					}
					socket.addEventListener("close", failed, { once: true });
					socket.addEventListener("error", failed, { once: true });
				}
			} catch {
				if (!controller.signal.aborted && !disposed) retry = setTimeout(() => {
					open();
				}, 1e3);
			}
		};
		const unsubscribe = connection.generation.subscribe(() => {
			open();
		});
		open();
		return () => {
			disposed = true;
			unsubscribe();
			stop();
			if (connection.rpc.intercept === binding.intercept) delete connection.rpc.intercept;
		};
	}, "binding: reverse RPC generation");
}
//#endregion
//#region src/client/index.ts
const inject = ["connection", "typert"];
/** Browser entrypoint loaded after the native Connection and Typert registry. */
function apply(ctx) {
	installModernClientBinding(ctx);
	installClientGateway(ctx);
}
//#endregion
exports.apply = apply;
exports.createClientConnectionBinding = createClientConnectionBinding;
exports.inject = inject;
exports.installClientGateway = installClientGateway;

return module.exports;
  },
});
//# sourceMappingURL=client.js.map