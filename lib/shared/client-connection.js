import { CONNECTION_CANCEL_METHOD, CONNECTION_OPEN_PATH, CONNECTION_RPC_METHOD, MAX_PENDING_CALLS, MAX_SEND_QUEUE_BYTES, internalFailure, isTbodServerFrame, parseServerControlFrame, } from './protocol.js';
const INTERNAL_BASE = 'http://dsh.internal';
export function createClientConnectionBinding(options = {}) {
    const fetch = options.fetch ?? globalThis.fetch;
    const kind = options.kind;
    const baseUrl = options.baseUrl ?? (() => {
        const location = globalThis.location;
        return location?.origin !== undefined && location.origin !== 'null'
            ? location.origin
            : INTERNAL_BASE;
    });
    const registrations = new Set();
    const generationWaiters = new Set();
    let current;
    let opening;
    const close = (generation, reason) => {
        if (generation.closed)
            return;
        generation.closed = true;
        if (current === generation)
            current = undefined;
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
        for (const controller of generation.incoming.values())
            controller.abort(reason);
        generation.incoming.clear();
        if (generation.host !== undefined
            && (generation.host.readyState === 0 || generation.host.readyState === 1)) {
            generation.host.close(1008, 'connection generation closed');
        }
    };
    const send = (generation, frame) => {
        const socket = generation.host;
        if (generation.closed || socket?.readyState !== 1)
            throw new Error('Connection host WebSocket is not open');
        const data = JSON.stringify(frame);
        const size = byteLength(data);
        if ((socket.bufferedAmount ?? 0) + size > MAX_SEND_QUEUE_BYTES) {
            throw new Error('Connection WebSocket send queue exceeds limit');
        }
        socket.send(data);
    };
    const ready = (generation, signal) => {
        if (generation.host?.readyState === 1)
            return Promise.resolve();
        if (generation.closed)
            return Promise.reject(new Error('Connection generation closed'));
        if (signal?.aborted === true)
            return Promise.reject(signal.reason);
        return new Promise((resolve, reject) => {
            const aborted = () => {
                generation.readyWaiters.delete(waiter);
                waiter.dispose();
                reject(signal?.reason);
            };
            const waiter = {
                resolve,
                reject,
                dispose: () => signal?.removeEventListener('abort', aborted),
            };
            generation.readyWaiters.add(waiter);
            signal?.addEventListener('abort', aborted, { once: true });
        });
    };
    const activeGeneration = (signal) => {
        if (current !== undefined && !current.closed)
            return Promise.resolve(current);
        if (signal?.aborted === true)
            return Promise.reject(signal.reason);
        return new Promise((resolve, reject) => {
            const aborted = () => {
                generationWaiters.delete(waiter);
                waiter.dispose();
                reject(signal?.reason);
            };
            const waiter = {
                resolve,
                reject,
                dispose: () => signal?.removeEventListener('abort', aborted),
            };
            generationWaiters.add(waiter);
            signal?.addEventListener('abort', aborted, { once: true });
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
        if (generation.closed || generation.host?.readyState !== 1)
            return;
        for (const waiter of generation.readyWaiters) {
            waiter.dispose();
            waiter.resolve();
        }
        generation.readyWaiters.clear();
    };
    const dispatch = async (generation, message) => {
        const request = message.payload;
        const registration = [...registrations].find(candidate => (candidate.channel === request.channel && candidate.matches(request.endpoint)));
        const controller = new AbortController();
        generation.incoming.set(message.rpcId, controller);
        registration?.active.add(controller);
        let result;
        try {
            result = registration === undefined
                ? internalFailure(`No Client Connection handler owns ${request.channel}/${request.endpoint}`)
                : await registration.handler(request.endpoint, request.payload, controller.signal);
        }
        catch (error) {
            result = internalFailure(error);
        }
        finally {
            registration?.active.delete(controller);
        }
        if (generation.incoming.get(message.rpcId) !== controller)
            return;
        generation.incoming.delete(message.rpcId);
        try {
            const response = {
                type: 'client-response',
                rpcId: message.rpcId,
                result,
            };
            send(generation, response);
        }
        catch (error) {
            close(generation, error);
        }
    };
    const binding = {
        intercept(channel, matches, handler) {
            const registration = { channel, matches, handler, active: new Set() };
            registrations.add(registration);
            return () => {
                registrations.delete(registration);
                for (const controller of registration.active) {
                    controller.abort(new Error('Client Connection handler disposed'));
                }
                registration.active.clear();
            };
        },
        async call(channel, endpoint, payload, signal) {
            const generation = await activeGeneration(signal);
            await ready(generation, signal);
            if (signal?.aborted === true)
                throw signal.reason;
            if (current !== generation || generation.closed || generation.host?.readyState !== 1) {
                throw new Error('Connection peer is not active');
            }
            if (generation.outgoing.size + generation.incoming.size >= MAX_PENDING_CALLS) {
                throw new Error('Connection generation RPC limit reached');
            }
            const rpcId = globalThis.crypto.randomUUID();
            return new Promise((resolve, reject) => {
                const aborted = () => {
                    const pending = generation.outgoing.get(rpcId);
                    if (pending === undefined)
                        return;
                    generation.outgoing.delete(rpcId);
                    pending.dispose();
                    reject(signal?.reason);
                    try {
                        send(generation, {
                            type: 'client-request',
                            rpcId,
                            method: CONNECTION_CANCEL_METHOD,
                            payload: null,
                        });
                    }
                    catch (error) {
                        close(generation, error);
                    }
                };
                const pending = {
                    resolve,
                    reject,
                    dispose: () => signal?.removeEventListener('abort', aborted),
                };
                generation.outgoing.set(rpcId, pending);
                signal?.addEventListener('abort', aborted, { once: true });
                try {
                    send(generation, {
                        type: 'client-request',
                        rpcId,
                        method: CONNECTION_RPC_METHOD,
                        payload: { channel, endpoint, payload },
                    });
                }
                catch (error) {
                    generation.outgoing.delete(rpcId);
                    pending.dispose();
                    reject(error);
                    close(generation, error);
                }
            });
        },
        async open(signal) {
            if (current !== undefined && !current.closed)
                return current;
            opening ??= (async () => {
                const response = await fetch(new URL(CONNECTION_OPEN_PATH, baseUrl()), {
                    method: 'POST',
                    ...(kind === undefined && options.nativeEvents !== true ? {} : {
                        headers: { 'content-type': 'application/json' },
                        body: JSON.stringify({ kind, eventTransport: options.eventTransport, ...(options.nativeEvents ? { nativeEvents: true } : {}) }),
                    }),
                    signal,
                });
                if (!response.ok)
                    throw new Error(`Connection open failed with HTTP ${response.status}: ${(await response.text()).slice(0, 300)}`);
                const body = await response.json();
                if (typeof body.id !== 'string' || body.id === '')
                    throw new Error('Connection open returned no peer id');
                const generation = {
                    id: body.id,
                    ...(body.eventTransport === 'gateway-v1' ? { eventTransport: 'gateway-v1' } : {}),
                    closed: false,
                    readyWaiters: new Set(),
                    outgoing: new Map(),
                    incoming: new Map(),
                };
                publishGeneration(generation);
                return generation;
            })().catch(error => {
                rejectGenerationWaiters(error);
                throw error;
            }).finally(() => {
                opening = undefined;
            });
            return opening;
        },
        attach(generation, kind, socket) {
            const active = current;
            if (kind !== 'host' || generation === undefined || active !== generation || active.closed)
                return;
            if (active.host !== undefined && active.host !== socket) {
                close(active, new Error('Connection generation has duplicate host WebSockets'));
                return;
            }
            active.host = socket;
            socket.addEventListener('open', () => activate(active), { once: true });
            activate(active);
        },
        release(generation) {
            if (generation !== undefined && current === generation) {
                close(current, new Error('Connection generation closed'));
            }
        },
        handle(message, generation) {
            const active = current;
            if (generation === undefined || active === undefined || active !== generation || active.closed)
                return false;
            const frame = parseServerControlFrame(message);
            if (frame === undefined) {
                if (!isTbodServerFrame(message))
                    return false;
                close(active, new Error('Malformed Connection control frame'));
                return true;
            }
            if (frame.type === 'server-response') {
                const pending = active.outgoing.get(frame.rpcId);
                if (pending === undefined)
                    return true;
                active.outgoing.delete(frame.rpcId);
                pending.dispose();
                pending.resolve(frame.result);
                return true;
            }
            if (frame.method === CONNECTION_CANCEL_METHOD) {
                active.incoming.get(frame.rpcId)?.abort(new Error('Remote call cancelled'));
                active.incoming.delete(frame.rpcId);
                return true;
            }
            if (active.outgoing.size + active.incoming.size >= MAX_PENDING_CALLS
                || active.incoming.has(frame.rpcId)) {
                close(active, new Error('Connection generation RPC limit reached'));
                return true;
            }
            void dispatch(active, frame);
            return true;
        },
    };
    return binding;
}
function byteLength(value) {
    return new TextEncoder().encode(value).byteLength;
}
//# sourceMappingURL=client-connection.js.map