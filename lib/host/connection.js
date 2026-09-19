import { randomUUID } from 'node:crypto';
import { CONNECTION_CANCEL_METHOD, CONNECTION_OPEN_PATH, CONNECTION_RPC_METHOD, MAX_PENDING_CALLS, MAX_SEND_QUEUE_BYTES, internalFailure, parseClientControlFrame, parseServerControlFrame, } from '../shared/protocol.js';
const sendQueues = new WeakMap();
export function sendConnectionMessage(socket, message) {
    const data = JSON.stringify(message);
    if (data === undefined)
        return Promise.reject(new Error('Connection message is not serializable'));
    const bytes = Buffer.byteLength(data);
    const queue = sendQueues.get(socket) ?? { tail: Promise.resolve(), bytes: 0 };
    if (queue.bytes + bytes > MAX_SEND_QUEUE_BYTES) {
        return Promise.reject(new Error('Connection WebSocket send queue exceeds limit'));
    }
    queue.bytes += bytes;
    const next = queue.tail.catch(() => undefined).then(() => new Promise((resolve, reject) => {
        if (socket.readyState !== 1)
            return reject(new Error('WebSocket connection is closed'));
        socket.send(data, error => error == null ? resolve() : reject(error));
    })).finally(() => {
        queue.bytes -= bytes;
    });
    queue.tail = next;
    sendQueues.set(socket, queue);
    return next;
}
export function createHostFetchDispatcher(fetchHandler) {
    return async (message, headers, signal) => {
        const requestHeaders = new Headers();
        for (const [name, value] of Object.entries(headers)) {
            if (typeof value === 'string')
                requestHeaders.set(name, value);
            else if (Array.isArray(value))
                requestHeaders.set(name, value.join(', '));
        }
        requestHeaders.set('content-type', 'application/json');
        const channel = message.payload.channel.endsWith('/')
            ? message.payload.channel.slice(0, -1)
            : message.payload.channel;
        const endpoint = message.payload.endpoint.startsWith('/')
            ? message.payload.endpoint.slice(1)
            : message.payload.endpoint;
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
        }));
        if (!response.ok)
            throw new Error(`Connection dispatcher failed with HTTP ${response.status}`);
        const frame = parseServerControlFrame(await response.json());
        if (frame?.type !== 'server-response' || frame.rpcId !== message.rpcId) {
            throw new Error('Connection dispatcher returned a malformed response');
        }
        return frame.result;
    };
}
export class HostConnectionBinding {
    nativeEvents;
    constructor(nativeEvents = true) {
        this.nativeEvents = nativeEvents;
    }
    generations = new Map();
    published = new Map();
    listeners = new Set();
    dispatcher;
    peers = {
        get: id => this.published.get(id),
        list: () => [...this.published.values()],
        subscribe: listener => {
            this.listeners.add(listener);
            return () => this.listeners.delete(listener);
        },
    };
    setDispatcher(dispatcher) {
        this.dispatcher = dispatcher;
    }
    fetch(request) {
        return new URL(request.url).pathname === CONNECTION_OPEN_PATH ? this.open(request) : undefined;
    }
    attach(kind, request, socket) {
        const header = request.headers['sec-websocket-protocol'];
        const id = typeof header === 'string' ? header.split(',', 1)[0]?.trim() : undefined;
        const generation = id === undefined ? undefined : this.generations.get(id);
        if (generation === undefined || generation.failed || generation[kind] !== undefined) {
            socket.close(1008, 'invalid connection peer');
            return false;
        }
        generation[kind] = socket;
        if (kind === 'host') {
            generation.headers = request.headers;
            socket.on('message', data => this.receive(generation, data));
        }
        const failed = () => this.fail(generation, new Error('Connection generation closed'));
        socket.once('close', failed);
        socket.once('error', failed);
        if (generation.mux !== undefined && generation.host !== undefined)
            this.publish(generation);
        return true;
    }
    dispose() {
        for (const generation of [...this.generations.values()]) {
            this.fail(generation, new Error('Connection service disposed'));
        }
        this.listeners.clear();
    }
    async open(request) {
        if (request.method !== 'POST')
            return new Response('method not allowed', { status: 405 });
        const options = request.headers.get('content-type')?.startsWith('application/json') === true
            ? await request.json()
            : {};
        if (!this.nativeEvents && options.nativeEvents === true && options.eventTransport !== 'gateway-v1') {
            return new Response('This host uses Gateway event streams; legacy browser event subscriptions require migration.', { status: 409 });
        }
        const id = randomUUID();
        this.generations.set(id, {
            id,
            kind: options.kind === 'node'
                ? 'node'
                : 'browser',
            failed: false,
            outgoing: new Map(),
            incoming: new Map(),
        });
        return Response.json({ id, ...(!this.nativeEvents ? { eventTransport: 'gateway-v1' } : {}) });
    }
    receive(generation, data) {
        if (generation.failed)
            return;
        let value;
        try {
            const text = socketText(data);
            value = JSON.parse(text);
        }
        catch (error) {
            this.fail(generation, error);
            return;
        }
        const frame = parseClientControlFrame(value);
        if (frame === undefined) {
            this.fail(generation, new Error('Malformed Connection control frame'));
            return;
        }
        if (frame.type === 'client-response') {
            const pending = generation.outgoing.get(frame.rpcId);
            if (pending === undefined)
                return;
            generation.outgoing.delete(frame.rpcId);
            pending.dispose();
            pending.resolve(frame.result);
            return;
        }
        if (frame.method === CONNECTION_CANCEL_METHOD) {
            generation.incoming.get(frame.rpcId)?.abort(new Error('Remote call cancelled'));
            generation.incoming.delete(frame.rpcId);
            return;
        }
        if (generation.outgoing.size + generation.incoming.size >= MAX_PENDING_CALLS
            || generation.incoming.has(frame.rpcId)) {
            this.fail(generation, new Error('Connection generation RPC limit reached'));
            return;
        }
        void this.dispatch(generation, frame);
    }
    async dispatch(generation, request) {
        const controller = new AbortController();
        generation.incoming.set(request.rpcId, controller);
        let result;
        try {
            result = this.dispatcher === undefined
                ? internalFailure('Host Connection dispatcher is not installed')
                : await this.dispatcher(request, generation.headers ?? {}, controller.signal);
        }
        catch (error) {
            result = internalFailure(error);
        }
        if (generation.incoming.get(request.rpcId) !== controller)
            return;
        generation.incoming.delete(request.rpcId);
        if (generation.host === undefined || generation.failed)
            return;
        void sendConnectionMessage(generation.host, {
            type: 'server-response',
            rpcId: request.rpcId,
            result,
        }).catch(error => this.fail(generation, error));
    }
    publish(generation) {
        if (generation.peer !== undefined || generation.failed)
            return;
        const peer = {
            id: generation.id,
            kind: generation.kind,
            call: (channel, endpoint, payload, signal) => this.call(generation, channel, endpoint, payload, signal),
        };
        generation.peer = peer;
        this.published.set(peer.id, peer);
        this.emit({ type: 'added', peer });
    }
    call(generation, channel, endpoint, payload, signal) {
        if (generation.failed || generation.host === undefined || generation.peer === undefined) {
            return Promise.reject(new Error('Connection peer is not active'));
        }
        if (signal?.aborted === true)
            return Promise.reject(signal.reason);
        if (generation.outgoing.size + generation.incoming.size >= MAX_PENDING_CALLS) {
            return Promise.reject(new Error('Connection generation RPC limit reached'));
        }
        const rpcId = randomUUID();
        return new Promise((resolve, reject) => {
            const aborted = () => {
                const pending = generation.outgoing.get(rpcId);
                if (pending === undefined)
                    return;
                generation.outgoing.delete(rpcId);
                pending.dispose();
                reject(signal?.reason);
                void sendConnectionMessage(generation.host, {
                    type: 'server-request',
                    rpcId,
                    method: CONNECTION_CANCEL_METHOD,
                    payload: null,
                }).catch(error => this.fail(generation, error));
            };
            const pending = {
                resolve,
                reject,
                dispose: () => signal?.removeEventListener('abort', aborted),
            };
            generation.outgoing.set(rpcId, pending);
            signal?.addEventListener('abort', aborted, { once: true });
            void sendConnectionMessage(generation.host, {
                type: 'server-request',
                rpcId,
                method: CONNECTION_RPC_METHOD,
                payload: { channel, endpoint, payload },
            }).catch(error => this.fail(generation, error));
        });
    }
    fail(generation, error) {
        if (generation.failed)
            return;
        generation.failed = true;
        this.generations.delete(generation.id);
        if (generation.peer !== undefined) {
            this.published.delete(generation.id);
            this.emit({ type: 'removed', peer: generation.peer });
        }
        for (const pending of generation.outgoing.values()) {
            pending.dispose();
            pending.reject(error);
        }
        generation.outgoing.clear();
        for (const controller of generation.incoming.values())
            controller.abort(error);
        generation.incoming.clear();
        for (const socket of [generation.mux, generation.host]) {
            if (socket !== undefined && (socket.readyState === 0 || socket.readyState === 1)) {
                socket.close(1008, 'connection generation closed');
            }
        }
    }
    emit(change) {
        for (const listener of [...this.listeners])
            listener(change);
    }
}
function socketText(data) {
    if (typeof data === 'string')
        return data;
    if (data instanceof ArrayBuffer)
        return Buffer.from(data).toString();
    if (ArrayBuffer.isView(data))
        return Buffer.from(data.buffer, data.byteOffset, data.byteLength).toString();
    if (Array.isArray(data) && data.every(value => ArrayBuffer.isView(value))) {
        return Buffer.concat(data.map(value => Buffer.from(value.buffer, value.byteOffset, value.byteLength))).toString();
    }
    throw new TypeError('Unsupported WebSocket frame');
}
//# sourceMappingURL=connection.js.map