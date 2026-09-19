import { Context } from '@deepseek-ai/cordis';
import TypertRegistry from '@deepseek-ai/dsh-typert-registry';
import WebSocket from 'ws';
import { PeerRemoteProjector, } from './shared/peer-remote.js';
import { createClientConnectionBinding, } from './shared/client-connection.js';
import {} from './shared/protocol.js';
const MUX_PATH = '/api/events.mux';
const HOST_PATH = '/api/events.host';
export class NodePeerClient {
    ctx = new Context();
    fetch;
    baseUrl;
    contribution;
    createWebSocket;
    connection;
    projector;
    caller;
    generation;
    mounting;
    disposeRemote;
    connecting;
    opening;
    remote;
    constructor(options) {
        new TypertRegistry(this.ctx);
        this.fetch = options.fetch ?? globalThis.fetch;
        this.baseUrl = new URL(options.baseUrl);
        this.contribution = options.contribution;
        this.createWebSocket = options.createWebSocket
            ?? ((url, protocol) => new WebSocket(url, protocol));
        this.connection = createClientConnectionBinding({
            fetch: this.fetch,
            baseUrl: () => this.baseUrl.href,
            kind: 'node',
        });
        this.projector = new PeerRemoteProjector(this.ctx);
        this.caller = { call: (channel, endpoint, payload, signal) => (this.call(channel, endpoint, payload, signal)) };
        this.remote = this.projector.bind(this.caller, this.ctx);
    }
    intercept(channel, matches, handler) {
        return this.connection.intercept(channel, matches, handler);
    }
    connect(signal) {
        if (this.generation?.active === true)
            return Promise.resolve();
        if (this.connecting !== undefined)
            return this.connecting;
        const opening = new AbortController();
        this.opening = opening;
        const combined = signal === undefined
            ? opening.signal
            : AbortSignal.any([opening.signal, signal]);
        this.connecting = this.open(combined).finally(() => {
            if (this.opening === opening)
                this.opening = undefined;
            this.connecting = undefined;
        });
        return this.connecting;
    }
    async close() {
        this.opening?.abort(new Error('Node peer client closed'));
        await this.connecting?.catch(() => undefined);
        this.dropGeneration(new Error('Node peer client closed'));
        await this.withdrawRemote();
    }
    dropGeneration(reason) {
        const generation = this.generation;
        this.generation = undefined;
        if (generation !== undefined) {
            generation.active = false;
            generation.abort.abort(reason);
            this.connection.release(generation.connection);
            generation.mux.close();
            generation.host.close();
        }
    }
    async withdrawRemote() {
        const disposeRemote = this.disposeRemote;
        this.disposeRemote = undefined;
        this.mounting = undefined;
        await disposeRemote?.();
    }
    async open(signal) {
        if (this.generation !== undefined) {
            this.dropGeneration(new Error('Node peer connection replaced'));
        }
        this.mounting ??= this.projector.mount(this.ctx, this.contribution);
        this.disposeRemote = await this.mounting;
        try {
            const connection = await this.connection.open(signal);
            const abort = new AbortController();
            const generation = {
                connection,
                mux: this.createWebSocket(webSocketUrl(this.baseUrl, MUX_PATH), connection.id),
                host: this.createWebSocket(webSocketUrl(this.baseUrl, HOST_PATH), connection.id),
                abort,
                active: false,
            };
            this.generation = generation;
            this.connection.attach(connection, 'host', generation.host);
            const receive = (event) => {
                if (this.generation !== generation || abort.signal.aborted)
                    return;
                try {
                    const raw = JSON.parse(webSocketText(event.data));
                    if (this.connection.handle(raw, generation.connection))
                        return;
                    // Native event frames are not consumed by this standalone RPC peer.
                    if (raw?.type !== 'server-request')
                        throw new Error('Unexpected peer frame');
                }
                catch (error) {
                    console.error('[the-binding-of-dsh] dropping malformed Node peer frame:', error);
                }
            };
            const failed = () => this.fail(generation, new Error('Node peer connection closed'));
            generation.mux.addEventListener('message', receive);
            generation.host.addEventListener('message', receive);
            generation.mux.addEventListener('close', failed);
            generation.host.addEventListener('close', failed);
            generation.mux.addEventListener('error', failed);
            generation.host.addEventListener('error', failed);
            await Promise.all([
                waitForOpen(generation.mux, signal),
                waitForOpen(generation.host, signal),
            ]);
            if (this.generation !== generation || abort.signal.aborted)
                throw abort.signal.reason;
            generation.active = true;
        }
        catch (error) {
            this.dropGeneration(new Error('Node peer connection failed'));
            await this.withdrawRemote();
            throw error;
        }
    }
    async call(channel, endpoint, payload, signal) {
        const generation = this.generation;
        if (generation?.active !== true)
            throw new Error('Node peer client is not connected');
        const requestSignal = signal === undefined
            ? generation.abort.signal
            : AbortSignal.any([generation.abort.signal, signal]);
        return this.connection.call(channel, endpoint, payload, requestSignal);
    }
    fail(generation, error) {
        if (this.generation !== generation || generation.abort.signal.aborted)
            return;
        this.dropGeneration(error);
    }
}
function webSocketUrl(baseUrl, path) {
    const url = new URL(path, baseUrl);
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
    return url.href;
}
function webSocketText(data) {
    if (typeof data === 'string')
        return data;
    if (data instanceof ArrayBuffer)
        return Buffer.from(data).toString();
    if (ArrayBuffer.isView(data)) {
        return Buffer.from(data.buffer, data.byteOffset, data.byteLength).toString();
    }
    throw new TypeError('binary WebSocket frame');
}
function waitForOpen(socket, signal) {
    if (socket.readyState === 1)
        return Promise.resolve();
    return new Promise((resolve, reject) => {
        const opened = () => finish(resolve);
        const failed = () => finish(() => reject(new Error('WebSocket failed before opening')));
        const aborted = () => finish(() => reject(signal.reason));
        const finish = (settle) => {
            socket.removeEventListener('open', opened);
            socket.removeEventListener('close', failed);
            socket.removeEventListener('error', failed);
            signal.removeEventListener('abort', aborted);
            settle();
        };
        socket.addEventListener('open', opened, { once: true });
        socket.addEventListener('close', failed, { once: true });
        socket.addEventListener('error', failed, { once: true });
        signal.addEventListener('abort', aborted, { once: true });
        if (signal.aborted)
            aborted();
    });
}
//# sourceMappingURL=node-peer-client.js.map