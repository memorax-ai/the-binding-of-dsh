/** One physical Gateway mux, with generation-scoped streams and waterfall requests. */
export class BrowserRemoteEvents {
    socket;
    call;
    listeners;
    handlers;
    failed;
    streams = new Map();
    pending = new Map();
    lifetime = new AbortController();
    clientId;
    disposed = false;
    constructor(socket, call, listeners, handlers, failed) {
        this.socket = socket;
        this.call = call;
        this.listeners = listeners;
        this.handlers = handlers;
        this.failed = failed;
        socket.addEventListener('message', event => {
            try {
                if (typeof event.data !== 'string')
                    throw new Error('Binary Gateway frame');
                const frame = JSON.parse(event.data);
                if (!record(frame) || typeof frame.streamId !== 'string')
                    throw new Error('Malformed Gateway frame');
                const stream = this.streams.get(frame.streamId);
                if (stream === undefined)
                    return; // Cancelled stream, possibly already in flight.
                if (frame.type === 'item') {
                    if (stream.done || stream.values.length >= 1024)
                        throw new Error('Gateway stream queue overflow or item after end');
                    stream.values.push(frame.value);
                }
                else if (frame.type === 'end')
                    stream.done = true;
                else if (frame.type === 'error' && record(frame.error) && typeof frame.error.message === 'string') {
                    stream.error = Object.assign(new Error(frame.error.message), frame.error);
                    stream.done = true;
                }
                else
                    throw new Error('Malformed Gateway stream frame');
                stream.wake?.();
            }
            catch (error) {
                this.fail(error);
            }
        });
        socket.addEventListener('close', () => this.fail(new Error('Gateway mux closed')), { once: true });
        socket.addEventListener('error', () => this.fail(new Error('Gateway mux failed')), { once: true });
    }
    async start(signal) {
        if (signal.aborted)
            throw signal.reason;
        const abort = () => this.dispose(signal.reason);
        signal.addEventListener('abort', abort, { once: true });
        try {
            if (this.socket.readyState !== 1)
                await new Promise((resolve, reject) => {
                    const finish = (error) => {
                        this.socket.removeEventListener('open', opened);
                        this.lifetime.signal.removeEventListener('abort', aborted);
                        error === undefined ? resolve() : reject(error);
                    };
                    const opened = () => finish();
                    const aborted = () => finish(this.lifetime.signal.reason);
                    this.socket.addEventListener('open', opened, { once: true });
                    this.lifetime.signal.addEventListener('abort', aborted, { once: true });
                    if (this.lifetime.signal.aborted)
                        aborted();
                });
            const events = this.stream('$events', { args: {} }, signal)[Symbol.asyncIterator]();
            const first = await events.next();
            const value = first.value;
            if (first.done || !record(value) || value.type !== 'ready' || typeof value.clientId !== 'string'
                || !value.clientId || !record(value.host) || typeof value.host.home !== 'string') {
                throw new Error('Gateway events did not provide a ready handshake');
            }
            this.clientId = value.clientId;
            void this.pump(events).catch(error => this.fail(error));
        }
        finally {
            signal.removeEventListener('abort', abort);
        }
    }
    async *stream(endpoint, payload, signal) {
        if (this.disposed)
            throw this.lifetime.signal.reason;
        if (signal?.aborted)
            throw signal.reason;
        if (this.streams.size >= 256)
            throw new Error('Gateway stream limit reached');
        const id = globalThis.crypto.randomUUID();
        const stream = { values: [], done: false };
        let cancelled = false;
        const cancel = () => {
            this.streams.delete(id);
            if (cancelled || this.disposed || this.socket.readyState !== 1)
                return;
            cancelled = true;
            try {
                this.socket.send(JSON.stringify({ type: 'cancel', streamId: id }));
            }
            catch (error) {
                this.fail(error);
            }
        };
        const abort = () => { stream.error = signal?.reason; stream.done = true; stream.wake?.(); cancel(); };
        this.streams.set(id, stream);
        signal?.addEventListener('abort', abort, { once: true });
        try {
            this.socket.send(JSON.stringify({ type: 'open', streamId: id, endpoint, payload }));
            while (true) {
                if (stream.error !== undefined)
                    throw stream.error;
                if (stream.values.length) {
                    yield stream.values.shift();
                    continue;
                }
                if (stream.done)
                    return;
                await new Promise(resolve => { stream.wake = resolve; });
                stream.wake = undefined;
            }
        }
        finally {
            signal?.removeEventListener('abort', abort);
            cancel();
        }
    }
    dispose(reason = new Error('Gateway generation closed')) {
        if (this.disposed)
            return;
        this.disposed = true;
        this.lifetime.abort(reason);
        for (const pending of this.pending.values())
            pending.abort(reason);
        this.pending.clear();
        for (const stream of this.streams.values()) {
            stream.error = reason;
            stream.done = true;
            stream.wake?.();
        }
        this.streams.clear();
        this.socket.close();
    }
    fail(error) {
        if (this.disposed)
            return;
        this.dispose(error);
        this.failed(error instanceof Error ? error : new Error(String(error)));
    }
    async pump(events) {
        while (!this.disposed) {
            const next = await events.next();
            if (next.done)
                throw new Error('Gateway event stream ended');
            const frame = next.value;
            if (!record(frame))
                throw new Error('Malformed Remote event');
            if (frame.type === 'emit' && typeof frame.event === 'string' && Array.isArray(frame.args)) {
                for (const listener of this.listeners.get(frame.event) ?? []) {
                    void Promise.resolve().then(() => listener(...frame.args)).catch(error => {
                        console.error('[the-binding-of-dsh] Remote event listener failed:', error);
                    });
                }
            }
            else if (frame.type === 'cancel' && typeof frame.eventId === 'string') {
                this.pending.get(frame.eventId)?.abort(new Error('Remote event cancelled'));
                this.pending.delete(frame.eventId);
            }
            else if (frame.type === 'waterfall' && typeof frame.event === 'string'
                && typeof frame.eventId === 'string' && typeof frame.agentId === 'string' && record(frame.request)
                && !Object.hasOwn(frame.request, 'agent') && !Object.hasOwn(frame.request, 'signal')) {
                if (this.pending.has(frame.eventId) || this.pending.size >= 1024)
                    throw new Error('Duplicate or excessive Remote requests');
                void this.dispatch(frame.event, frame.eventId, frame.agentId, frame.request).catch(error => this.fail(error));
            }
            else
                throw new Error('Malformed Remote event');
        }
    }
    async dispatch(event, eventId, agentId, request) {
        const abort = new AbortController();
        this.pending.set(eventId, abort);
        let outcome = { kind: 'next' };
        try {
            for (const handler of this.handlers.get(event) ?? []) {
                const value = await handler({ eventId, agentId, request, signal: abort.signal });
                if (abort.signal.aborted)
                    return;
                if (value !== undefined) {
                    outcome = { kind: 'result', value };
                    break;
                }
            }
        }
        catch (error) {
            outcome = { kind: 'rejected', error: { name: error instanceof Error ? error.name : 'Error', message: error instanceof Error ? error.message : String(error) } };
        }
        try {
            if (abort.signal.aborted || this.disposed)
                return;
            const result = await this.call('$events/result', { args: { clientId: this.clientId, eventId, outcome } }, abort.signal);
            if (!result.ok)
                throw new Error('Gateway rejected Remote event result');
        }
        catch (error) {
            if (!abort.signal.aborted)
                throw error;
        }
        finally {
            if (this.pending.get(eventId) === abort)
                this.pending.delete(eventId);
        }
    }
}
function record(value) {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}
//# sourceMappingURL=browser-events.js.map