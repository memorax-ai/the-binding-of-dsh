import { createClientConnectionBinding } from './client-connection.js';
/** Native RPC keeps its carrier; only reverse calls use Binding sockets. */
export function installModernClientBinding(ctx) {
    const connection = ctx.get('connection');
    if (connection.rpc.intercept !== undefined || connection.generation === undefined)
        return;
    const binding = createClientConnectionBinding();
    connection.rpc.intercept = binding.intercept;
    ctx.effect(() => {
        let disposed = false;
        let abort;
        let retry;
        let closeSockets;
        const stop = () => {
            clearTimeout(retry);
            abort?.abort(new Error('Native connection generation changed'));
            closeSockets?.();
            closeSockets = undefined;
        };
        const open = async () => {
            stop();
            if (disposed || connection.generation?.getSnapshot() === undefined)
                return;
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
                    for (const socket of sockets)
                        socket.close();
                };
                const failed = () => {
                    if (controller.signal.aborted || disposed)
                        return;
                    stop();
                    retry = setTimeout(() => { void open(); }, 1000);
                };
                for (const kind of ['mux', 'host']) {
                    const url = new URL(`/api/events.${kind}`, globalThis.location.origin);
                    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
                    const socket = new WebSocket(url, generation.id);
                    sockets.push(socket);
                    if (kind === 'host') {
                        binding.attach(generation, kind, socket);
                        socket.addEventListener('message', event => {
                            if (controller.signal.aborted)
                                return;
                            try {
                                if (!binding.handle(JSON.parse(event.data), generation))
                                    failed();
                            }
                            catch {
                                failed();
                            }
                        });
                    }
                    socket.addEventListener('close', failed, { once: true });
                    socket.addEventListener('error', failed, { once: true });
                }
            }
            catch {
                if (!controller.signal.aborted && !disposed)
                    retry = setTimeout(() => { void open(); }, 1000);
            }
        };
        const unsubscribe = connection.generation.subscribe(() => { void open(); });
        void open();
        return () => {
            disposed = true;
            unsubscribe();
            stop();
            if (connection.rpc.intercept === binding.intercept)
                delete connection.rpc.intercept;
        };
    }, 'binding: reverse RPC generation');
}
//# sourceMappingURL=modern-client.js.map