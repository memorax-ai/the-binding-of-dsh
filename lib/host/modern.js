import { WebSocketServer } from 'ws';
import { createHostFetchDispatcher, HostConnectionBinding } from './connection.js';
export class ModernHostConnectionBinding extends HostConnectionBinding {
    constructor() { super(false); }
}
/** Keep Binding RPC separate from Gateway's native stream multiplexing. */
export function installModernHost(ctx, connection, fetchHandler) {
    connection.bidirectional.setDispatcher(createHostFetchDispatcher({
        fetch(request) {
            const admission = connection.admit?.(request);
            if (admission && 'rejection' in admission)
                return new Response('unauthorized', { status: admission.rejection });
            return fetchHandler.fetch(request, admission?.peer);
        },
    }));
    ctx.inject(['webServer'], webCtx => {
        const webServer = webCtx.get('webServer');
        webCtx.effect(() => {
            const server = new WebSocketServer({ noServer: true, maxPayload: 8 * 1024 * 1024 });
            const unregister = [];
            try {
                for (const kind of ['mux', 'host']) {
                    unregister.push(webServer.registerUpgrade({
                        path: `/api/events.${kind}`,
                        handler(request, socket, head) {
                            const admission = connection.admit?.(request);
                            const rejection = admission === undefined ? connection.requestRejection(request)
                                : 'rejection' in admission ? admission.rejection
                                    : admission.peer.principal === undefined || admission.peer.principal.kind === 'owner' ? undefined : 403;
                            if (rejection !== undefined) {
                                socket.end(`HTTP/1.1 ${rejection} Rejected\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
                                return;
                            }
                            server.handleUpgrade(request, socket, head, websocket => {
                                connection.bidirectional.attach(kind, request, websocket);
                            });
                        },
                    }));
                }
            }
            catch (error) {
                for (const dispose of unregister)
                    dispose();
                server.close();
                throw error;
            }
            return () => {
                for (const dispose of unregister)
                    dispose();
                connection.bidirectional.dispose();
                for (const socket of server.clients)
                    socket.terminate();
                server.close();
            };
        }, 'binding: authenticated bidirectional transport');
    });
}
//# sourceMappingURL=modern.js.map