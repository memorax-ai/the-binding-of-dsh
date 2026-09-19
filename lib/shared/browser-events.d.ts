import type { RpcResult } from './protocol.js';
export interface RemoteEventRequest {
    readonly eventId: string;
    readonly agentId: string;
    readonly request: Readonly<Record<string, unknown>>;
    readonly signal: AbortSignal;
}
/** Return undefined to continue to the next handler (including Host handlers). */
export type RemoteEventHandler = (request: RemoteEventRequest) => unknown | Promise<unknown>;
export type RemoteEventListener = (...args: unknown[]) => void | Promise<void>;
/** One physical Gateway mux, with generation-scoped streams and waterfall requests. */
export declare class BrowserRemoteEvents {
    private readonly socket;
    private readonly call;
    private readonly listeners;
    private readonly handlers;
    private readonly failed;
    private readonly streams;
    private readonly pending;
    private readonly lifetime;
    private clientId?;
    private disposed;
    constructor(socket: WebSocket, call: (endpoint: string, payload: unknown, signal: AbortSignal) => Promise<RpcResult<unknown>>, listeners: Map<string, Set<RemoteEventListener>>, handlers: Map<string, Set<RemoteEventHandler>>, failed: (error: Error) => void);
    start(signal: AbortSignal): Promise<void>;
    stream(endpoint: string, payload: unknown, signal?: AbortSignal): AsyncGenerator<unknown>;
    dispose(reason?: unknown): void;
    private fail;
    private pump;
    private dispatch;
}
//# sourceMappingURL=browser-events.d.ts.map