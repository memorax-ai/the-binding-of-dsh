import { CONNECTION_RPC_METHOD, type ClientRequest, type RpcResult } from '../shared/protocol.js';
export interface ConnectionPeer {
    readonly id: string;
    readonly kind: 'browser' | 'node';
    call(channel: string, endpoint: string, payload: unknown, signal?: AbortSignal): Promise<RpcResult<unknown>>;
}
export type PeerChange = {
    type: 'added';
    peer: ConnectionPeer;
} | {
    type: 'removed';
    peer: ConnectionPeer;
};
export interface HostConnectionPeers {
    get(id: string): ConnectionPeer | undefined;
    list(): readonly ConnectionPeer[];
    subscribe(listener: (change: PeerChange) => void): () => void;
}
interface SocketLike {
    readonly readyState: number;
    send(data: string, callback: (error?: Error | null) => void): void;
    close(code?: number, reason?: string): void;
    once(event: 'close' | 'error', listener: () => void): void;
    on(event: 'message', listener: (data: unknown) => void): void;
}
export type HostConnectionDispatcher = (request: ClientRequest & {
    method: typeof CONNECTION_RPC_METHOD;
}, headers: Record<string, unknown>, signal: AbortSignal) => Promise<RpcResult<unknown>>;
interface FetchHandler {
    fetch(request: Request): Response | Promise<Response>;
}
export declare function sendConnectionMessage(socket: SocketLike, message: unknown): Promise<void>;
export declare function createHostFetchDispatcher(fetchHandler: FetchHandler): HostConnectionDispatcher;
export interface ReverseConnectionHost {
    readonly peers: HostConnectionPeers;
    fetch(request: Request): Promise<Response> | undefined;
    setDispatcher(dispatcher: HostConnectionDispatcher): void;
    attach(kind: 'mux' | 'host', request: {
        headers: Record<string, unknown>;
    }, socket: SocketLike): boolean;
    dispose(): void;
}
export declare class HostConnectionBinding implements ReverseConnectionHost {
    private readonly nativeEvents;
    constructor(nativeEvents?: boolean);
    private readonly generations;
    private readonly published;
    private readonly listeners;
    private dispatcher;
    readonly peers: HostConnectionPeers;
    setDispatcher(dispatcher: HostConnectionDispatcher): void;
    fetch(request: Request): Promise<Response> | undefined;
    attach(kind: 'mux' | 'host', request: {
        headers: Record<string, unknown>;
    }, socket: SocketLike): boolean;
    dispose(): void;
    private open;
    private receive;
    private dispatch;
    private publish;
    private call;
    private fail;
    private emit;
}
declare module '@deepseek-ai/dsh-client-connection' {
    interface HostConnectionHandle {
        readonly peers: HostConnectionPeers;
    }
}
export {};
//# sourceMappingURL=connection.d.ts.map