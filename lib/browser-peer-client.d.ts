import type { TypertRemoteContribution } from '@deepseek-ai/dsh-typert-protocol';
import { type PeerRemoteApi } from './shared/peer-remote.js';
import { type RemoteEventHandler, type RemoteEventListener } from './shared/browser-events.js';
export type { RemoteEventRequest, RemoteEventHandler, RemoteEventListener } from './shared/browser-events.js';
export type BrowserPeerChannel = 'mux' | 'host';
/** Legacy event envelope. Domain payload types belong to the caller's DSH version. */
export interface BrowserPeerEvent<T = {
    type: string;
    [key: string]: unknown;
}> {
    channel: BrowserPeerChannel;
    envelope: {
        rpcId: string;
        payload: T;
    };
}
export interface BrowserPeerClientOptions {
    readonly baseUrl?: string | URL;
    readonly contribution: TypertRemoteContribution;
    readonly fetch?: typeof globalThis.fetch;
    readonly createWebSocket?: (url: string, protocol?: string) => WebSocket;
    /** Opt in to automatic reconnection; consumers must refetch state after onState(true). */
    readonly reconnectDelayMs?: number;
}
/** Standalone browser owner for one native DSH Connection and both event streams. */
export declare class BrowserPeerClient {
    private readonly ctx;
    private readonly fetch;
    private readonly baseUrl;
    private readonly contribution;
    private readonly createWebSocket;
    private readonly connection;
    private readonly projector;
    private readonly caller;
    private readonly eventListeners;
    private readonly stateListeners;
    private readonly remoteListeners;
    private readonly remoteHandlers;
    private legacySchemas?;
    private readonly reconnectDelayMs?;
    private reconnectTimer?;
    private stopped;
    private generation;
    private mounting;
    private disposeRemote;
    private connecting;
    private opening;
    readonly remote: PeerRemoteApi;
    constructor(options: BrowserPeerClientOptions);
    get connected(): boolean;
    get eventTransport(): 'legacy' | 'gateway-v1' | undefined;
    /** Native Gateway notification; event names and arguments are forwarded unchanged. */
    subscribe(event: string, listener: RemoteEventListener): () => void;
    /** Native Gateway waterfall, with generation-scoped cancellation and automatic replies. */
    handleEvent(event: string, handler: RemoteEventHandler): () => void;
    /** Open a Gateway stream, e.g. a session subscription; reconnect requires opening it again. */
    stream(endpoint: string, payload: unknown, signal?: AbortSignal): AsyncIterable<unknown>;
    connect(signal?: AbortSignal): Promise<void>;
    onEvent<T = BrowserPeerEvent['envelope']['payload']>(listener: (event: BrowserPeerEvent<T>) => void): () => void;
    onState(listener: (connected: boolean) => void): () => void;
    close(): Promise<void>;
    private open;
    private attach;
    private fail;
    private scheduleReconnect;
    private dropGeneration;
    private emitState;
    private call;
}
//# sourceMappingURL=browser-peer-client.d.ts.map