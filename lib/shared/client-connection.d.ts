import { type RpcResult } from './protocol.js';
export type ClientConnectionHandler = (endpoint: string, payload: unknown, signal: AbortSignal) => Promise<RpcResult<unknown>>;
export interface ClientConnectionGeneration {
    readonly id: string;
    readonly eventTransport?: 'gateway-v1';
}
interface ClientSocket {
    readonly readyState: number;
    readonly bufferedAmount?: number;
    addEventListener(event: 'open', listener: () => void, options?: {
        once?: boolean;
    }): void;
    send(data: string): void;
    close(code?: number, reason?: string): void;
}
export interface ClientConnectionBinding {
    readonly intercept: (channel: string, matches: (endpoint: string) => boolean, handler: ClientConnectionHandler) => () => void;
    readonly call: (channel: string, endpoint: string, payload: unknown, signal?: AbortSignal) => Promise<RpcResult<unknown>>;
    open(signal?: AbortSignal): Promise<ClientConnectionGeneration>;
    attach(generation: ClientConnectionGeneration | undefined, kind: 'mux' | 'host', socket: ClientSocket): void;
    release(generation: ClientConnectionGeneration | undefined): void;
    handle(message: unknown, generation: ClientConnectionGeneration | undefined): boolean;
}
export interface ClientConnectionBindingOptions {
    nativeEvents?: boolean;
    eventTransport?: 'gateway-v1';
    fetch?: typeof globalThis.fetch;
    baseUrl?: () => string;
    kind?: 'browser' | 'node';
}
export declare function createClientConnectionBinding(options?: ClientConnectionBindingOptions): ClientConnectionBinding;
declare module '@deepseek-ai/dsh-client-connection/client' {
    interface ClientConnectionRpc {
        intercept(channel: string, matches: (endpoint: string) => boolean, handler: ClientConnectionHandler): () => void;
    }
}
export {};
//# sourceMappingURL=client-connection.d.ts.map