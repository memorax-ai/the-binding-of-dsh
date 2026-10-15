import type { Context } from '@deepseek-ai/cordis';
import type { IncomingMessage } from 'node:http';
import { HostConnectionBinding } from './connection.js';
export declare class ModernHostConnectionBinding extends HostConnectionBinding {
    constructor();
}
interface ModernConnection {
    bidirectional: HostConnectionBinding;
    requestRejection(request: IncomingMessage): number | undefined;
    admit?(request: {
        headers: Headers | IncomingMessage['headers'];
    }): {
        peer: {
            principal?: {
                kind: string;
            };
        };
    } | {
        rejection: number;
    };
}
/** Keep Binding RPC separate from Gateway's native stream multiplexing. */
export declare function installModernHost(ctx: Context, connection: ModernConnection, fetchHandler: {
    fetch(request: Request, peer?: unknown): Response | Promise<Response>;
}): void;
export {};
//# sourceMappingURL=modern.d.ts.map