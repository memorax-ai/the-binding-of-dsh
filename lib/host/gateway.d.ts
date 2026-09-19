import * as Protocol from '@deepseek-ai/dsh-typert-protocol';
import { Service } from '@deepseek-ai/cordis';
import type { Context } from '@deepseek-ai/cordis';
import { type TypertRemoteContribution } from '@deepseek-ai/dsh-typert-protocol';
import { type GatewayDispatcher, type GatewayErrorCode, type GatewayErrorOptions } from '../shared/gateway-dispatcher.js';
import { type HostPeerRemote, type PeerRemoteApi } from '../shared/peer-remote.js';
import type { ConnectionPeer } from './connection.js';
type GatewayErrorFactory = (code: GatewayErrorCode, endpoint: string, message: string, options?: GatewayErrorOptions) => Error;
export declare function createHostGatewayDispatcher(ctx: Context, createError: GatewayErrorFactory): GatewayDispatcher;
export declare class HostRemoteService extends Service implements HostPeerRemote {
    private readonly ownerCtx;
    private readonly projector;
    constructor(ctx: Context);
    for(peer: ConnectionPeer): PeerRemoteApi;
    $mount(contribution: TypertRemoteContribution): Promise<Protocol.TypertDisposer>;
}
declare module '@deepseek-ai/cordis' {
    interface Context {
        remote: HostPeerRemote;
    }
}
export {};
//# sourceMappingURL=gateway.d.ts.map