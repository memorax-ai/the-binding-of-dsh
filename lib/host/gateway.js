import * as Protocol from '@deepseek-ai/dsh-typert-protocol';
import { Service } from '@deepseek-ai/cordis';
import { remoteMethods, } from '@deepseek-ai/dsh-typert-protocol';
import { createGatewayDispatcher, } from '../shared/gateway-dispatcher.js';
import { PeerRemoteProjector, } from '../shared/peer-remote.js';
export function createHostGatewayDispatcher(ctx, createError) {
    return createGatewayDispatcher(ctx, {
        createError,
        remoteMethods,
        lookupFailure: error => {
            const protocol = Protocol;
            if (protocol.remoteErrorOf !== undefined)
                return protocol.remoteErrorOf(error);
            return protocol.TypertLookupFailure !== undefined && error instanceof protocol.TypertLookupFailure
                ? error.failure : undefined;
        },
    });
}
export class HostRemoteService extends Service {
    ownerCtx;
    projector;
    constructor(ctx) {
        super(ctx, 'remote');
        this.ownerCtx = ctx;
        this.projector = new PeerRemoteProjector(ctx);
    }
    for(peer) {
        return this.projector.bind(peer, this.ctx);
    }
    $mount(contribution) {
        return this.projector.mount(this.ctx, contribution);
    }
}
//# sourceMappingURL=gateway.js.map