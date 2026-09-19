import * as Protocol from '@deepseek-ai/dsh-typert-protocol'
import { Service } from '@deepseek-ai/cordis'
import type { Context } from '@deepseek-ai/cordis'
import {
  remoteMethods,
  type TypertRemoteContribution,
} from '@deepseek-ai/dsh-typert-protocol'
import {
  createGatewayDispatcher,
  type GatewayDispatcher,
  type GatewayErrorCode,
  type GatewayErrorOptions,
} from '../shared/gateway-dispatcher.js'
import {
  PeerRemoteProjector,
  type HostPeerRemote,
  type PeerRemoteApi,
} from '../shared/peer-remote.js'
import type { ConnectionPeer } from './connection.js'

type GatewayErrorFactory = (
  code: GatewayErrorCode,
  endpoint: string,
  message: string,
  options?: GatewayErrorOptions,
) => Error

export function createHostGatewayDispatcher(
  ctx: Context,
  createError: GatewayErrorFactory,
): GatewayDispatcher {
  return createGatewayDispatcher(ctx, {
    createError,
    remoteMethods,
    lookupFailure: error => {
      const protocol = Protocol as unknown as {
        TypertLookupFailure?: new (...args: never[]) => Error & { failure: { code: string; message: string; details: unknown } }
        remoteErrorOf?: (error: unknown) => { code: string; message: string; details: unknown } | undefined
      }
      if (protocol.remoteErrorOf !== undefined) return protocol.remoteErrorOf(error)
      return protocol.TypertLookupFailure !== undefined && error instanceof protocol.TypertLookupFailure
        ? error.failure : undefined
    },
  })
}

export class HostRemoteService extends Service implements HostPeerRemote {
  private readonly ownerCtx: Context
  private readonly projector: PeerRemoteProjector

  constructor(ctx: Context) {
    super(ctx, 'remote')
    this.ownerCtx = ctx
    this.projector = new PeerRemoteProjector(ctx)
  }

  for(peer: ConnectionPeer): PeerRemoteApi {
    return this.projector.bind(peer, this.ctx)
  }

  $mount(contribution: TypertRemoteContribution) {
    return this.projector.mount(this.ctx, contribution)
  }
}

declare module '@deepseek-ai/cordis' {
  interface Context {
    remote: HostPeerRemote
  }
}
