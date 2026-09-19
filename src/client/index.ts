import type { Context } from '@deepseek-ai/cordis'
import { installClientGateway } from '../shared/client-gateway.js'
import { installModernClientBinding } from '../shared/modern-client.js'

export const inject = ['connection', 'typert']

/** Browser entrypoint loaded after the native Connection and Typert registry. */
export function apply(ctx: Context): void {
  installModernClientBinding(ctx)
  installClientGateway(ctx)
}

export { createClientConnectionBinding } from '../shared/client-connection.js'
export { installClientGateway }
