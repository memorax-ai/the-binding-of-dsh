// Built against the pinned legacy development dependency. The published chunk
// is self-contained so a modern DSH installation need not install api-proxy.
export { serverRequestSchema } from '@deepseek-ai/dsh-host-apiproxy/api/rpc.schema'
export { hostFrameSchema, muxFrameSchema } from '@deepseek-ai/dsh-host-apiproxy/api/events.schema'
