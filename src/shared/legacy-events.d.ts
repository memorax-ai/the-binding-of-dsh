// Declaration for the self-contained legacy chunk produced by build-legacy.mjs.
interface Schema<T> { parse(value: unknown): T }
type Frame = { type: string; [key: string]: unknown }
export const serverRequestSchema: Schema<{ rpcId: string; payload: unknown }>
export const hostFrameSchema: Schema<Frame>
export const muxFrameSchema: Schema<Frame>
