/**
 * Worker entry module: OTLP/HTTP traces receiver for Claude Platform.
 *
 * POST <url>/v1/traces (protobuf, optionally gzip) -> one record per span
 * -> Pipelines stream -> SQL pipeline -> R2 sink (gzipped JSON lines).
 */
import type { ReceiverEnv } from "../alchemy.run.ts";
import { type Env, handle } from "./handler.ts";

// Compile-time check: the env that Alchemy infers from the stack has the
// bindings the handler needs, with the right names and types.
const asHandlerEnv = (env: ReceiverEnv): Env => env;

export default {
  fetch: (request: Request, env: ReceiverEnv) => handle(request, asHandlerEnv(env)),
};
