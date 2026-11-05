/**
 * Request handling for the OTLP receiver. worker.ts is the entry module.
 *
 * POST <url>/v1/traces (protobuf, optionally gzip) -> one record per span
 * -> Pipelines stream -> SQL pipeline -> R2 sink (gzipped JSON lines).
 */
import { decodeRequest, packBatches, spanRecords, type SpanRecord } from "./otlp.ts";

/** What the handler needs from `env`. Bindings are declared in alchemy.run.ts. */
export interface Env {
  STREAM: { send(records: object[]): Promise<void> };
  BUCKET: { put(key: string, value: ArrayBuffer, options?: R2PutOptions): Promise<unknown> };
  AUTH_TOKEN: string;
  OVERSIZED_PREFIX?: string;
}

/** Pipelines limit per ingest request is 5 MB. Keep a margin. */
export const MAX_SEND_BYTES = 4_800_000;
const SEND_ATTEMPTS = 3;

const encoder = new TextEncoder();

async function tokenMatches(given: string, expected: string): Promise<boolean> {
  // Hash both so the compare runs on equal-length inputs.
  const [a, b] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder.encode(given)),
    crypto.subtle.digest("SHA-256", encoder.encode(expected)),
  ]);
  const subtle = crypto.subtle as SubtleCrypto & {
    timingSafeEqual?: (a: ArrayBuffer, b: ArrayBuffer) => boolean;
  };
  if (typeof subtle.timingSafeEqual === "function") return subtle.timingSafeEqual(a, b);
  const x = new Uint8Array(a);
  const y = new Uint8Array(b);
  let diff = 0;
  for (let i = 0; i < x.length; i++) diff |= x[i]! ^ y[i]!;
  return diff === 0;
}

async function readBody(request: Request): Promise<Uint8Array> {
  const enc = (request.headers.get("content-encoding") ?? "").trim().toLowerCase();
  if (enc === "" || enc === "identity") return new Uint8Array(await request.arrayBuffer());
  if (enc !== "gzip" && enc !== "deflate") throw new HttpError(415, `unsupported content-encoding: ${enc}`);
  if (!request.body) return new Uint8Array();
  const stream = request.body.pipeThrough(new DecompressionStream(enc));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

async function gzip(text: string): Promise<ArrayBuffer> {
  const stream = new Blob([text]).stream().pipeThrough(new CompressionStream("gzip"));
  return new Response(stream).arrayBuffer();
}

class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

const text = (status: number, body: string, headers: Record<string, string> = {}) =>
  new Response(body, { status, headers: { "content-type": "text/plain", ...headers } });

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function sendWithRetry(env: Env, batch: SpanRecord[]): Promise<void> {
  let last: unknown;
  for (let attempt = 0; attempt < SEND_ATTEMPTS; attempt++) {
    try {
      await env.STREAM.send(batch);
      return;
    } catch (err) {
      last = err;
      await sleep(100 * 2 ** attempt);
    }
  }
  throw last;
}

/** A span too large for one ingest request goes straight to R2, gzipped. */
async function storeOversized(env: Env, rec: SpanRecord, bytes: number): Promise<string> {
  const now = new Date().toISOString();
  const [date, time] = now.split("T") as [string, string];
  const [y, m, d] = date.split("-");
  const hh = time.slice(0, 2);
  const prefix = env.OVERSIZED_PREFIX ?? "otel/_oversized/";
  const key = `${prefix}year=${y}/month=${m}/day=${d}/hour=${hh}/${rec.trace_id}-${rec.span_id}-${crypto.randomUUID().slice(0, 8)}.json.gz`;
  await env.BUCKET.put(key, await gzip(JSON.stringify(rec) + "\n"), {
    httpMetadata: { contentType: "application/x-ndjson", contentEncoding: "gzip" },
  });
  console.warn(JSON.stringify({ msg: "oversized span written to R2", key, bytes }));
  return key;
}

export async function handle(request: Request, env: Env): Promise<Response> {
  if (request.method !== "POST") return text(405, "method not allowed", { allow: "POST" });

  const auth = request.headers.get("authorization") ?? "";
  if (!env.AUTH_TOKEN || !(await tokenMatches(auth, `Bearer ${env.AUTH_TOKEN}`))) {
    return text(401, "unauthorized");
  }

  const ctype = (request.headers.get("content-type") ?? "application/x-protobuf").split(";")[0]!.trim();
  if (ctype !== "application/x-protobuf" && ctype !== "application/protobuf") {
    // Claude Platform sends protobuf. OTLP/JSON is not handled here.
    return text(415, `unsupported content-type: ${ctype}`);
  }

  let records: SpanRecord[];
  try {
    const body = await readBody(request);
    records = spanRecords(decodeRequest(body), new Date().toISOString());
  } catch (err) {
    if (err instanceof HttpError) return text(err.status, err.message);
    // Malformed payload: 400 tells the sender not to retry.
    return text(400, `bad request: ${(err as Error).message}`);
  }

  const { batches, oversized } = packBatches(records, MAX_SEND_BYTES);
  try {
    for (const { record, bytes } of oversized) await storeOversized(env, record, bytes);
    for (const batch of batches) await sendWithRetry(env, batch);
  } catch (err) {
    console.error(JSON.stringify({ msg: "ingest failed", error: String(err), spans: records.length }));
    // Retryable per the OTLP spec. A retry can repeat spans that did land.
    return text(503, "ingest unavailable", { "retry-after": "5" });
  }

  // An empty ExportTraceServiceResponse encodes to zero bytes.
  return new Response(null, { status: 200, headers: { "content-type": "application/x-protobuf" } });
}
