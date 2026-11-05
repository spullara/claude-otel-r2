/**
 * Handler tests. Run: npm test
 *
 * basic.bin is encoded by the official Python opentelemetry-proto package
 * (scripts/make_fixtures.py), so decoding is checked against an independent
 * encoder. basic.lambda.json is what the AWS Lambda version produced for it.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { gunzipSync } from "node:zlib";
import { test } from "node:test";
import { create, toBinary } from "@bufbuild/protobuf";
import { ExportTraceServiceRequestSchema } from "../src/gen/opentelemetry/proto/collector/trace/v1/trace_service_pb.ts";
import { type Env, handle, MAX_SEND_BYTES } from "../src/handler.ts";
import { SPAN_STREAM_SCHEMA } from "../src/stream-schema.ts";

const TOKEN = "t".repeat(64);
const fixturePath = (name: string) => join(import.meta.dirname, "fixtures", name);
const fixture = (name: string) => new Uint8Array(readFileSync(fixturePath(name)));

interface Put {
  key: string;
  value: ArrayBuffer;
  options?: R2PutOptions;
}

function fakeEnv(opts: { failSends?: number } = {}) {
  let failSends = opts.failSends ?? 0;
  const sent: object[][] = [];
  const puts: Put[] = [];
  const env: Env = {
    AUTH_TOKEN: TOKEN,
    OVERSIZED_PREFIX: "otel/_oversized/",
    STREAM: {
      async send(records) {
        if (failSends > 0) {
          failSends--;
          throw new Error("pipelines unavailable");
        }
        sent.push(structuredClone(records));
      },
    },
    BUCKET: {
      async put(key, value, options) {
        puts.push({ key, value, options });
        return {};
      },
    },
  };
  return { env, sent, puts, records: () => sent.flat() as Record<string, any>[] };
}

const post = (body: BodyInit | null, headers: Record<string, string> = {}, method = "POST") =>
  new Request("https://receiver.example.workers.dev/v1/traces", {
    method,
    body: method === "GET" ? null : body,
    headers: { "content-type": "application/x-protobuf", authorization: `Bearer ${TOKEN}`, ...headers },
  });

/** Build a request with n spans in TypeScript (for the large cases). */
function bigRequest(n: number, bigAttr?: string): Uint8Array {
  const spans = Array.from({ length: n }, (_, i) => ({
    traceId: new Uint8Array(16).fill(0xab),
    spanId: new Uint8Array(8).fill(i % 255 + 1),
    name: `span.${i}`,
    kind: 1,
    startTimeUnixNano: 1_762_131_000_000_000_000n + BigInt(i),
    endTimeUnixNano: 1_762_131_000_500_000_000n + BigInt(i),
    attributes: [
      { key: "i", value: { value: { case: "intValue" as const, value: BigInt(i) } } },
      ...(bigAttr && i === 0 ? [{ key: "gen_ai.prompt", value: { value: { case: "stringValue" as const, value: bigAttr } } }] : []),
    ],
  }));
  const req = create(ExportTraceServiceRequestSchema, {
    resourceSpans: [{ resource: { attributes: [] }, scopeSpans: [{ scope: { name: "t" }, spans }] }],
  });
  return toBinary(ExportTraceServiceRequestSchema, req);
}

test("protobuf request: 200, empty protobuf body, one record per span", async () => {
  const f = fakeEnv();
  const res = await handle(post(fixture("basic.bin")), f.env);
  assert.equal(res.status, 200);
  assert.equal(res.headers.get("content-type"), "application/x-protobuf");
  assert.equal((await res.arrayBuffer()).byteLength, 0);
  const recs = f.records();
  assert.equal(recs.length, 3);
  const [root, child, errored] = recs as [any, any, any];
  assert.equal(root.trace_id, "0af7651916cd43dd8448eb211c80319c");
  assert.equal(root.span_id, "0000000000000001");
  assert.equal("parent_span_id" in root, false);
  assert.equal(child.parent_span_id, "0000000000000001");
  assert.equal(root.kind, "SPAN_KIND_SERVER");
  assert.equal(root.start_time, "2025-11-03T00:50:00.123456789Z");
  assert.equal(root.start_time_unix_nano, "1762131000123456789");
  assert.equal(root.duration_ms, 250.5);
  assert.equal(root.status_code, "STATUS_CODE_OK");
  assert.equal(errored.status_code, "STATUS_CODE_ERROR");
  assert.equal(errored.status_message, "tool timeout");
  const a = root.attributes;
  assert.equal(a["gen_ai.request.model"], "claude-opus-5-5");
  assert.equal(a["gen_ai.usage.input_tokens"], 1234);
  assert.equal(a["gen_ai.usage.cost"], 0.0123);
  assert.equal(a.stream, true);
  assert.deepEqual(a.stop_sequences, ["\n\nHuman:", "END"]);
  assert.deepEqual(a["tool.input"], { query: "otel", limit: 5 });
  assert.equal(a.blob, "AAEC");
  assert.equal(a["big.int"], "9007199254740993"); // > 2^53: kept exact as a string
  assert.equal(a["neg.int"], -42);
  assert.equal(a["not.a.number"], "NaN"); // JSON has no NaN
  assert.equal(a.unicode, "héllo ✓ 日本");
  assert.deepEqual(root.resource, { "service.name": "claude-api", "anthropic.workspace_id": "wrkspc_1" });
  assert.deepEqual(root.scope, { name: "anthropic", version: "1.0" });
  assert.equal(root.resource_schema_url, "https://opentelemetry.io/schemas/1.27.0");
  assert.deepEqual(root.events, [
    {
      name: "first_token",
      time_unix_nano: "1762131000203456789",
      time: "2025-11-03T00:50:00.203456789Z",
      attributes: { ttft_ms: 80 },
    },
  ]);
  assert.deepEqual(child.links, [
    {
      trace_id: "11111111111111111111111111111111",
      span_id: "2222222222222222",
      attributes: { "link.kind": "follows_from" },
    },
  ]);
  assert.match(root.received_at, /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/);
});

test("records match the stream schema: required fields present, no extra fields", async () => {
  const f = fakeEnv();
  await handle(post(fixture("basic.bin")), f.env);
  const names = new Set(SPAN_STREAM_SCHEMA.fields.map((x) => x.name));
  const required = SPAN_STREAM_SCHEMA.fields.filter((x) => "required" in x && x.required).map((x) => x.name);
  const typeOf = Object.fromEntries(SPAN_STREAM_SCHEMA.fields.map((x) => [x.name, x.type]));
  for (const rec of f.records()) {
    for (const k of Object.keys(rec)) assert.ok(names.has(k), `unexpected field ${k}`);
    for (const k of required) assert.ok(k in rec, `missing required field ${k}`);
    for (const [k, v] of Object.entries(rec)) {
      const t = typeOf[k];
      if (t === "string") assert.equal(typeof v, "string", k);
      if (t === "int64") assert.ok(Number.isInteger(v), k);
      if (t === "float64") assert.equal(typeof v, "number", k);
      if (t === "json") assert.equal(typeof v, "object", k);
      assert.notEqual(v, null, `${k} is null`);
    }
  }
});

test("same content as the AWS Lambda version, apart from documented differences", async () => {
  const f = fakeEnv();
  await handle(post(fixture("basic.bin")), f.env);
  const lambda = JSON.parse(
    readFileSync(fixturePath("basic.lambda.json"), "utf8")
      // JSON.parse loses precision past 2^53, which is why the Worker emits
      // nanosecond times as strings. Quote big integers before parsing.
      .replace(/(:\s*)(-?\d{16,})\b/g, '$1"$2"'),
  );
  const norm = (r: any) => {
    const out: any = {};
    for (const [k, v] of Object.entries(r)) {
      if (v === null || k === "received_at") continue; // Worker omits nulls
      if (k.endsWith("_unix_nano")) out[k] = String(v); // Worker uses strings
      else if (k === "events") out[k] = (v as any[]).map((e) => ({ ...e, time_unix_nano: String(e.time_unix_nano) }));
      else if (k === "links") out[k] = (v as any[]).map((l) => Object.fromEntries(Object.entries(l).filter(([, x]) => x !== null)));
      else if (k === "scope") out[k] = Object.fromEntries(Object.entries(v as object).filter(([, x]) => x !== null));
      else if (k === "attributes") out[k] = { ...(v as object), "big.int": String((v as any)["big.int"]) };
      else out[k] = v;
    }
    return out;
  };
  const worker = f.records().map(norm);
  assert.deepEqual(worker, lambda.map(norm));
});

test("gzip request body", async () => {
  const f = fakeEnv();
  const res = await handle(post(fixture("basic.bin.gz"), { "content-encoding": "gzip" }), f.env);
  assert.equal(res.status, 200);
  assert.equal(f.records().length, 3);
});

test("auth, method, content-type, encoding, and bad payload errors", async () => {
  const f = fakeEnv();
  assert.equal((await handle(post(fixture("basic.bin"), { authorization: "Bearer nope" }), f.env)).status, 401);
  assert.equal((await handle(post(fixture("basic.bin"), { authorization: "" }), f.env)).status, 401);
  assert.equal((await handle(post(fixture("basic.bin"), { authorization: TOKEN }), f.env)).status, 401);
  assert.equal((await handle(post(null, {}, "GET"), f.env)).status, 405);
  assert.equal((await handle(post("{}", { "content-type": "application/json" }), f.env)).status, 415);
  assert.equal((await handle(post(fixture("basic.bin"), { "content-encoding": "br" }), f.env)).status, 415);
  assert.equal((await handle(post(new Uint8Array([0xff, 0xfe, 0x01])), f.env)).status, 400);
  assert.equal((await handle(post(fixture("basic.bin"), { "content-encoding": "gzip" }), f.env)).status, 400);
  assert.equal(f.sent.length, 0);
});

test("empty request: 200 and nothing sent", async () => {
  const f = fakeEnv();
  assert.equal((await handle(post(new Uint8Array()), f.env)).status, 200);
  assert.equal(f.sent.length, 0);
});

test("batches stay under the 5 MB ingest limit and keep order", async () => {
  const f = fakeEnv();
  const res = await handle(post(bigRequest(10, "x".repeat(1_500_000))), f.env);
  assert.equal(res.status, 200);
  // 1.5 MB first span + 9 small ones: still one batch
  assert.equal(f.sent.length, 1);

  const g = fakeEnv();
  const fat = create(ExportTraceServiceRequestSchema, {
    resourceSpans: [
      {
        scopeSpans: [
          {
            spans: Array.from({ length: 12 }, (_, i) => ({
              traceId: new Uint8Array(16).fill(1),
              spanId: new Uint8Array(8).fill(i + 1),
              name: `fat.${i}`,
              attributes: [{ key: "p", value: { value: { case: "stringValue" as const, value: "z".repeat(700_000) } } }],
            })),
          },
        ],
      },
    ],
  });
  assert.equal((await handle(post(toBinary(ExportTraceServiceRequestSchema, fat)), g.env)).status, 200);
  assert.ok(g.sent.length >= 2, `expected several sends, got ${g.sent.length}`);
  for (const batch of g.sent) {
    assert.ok(new TextEncoder().encode(JSON.stringify(batch)).byteLength <= MAX_SEND_BYTES);
  }
  assert.deepEqual(g.records().map((r) => r.name), Array.from({ length: 12 }, (_, i) => `fat.${i}`));
});

test("many small spans in one request", async () => {
  const f = fakeEnv();
  assert.equal((await handle(post(bigRequest(5000)), f.env)).status, 200);
  assert.equal(f.records().length, 5000);
  assert.equal(f.records()[4999]!.attributes.i, 4999);
});

test("transient send failure is retried", async () => {
  const f = fakeEnv({ failSends: 2 });
  assert.equal((await handle(post(fixture("basic.bin")), f.env)).status, 200);
  assert.equal(f.records().length, 3);
});

test("persistent send failure returns 503 with retry-after", async () => {
  const f = fakeEnv({ failSends: 100 });
  const res = await handle(post(fixture("basic.bin")), f.env);
  assert.equal(res.status, 503);
  assert.equal(res.headers.get("retry-after"), "5");
});

test("a span over the ingest limit goes to R2 as gzipped JSON", async () => {
  const f = fakeEnv();
  const res = await handle(post(bigRequest(3, "y".repeat(5_100_000))), f.env);
  assert.equal(res.status, 200);
  assert.equal(f.records().length, 2);
  assert.equal(f.puts.length, 1);
  const put = f.puts[0]!;
  assert.match(put.key, /^otel\/_oversized\/year=\d{4}\/month=\d\d\/day=\d\d\/hour=\d\d\/abab[0-9a-f]+-0101010101010101-[0-9a-f]{8}\.json\.gz$/);
  assert.equal(put.options?.httpMetadata && (put.options.httpMetadata as R2HTTPMetadata).contentEncoding, "gzip");
  const rec = JSON.parse(gunzipSync(Buffer.from(put.value)).toString("utf8"));
  assert.equal(rec.name, "span.0");
  assert.equal(rec.attributes["gen_ai.prompt"].length, 5_100_000);
});
