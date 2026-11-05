// Runs the bundled Worker in workerd (via Miniflare) with a fake Pipelines
// stream (an RPC service with send()) and a local R2 bucket.
// Run: npm run smoke   (bundles to .smoke/worker.js first)
import { readFileSync } from "node:fs";
import { gzipSync, gunzipSync } from "node:zlib";
import { Miniflare } from "miniflare";

const TOKEN = "s".repeat(48);
const script = process.argv[2] ?? ".smoke/worker.js";

const mf = new Miniflare({
  workers: [
    {
      name: "receiver",
      modules: true,
      scriptPath: script,
      compatibilityDate: "2026-07-01",
      bindings: { AUTH_TOKEN: TOKEN, OVERSIZED_PREFIX: "otel/_oversized/" },
      r2Buckets: ["BUCKET"],
      serviceBindings: { STREAM: { name: "stream", entrypoint: "Stream" } },
    },
    {
      name: "stream",
      modules: true,
      compatibilityDate: "2026-07-01",
      script: `
        import { WorkerEntrypoint } from "cloudflare:workers";
        const sent = [];
        export class Stream extends WorkerEntrypoint {
          async send(records) { sent.push(records); }
        }
        export default { fetch() { return Response.json(sent); } };
      `,
    },
  ],
});

const url = "http://localhost/v1/traces";
const basic = readFileSync(new URL("./fixtures/basic.bin", import.meta.url));
const headers = { "content-type": "application/x-protobuf", authorization: `Bearer ${TOKEN}` };
let failed = 0;
const check = (name, cond, extra = "") => {
  console.log(`${cond ? "PASS" : "FAIL"} ${name} ${extra}`);
  if (!cond) failed++;
};

try {
  let r = await mf.dispatchFetch(url, { method: "POST", body: basic, headers });
  check("protobuf 200", r.status === 200, `(got ${r.status}: ${r.status !== 200 ? await r.text() : ""})`);

  r = await mf.dispatchFetch(url, { method: "POST", body: gzipSync(basic), headers: { ...headers, "content-encoding": "gzip" } });
  check("gzip 200", r.status === 200, `(got ${r.status})`);

  r = await mf.dispatchFetch(url, { method: "POST", body: basic, headers: { ...headers, authorization: "Bearer wrong" } });
  check("bad token 401 (timingSafeEqual path)", r.status === 401, `(got ${r.status})`);

  r = await mf.dispatchFetch(url, { method: "POST", body: new Uint8Array([0xff, 0x01]), headers });
  check("corrupt 400", r.status === 400, `(got ${r.status})`);

  // Oversized span (> 5 MB): built by hand as raw protobuf would be long, so
  // reuse the TS encoder output written by the bundler step.
  const oversized = readFileSync(new URL("../.smoke/oversized.bin", import.meta.url));
  r = await mf.dispatchFetch(url, { method: "POST", body: oversized, headers });
  check("oversized 200", r.status === 200, `(got ${r.status})`);

  const stream = await mf.getWorker("stream");
  const sent = await (await stream.fetch("http://stream/")).json();
  const recs = sent.flat();
  check("records sent through RPC binding", recs.length === 3 + 3 + 2, `(got ${recs.length})`);
  check("trace id hex", recs[0]?.trace_id === "0af7651916cd43dd8448eb211c80319c");
  check("big int kept as string", recs[0]?.attributes?.["big.int"] === "9007199254740993");

  const bucket = await mf.getR2Bucket("BUCKET");
  const listed = await bucket.list({ prefix: "otel/_oversized/" });
  check("oversized span stored in R2", listed.objects.length === 1, `(got ${listed.objects.length})`);
  if (listed.objects[0]) {
    const obj = await bucket.get(listed.objects[0].key);
    const rec = JSON.parse(gunzipSync(Buffer.from(await obj.arrayBuffer())).toString());
    check("R2 object is gzipped JSON line", rec.name === "span.0" && obj.httpMetadata?.contentEncoding === "gzip", listed.objects[0].key);
  }
} finally {
  await mf.dispose();
}
process.exit(failed ? 1 : 0);
