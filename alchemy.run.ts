/**
 * Claude Platform OpenTelemetry traces -> Cloudflare R2, as gzipped JSON lines.
 *
 *   Claude Platform --OTLP/HTTP--> Worker --1 record per span--> Basin stream
 *     --SQL pipeline--> R2 sink (JSON, gzip) --> r2://<bucket>/otel/traces/year=/month=/day=/hour=/
 *
 * Deploy:  OTLP_AUTH_TOKEN=... npx alchemy deploy
 */
import * as Alchemy from "alchemy";
import * as Output from "alchemy/Output";
import { sha256 } from "alchemy/Util/sha256";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import { SPAN_STREAM_SCHEMA } from "./src/stream-schema.ts";

/** Key prefix for the trace files. */
const TRACE_PATH = "otel/traces";
/** Key prefix for single spans too large for one ingest request (written by the Worker). */
const OVERSIZED_PREFIX = "otel/_oversized/";

// ---------------------------------------------------------------- resources

export const TraceBucket = Cloudflare.R2.Bucket("TraceBucket");

export const TraceStream = Cloudflare.Basin.Stream("SpanStream", {
  schema: SPAN_STREAM_SCHEMA,
  // No public HTTP ingest endpoint. Only the Worker binding writes to it.
  http: false,
  workerBinding: { enabled: true },
});

export const Receiver = Cloudflare.Worker("Receiver", {
  main: "./src/worker.ts",
  compatibility: { date: "2026-10-01" },
  observability: { enabled: true },
  env: {
    STREAM: TraceStream,
    BUCKET: TraceBucket,
    AUTH_TOKEN: Config.Redacted("OTLP_AUTH_TOKEN"),
    OVERSIZED_PREFIX,
  },
});

export type ReceiverEnv = Cloudflare.InferEnv<typeof Receiver>;

// -------------------------------------------------------------------- stack

export default Alchemy.Stack(
  "ClaudeOtelR2",
  {
    providers: Cloudflare.providers(),
    state: Cloudflare.state(),
  },
  Effect.gen(function* () {
    const { accountId } = yield* yield* Cloudflare.CloudflareEnvironment;

    const bucket = yield* TraceBucket;
    const stream = yield* TraceStream;

    // The R2 sink writes through R2's S3 API. Mint an account token scoped
    // to this bucket's objects, and derive S3 credentials from it:
    // access key id = token id, secret = SHA-256 hex of the token value.
    const sinkToken = yield* Cloudflare.ApiToken.AccountApiToken("TraceSinkToken", {
      policies: [
        {
          effect: "allow",
          permissionGroups: [
            "Workers R2 Storage Bucket Item Read",
            "Workers R2 Storage Bucket Item Write",
          ],
          resources: Output.all(bucket.bucketName, bucket.jurisdiction).pipe(
            Output.map(([bucketName, jurisdiction]) => ({
              [`com.cloudflare.edge.r2.bucket.${accountId}_${jurisdiction}_${bucketName}`]: "*",
            })),
          ),
        },
      ],
    });

    const sink = yield* Cloudflare.Basin.Sink("SpanSink", {
      type: "r2",
      // The Pipelines API accepts { type: "json", compression: "gzip" }.
      // Alchemy beta.85's SinkFormat type has no `compression` on JSON, but
      // passes `format` to the API as-is, so cast. Note: its diff ignores
      // JSON compression, so changing this later does not replace the sink.
      format: { type: "json", compression: "gzip" } as unknown as Cloudflare.Basin.SinkFormat,
      config: {
        bucket,
        credentials: {
          accessKeyId: sinkToken.tokenId.pipe(Output.map((id) => Redacted.make(id))),
          secretAccessKey: sinkToken.value.pipe(
            Output.mapEffect((value) => sha256(Redacted.value(value)).pipe(Effect.map(Redacted.make))),
          ),
        },
        path: TRACE_PATH,
        partitioning: { timePattern: "year=%Y/month=%m/day=%d/hour=%H" },
        rollingPolicy: {
          intervalSeconds: 300,
          fileSizeBytes: 64 * 1024 * 1024,
        },
      },
    });

    yield* Cloudflare.Basin.Pipeline("SpanPipeline", {
      sql: Output.interpolate`INSERT INTO ${sink.name} SELECT * FROM ${stream.name}`,
    });

    const receiver = yield* Receiver;

    return {
      /** Paste into "OTLP endpoint URL" in the Claude Console. The API appends /v1/traces. */
      otlpEndpointUrl: receiver.url,
      bucket: bucket.bucketName,
      tracePrefix: Output.interpolate`r2://${bucket.bucketName}/${TRACE_PATH}/`,
      stream: stream.name,
      sink: sink.name,
    };
  }),
);
