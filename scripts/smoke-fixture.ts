// Writes .smoke/oversized.bin: 3 spans, the first one over 5 MB.
import { writeFileSync, mkdirSync } from "node:fs";
import { create, toBinary } from "@bufbuild/protobuf";
import { ExportTraceServiceRequestSchema } from "../src/gen/opentelemetry/proto/collector/trace/v1/trace_service_pb.ts";

const spans = Array.from({ length: 3 }, (_, i) => ({
  traceId: new Uint8Array(16).fill(0xab),
  spanId: new Uint8Array(8).fill(i + 1),
  name: `span.${i}`,
  startTimeUnixNano: 1_762_131_000_000_000_000n,
  endTimeUnixNano: 1_762_131_000_500_000_000n,
  attributes: i === 0 ? [{ key: "gen_ai.prompt", value: { value: { case: "stringValue" as const, value: "y".repeat(5_100_000) } } }] : [],
}));
mkdirSync(".smoke", { recursive: true });
writeFileSync(
  ".smoke/oversized.bin",
  toBinary(ExportTraceServiceRequestSchema, create(ExportTraceServiceRequestSchema, { resourceSpans: [{ scopeSpans: [{ spans }] }] })),
);
