/**
 * OTLP ExportTraceServiceRequest -> one flat record per span.
 *
 * Pure functions with no Worker APIs, so they run under Node tests too.
 */
import { fromBinary } from "@bufbuild/protobuf";
import {
  type ExportTraceServiceRequest,
  ExportTraceServiceRequestSchema,
} from "./gen/opentelemetry/proto/collector/trace/v1/trace_service_pb.ts";
import type { AnyValue, KeyValue } from "./gen/opentelemetry/proto/common/v1/common_pb.ts";
import { Span_SpanKind, Status_StatusCode } from "./gen/opentelemetry/proto/trace/v1/trace_pb.ts";

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type Attributes = { [key: string]: Json };

/** One span. Matches SPAN_STREAM_SCHEMA in stream-schema.ts. */
export interface SpanRecord {
  trace_id: string;
  span_id: string;
  parent_span_id?: string;
  trace_state?: string;
  flags?: number;
  name: string;
  kind: string;
  start_time?: string;
  end_time?: string;
  start_time_unix_nano: string;
  end_time_unix_nano: string;
  duration_ms?: number;
  status_code: string;
  status_message?: string;
  attributes: Attributes;
  events: Json;
  links: Json;
  resource: Attributes;
  scope: Attributes;
  resource_schema_url?: string;
  scope_schema_url?: string;
  dropped_attributes_count: number;
  dropped_events_count: number;
  dropped_links_count: number;
  received_at: string;
}

export function decodeRequest(bytes: Uint8Array): ExportTraceServiceRequest {
  return fromBinary(ExportTraceServiceRequestSchema, bytes);
}

const hex = (b: Uint8Array): string => {
  let s = "";
  for (let i = 0; i < b.length; i++) s += b[i]!.toString(16).padStart(2, "0");
  return s;
};

const base64 = (b: Uint8Array): string => {
  let s = "";
  for (let i = 0; i < b.length; i++) s += String.fromCharCode(b[i]!);
  return btoa(s);
};

/** int64 -> number when exact, else decimal string. */
const int64 = (v: bigint): number | string =>
  v >= BigInt(Number.MIN_SAFE_INTEGER) && v <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(v) : v.toString();

export function anyValue(v: AnyValue | undefined): Json {
  if (!v) return null;
  const val = v.value;
  switch (val.case) {
    case "stringValue":
    case "boolValue":
      return val.value;
    case "intValue":
      return int64(val.value);
    case "doubleValue":
      // JSON has no NaN or Infinity.
      return Number.isFinite(val.value) ? val.value : String(val.value);
    case "bytesValue":
      return base64(val.value);
    case "arrayValue":
      return val.value.values.map(anyValue);
    case "kvlistValue":
      return attributes(val.value.values);
    case "stringValueStrindex": // profiles dictionary index; not used by traces
      return val.value;
    default:
      return null;
  }
}

export function attributes(kvs: KeyValue[]): Attributes {
  const out: Attributes = {};
  for (const kv of kvs) out[kv.key] = anyValue(kv.value);
  return out;
}

/** Nanoseconds since epoch -> RFC 3339 with 9 fractional digits. */
export function isoNanos(ns: bigint): string | undefined {
  if (ns <= 0n) return undefined;
  const secs = ns / 1_000_000_000n;
  const frac = ns % 1_000_000_000n;
  const base = new Date(Number(secs) * 1000).toISOString().slice(0, 19);
  return `${base}.${frac.toString().padStart(9, "0")}Z`;
}

const kindName = (k: number): string => Span_SpanKind[k] ? `SPAN_KIND_${Span_SpanKind[k]}` : String(k);
const statusName = (c: number): string =>
  Status_StatusCode[c] ? `STATUS_CODE_${Status_StatusCode[c]}` : String(c);

const opt = (s: string): string | undefined => (s === "" ? undefined : s);

export function spanRecords(req: ExportTraceServiceRequest, receivedAt: string): SpanRecord[] {
  const out: SpanRecord[] = [];
  for (const rs of req.resourceSpans) {
    const resource = attributes(rs.resource?.attributes ?? []);
    for (const ss of rs.scopeSpans) {
      const scope: Attributes = {};
      if (ss.scope?.name) scope.name = ss.scope.name;
      if (ss.scope?.version) scope.version = ss.scope.version;
      if (ss.scope?.attributes.length) scope.attributes = attributes(ss.scope.attributes);
      for (const sp of ss.spans) {
        const start = sp.startTimeUnixNano;
        const end = sp.endTimeUnixNano;
        const rec: SpanRecord = {
          trace_id: hex(sp.traceId),
          span_id: hex(sp.spanId),
          parent_span_id: sp.parentSpanId.length ? hex(sp.parentSpanId) : undefined,
          trace_state: opt(sp.traceState),
          flags: sp.flags || undefined,
          name: sp.name,
          kind: kindName(sp.kind),
          start_time: isoNanos(start),
          end_time: isoNanos(end),
          start_time_unix_nano: start.toString(),
          end_time_unix_nano: end.toString(),
          duration_ms: start > 0n && end >= start ? Number(end - start) / 1e6 : undefined,
          status_code: statusName(sp.status?.code ?? 0),
          status_message: opt(sp.status?.message ?? ""),
          attributes: attributes(sp.attributes),
          events: sp.events.map((e) => {
            const ev: Attributes = {
              name: e.name,
              time_unix_nano: e.timeUnixNano.toString(),
              attributes: attributes(e.attributes),
            };
            const t = isoNanos(e.timeUnixNano);
            if (t) ev.time = t;
            if (e.droppedAttributesCount) ev.dropped_attributes_count = e.droppedAttributesCount;
            return ev;
          }),
          links: sp.links.map((l) => {
            const link: Attributes = {
              trace_id: hex(l.traceId),
              span_id: hex(l.spanId),
              attributes: attributes(l.attributes),
            };
            if (l.traceState) link.trace_state = l.traceState;
            if (l.flags) link.flags = l.flags;
            if (l.droppedAttributesCount) link.dropped_attributes_count = l.droppedAttributesCount;
            return link;
          }),
          resource,
          scope,
          resource_schema_url: opt(rs.schemaUrl),
          scope_schema_url: opt(ss.schemaUrl),
          dropped_attributes_count: sp.droppedAttributesCount,
          dropped_events_count: sp.droppedEventsCount,
          dropped_links_count: sp.droppedLinksCount,
          received_at: receivedAt,
        };
        // Drop undefined keys so optional fields are absent, not null.
        for (const k of Object.keys(rec) as (keyof SpanRecord)[]) {
          if (rec[k] === undefined) delete rec[k];
        }
        out.push(rec);
      }
    }
  }
  return out;
}

const encoder = new TextEncoder();

/** Size of a record as JSON, in bytes. */
export const jsonSize = (rec: unknown): number => encoder.encode(JSON.stringify(rec)).byteLength;

/**
 * Pack records into batches that fit one Pipelines ingest request.
 * Records larger than `maxBytes` on their own are returned in `oversized`.
 */
export function packBatches<T>(
  records: T[],
  maxBytes: number,
): { batches: T[][]; oversized: { record: T; bytes: number }[] } {
  const batches: T[][] = [];
  const oversized: { record: T; bytes: number }[] = [];
  let batch: T[] = [];
  let size = 2; // "[]"
  for (const rec of records) {
    const bytes = jsonSize(rec);
    if (bytes + 2 > maxBytes) {
      oversized.push({ record: rec, bytes });
      continue;
    }
    const add = bytes + (batch.length ? 1 : 0); // comma
    if (batch.length && size + add > maxBytes) {
      batches.push(batch);
      batch = [];
      size = 2;
    }
    size += batch.length ? bytes + 1 : bytes;
    batch.push(rec);
  }
  if (batch.length) batches.push(batch);
  return { batches, oversized };
}
