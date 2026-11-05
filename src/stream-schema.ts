import type * as Cloudflare from "@alchemy.run/cloudflare";

/**
 * Pipelines stream schema: one record per span.
 *
 * Keep this in sync with `SpanRecord` in `otlp.ts`. Cloudflare accepts
 * records that do not match the schema at ingest and drops them later,
 * at the sink. Changing this list replaces the stream on the next deploy.
 *
 * - IDs are lowercase hex.
 * - `*_unix_nano` are decimal strings: nanosecond values do not fit in a
 *   JavaScript number without loss.
 * - `attributes`, `events`, `links`, `resource`, `scope` are JSON values.
 * - Optional fields are left out of the record when they have no value.
 */
export const SPAN_STREAM_SCHEMA = {
  fields: [
    { name: "trace_id", type: "string", required: true },
    { name: "span_id", type: "string", required: true },
    { name: "parent_span_id", type: "string" },
    { name: "trace_state", type: "string" },
    { name: "flags", type: "int64" },
    { name: "name", type: "string", required: true },
    { name: "kind", type: "string", required: true },
    { name: "start_time", type: "string" },
    { name: "end_time", type: "string" },
    { name: "start_time_unix_nano", type: "string", required: true },
    { name: "end_time_unix_nano", type: "string", required: true },
    { name: "duration_ms", type: "float64" },
    { name: "status_code", type: "string", required: true },
    { name: "status_message", type: "string" },
    { name: "attributes", type: "json", required: true },
    { name: "events", type: "json", required: true },
    { name: "links", type: "json", required: true },
    { name: "resource", type: "json", required: true },
    { name: "scope", type: "json", required: true },
    { name: "resource_schema_url", type: "string" },
    { name: "scope_schema_url", type: "string" },
    { name: "dropped_attributes_count", type: "int64", required: true },
    { name: "dropped_events_count", type: "int64", required: true },
    { name: "dropped_links_count", type: "int64", required: true },
    { name: "received_at", type: "string", required: true },
  ],
} satisfies Cloudflare.Basin.StreamFieldsSchema;
