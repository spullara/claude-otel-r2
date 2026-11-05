# claude-otel-r2

This project receives Claude Platform OpenTelemetry traces and stores one JSON record for each span in Cloudflare R2.

Claude Platform sends OTLP HTTP protobuf to a Cloudflare Worker. The body can use gzip. The Worker checks a bearer token and decodes the request. It writes one flat record for each span.

A record that fits in one ingest request goes to a Basin stream. A SQL pipeline copies the stream into an R2 sink. The sink writes gzip JSON lines under this prefix:

`otel/traces/year=YYYY/month=MM/day=DD/hour=HH/`

A span that is larger than one ingest request does not use the stream. The Worker writes that span as its own gzip file under `otel/_oversized/` with the same date folders.

The sink rolls a file after 300 seconds or at 64 MB. Delivery is at least once.

## Requirements

You need:

- Node.js and npm
- A Cloudflare account with R2 and Pipelines
- Alchemy, through the npm scripts in this repository
- The DuckDB CLI, when you want to query the stored spans

## Configure

1. Copy `.env.example` to `.env`.
2. Set `OTLP_AUTH_TOKEN` to a long random value.
3. Create a value with `openssl rand -hex 32`.
4. Log in to Cloudflare for Alchemy before you deploy.

Do not commit `.env`. Do not commit the `.alchemy/` directory. Deploy creates the bucket token. Git does not store that token.

## Deploy

Run:

```bash
npm install
npm run deploy
```

The stack prints `otlpEndpointUrl`. Paste that URL into the Claude Console as the OTLP endpoint URL. The API adds `/v1/traces`.

Send traces with this header:

```text
authorization: Bearer <OTLP_AUTH_TOKEN>
```

The Worker accepts `application/x-protobuf` and `application/protobuf`.

## Query

Run:

```bash
npm run duckdb
```

The script reads the bucket and credentials from Alchemy state for your stage. You can pass another stage with `--stage`.

Use these views:

| View | Contents |
| --- | --- |
| `spans` | Every stored span, plus the `year`, `month`, `day`, and `hour` columns |
| `spans_dedup` | One row for each `trace_id` and `span_id` |

## Develop

| Command | Action |
| --- | --- |
| `npm test` | Run the handler tests |
| `npm run typecheck` | Check TypeScript |
| `npm run smoke` | Run the Worker in Miniflare |
| `npm run gen` | Generate TypeScript from the protobuf files |
| `npm run fixtures` | Rebuild the protobuf fixtures |

Keep `src/stream-schema.ts` equal to the span record in `src/otlp.ts`. A change to that field list replaces the Basin stream on the next deploy.

## Layout

| Path | Role |
| --- | --- |
| `alchemy.run.ts` | Bucket, stream, sink, pipeline, token, and Worker |
| `src/worker.ts` | Worker entry |
| `src/handler.ts` | Auth, decode, batch send, and the oversized write |
| `src/otlp.ts` | Protobuf to span records, and batch packing |
| `src/stream-schema.ts` | Basin field list |
| `proto/` | OpenTelemetry trace protobuf files |
| `src/gen/` | Generated TypeScript. Do not edit these files by hand. |
| `scripts/duckdb.mjs` | SQL shell for the stored spans |
| `test/` | Handler tests and the Miniflare smoke test |

## License

MIT. See `LICENSE`. The files in `proto/` keep their own license in `proto/LICENSE`.
