#!/usr/bin/env node
/**
 * Open DuckDB connected to the trace files in R2.
 *
 *   npm run duckdb                       # interactive shell
 *   npm run duckdb -- -c "SELECT count(*) FROM spans"
 *   npm run duckdb -- --stage prod       # another Alchemy stage
 *   npm run duckdb -- --print-sql        # print the setup SQL (secrets masked)
 *
 * Bucket, account, and credentials come from Alchemy state for the stage.
 * The credentials are the sink's bucket-scoped token (S3 key id = token id,
 * secret = SHA-256 hex of the token value). To use other credentials, set
 * R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY (and optionally R2_BUCKET,
 * R2_ACCOUNT_ID) in the shell or in .env.
 *
 * Views:
 *   spans        every span, with year/month/day/hour partition columns
 *   spans_dedup  one row per (trace_id, span_id); delivery is at least once
 *
 * Any extra arguments are passed to the duckdb CLI.
 */
import { execFileSync, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync, existsSync, readFileSync } from "node:fs";
import { tmpdir, userInfo } from "node:os";
import { join } from "node:path";

const STACK = "ClaudeOtelR2";
const TRACE_PATH = "otel/traces";

// ------------------------------------------------------------------ args

const args = process.argv.slice(2);
let stage = `live_${userInfo().username}`;
let printSql = false;
const passthrough = [];
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--stage") stage = args[++i];
  else if (args[i] === "--print-sql") printSql = true;
  else passthrough.push(args[i]);
}

// ------------------------------------------------------------------ config

loadDotEnv(".env");

function loadDotEnv(path) {
  if (!existsSync(path)) return;
  for (const line of readFileSync(path, "utf8").split("\n")) {
    const m = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$/);
    if (m && process.env[m[1]] === undefined) process.env[m[1]] = m[2].replace(/^["']|["']$/g, "");
  }
}

function readState(resource) {
  const out = execFileSync(
    "npx",
    ["--no-install", "alchemy", "state", "cat", `${STACK}/${stage}/${resource}`, "--no-input", "--log-level", "error"],
    { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] },
  );
  return JSON.parse(out);
}

function unredact(v) {
  return v && typeof v === "object" && "__redacted__" in v ? v.__redacted__ : v;
}

let bucket = process.env.R2_BUCKET;
let accountId = process.env.R2_ACCOUNT_ID;
let keyId = process.env.R2_ACCESS_KEY_ID;
let secret = process.env.R2_SECRET_ACCESS_KEY;

try {
  if (!bucket || !accountId) {
    const b = readState("TraceBucket").attr;
    bucket ??= b.bucketName;
    accountId ??= b.accountId;
  }
  if (!keyId || !secret) {
    const t = readState("TraceSinkToken").attr;
    keyId = t.tokenId;
    secret = createHash("sha256").update(unredact(t.value)).digest("hex");
  }
} catch (err) {
  console.error(`Could not read Alchemy state for stage "${stage}": ${err.stderr || err.message}`);
  console.error("Deploy first, pass --stage, or set R2_BUCKET, R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY.");
  process.exit(1);
}

// ------------------------------------------------------------------ SQL

const q = (s) => `'${String(s).replaceAll("'", "''")}'`;
const glob = `r2://${bucket}/${TRACE_PATH}/**/*`;

const setupSql = (maskSecrets) => `
INSTALL httpfs; LOAD httpfs;
INSTALL json;   LOAD json;
SET TimeZone = 'UTC';  -- partitions are UTC

CREATE OR REPLACE SECRET r2_otel (
  TYPE r2,
  KEY_ID ${maskSecrets ? "'***'" : q(keyId)},
  SECRET ${maskSecrets ? "'***'" : q(secret)},
  ACCOUNT_ID ${q(accountId)}
);

-- One span per line, gzipped JSON lines. Columns match src/stream-schema.ts.
CREATE OR REPLACE VIEW spans AS
SELECT * FROM read_json(${q(glob)},
  format = 'newline_delimited',
  compression = 'gzip',
  hive_partitioning = true,
  union_by_name = true,
  filename = true,
  columns = {
    trace_id: 'VARCHAR',
    span_id: 'VARCHAR',
    parent_span_id: 'VARCHAR',
    trace_state: 'VARCHAR',
    flags: 'BIGINT',
    name: 'VARCHAR',
    kind: 'VARCHAR',
    start_time: 'TIMESTAMPTZ',
    end_time: 'TIMESTAMPTZ',
    start_time_unix_nano: 'VARCHAR',
    end_time_unix_nano: 'VARCHAR',
    duration_ms: 'DOUBLE',
    status_code: 'VARCHAR',
    status_message: 'VARCHAR',
    attributes: 'JSON',
    events: 'JSON',
    links: 'JSON',
    resource: 'JSON',
    scope: 'JSON',
    resource_schema_url: 'VARCHAR',
    scope_schema_url: 'VARCHAR',
    dropped_attributes_count: 'BIGINT',
    dropped_events_count: 'BIGINT',
    dropped_links_count: 'BIGINT',
    received_at: 'TIMESTAMPTZ'
  }
);

-- Delivery is at least once: keep the first-received copy of each span.
CREATE OR REPLACE VIEW spans_dedup AS
SELECT * EXCLUDE (_rn) FROM (
  SELECT *, row_number() OVER (PARTITION BY trace_id, span_id ORDER BY received_at) AS _rn
  FROM spans
) WHERE _rn = 1;
`;

if (printSql) {
  console.log(setupSql(true));
  process.exit(0);
}

// The init file holds the secret: write it 0600 in a private temp dir and
// delete it when duckdb exits.
const dir = mkdtempSync(join(tmpdir(), "otel-duckdb-"));
const initFile = join(dir, "init.sql");
writeFileSync(initFile, `.bail on\n.output /dev/null\n${setupSql(false)}\n.output\n.bail off\n`, { mode: 0o600 });
const cleanup = () => rmSync(dir, { recursive: true, force: true });

if (passthrough.length === 0 && process.stdin.isTTY) {
  console.error(`R2 bucket ${bucket} → views: spans, spans_dedup`);
}

const child = spawn("duckdb", ["-init", initFile, ...passthrough], { stdio: "inherit" });
child.on("error", (err) => {
  cleanup();
  console.error(err.code === "ENOENT" ? "duckdb not found. Install it: brew install duckdb" : err.message);
  process.exit(1);
});
child.on("exit", (code) => {
  cleanup();
  process.exit(code ?? 1);
});
for (const sig of ["SIGINT", "SIGTERM"]) process.on(sig, () => {}); // let duckdb handle Ctrl-C
