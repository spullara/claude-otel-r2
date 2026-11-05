"""Write OTLP protobuf fixtures, plus what the Lambda version produces for them.

Run: python3 scripts/make_fixtures.py <path-to-otlp-firehose/src>
Uses the official Python opentelemetry-proto package as an independent encoder.
"""
import gzip, json, os, sys
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.trace.v1.trace_pb2 import Span, Status

OUT = os.path.join(os.path.dirname(__file__), "..", "test", "fixtures")

def kv(k, v):
    a = AnyValue()
    if isinstance(v, bool): a.bool_value = v
    elif isinstance(v, int): a.int_value = v
    elif isinstance(v, float): a.double_value = v
    elif isinstance(v, bytes): a.bytes_value = v
    elif isinstance(v, list):
        for x in v: a.array_value.values.append(kv("_", x).value)
    elif isinstance(v, dict):
        for kk, vv in v.items(): a.kvlist_value.values.append(kv(kk, vv))
    else: a.string_value = v
    return KeyValue(key=k, value=a)

def make_request(n_spans=3, big_attr=None):
    req = ExportTraceServiceRequest()
    rs = req.resource_spans.add()
    rs.resource.attributes.extend([kv("service.name", "claude-api"), kv("anthropic.workspace_id", "wrkspc_1")])
    rs.schema_url = "https://opentelemetry.io/schemas/1.27.0"
    ss = rs.scope_spans.add()
    ss.scope.name = "anthropic"; ss.scope.version = "1.0"
    trace_id = bytes.fromhex("0af7651916cd43dd8448eb211c80319c")
    for i in range(n_spans):
        sp = ss.spans.add()
        sp.trace_id = trace_id
        sp.span_id = (i + 1).to_bytes(8, "big")
        if i: sp.parent_span_id = (1).to_bytes(8, "big")
        sp.name = "messages.create" if i == 0 else f"tool.call.{i}"
        sp.kind = Span.SPAN_KIND_SERVER if i == 0 else Span.SPAN_KIND_INTERNAL
        sp.start_time_unix_nano = 1_762_131_000_123_456_789 + i * 1_000_000
        sp.end_time_unix_nano = sp.start_time_unix_nano + 250_500_000
        sp.attributes.extend([
            kv("gen_ai.request.model", "claude-opus-5-5"),
            kv("gen_ai.usage.input_tokens", 1234),
            kv("gen_ai.usage.cost", 0.0123),
            kv("stream", True),
            kv("stop_sequences", ["\n\nHuman:", "END"]),
            kv("tool.input", {"query": "otel", "limit": 5}),
            kv("blob", b"\x00\x01\x02"),
            kv("big.int", 9_007_199_254_740_993),
            kv("neg.int", -42),
            kv("not.a.number", float("nan")),
            kv("unicode", "héllo ✓ 日本"),
        ])
        if big_attr and i == 0:
            sp.attributes.append(kv("gen_ai.prompt", big_attr))
        ev = sp.events.add()
        ev.name = "first_token"
        ev.time_unix_nano = sp.start_time_unix_nano + 80_000_000
        ev.attributes.append(kv("ttft_ms", 80))
        if i == 1:
            ln = sp.links.add()
            ln.trace_id = bytes.fromhex("11111111111111111111111111111111")
            ln.span_id = bytes.fromhex("2222222222222222")
            ln.attributes.append(kv("link.kind", "follows_from"))
        sp.status.code = Status.STATUS_CODE_OK if i != 2 else Status.STATUS_CODE_ERROR
        if i == 2: sp.status.message = "tool timeout"
    return req

def main():
    os.makedirs(OUT, exist_ok=True)
    basic = make_request(3).SerializeToString()
    open(os.path.join(OUT, "basic.bin"), "wb").write(basic)
    open(os.path.join(OUT, "basic.bin.gz"), "wb").write(gzip.compress(basic))

    # Lambda (Python) output for basic.bin, for a cross-implementation check.
    if len(sys.argv) > 1:
        sys.path.insert(0, sys.argv[1])
        os.environ.setdefault("DELIVERY_STREAM", "x"); os.environ.setdefault("AUTH_TOKEN", "x")
        os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")
        import handler as H
        req = ExportTraceServiceRequest(); req.ParseFromString(basic)
        recs = list(H.span_records(req, "RECEIVED"))
        json.dump(recs, open(os.path.join(OUT, "basic.lambda.json"), "w"), indent=1, default=str)
    print("fixtures written to", os.path.abspath(OUT))

main()
