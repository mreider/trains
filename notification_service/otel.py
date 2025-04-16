import os
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

SERVICE_NAME = os.getenv("SERVICE_NAME", "notification-service")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "1.0.0")
ENVIRONMENT = os.getenv("DEPLOYMENT_ENV", "production")
DT_ENDPOINT = os.getenv("DT_ENDPOINT")
DT_API_TOKEN = os.getenv("DT_API_TOKEN")

import json
merged = {}
for name in ["/var/lib/dynatrace/enrichment/dt_metadata.json", "/var/lib/dynatrace/enrichment/dt_host_metadata.json"]:
    try:
        with open(name) as f:
            data = json.load(f)
            merged.update(data)
    except Exception:
        pass
merged.update({
    "service.name": SERVICE_NAME,
    "service.version": SERVICE_VERSION,
    "deployment.environment": ENVIRONMENT,
})
resource = Resource.create(merged)

# Tracing
tracer_provider = TracerProvider(resource=resource)
otel_exporter = OTLPSpanExporter(
    endpoint=DT_ENDPOINT,
    headers={"Authorization": f"Api-Token {DT_API_TOKEN}"},
)
span_processor = BatchSpanProcessor(otel_exporter)
tracer_provider.add_span_processor(span_processor)
trace.set_tracer_provider(tracer_provider)
tracer = trace.get_tracer(__name__)

__all__ = ["tracer"]
