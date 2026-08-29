"""
OpenTelemetry traces.

One trace per batch, with a span for each stage, so a slow message can be
followed from the moment it left Kafka to the moment its verdict was written.
Traces go to an OTLP collector when OTEL_EXPORTER_OTLP_ENDPOINT is set;
otherwise the spans are created and dropped, which costs almost nothing.
"""

import os

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

_configured = False


def setup(service_name: str = "streamguard-worker") -> None:
    """Point traces at a collector, if one is configured."""
    global _configured
    if _configured:
        return
    _configured = True

    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if endpoint:
        # Imported here so the exporter package stays optional.
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import \
            OTLPSpanExporter
        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)


def tracer():
    return trace.get_tracer("streamguard")
