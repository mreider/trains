import time
import sys
import requests
import random
from otel import tracer
from opentelemetry.trace import SpanKind, Status, StatusCode

def generate_load():
    services = [
        "http://train-service/trigger",
        "http://ticket-service/trigger",
        "http://passenger-service/trigger",
        "http://train-management-service/trigger"
    ]
    while True:
        for url in services:
            with tracer.start_as_current_span(
                "proxy_http_request",
                kind=SpanKind.CLIENT,
                attributes={
                    "http.method": "GET",
                    "http.url": url,
                    "peer.service": url.split('//')[1].split('/')[0],
                },
            ) as span:
                try:
                    # Random error injection for HTTP request
                    if random.random() < 0.10:
                        raise RuntimeError("Simulated proxy HTTP error")
                    response = requests.get(url)
                    # otel_logger.info(f"Triggered {url}: Status {response.status_code}", attributes={"http.status_code": response.status_code})
                    span.set_attribute("http.status_code", response.status_code)
                    span.set_status(Status(StatusCode.OK))
                except Exception as e:
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.set_attribute("error.type", type(e).__name__)
                    # otel_logger.error(f"Error triggering {url}: {e}", attributes={"error.type": type(e).__name__})
                    print(f"Error triggering {url}: {e}", flush=True, file=sys.stdout)
        time.sleep(2)

if __name__ == "__main__":
    generate_load()
