import os
import json
from celery_app import celery_app
import redis
import random
import sys
from otel import tracer
from opentelemetry.trace import SpanKind, Status, StatusCode
import time

@celery_app.task(name="aggregation_service.aggregate_and_publish")
def aggregate_and_publish(schedule, ticket, passenger):
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    with tracer.start_as_current_span(
        "aggregate_messages",
        kind=SpanKind.INTERNAL,
        attributes={
            "aggregation.has_schedule": bool(schedule),
            "aggregation.has_ticket": bool(ticket),
            "aggregation.has_passenger": bool(passenger),
            "messaging.system": "rabbitmq",
            "messaging.destination": "AggregationQueue",
            "messaging.destination_kind": "queue",
        },
    ) as agg_span:
        try:
            # Random error injection for aggregation
            if random.random() < 0.13:
                raise RuntimeError("Simulated aggregation error")
            aggregated = {
                "train_id": "123",
                "schedule": schedule,
                "tickets": [ticket] if ticket else [],
                "passengers": [passenger] if passenger else []
            }
            # Publish to AggregationQueue as a Celery task
            celery_app.send_task("processing_service.process_aggregated", args=[aggregated])
            # Redis operation
            with tracer.start_as_current_span(
                "redis_set_last_message",
                kind=SpanKind.CLIENT,
                attributes={
                    "db.system": "redis",
                    "db.operation.name": "SET",
                    "db.query.text": "SET aggregation_last_message ...",
                    "db.namespace": "0"
                },
            ) as db_span:
                try:
                    r = redis.Redis(host=redis_host, port=redis_port, password="password")
                    r.set("aggregation_last_message", json.dumps(aggregated))
                    db_span.set_status(Status(StatusCode.OK))
                except Exception as exc:
                    db_span.set_status(Status(StatusCode.ERROR, str(exc)))
                    db_span.set_attribute("error.type", type(exc).__name__)
                    raise
            agg_span.set_status(Status(StatusCode.OK))
        except Exception as exc:
            agg_span.set_status(Status(StatusCode.ERROR, str(exc)))
            agg_span.set_attribute("error.type", type(exc).__name__)
            raise

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    app.run(host="0.0.0.0", port=5000)
