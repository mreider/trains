import os
import json
from celery_app import celery_app
import redis
import random
import sys
import time
from otel import tracer
from opentelemetry.trace import SpanKind, Status, StatusCode

@celery_app.task(name="processing_service.process_aggregated")
def process_aggregated(aggregated):
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    with tracer.start_as_current_span(
        "receive_aggregation_message",
        kind=SpanKind.CLIENT,
        attributes={
            "messaging.operation": "receive",
            "messaging.destination.name": "AggregationQueue",
            "messaging.message.id": aggregated.get("message_id", "unknown"),
            "messaging.message.conversation_id": aggregated.get("conversation_id", "unknown"),
        },
    ) as recv_span:
        try:
            # Random error injection for message processing
            if random.random() < 0.12:
                raise RuntimeError("Simulated message processing error")
            passengers = aggregated.get("passengers", [{}])
            if passengers and len(passengers) > 0:
                passenger = passengers[0]
            else:
                passenger = {"passenger_id": "unknown"}
            notification = {
                "passenger_id": passenger.get("passenger_id", "unknown"),
                "message": f"Your train (ID: {aggregated.get('train_id')}) is scheduled to depart at {aggregated.get('schedule', {}).get('departure_time', '')} from {aggregated.get('schedule', {}).get('route', [''])[0]}."
            }
            # Send notification as Celery task
            from notification_service.app import process_notification
            process_notification.delay(notification)
            # Redis operation
            with tracer.start_as_current_span(
                "redis_set_last_message",
                kind=SpanKind.CLIENT,
                attributes={
                    "db.system": "redis",
                    "db.operation.name": "SET",
                    "db.query.text": "SET processing_last_message ...",
                    "db.namespace": "0"
                },
            ) as db_span:
                try:
                    r = redis.Redis(host=redis_host, port=redis_port, password="password")
                    r.set("processing_last_message", json.dumps(aggregated))
                    db_span.set_status(Status(StatusCode.OK))
                except Exception as exc:
                    db_span.set_status(Status(StatusCode.ERROR, str(exc)))
                    db_span.set_attribute("error.type", type(exc).__name__)
                    raise
            recv_span.set_status(Status(StatusCode.OK))
        except Exception as exc:
            recv_span.set_status(Status(StatusCode.ERROR, str(exc)))
            recv_span.set_attribute("error.type", type(exc).__name__)
            raise

# No __main__ needed; Celery worker will process tasks

if __name__ == "__main__":
    main()
