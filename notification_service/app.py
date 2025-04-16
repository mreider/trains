import os
import json
from celery_app import celery_app
import redis
import random
import sys
from flask import Flask

app = Flask(__name__)
import time
from otel import tracer
from opentelemetry.trace import SpanKind, Status, StatusCode
from flask import request, jsonify

@app.route('/trigger', methods=['GET'])
def trigger():
    with tracer.start_as_current_span(
        "http_trigger",
        kind=SpanKind.SERVER,
        attributes={
            "service.name": os.getenv("SERVICE_NAME", "notification-service"),
            "http.method": request.method,
            "http.route": "/trigger",
            "http.scheme": request.scheme,
            "net.peer.ip": request.remote_addr,
            "server.address": request.host,
        },
    ) as route_span:
        try:
            # Simulate trigger logic (customize as needed)
            route_span.set_status(Status(StatusCode.OK))
            return jsonify({"status": "NotificationService triggered"}), 200
        except Exception as e:
            route_span.set_status(Status(StatusCode.ERROR, str(e)))
            route_span.set_attribute("error.type", type(e).__name__)
            return jsonify({"error": str(e)}), 500

@celery_app.task(name="notification_service.process_notification")
def process_notification(notification):
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    with tracer.start_as_current_span(
        "receive_notification_message",
        kind=SpanKind.CLIENT,
        attributes={
            "messaging.operation": "receive",
            "messaging.destination.name": "NotificationQueue",
            "messaging.message.id": notification.get("message_id", "unknown"),
            "messaging.message.conversation_id": notification.get("conversation_id", "unknown"),
        },
    ) as recv_span:
        try:
            # Random error injection for notification processing
            if random.random() < 0.12:
                raise RuntimeError("Simulated notification processing error")
            # Redis operation
            with tracer.start_as_current_span(
                "redis_set_last_message",
                kind=SpanKind.CLIENT,
                attributes={
                    "db.system": "redis",
                    "db.operation.name": "SET",
                    "db.query.text": "SET notification_last_message ...",
                    "db.namespace": "0"
                },
            ) as db_span:
                try:
                    r = redis.Redis(host=redis_host, port=redis_port, password="password")
                    r.set("notification_last_message", json.dumps(notification))
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

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    app.run(host="0.0.0.0", port=5000)
