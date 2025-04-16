import os
import json
from celery_app import celery_app
import redis
import random
import sys
from flask import Flask, jsonify, request
from otel import tracer
from opentelemetry.trace import SpanKind, Status, StatusCode

app = Flask(__name__)

@celery_app.task(name="passenger_service.publish_passenger")
def publish_passenger(message):
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    with tracer.start_as_current_span(
        "publish_passenger_message",
        kind=SpanKind.PRODUCER,
        attributes={
            "messaging.system": "rabbitmq",
            "messaging.destination": "PassengerQueue",
            "messaging.destination_kind": "queue",
            "messaging.operation": "send",
            "messaging.message.id": message.get("message_id", "unknown"),
            "messaging.message.conversation_id": message.get("conversation_id", "unknown"),
        },
    ) as msg_span:
        try:
            # Random error injection for messaging
            if random.random() < 0.15:
                raise RuntimeError("Simulated messaging failure")
            # In Celery, the message is already delivered
            msg_span.set_status(Status(StatusCode.OK))
        except Exception as exc:
            msg_span.set_status(Status(StatusCode.ERROR, str(exc)))
            msg_span.set_attribute("error.type", type(exc).__name__)
            raise
    # Redis operation
    with tracer.start_as_current_span(
        "redis_set_last_message",
        kind=SpanKind.CLIENT,
        attributes={
            "db.system": "redis",
            "db.operation.name": "SET",
            "db.query.text": "SET passenger_service_last_message ...",
            "db.namespace": "0"
        },
    ) as db_span:
        try:
            r = redis.Redis(host=redis_host, port=redis_port, password="password")
            # Random error injection for Redis
            if random.random() < 0.10:
                raise redis.RedisError("Simulated Redis failure")
            r.set("passenger_service_last_message", json.dumps(message))
            db_span.set_status(Status(StatusCode.OK))
        except Exception as exc:
            db_span.set_status(Status(StatusCode.ERROR, str(exc)))
            db_span.set_attribute("error.type", type(exc).__name__)
            raise

@app.route('/trigger', methods=['GET'])
def trigger():
    with tracer.start_as_current_span(
        "http_trigger",
        kind=SpanKind.SERVER,
        attributes={
            "http.method": request.method,
            "http.route": "/trigger",
            "http.route": "/trigger",
            "http.scheme": request.scheme,
            "net.peer.ip": request.remote_addr,
            "server.address": request.host,
        },
    ) as route_span:
        try:
            # Random error injection for HTTP
            if random.random() < 0.10:
                raise ValueError("Simulated HTTP error")
            message_id = f"msg-{random.randint(1000,9999)}"
            conversation_id = f"conv-{random.randint(100,999)}"
            message = {
                "passenger_id": "789",
                "name": "John Doe",
                "contact_info": "john.doe@example.com",
                "message_id": message_id,
                "conversation_id": conversation_id
            }
            publish_passenger.delay(message)
            route_span.set_status(Status(StatusCode.OK))
            return jsonify({"status": "PassengerService triggered"}), 200
        except Exception as e:
            route_span.set_status(Status(StatusCode.ERROR, str(e)))
            route_span.set_attribute("error.type", type(e).__name__)
            return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
