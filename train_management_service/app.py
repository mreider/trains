import os
import json
from celery_app import celery_app
import redis
import threading
import random
import sys
from flask import Flask, jsonify, request
from otel import tracer
from opentelemetry.trace import SpanKind, Status, StatusCode

app = Flask(__name__)

import time

@celery_app.task(name="train_management_service.consume_and_fanout")
def consume_and_fanout(message):
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    with tracer.start_as_current_span(
        "receive_train_management_message",
        kind=SpanKind.CONSUMER,
        attributes={
            "messaging.system": "rabbitmq",
            "messaging.destination": "TrainManagementQueue",
            "messaging.destination_kind": "queue",
            "messaging.operation": "receive",
            "messaging.message.id": message.get("message_id", "unknown"),
            "messaging.message.conversation_id": message.get("conversation_id", "unknown"),
        },
    ) as recv_span:
        try:
            # Random error injection for message processing
            if random.random() < 0.12:
                raise RuntimeError("Simulated message processing error")
            for queue, task_name in [
                ("ScheduleQueue", "train_service.send_schedule_update"),
                ("TicketQueue", "ticket_service.publish_ticket"),
                ("PassengerQueue", "passenger_service.publish_passenger")
            ]:
                with tracer.start_as_current_span(
                    f"send_fanout_{queue}",
                    kind=SpanKind.PRODUCER,
                    attributes={
                        "messaging.operation": "send",
                        "messaging.destination.name": queue,
                        "messaging.message.id": f"fanout-{random.randint(1000,9999)}",
                        "messaging.message.conversation_id": message.get("conversation_id", "unknown"),
                    },
                ) as send_span:
                    mod_ref = __import__(mod, fromlist=[task_name])
                    task = getattr(mod_ref, task_name)
                    task.delay(message)
                    send_span.set_status(Status(StatusCode.OK))
            # Redis operation
            with tracer.start_as_current_span(
                "redis_set_last_message",
                kind=SpanKind.CLIENT,
                attributes={
                    "db.system": "redis",
                    "db.operation.name": "SET",
                    "db.query.text": "SET train_management_last_message ...",
                    "db.namespace": "0"
                },
            ) as db_span:
                try:
                    r = redis.Redis(host=redis_host, port=redis_port, password="password")
                    r.set("train_management_last_message", json.dumps(message))
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

# No consumer thread

def publish_message():
    # For load testing, send a message to TrainManagementQueue
    rabbit_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    rabbit_port = int(os.getenv("RABBITMQ_PORT", "5672"))
    message_id = f"msg-{random.randint(1000,9999)}"
    conversation_id = f"conv-{random.randint(100,999)}"
    with tracer.start_as_current_span(
        "publish_train_management_message",
        kind=SpanKind.PRODUCER,
        attributes={
            "messaging.operation": "send",
            "messaging.destination.name": "TrainManagementQueue",
            "messaging.message.id": message_id,
            "messaging.message.conversation_id": conversation_id,
            "server.address": rabbit_host,
        },
    ) as msg_span:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host, port=rabbit_port, credentials=pika.PlainCredentials("admin", "password")))
            channel = connection.channel()
            channel.queue_declare(queue='TrainManagementQueue', durable=True)
            message = {
                "operation": "update_schedule",
                "train_id": "123",
                "departure_time": "2025-04-15T10:00:00",
                "arrival_time": "2025-04-15T14:00:00",
                "route": ["StationA", "StationB", "StationC"],
                "message_id": message_id,
                "conversation_id": conversation_id
            }
            # Random error injection for messaging
            if random.random() < 0.15:
                raise RuntimeError("Simulated messaging failure")
            channel.basic_publish(exchange='', routing_key='TrainManagementQueue', body=json.dumps(message))
            # otel_logger.info("TrainManagementService: Sent message to TrainManagementQueue", attributes={"messaging.message.id": message_id})
            msg_span.set_status(Status(StatusCode.OK))
        except Exception as exc:
            msg_span.set_status(Status(StatusCode.ERROR, str(exc)))
            msg_span.set_attribute("error.type", type(exc).__name__)
            # otel_logger.error(f"Error sending message: {exc}", attributes={"error.type": type(exc).__name__})
            raise
        finally:
            if 'connection' in locals():
                connection.close()

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
            publish_message()
            route_span.set_status(Status(StatusCode.OK))
            return jsonify({"status": "TrainManagementService triggered"}), 200
        except Exception as e:
            route_span.set_status(Status(StatusCode.ERROR, str(e)))
            route_span.set_attribute("error.type", type(e).__name__)
            # otel_logger.error(f"HTTP error: {e}", attributes={"error.type": type(e).__name__})
            return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
