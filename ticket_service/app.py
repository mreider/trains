import os
import json
import pika
import redis
import random
import sys
from flask import Flask, jsonify, request
from otel import tracer
from opentelemetry.trace import SpanKind, Status, StatusCode

app = Flask(__name__)

def publish_message():
    rabbit_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    rabbit_port = int(os.getenv("RABBITMQ_PORT", "5672"))
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    message_id = f"msg-{random.randint(1000,9999)}"
    conversation_id = f"conv-{random.randint(100,999)}"

    with tracer.start_as_current_span(
        "publish_ticket_message",
        kind=SpanKind.PRODUCER,
        attributes={
            "messaging.operation": "send",
            "messaging.destination.name": "TicketQueue",
            "messaging.message.id": message_id,
            "messaging.message.conversation_id": conversation_id,
            "server.address": rabbit_host,
        },
    ) as msg_span:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host, port=rabbit_port, credentials=pika.PlainCredentials("admin", "password")))
            channel = connection.channel()
            channel.queue_declare(queue='TicketQueue', durable=True)
            message = {
                "ticket_id": "456",
                "train_id": "123",
                "passenger_id": "789",
                "seat_number": "12A",
                "departure_time": "2025-04-15T10:00:00",
                "message_id": message_id,
                "conversation_id": conversation_id
            }
            # Random error injection for messaging
            if random.random() < 0.001:
                raise RuntimeError("Simulated messaging failure")
            channel.basic_publish(exchange='', routing_key='TicketQueue', body=json.dumps(message))
            # otel_logger.info("TicketService: Sent ticket booking message", attributes={"messaging.message.id": message_id})
            msg_span.set_status(Status(StatusCode.OK))
        except Exception as exc:
            import traceback
            msg_span.set_status(Status(StatusCode.ERROR, str(exc)))
            msg_span.set_attribute("error.type", type(exc).__name__)
            print(f"[TicketService] Error sending message: {exc}", file=sys.stdout)
            traceback.print_exc()
            raise
        finally:
            if 'connection' in locals():
                connection.close()
    # Redis operation
    with tracer.start_as_current_span(
        "redis_set_last_message",
        kind=SpanKind.CLIENT,
        attributes={
            "db.system": "redis",
            "db.operation.name": "SET",
            "db.query.text": "SET ticket_service_last_message ...",
            "network.peer.address": redis_host,
            "db.namespace": "0"
        },
    ) as db_span:
        try:
            r = redis.Redis(host=redis_host, port=redis_port, password="password")
            # Random error injection for Redis
            if random.random() < 0.001:
                raise redis.RedisError("Simulated Redis failure")
            r.set("ticket_service_last_message", json.dumps(message))
            db_span.set_status(Status(StatusCode.OK))
        except Exception as exc:
            import traceback
            db_span.set_status(Status(StatusCode.ERROR, str(exc)))
            db_span.set_attribute("error.type", type(exc).__name__)
            print(f"[TicketService] Error saving to Redis: {exc}", file=sys.stdout)
            traceback.print_exc()
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
            if random.random() < 0.001:
                raise ValueError("Simulated HTTP error")
            publish_message()
            route_span.set_status(Status(StatusCode.OK))
            return jsonify({"status": "TicketService triggered"}), 200
        except Exception as e:
            import traceback
            route_span.set_status(Status(StatusCode.ERROR, str(e)))
            route_span.set_attribute("error.type", type(e).__name__)
            print(f"[TicketService] HTTP error: {e}", file=sys.stdout)
            traceback.print_exc()
            return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
