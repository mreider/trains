import os
import json
import pika
import redis
import threading
import random
import sys
from flask import Flask, jsonify, request
from otel import tracer
from opentelemetry.trace import SpanKind, Status, StatusCode

app = Flask(__name__)

import time

def consumer():
    rabbit_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    rabbit_port = int(os.getenv("RABBITMQ_PORT", "5672"))
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host, port=rabbit_port, credentials=pika.PlainCredentials("admin", "password")))
            channel = connection.channel()
            channel.queue_declare(queue='TrainManagementQueue', durable=True)
            for q in ['ScheduleQueue', 'TicketQueue', 'PassengerQueue']:
                channel.queue_declare(queue=q, durable=True)

            def callback(ch, method, properties, body):
                with tracer.start_as_current_span(
                    "receive_train_management_message",
                    kind=SpanKind.CLIENT,
                    attributes={
                        "messaging.operation": "receive",
                        "messaging.destination.name": "TrainManagementQueue",
                        "messaging.message.id": getattr(properties, "message_id", None) or "unknown",
                        "messaging.message.conversation_id": getattr(properties, "correlation_id", None) or "unknown",
                        "server.address": rabbit_host,
                    },
                ) as recv_span:
                    try:
                        with tracer.start_as_current_span(
                            "process_train_management_message",
                            kind=SpanKind.CONSUMER,
                            attributes={
                                "messaging.operation": "process",
                                "messaging.destination.name": "TrainManagementQueue",
                                "messaging.message.id": getattr(properties, "message_id", None) or "unknown",
                                "messaging.message.conversation_id": getattr(properties, "correlation_id", None) or "unknown",
                                "server.address": rabbit_host,
                            },
                        ) as process_span:
                            message = json.loads(body)
                            # otel_logger.info("TrainManagementService: Received management message", attributes={"messaging.message.id": getattr(properties, "message_id", None) or "unknown",})
                            # Random error injection for message processing
                            if random.random() < 0.001:
                                raise RuntimeError("Simulated message processing error")
                            for queue in ['ScheduleQueue', 'TicketQueue', 'PassengerQueue']:
                                with tracer.start_as_current_span(
                                    f"send_fanout_{queue}",
                                    kind=SpanKind.PRODUCER,
                                    attributes={
                                        "messaging.operation": "send",
                                        "messaging.destination.name": queue,
                                        "messaging.message.id": f"fanout-{random.randint(1000,9999)}",
                                        "server.address": rabbit_host,
                                        "messaging.message.conversation_id": getattr(properties, "correlation_id", None) or "unknown",
                                    },
                                ) as send_span:
                                    channel.basic_publish(exchange='', routing_key=queue, body=json.dumps(message))
                                    # otel_logger.info(f"TrainManagementService: Fanned out message to {queue}")
                                    send_span.set_status(Status(StatusCode.OK))
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                            # Redis operation
                            with tracer.start_as_current_span(
                                "redis_set_last_message",
                                kind=SpanKind.CLIENT,
                                attributes={
                                    "db.system": "redis",
                                    "db.operation.name": "SET",
                                    "db.query.text": "SET train_management_last_message ...",
                                    "network.peer.address": redis_host,
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
                                    # otel_logger.error(f"Error saving to Redis: {exc}", attributes={"error.type": type(exc).__name__})
                                    raise
                            process_span.set_status(Status(StatusCode.OK))
                        recv_span.set_status(Status(StatusCode.OK))
                    except Exception as exc:
                        recv_span.set_status(Status(StatusCode.ERROR, str(exc)))
                        recv_span.set_attribute("error.type", type(exc).__name__)
                        # otel_logger.error(f"Error processing management message: {exc}", attributes={"error.type": type(exc).__name__})
                        raise

            channel.basic_consume(queue='TrainManagementQueue', on_message_callback=callback)
            print("TrainManagementService: Consumer started, waiting for messages...", flush=True)
            channel.start_consuming()
        except KeyboardInterrupt:
            print("TrainManagementService: Shutting down...", flush=True)
            break
        except Exception as e:
            print(f"TrainManagementService: Error occurred: {e}", flush=True, file=sys.stdout)
            time.sleep(5)  # Sleep before retrying after error



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
            if random.random() < 0.001:
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
            if random.random() < 0.001:
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
    t = threading.Thread(target=consumer, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=5000)
