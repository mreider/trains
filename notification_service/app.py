import os
import json
import pika
import redis
import random
import sys
import time
from otel import tracer
from opentelemetry.trace import SpanKind, Status, StatusCode

def main():
    rabbit_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    rabbit_port = int(os.getenv("RABBITMQ_PORT", "5672"))
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))

    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host, port=rabbit_port, credentials=pika.PlainCredentials("admin", "password")))
            channel = connection.channel()
            channel.queue_declare(queue='NotificationQueue', durable=True)

            def callback(ch, method, properties, body):
                with tracer.start_as_current_span(
                    "receive_notification_message",
                    kind=SpanKind.CLIENT,
                    attributes={
                        "messaging.operation": "receive",
                        "messaging.destination.name": "NotificationQueue",
                        "messaging.message.id": getattr(properties, "message_id", None) or "unknown",
                        "server.address": rabbit_host,
                        "messaging.message.conversation_id": getattr(properties, "correlation_id", None) or "unknown",
                    },
                ) as recv_span:
                    try:
                        notification = json.loads(body)
                        # Random error injection for notification processing
                        if random.random() < 0.001:
                            raise RuntimeError("Simulated notification processing error")
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        # Redis operation
                        with tracer.start_as_current_span(
                            "redis_set_last_message",
                            kind=SpanKind.CLIENT,
                            attributes={
                                "db.system": "redis",
                                "db.operation.name": "SET",
                                "db.query.text": "SET notification_last_message ...",
                                "network.peer.address": redis_host,
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

            channel.basic_consume(queue='NotificationQueue', on_message_callback=callback)
            print("NotificationService: Waiting for notification messages...", flush=True)
            channel.start_consuming()
        except KeyboardInterrupt:
            print("NotificationService: Shutting down...", flush=True)
            break
        except Exception as e:
            print(f"NotificationService: Error occurred: {e}", flush=True, file=sys.stdout)
            time.sleep(5)  # Sleep before retrying after error



if __name__ == "__main__":
    main()
