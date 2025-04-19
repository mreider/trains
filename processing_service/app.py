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
            channel.queue_declare(queue='AggregationQueue', durable=True)
            channel.queue_declare(queue='NotificationQueue', durable=True)

            def callback(ch, method, properties, body):
                with tracer.start_as_current_span(
                    "receive_aggregation_message",
                    kind=SpanKind.CLIENT,
                    attributes={
                        "messaging.operation": "receive",
                        "messaging.destination.name": "AggregationQueue",
                        "messaging.message.id": getattr(properties, "message_id", None) or "unknown",
                        "server.address": rabbit_host,
                        "messaging.message.conversation_id": getattr(properties, "correlation_id", None) or "unknown",
                    },
                ) as recv_span:
                    try:
                        with tracer.start_as_current_span(
                            "process_aggregation_message",
                            kind=SpanKind.CONSUMER,
                            attributes={
                                "messaging.operation": "process",
                                "messaging.destination.name": "AggregationQueue",
                                "messaging.message.id": getattr(properties, "message_id", None) or "unknown",
                                "server.address": rabbit_host,
                                "messaging.message.conversation_id": getattr(properties, "correlation_id", None) or "unknown",
                            },
                        ) as process_span:
                            aggregated = json.loads(body)
                            # Random error injection for message processing
                            if random.random() < 0.0001:
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
                            with tracer.start_as_current_span(
                                "send_notification_message",
                                kind=SpanKind.PRODUCER,
                                attributes={
                                    "messaging.operation": "send",
                                    "messaging.destination.name": "NotificationQueue",
                                    "messaging.message.id": f"notif-{random.randint(1000,9999)}",
                                    "server.address": rabbit_host,
                                    "messaging.message.conversation_id": getattr(properties, "correlation_id", None) or "unknown",
                                },
                            ) as send_span:
                                channel.basic_publish(exchange='', routing_key='NotificationQueue', body=json.dumps(notification))
                                send_span.set_status(Status(StatusCode.OK))
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                            # Redis operation
                            with tracer.start_as_current_span(
                                "redis_set_last_message",
                                kind=SpanKind.CLIENT,
                                attributes={
                                    "db.system": "redis",
                                    "db.operation.name": "SET",
                                    "db.query.text": "SET processing_last_message ...",
                                    "network.peer.address": redis_host,
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
                            process_span.set_status(Status(StatusCode.OK))
                        recv_span.set_status(Status(StatusCode.OK))
                    except Exception as exc:
                        recv_span.set_status(Status(StatusCode.ERROR, str(exc)))
                        recv_span.set_attribute("error.type", type(exc).__name__)
                        raise

            channel.basic_consume(queue='AggregationQueue', on_message_callback=callback)
            print("ProcessingService: Waiting for aggregated messages...", flush=True)
            channel.start_consuming()
        except KeyboardInterrupt:
            print("ProcessingService: Shutting down...", flush=True)
            break
        except Exception as e:
            print(f"ProcessingService: Error occurred: {e}", flush=True, file=sys.stdout)
            time.sleep(5)  # Sleep before retrying after error



if __name__ == "__main__":
    main()
