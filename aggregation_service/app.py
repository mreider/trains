import os
import json
import pika
import redis
import random
import sys
from otel import tracer
from opentelemetry.trace import SpanKind, Status, StatusCode
import time

def main():
    rabbit_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    rabbit_port = int(os.getenv("RABBITMQ_PORT", "5672"))
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))

    while True:
        try:
            with tracer.start_as_current_span(
                "aggregate_poll_cycle",
                kind=SpanKind.SERVER,
                attributes={
                    "messaging.system": "rabbitmq",
                    "messaging.destination.name": "AggregationQueues",
                    "server.address": rabbit_host,
                },
            ) as poll_span:
                connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host, port=rabbit_port, credentials=pika.PlainCredentials("admin", "password")))
                channel = connection.channel()
                for q in ['ScheduleQueue', 'TicketQueue', 'PassengerQueue', 'AggregationQueue']:
                    channel.queue_declare(queue=q, durable=True)

                # otel_logger.info("AggregationService: Attempting to aggregate messages...")

                schedule_method, schedule_props, schedule_body = channel.basic_get(queue='ScheduleQueue')
                ticket_method, ticket_props, ticket_body = channel.basic_get(queue='TicketQueue')
                passenger_method, passenger_props, passenger_body = channel.basic_get(queue='PassengerQueue')

                if schedule_body or ticket_body or passenger_body:
                    with tracer.start_as_current_span(
                        "aggregate_messages",
                        kind=SpanKind.INTERNAL,
                        attributes={
                            "aggregation.has_schedule": bool(schedule_body),
                            "aggregation.has_ticket": bool(ticket_body),
                            "aggregation.has_passenger": bool(passenger_body),
                        },
                    ) as agg_span:
                        try:
                            # Random error injection for aggregation
                            if random.random() < 0.13:
                                raise RuntimeError("Simulated aggregation error")
                            schedule = json.loads(schedule_body) if schedule_body else {}
                            ticket = json.loads(ticket_body) if ticket_body else {}
                            passenger = json.loads(passenger_body) if passenger_body else {}
                            aggregated = {
                                "train_id": "123",
                                "schedule": schedule,
                                "tickets": [ticket] if ticket else [],
                                "passengers": [passenger] if passenger else []
                            }
                            with tracer.start_as_current_span(
                                "publish_aggregated_message",
                                kind=SpanKind.PRODUCER,
                                attributes={
                                    "messaging.operation": "send",
                                    "messaging.destination.name": "AggregationQueue",
                                    "server.address": rabbit_host,
                                },
                            ) as send_span:
                                channel.basic_publish(exchange='', routing_key='AggregationQueue', body=json.dumps(aggregated))
                                # otel_logger.info("AggregationService: Sent aggregated message.")
                                send_span.set_status(Status(StatusCode.OK))
                            if schedule_method:
                                channel.basic_ack(schedule_method.delivery_tag)
                            if ticket_method:
                                channel.basic_ack(ticket_method.delivery_tag)
                            if passenger_method:
                                channel.basic_ack(passenger_method.delivery_tag)
                            # Redis operation
                            with tracer.start_as_current_span(
                                "redis_set_last_message",
                                kind=SpanKind.CLIENT,
                                attributes={
                                    "db.system": "redis",
                                    "db.operation.name": "SET",
                                    "db.query.text": "SET aggregation_last_message ...",
                                    "network.peer.address": redis_host,
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
                                    # otel_logger.error(f"Error saving to Redis: {exc}", attributes={"error.type": type(exc).__name__})
                                    raise
                            agg_span.set_status(Status(StatusCode.OK))
                        except Exception as exc:
                            agg_span.set_status(Status(StatusCode.ERROR, str(exc)))
                            agg_span.set_attribute("error.type", type(exc).__name__)
                            # otel_logger.error(f"Error during aggregation: {exc}", attributes={"error.type": type(exc).__name__})
                            raise
                else:
                    # otel_logger.info("AggregationService: No messages available to aggregate.")
                    time.sleep(5)  # Sleep before retrying if no messages
                connection.close()
        except KeyboardInterrupt:
            print("AggregationService: Shutting down...", flush=True)
            break
        except Exception as e:
            # otel_logger.error(f"AggregationService: Error occurred: {e}", attributes={"error.type": type(e).__name__})
            print(f"AggregationService: Error occurred: {e}", flush=True, file=sys.stdout)
            time.sleep(5)  # Sleep before retrying after error



if __name__ == "__main__":
    main()
