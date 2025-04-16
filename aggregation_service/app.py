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
            from processing_service.app import process_aggregated
            process_aggregated.delay(aggregated)
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

# No __main__ needed; Celery worker will process tasks
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
