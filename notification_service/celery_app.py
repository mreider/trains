from celery import Celery
import os
from opentelemetry.instrumentation.celery import CeleryInstrumentor
from opentelemetry import trace
import otel  # Ensures tracer provider and exporter are set up
trace.set_tracer_provider(otel.tracer._tracer_provider)

rabbit_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
rabbit_port = os.getenv("RABBITMQ_PORT", "5672")
broker_url = f'amqp://admin:password@{rabbit_host}:{rabbit_port}//'

celery_app = Celery('notification_service', broker=broker_url, backend='rpc://')

# Instrument Celery for OpenTelemetry
CeleryInstrumentor().instrument()

# Optional: Celery config
celery_app.conf.task_acks_late = True
celery_app.conf.worker_prefetch_multiplier = 1

# Ensure tasks are registered
import app
