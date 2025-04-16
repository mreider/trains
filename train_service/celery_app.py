from celery import Celery
import os

rabbit_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
rabbit_port = os.getenv("RABBITMQ_PORT", "5672")
broker_url = f'amqp://admin:password@{rabbit_host}:{rabbit_port}//'

celery_app = Celery('train_service', broker=broker_url, backend='rpc://')

# Optional: Celery config
celery_app.conf.task_acks_late = True
celery_app.conf.worker_prefetch_multiplier = 1
