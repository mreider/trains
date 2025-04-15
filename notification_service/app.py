import os
import json
import pika
import redis


def main():
    rabbit_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    rabbit_port = int(os.getenv("RABBITMQ_PORT", "5672"))
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host, port=rabbit_port, credentials=pika.PlainCredentials("admin", "password")))
    channel = connection.channel()
    channel.queue_declare(queue='NotificationQueue', durable=True)

    def callback(ch, method, properties, body):
        notification = json.loads(body)
        print("NotificationService: Sending notification for passenger", notification.get("passenger_id"), ":", notification.get("message"))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        r = redis.Redis(host=redis_host, port=redis_port, password="password")
        r.set("notification_last_message", json.dumps(notification))

    channel.basic_consume(queue='NotificationQueue', on_message_callback=callback)
    print("NotificationService: Waiting for notification messages...")
    channel.start_consuming()


if __name__ == "__main__":
    main()
