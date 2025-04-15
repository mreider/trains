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
    channel.queue_declare(queue='AggregationQueue', durable=True)
    channel.queue_declare(queue='NotificationQueue', durable=True)

    def callback(ch, method, properties, body):
        aggregated = json.loads(body)
        print("ProcessingService: Received aggregated message:", aggregated)
        passenger = aggregated.get("passengers", [{}])[0]
        notification = {
            "passenger_id": passenger.get("passenger_id", "unknown"),
            "message": f"Your train (ID: {aggregated.get('train_id')}) is scheduled to depart at {aggregated.get('schedule', {}).get('departure_time', '')} from {aggregated.get('schedule', {}).get('route', [''])[0]}."
        }
        channel.basic_publish(exchange='', routing_key='NotificationQueue', body=json.dumps(notification))
        print("ProcessingService: Sent notification message.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        r = redis.Redis(host=redis_host, port=redis_port, password="password")
        r.set("processing_last_message", json.dumps(aggregated))

    channel.basic_consume(queue='AggregationQueue', on_message_callback=callback)
    print("ProcessingService: Waiting for aggregated messages...")
    channel.start_consuming()


if __name__ == "__main__":
    main()
