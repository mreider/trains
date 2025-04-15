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
    for q in ['ScheduleQueue', 'TicketQueue', 'PassengerQueue', 'AggregationQueue']:
        channel.queue_declare(queue=q, durable=True)

    print("AggregationService: Attempting to aggregate messages...")

    schedule_method, schedule_props, schedule_body = channel.basic_get(queue='ScheduleQueue')
    ticket_method, ticket_props, ticket_body = channel.basic_get(queue='TicketQueue')
    passenger_method, passenger_props, passenger_body = channel.basic_get(queue='PassengerQueue')

    if schedule_body or ticket_body or passenger_body:
        schedule = json.loads(schedule_body) if schedule_body else {}
        ticket = json.loads(ticket_body) if ticket_body else {}
        passenger = json.loads(passenger_body) if passenger_body else {}
        aggregated = {
            "train_id": "123",
            "schedule": schedule,
            "tickets": [ticket] if ticket else [],
            "passengers": [passenger] if passenger else []
        }
        channel.basic_publish(exchange='', routing_key='AggregationQueue', body=json.dumps(aggregated))
        print("AggregationService: Sent aggregated message.")
        if schedule_method:
            channel.basic_ack(schedule_method.delivery_tag)
        if ticket_method:
            channel.basic_ack(ticket_method.delivery_tag)
        if passenger_method:
            channel.basic_ack(passenger_method.delivery_tag)
        r = redis.Redis(host=redis_host, port=redis_port, password="password")
        r.set("aggregation_last_message", json.dumps(aggregated))
    else:
        print("AggregationService: No messages available to aggregate.")

    connection.close()


if __name__ == "__main__":
    main()
