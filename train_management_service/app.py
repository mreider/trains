import os
import json
import pika
import redis
import threading
from flask import Flask, jsonify

app = Flask(__name__)

def consumer():
    rabbit_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    rabbit_port = int(os.getenv("RABBITMQ_PORT", "5672"))
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host, port=rabbit_port, credentials=pika.PlainCredentials("admin", "password")))
    channel = connection.channel()
    channel.queue_declare(queue='TrainManagementQueue', durable=True)
    for q in ['ScheduleQueue', 'TicketQueue', 'PassengerQueue']:
        channel.queue_declare(queue=q, durable=True)

    def callback(ch, method, properties, body):
        message = json.loads(body)
        for queue in ['ScheduleQueue', 'TicketQueue', 'PassengerQueue']:
            channel.basic_publish(exchange='', routing_key=queue, body=json.dumps(message))
        print("TrainManagementService: Fanned out message to ScheduleQueue, TicketQueue, and PassengerQueue.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        r = redis.Redis(host=redis_host, port=redis_port, password="password")
        r.set("train_management_last_message", json.dumps(message))

    channel.basic_consume(queue='TrainManagementQueue', on_message_callback=callback)
    print("TrainManagementService: Consumer started, waiting for messages...")
    channel.start_consuming()


def publish_message():
    # For load testing, send a message to TrainManagementQueue
    rabbit_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    rabbit_port = int(os.getenv("RABBITMQ_PORT", "5672"))
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host, port=rabbit_port, credentials=pika.PlainCredentials("admin", "password")))
    channel = connection.channel()
    channel.queue_declare(queue='TrainManagementQueue', durable=True)
    message = {
        "operation": "update_schedule",
        "train_id": "123",
        "departure_time": "2025-04-15T10:00:00",
        "arrival_time": "2025-04-15T14:00:00",
        "route": ["StationA", "StationB", "StationC"]
    }
    channel.basic_publish(exchange='', routing_key='TrainManagementQueue', body=json.dumps(message))
    print("TrainManagementService: Sent message to TrainManagementQueue.")
    connection.close()

@app.route('/trigger', methods=['GET'])
def trigger():
    try:
        publish_message()
        return jsonify({"status": "TrainManagementService triggered"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    t = threading.Thread(target=consumer, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=5000)
