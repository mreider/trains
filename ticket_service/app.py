import os
import json
import pika
import redis
from flask import Flask, jsonify

app = Flask(__name__)

def publish_message():
    rabbit_host = os.getenv("RABBITMQ_HOST", "rabbitmq")
    rabbit_port = int(os.getenv("RABBITMQ_PORT", "5672"))
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host, port=rabbit_port, credentials=pika.PlainCredentials("admin", "password")))
    channel = connection.channel()
    channel.queue_declare(queue='TicketQueue', durable=True)

    message = {
        "ticket_id": "456",
        "train_id": "123",
        "passenger_id": "789",
        "seat_number": "12A",
        "departure_time": "2025-04-15T10:00:00"
    }

    channel.basic_publish(exchange='', routing_key='TicketQueue', body=json.dumps(message))
    print("TicketService: Sent ticket booking message.")

    r = redis.Redis(host=redis_host, port=redis_port, password="password")
    r.set("ticket_service_last_message", json.dumps(message))
    connection.close()

@app.route('/trigger', methods=['GET'])
def trigger():
    try:
        publish_message()
        return jsonify({"status": "TicketService triggered"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
