import time
import requests
import threading


def generate_load():
    services = [
        "http://train-service:5000/trigger",
        "http://ticket-service:5000/trigger",
        "http://passenger-service:5000/trigger",
        "http://train-management-service:5000/trigger"
    ]
    while True:
        for url in services:
            try:
                response = requests.get(url)
                print(f"Triggered {url}: Status {response.status_code}")
            except Exception as e:
                print(f"Error triggering {url}: {e}")
        time.sleep(2)


if __name__ == "__main__":
    load_thread = threading.Thread(target=generate_load)
    load_thread.start()
    load_thread.join()
