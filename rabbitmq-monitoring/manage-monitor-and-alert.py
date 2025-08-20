import pika
import schedule
import time
import requests
import socket
import logging
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Config from .env
slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
rabbitmq_port = int(os.getenv("RABBITMQ_PORT", 5672))
rabbitmq_username = os.getenv("RABBITMQ_USERNAME")
rabbitmq_password = os.getenv("RABBITMQ_PASSWORD")

# Load queue list from .env (comma separated)
rabbitmq_queues = os.getenv("RABBITMQ_QUEUES", "").split(",")
rabbitmq_queues = [q.strip() for q in rabbitmq_queues if q.strip()]  # clean empty values

server_hostname = socket.gethostname()
connection = None
channel = None

# Alert tracking
queue_last_count = {}
queue_alert_counts = {}

def send_slack_notification(message, details):
    try:
        payload = {
            "text": message,
            "blocks": [
                {
                    "type": "section",
                    "fields": [{"type": "mrkdwn", "text": f"*{key}:*\n{value}"} for key, value in details.items()]
                }
            ]
        }
        requests.post(slack_webhook_url, json=payload)
        logging.info("Slack notification sent successfully.")
    except Exception as e:
        logging.error(f"Error sending Slack notification: {e}")

def initialize_rabbitmq():
    global connection, channel
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rabbitmq_host,
                port=rabbitmq_port,
                credentials=pika.PlainCredentials(rabbitmq_username, rabbitmq_password)
            )
        )
        channel = connection.channel()
        logging.info("Connected to RabbitMQ.")
        return True
    except Exception as e:
        logging.error(f"RabbitMQ connection error: {e}")
        return False

def check_queue_count(queue_name):
    try:
        if not channel or channel.is_closed:
            if not initialize_rabbitmq():
                return

        queue_info = channel.queue_declare(queue=queue_name, passive=True)
        current_count = queue_info.method.message_count
        consumer_count = queue_info.method.consumer_count

        logging.info(f"Queue: {queue_name} | Count: {current_count} | Consumers: {consumer_count}")

        last_count = queue_last_count.get(queue_name, None)

        # Reset if queue is empty
        if current_count == 0:
            queue_alert_counts[queue_name] = 0
            queue_last_count[queue_name] = 0
            return

        if last_count is None:
            queue_last_count[queue_name] = current_count
            return

        if current_count >= last_count:
            queue_alert_counts[queue_name] = queue_alert_counts.get(queue_name, 0) + 1

            send_slack_notification(
                "🚨 RABBITMQ ALERT: Queue Possibly Stuck",
                {
                    "Queue": queue_name,
                    "Server": server_hostname,
                    "Current count": current_count,
                    "Consumers": consumer_count,
                    "Previous count": last_count,
                    "Consecutive Alerts": queue_alert_counts[queue_name],
                    "Action": "Investigate processing (no auto-restart)"
                }
            )
        else:
            queue_alert_counts[queue_name] = 0

        queue_last_count[queue_name] = current_count

    except pika.exceptions.ChannelClosedByBroker:
        logging.error("RabbitMQ channel closed by broker.")
        initialize_rabbitmq()
    except Exception as e:
        logging.error(f"Error checking queue '{queue_name}': {e}")

def schedule_jobs():
    for queue_name in rabbitmq_queues:
        schedule.every(10).minutes.do(check_queue_count, queue_name)

def main():
    initialize_rabbitmq()
    schedule_jobs()

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Terminated by user.")
            break
        except Exception as e:
            logging.error(f"Error in main loop: {e}")

    if connection:
        connection.close()
        logging.info("RabbitMQ connection closed.")

if __name__ == "__main__":
    main()
