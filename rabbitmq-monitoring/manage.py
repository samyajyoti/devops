import os
import pika
import schedule
import time
import requests
import socket
import logging
import subprocess
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Config from .env
rabbitmq_host = os.getenv("RABBITMQ_HOST", "localhost")
rabbitmq_port = int(os.getenv("RABBITMQ_PORT", 5672))
rabbitmq_username = os.getenv("RABBITMQ_USER")
rabbitmq_password = os.getenv("RABBITMQ_PASSWORD")
rabbitmq_queues = [q.strip() for q in os.getenv("RABBITMQ_QUEUES", "").split(",") if q.strip()]
docker_container_name = os.getenv("DOCKER_CONTAINER_NAME")
slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")
slack_alert_headline = os.getenv("SLACK_ALERT_HEADLINE", "RABBITMQ_ALERT")

server_hostname = socket.gethostname()

# Alert tracking
queue_last_count = {}
queue_alert_counts = {}

connection = None
channel = None

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

def restart_docker_container(container_name):
    try:
        subprocess.run(["docker", "restart", container_name], check=True)
        logging.info(f"Container '{container_name}' restarted successfully.")
        send_slack_notification(
            "QUEUE STUCK → CONTAINER RESTARTED",
            {
                "Server": server_hostname,
                "Container": container_name,
                "Action": "Restarted due to queue not draining"
            }
        )
    except subprocess.CalledProcessError as err:
        logging.error(f"Failed to restart Docker container: {err}")
        send_slack_notification(
            "RESTART FAILURE",
            {
                "Server": server_hostname,
                "Error": str(err),
                "Action": "Manual intervention needed"
            }
        )

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
                slack_alert_headline,
                {
                    "Queue": queue_name,
                    "Server": server_hostname,
                    "Current count": current_count,
                    "Consumers": consumer_count,
                    "Previous count": last_count,
                    "Consecutive Alerts": queue_alert_counts[queue_name],
                    "Action": "Investigate processing"
                }
            )

            if queue_alert_counts[queue_name] >= 2:
                restart_docker_container(docker_container_name)
                queue_alert_counts[queue_name] = 0

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
