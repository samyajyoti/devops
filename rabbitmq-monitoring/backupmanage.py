import pika
import schedule
import time
import requests
import socket
import logging

# Set logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set your Slack webhook URL here
slack_webhook_url = 'https://hooks.slack.com/services/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'

rabbitmq_host = 'localhost'
rabbitmq_port = 5672
rabbitmq_username = 'stageodysy'
rabbitmq_password = '64LHMwCVZF2zi'

rabbitmq_queues = ['action', 'app_push_queue', 'assign_policy_save', 'asynchronous_sync_queue', 'audit_data_queue', 'call_queue', 
                   'callback_queue', 'checksum_queue', 'device_callback_queue', 'emm_frp_queue', 'emm_upload_queue', 'fcm_queue', 'first_sync_queue', 'ios-process',
                   'knox_action_queue', 'policy_process_queue', 'process_data_queue', 'release_device', 'smartSwitch_queue', 'update_schedule_activity', 
                   'upload_queue']

# Get the server hostname
server_hostname = socket.gethostname()

# Initialize RabbitMQ connection
connection = None
channel = None

def initialize_rabbitmq():
    global connection, channel
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, credentials=pika.PlainCredentials(rabbitmq_username, rabbitmq_password))
        )
        channel = connection.channel()
        logging.info("Connected to RabbitMQ successfully!")
        return True
    except Exception as e:
        logging.error(f"Error connecting to RabbitMQ: {e}")
        return False

def send_slack_notification(message, details):
    try:
        payload = {
            "text": message,
            "blocks": [
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*{key}:*\n{value}"} for key, value in details.items()
                    ]
                }
            ]
        }
        requests.post(slack_webhook_url, json=payload)
        logging.info("Slack notification sent successfully.")
    except Exception as e:
        logging.error(f"Error sending Slack notification: {e}")

def check_queue_count(queue_name):
    try:
        # Check if the channel is closed, reconnect if necessary
        if not channel or channel.is_closed:
            if not initialize_rabbitmq():
                send_slack_notification("RABBITMQ ERROR", {"Server": server_hostname, "Message": "RabbitMQ server is down!"})
                return

        # Get the queue information
        queue_info = channel.queue_declare(queue=queue_name, passive=True)

        # Get the current message count in the queue
        current_count = queue_info.method.message_count

        # Get the number of consumers on the queue
        consumer_count = queue_info.method.consumer_count

        # Print the current count, consumer count, and server hostname
        logging.info(f"Queue: {queue_name} | Current count: {current_count} | Consumer count: {consumer_count} | Server hostname: {server_hostname}")

        # Skip alerts when the current count is 0
        if current_count == 0:
            logging.info(f"Skipping alert for {queue_name} as the current count is 0.")
            return

        # Check if the count has decreased
        if hasattr(check_queue_count, f'last_count_{queue_name}') and current_count >= getattr(check_queue_count, f'last_count_{queue_name}'):
            custom_message = "RABBITMQ ALERT"
            details = {
                "Queue": queue_name,
                "Server": server_hostname,
                "Status": "Queue count has not decreased for 10 minutes",
                "Current count": current_count,
                "Consumer count": consumer_count,
                "Action": "PLEASE CHECK stageodysy RabbitMq!"
            }
            logging.warning(f"{custom_message}: {details}")
            
            # Send Slack notification with the structured details
            send_slack_notification(custom_message, details)

        # Update the last count for the specific queue
        setattr(check_queue_count, f'last_count_{queue_name}', current_count)

    except pika.exceptions.ChannelClosedByBroker as e:
        logging.error("RabbitMQ channel closed by broker.")
        initialize_rabbitmq()  # Reinitialize RabbitMQ connection
    except Exception as e:
        logging.error(f"Error: {e}")

# Schedule the job for each queue to run every 60 minutes
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
            logging.info("Script terminated by user.")
            break
        except Exception as e:
            logging.error(f"Error in main loop: {e}")
            # Send alert if RabbitMQ server is down
            send_slack_notification("RABBITMQ ERROR", {"Server": server_hostname, "Message": "RabbitMQ server is down!"})

    if connection:
        connection.close()
        logging.info("RabbitMQ connection closed.")

if __name__ == "__main__":
    main()

