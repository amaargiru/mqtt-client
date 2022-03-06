import pathlib
import random
import time

from pylogger import PyLogger
from mqtt_connector import MqttConnector

broker = "test.mosquitto.org"
port = 1883
client_id = f"publisher_{random.randint(0, 1000000)}"
mqtt_keepalive = 5 * 60
publish_topic = "publisher/"
subscribe_topic = "publisher/#"  # Multi-level wildcard for cover all topic levels
publish_period = 10

# Path to logs
log_file_path = "logs/publisher.log"
# Max log file size
log_max_file_size = 1024 ** 2
# Max number of log files
log_max_file_count = 10

if __name__ == '__main__':
    # Create a path to the log file(s) if it doesn't exist
    path = pathlib.Path(log_file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    logger = PyLogger.get_logger(log_file_path, log_max_file_size, log_max_file_count)

    connector = MqttConnector(broker, port, client_id, mqtt_keepalive, logger, publish_topic)

    # Try to connect to MQTT broker
    client = connector.connect()
    while not client.is_connected():
        pass

    while True:
        random_message = f"Random message {random.randint(0, 100)}"
        result = client.publish(publish_topic, random_message, qos=1)

        if result[0] == 0:
            logger.info(f"Message published \"{random_message}\" to topic \"{publish_topic}\"")
        else:
            logger.error(f"Publish message error \"{random_message}\" to topic \"{publish_topic}\"")

        time.sleep(publish_period)
