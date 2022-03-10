import pathlib
import random

from mqtt_connector import MqttConnector
from pylogger import PyLogger

broker: str = "test.mosquitto.org"
port: int = 1883
client_id: str = f"subscriber_{random.randint(0, 1000000)}"
mqtt_keepalive: int = 5 * 60
subscribe_topic: str = "amaargiru/#"  # Multi-level wildcard for cover all topic levels

# Path to logs
log_file_path: str = "logs/subscriber.log"
# Max log file size
log_max_file_size: int = 1024 ** 2
# Max number of log files
log_max_file_count: int = 10


def log_message(message):
    logger.info(f"Message \"{message}\" received")


if __name__ == '__main__':
    # Create a path to the log file(s) if it doesn't exist
    path = pathlib.Path(log_file_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    logger = PyLogger.get_logger(log_file_path, log_max_file_size, log_max_file_count)

    connector = MqttConnector(broker, port, client_id, mqtt_keepalive, logger, subscribe_topic=subscribe_topic)

    # Simple waiting for connect to MQTT broker
    connector.connect(on_message_callback=log_message)
    while not connector.is_connected():
        pass

    while True:
        pass
