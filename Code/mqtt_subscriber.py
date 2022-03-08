import pathlib
import random
import time

from pylogger import PyLogger
from mqtt_connector import MqttConnector

broker = "test.mosquitto.org"
port = 1883
client_id = f"subscriber_{random.randint(0, 1000000)}"
mqtt_keepalive = 5 * 60
subscribe_topic = "amaargiru/#"  # Multi-level wildcard for cover all topic levels

# Path to logs
log_file_path = "logs/subscriber.log"
# Max log file size
log_max_file_size = 1024 ** 2
# Max number of log files
log_max_file_count = 10

if __name__ == '__main__':
    # Create a path to the log file(s) if it doesn't exist
    path = pathlib.Path(log_file_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    logger = PyLogger.get_logger(log_file_path, log_max_file_size, log_max_file_count)

    connector = MqttConnector(broker, port, client_id, mqtt_keepalive, logger, subscribe_topic=subscribe_topic)

    # Wait for connect to MQTT broker
    client = connector.connect()
    while not client.is_connected():
        pass

    while True:
        pass
