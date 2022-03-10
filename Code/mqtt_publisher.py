import pathlib
import random
import time

from mqtt_connector import MqttConnector
from pylogger import PyLogger

broker = "test.mosquitto.org"
port = 1883
client_id = f"publisher_{random.randint(0, 1000000)}"
mqtt_keepalive = 5 * 60
publish_topic = "amaargiru/"
publish_period = 5

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

    connector = MqttConnector(broker, port, client_id, mqtt_keepalive, logger, publish_topic=publish_topic)

    # Simple waiting for connect to MQTT broker
    connector.connect()
    while not connector.is_connected():
        pass

    for i in range(10):
        message = f"Message {i}"
        connector.publish(publish_topic, message, qos=1)

        time.sleep(publish_period)
