import pathlib
import random
import sys
import time
import json

sys.path.append('.')
from Client.mqtt_connector import MqttConnector
from Logger.pylogger import PyLogger

broker: str = "mqtt.cloud.yandex.net"
port: int = 8883
client_id: str = f"subscriber_{random.randint(0, 1000000)}"
mqtt_keepalive: int = 5 * 60
broker_connect_timeout: int = 1
broker_reconnect_timeout: int = 10

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

    # Load private topic name from config file
    with open('config/private_config.json') as config_file:
        config = json.load(config_file)
        subscribe_topic = config["topic_name"]

    connector = MqttConnector(broker, port, client_id, mqtt_keepalive, logger, subscribe_topic=subscribe_topic)

    # Waiting for connect to MQTT broker
    connector.connect(on_message_callback=log_message)
    time.sleep(broker_connect_timeout)
    while not connector.is_connected():
        logger.debug(f"Timeout {broker_reconnect_timeout} seconds before next connection attempt...")
        time.sleep(broker_reconnect_timeout)
        connector.connect(on_message_callback=log_message)

    while True:
        pass
