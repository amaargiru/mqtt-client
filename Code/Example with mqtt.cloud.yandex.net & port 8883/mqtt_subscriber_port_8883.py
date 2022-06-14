import json
import pathlib
import sys
import time
import uuid

sys.path.append('.')
from Client.mqtt_connector import MqttConnector
from Logger.pylogger import PyLogger

broker: str = "mqtt.cloud.yandex.net"
port: int = 8883
# Only alphanumerical and limit length (http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc385349242)
client_id: str = f"sub_{str(uuid.uuid4())}".replace("-", "")[:23]
mqtt_keepalive: int = 5 * 60
broker_first_connect_timeout: int = 1
broker_reconnect_timeout: int = 10

# Yandex Cloud certificates
cafile = "certificates/rootCA.crt"
certfile = "certificates/subscriber_cert.pem"
keyfile = "certificates/subscriber_key.pem"

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

    connector = MqttConnector(broker, port, client_id, mqtt_keepalive, logger, publish_topic="", subscribe_topic=subscribe_topic,
                              cafile=cafile, certfile=certfile, keyfile=keyfile)

    # Waiting for connect to MQTT broker
    connector.connect(on_message_callback=log_message)
    time.sleep(broker_first_connect_timeout)
    while not connector.is_connected():
        logger.debug(f"Timeout {broker_reconnect_timeout} seconds before next connection attempt...")
        time.sleep(broker_reconnect_timeout)
        connector.connect(on_message_callback=log_message)

    while True:
        pass
