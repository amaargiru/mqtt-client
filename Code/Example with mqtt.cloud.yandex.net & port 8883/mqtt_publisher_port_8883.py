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
client_id: str = f"publisher_{str(uuid.uuid4())}"
mqtt_keepalive: int = 5 * 60
broker_first_connect_timeout: int = 1
broker_reconnect_timeout: int = 10
publish_period: int = 5

# Yandex Cloud certificates
cafile = "certificates/rootCA.crt"
certfile = "certificates/publisher_cert.pem"
keyfile = "certificates/publisher_key.pem"

# Path to logs
log_file_path: str = "logs/publisher.log"
# Max log file size
log_max_file_size: int = 1024 ** 2
# Max number of log files
log_max_file_count: int = 10

if __name__ == '__main__':
    # Create a path to the log file(s) if it doesn't exist
    path = pathlib.Path(log_file_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    logger = PyLogger.get_logger(log_file_path, log_max_file_size, log_max_file_count)

    # Load private topic name from config file
    with open('config/private_config.json') as config_file:
        config = json.load(config_file)
        publish_topic = config["topic_name"]

    connector = MqttConnector(broker, port, client_id, mqtt_keepalive, logger, publish_topic, subscribe_topic="",
                              cafile=cafile, certfile=certfile, keyfile=keyfile)

    # Waiting for connect to MQTT broker
    connector.connect()
    time.sleep(broker_first_connect_timeout)
    while not connector.is_connected():
        logger.debug(f"Timeout {broker_reconnect_timeout} seconds before next connection attempt...")
        time.sleep(broker_reconnect_timeout)
        connector.connect()

    for i in range(10):
        message = f"Message {i}"
        connector.publish(publish_topic, message, qos=1)

        time.sleep(publish_period)

    logger.debug("All done...")
