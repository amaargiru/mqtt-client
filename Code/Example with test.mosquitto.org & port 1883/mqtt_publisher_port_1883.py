import pathlib
import sys
import time
import uuid

sys.path.append('.')
from Client.mqtt_connector import MqttConnector
from Logger.pylogger import PyLogger

broker: str = "test.mosquitto.org"  # Or, for example, broker.emqx.io, mqtt.eclipse.org
port: int = 1883
client_id: str = f"publisher_{str(uuid.uuid4())}"
mqtt_keepalive: int = 5 * 60
publish_topic: str = "amaargiru/"
broker_first_connect_timeout: int = 1
broker_reconnect_timeout: int = 10
publish_period: int = 5

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

    connector = MqttConnector(broker, port, client_id, mqtt_keepalive, logger, publish_topic=publish_topic)

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
