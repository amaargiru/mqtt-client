import pathlib
import random
import time

from paho.mqtt import client as mqtt_client

from pylogger import PyLogger

mqtt_broker = "test.mosquitto.org"
mqtt_broker_port = 1883
mqtt_main_topic = "ya_publisher/sample"
mqtt_main_topic_subscribe = "ya_publisher/sample/#"  # Multi-level wildcard for cover all topic levels
mqtt_client_id = f"ya_publisher_{random.randint(0, 1000000)}"
mqtt_keepalive = 5 * 60
broker_reconnect_timeout = 10
publish_period = 10

# Path to logs
log_file_path = "logs//ya_publisher.log"
# Max log file size
log_max_file_size = 1024 ** 2
# Max number of log files
log_max_file_count = 10


def connect_mqtt():
    def on_connect(client, userdata, flags, rc) -> None:
        if rc == 0:
            logger.info(f"MQTT client \"{mqtt_client_id}\" connected to MQTT broker \"{mqtt_broker}\"")
        else:
            logger.error(f"MQTT client \"{mqtt_client_id}\" unable to connect to MQTT broker \"{mqtt_broker}\" and return code \"{rc}\"")

        # Subscribe after connection lost
        client.subscribe(mqtt_main_topic_subscribe)

    def on_disconnect(client, userdata, rc) -> None:
        if rc != 0:
            logger.error(f"MQTT client \"{mqtt_client_id}\" disconnect from MQTT broker \"{mqtt_broker}\"")

    def on_message(client, userdata, message) -> None:
        logger.info(f"Received message \"{message.payload.decode()}\" from topic \"{message.topic}\"")

    def on_log(client, userdata, level, buff) -> None:
        logger.debug(buff)

    def on_publish(client, userdata, mid) -> None:
        logger.debug(f"Message published, mid = {mid}")

    def on_subscribe(mqttc, obj, mid, granted_qos) -> None:
        logger.debug("Subscribed " + str(mid) + " " + str(granted_qos))

    client = mqtt_client.Client(mqtt_client_id)
    client.connect(mqtt_broker, mqtt_broker_port, keepalive=mqtt_keepalive)
    client.subscribe(mqtt_main_topic_subscribe)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.on_log = on_log
    client.on_publish = on_publish
    client.on_subscribe = on_subscribe
    return client


if __name__ == '__main__':
    # Create a path to the log file if it doesn't exist
    path = pathlib.Path(log_file_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    logger = PyLogger.get_logger(log_file_path, log_max_file_size, log_max_file_count)
    client: mqtt_client = None

    # Try to connect to Yandex MQTT broker
    connected_to_broker = False
    while not connected_to_broker:
        try:
            client = connect_mqtt()
            client.loop_start()
            connected_to_broker = True
        except Exception as e:
            logger.error(f"Error \"{str(e)}\" when trying to connect MQTT client \"{mqtt_client_id}\" to MQTT broker \"{mqtt_broker}\"")
            logger.debug(f"Timeout {broker_reconnect_timeout} seconds before reconnect")
            connected_to_broker = False
            time.sleep(broker_reconnect_timeout)

    while True:
        time.sleep(publish_period)

        random_message = f"Random message_{random.randint(0, 100)}"
        result = client.publish(mqtt_main_topic, random_message, qos=1)

        if result[0] == 0:
            logger.info(f"Message published \"{random_message}\" to topic \"{mqtt_main_topic}\"")
        else:
            logger.error(f"Publish message error \"{random_message}\" to topic \"{mqtt_main_topic}\"")
