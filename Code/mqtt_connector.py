from paho.mqtt import client as mqtt_client


class MqttConnector:

    def __init__(self, broker, broker_port, client_id, keepalive, logger, publish_topic="", subscribe_topic=""):
        self.broker = broker
        self.broker_port = broker_port
        self.client_id = client_id
        self.keepalive = keepalive
        self.publish_topic = publish_topic
        self.subscribe_topic = subscribe_topic
        self.logger = logger
        self.client = mqtt_client.Client(self.client_id)

    def connect(self):
        def on_connect(client, userdata, flags, rc) -> None:
            if rc == 0:
                self.logger.info(f"MQTT client \"{self.client_id}\" connected to MQTT broker \"{self.broker}\"")
            else:
                self.logger.error(
                    f"MQTT client \"{self.client_id}\" unable to connect to MQTT broker \"{self.broker}\" and return code \"{rc}\"")

            # Subscribe after connection lost
            if not "".__eq__(self.subscribe_topic):
                self.client.subscribe(self.subscribe_topic)

        def on_disconnect(client, userdata, rc) -> None:
            if rc != 0:
                self.logger.error(f"MQTT client \"{self.client_id}\" disconnect from MQTT broker \"{self.broker}\"")

        def on_message(client, userdata, message) -> None:
            self.logger.info(f"Message received \"{message.payload.decode()}\" from topic \"{message.topic}\"")

        def on_log(client, userdata, level, buff) -> None:
            self.logger.debug(buff)

        def on_publish(client, userdata, mid) -> None:
            self.logger.debug(f"Message published, mid = {mid}")

        def on_subscribe(mqttc, obj, mid, granted_qos) -> None:
            self.logger.debug("Subscribed " + str(mid) + " " + str(granted_qos))

        self.client.connect(self.broker, self.broker_port, self.keepalive)
        self.client.on_connect = on_connect
        self.client.on_disconnect = on_disconnect
        self.client.on_message = on_message
        self.client.on_log = on_log
        self.client.on_publish = on_publish
        self.client.on_subscribe = on_subscribe
        self.client.loop_start()
        return self.client
