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
                if not "".__eq__(self.subscribe_topic):
                    self.client.subscribe(self.subscribe_topic)

                self.logger.info(f"Client \"{self.client_id}\" connected to broker \"{self.broker}\"")
            else:
                self.logger.error(
                    f"Client \"{self.client_id}\" unable to connect to broker \"{self.broker}\" and return code \"{rc}\"")

        def on_disconnect(client, userdata, rc) -> None:
            if rc != 0:
                self.logger.error(f"Client \"{self.client_id}\" disconnected from broker \"{self.broker}\"")

        def on_message(client, userdata, message) -> None:
            self.logger.info(f"Client \"{self.client_id}\" received message \"{message.payload.decode()}\" from topic \"{message.topic}\"")

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
        return self.client.loop_start()

    def disconnect(self):
        result = self.client.loop_stop()
        self.logger.info(f"Client \"{self.client_id}\" disconnected from broker \"{self.broker}\"")
        return result

    def publish(self, topic, payload=None, qos=0, retain=False, properties=None):

        result = self.client.publish(topic, payload, qos)

        if result[0] == 0:
            self.logger.info(f"Message \"{str(payload)}\" published to topic \"{topic}\"")
        else:
            self.logger.error(f"Publish message error \"{str(payload)}\" to topic \"{topic}\"")

        return result

    def is_connected(self):
        return self.client.is_connected()
