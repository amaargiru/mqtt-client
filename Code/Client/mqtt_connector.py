import logging

from paho.mqtt import client as mqtt_client


class MqttConnector:

    def __init__(self, broker: str, broker_port: int, client_id: str, keepalive: int, logger: logging.Logger,
                 publish_topic: str = "", subscribe_topic: str = "",
                 cafile: str = "", certfile: str = "", keyfile: str = ""):
        self.broker = broker
        self.broker_port = broker_port
        self.client_id = client_id
        self.keepalive = keepalive
        self.publish_topic = publish_topic
        self.subscribe_topic = subscribe_topic
        self.logger = logger
        self.client = mqtt_client.Client(self.client_id)
        self.cafile = cafile
        self.certfile = certfile
        self.keyfile = keyfile

    def connect(self, on_message_callback=None):
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
            decoded_message = message.payload.decode()

            if on_message_callback:
                try:
                    on_message_callback(decoded_message)
                except Exception as e:
                    self.logger.error(f"Callback error: {str(e)}")

            self.logger.debug(f"Client \"{self.client_id}\" received message \"{decoded_message}\" from topic \"{message.topic}\"")

        def on_log(client, userdata, level, buff) -> None:
            self.logger.debug(buff)

        def on_publish(client, userdata, mid) -> None:
            self.logger.debug(f"Message published, mid = {mid}")

        def on_subscribe(mqttc, obj, mid, granted_qos) -> None:
            self.logger.debug("Subscribed " + str(mid) + " " + str(granted_qos))

        try:
            if not "".__eq__(self.cafile) and not "".__eq__(self.certfile) and not "".__eq__(self.keyfile):
                self.client.tls_set(self.cafile, self.certfile, self.keyfile)

            self.client.connect(self.broker, self.broker_port, self.keepalive)
            self.client.on_connect = on_connect
            self.client.on_disconnect = on_disconnect
            self.client.on_message = on_message
            self.client.on_log = on_log
            self.client.on_publish = on_publish
            self.client.on_subscribe = on_subscribe
            return self.client.loop_start()

        except Exception as e:
            self.logger.error(f"Connection failed: \"{str(e)}\"")

    def disconnect(self):
        result = self.client.loop_stop()
        self.logger.info(f"Client \"{self.client_id}\" disconnected from broker \"{self.broker}\"")
        return result

    def publish(self, topic, payload=None, qos=0, retain=False, properties=None):

        result = self.client.publish(topic, payload, qos)

        if result[0] == 0:
            self.logger.info(f"Message \"{str(payload)}\" published to topic \"{topic}\"")
        else:
            self.logger.error(f"Error publishing message \"{str(payload)}\" to topic \"{topic}\"")

        return result

    def is_connected(self):
        return self.client.is_connected()
