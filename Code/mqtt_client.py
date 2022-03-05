class MqttClient:

    def __init__(self, broker, broker_port, client_id, keepalive, publish_topic, subscribe_topic, logger):
        self.broker = broker
        self.broker_port = broker_port
        self.client_id = client_id
        self.keepalive = keepalive
        self.publish_topic = publish_topic
        self.subscribe_topic = subscribe_topic
        self.logger = logger
