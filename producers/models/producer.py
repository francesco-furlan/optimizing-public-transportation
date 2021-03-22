"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://kafka0:19092",
            "schema.registry.url": "http://schema-registry:8081",
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema = self.key_schema,
            default_value_schema = self.value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        client = AdminClient({
            "bootstrap.servers": self.broker_properties["bootstrap.servers"]
        })
        topic_exists = self.check_topic_exists(client, self.topic_name)
        
        if topic_exists:
            logger.warn(f"Topic {self.topic_name} already exist!")
            return

        logger.info(f"Creating topic: {self.topic_name}")

        create_topic_futures = client.create_topics(
            [
                NewTopic(
                    topic = self.topic_name,
                    num_partitions = self.num_partitions,
                    replication_factor = self.num_replicas
                )
            ]
        )

        for topic, create_topic_future in create_topic_futures.items():
            try:
                create_topic_future.result()
                logger.info(f"Topic {self.topic_name} created")
            except Exception as e:
                logger.fatal(f"Topic {topic} failed to create: {e}")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        if self.producer is not None:
            self.producer.flush()
            logger.info("Producer flushed!")

    def check_topic_exists(self, client, topic_name):
        """Use this function to get the key for Kafka Events"""
        topic_metadata = client.list_topics()
        topics = topic_metadata.topics
        return topic_name in topics
