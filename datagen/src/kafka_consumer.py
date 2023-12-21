#!/usr/bin/env python

import sys
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
import logging

from confluent_kafka import Consumer, OFFSET_BEGINNING, KafkaError
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from typing import Optional, Generator

# Parse the command line.
parser = ArgumentParser()
parser.add_argument('-t', '--topics', help="Topic to be consumed from", required= False, type=str, default=['nqa_raw','TCI','Zabbix_events'])
parser.add_argument('-c', '--config_file', type=FileType('r'), required= False, default="./kafka_config.ini")
parser.add_argument('-to', '--time_out', help="How long will the consumer poll for message", required= False, type=int, default=1000)
parser.add_argument('--reset', action='store_true')
args = parser.parse_args()


class kafka_consumer:

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    POLL_TIMEOUT_S = args.time_out

    # Create Consumer instance
    consumer = Consumer(config)
    deserialize_string = StringDeserializer()

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    topic = args.topics
    consumer.subscribe([topic], on_assign=reset_offset)

    # Write into log
    logging.basicConfig(filename='./log/consumer.log', encoding='utf-8', level=logging.DEBUG)


    # Fancy Version of polling for new messages from Kafka and print them.
    def poll_message(self) -> Optional[tuple[Optional[str], Optional[dict]]]:
        msg = self.consumer.poll(self.POLL_TIMEOUT_S)
        self._error_counter = 0
        if msg is None:
            return
        elif msg.error():
            err = msg.error()
            if err.code() in [KafkaError.TOPIC_AUTHORIZATION_FAILED, KafkaError.CLUSTER_AUTHORIZATION_FAILED,
                            KafkaError.GROUP_AUTHORIZATION_FAILED, KafkaError.SASL_AUTHENTICATION_FAILED]:
                raise RuntimeError(f"Kafka Authentication Error: {err}")
            logging.error(f"Consumer error: {err}") 
            return
        else:
            record_key = self.deserialize_string(msg.key())
            record_value = self.deserialize_string(msg.value())
            return record_key, record_value

    # Fancy version of consuming message
    def consume(self) -> Generator[tuple[Optional[str], Optional[dict]], None, None]:
        try:
            while True:
                record = self.poll_message()

                if record is None:
                    continue

                yield record  # yield is like return, but returns a generator instead.
        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            self.consumer.close()

consumer_instance = kafka_consumer()
for key, value in consumer_instance.consume():
    logging.debug(value)
