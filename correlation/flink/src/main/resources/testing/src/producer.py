import os.path
import json
from argparse import ArgumentParser, FileType
from uuid import uuid4
from time import sleep
import logging

from configparser import ConfigParser
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer, JSONDeserializer


# Parse the command line.
parser = ArgumentParser()

parser.add_argument('-l', '--log', help="Logging", type=str, required= False, default="../log/test_producer.log")
parser.add_argument('-c', '--config_file', type=FileType('r'), required= False, default="./kafka_config_local.ini")
parser.add_argument('-kk', '--kafka_key', help="Kafka Key", type=str, required= False, default="test")
parser.add_argument('-id', '--input_dir', help="Generated json file directory", required= False, type=str, default="../sample/")
parser.add_argument('-f', '--file', help="Name of json file to be written into topic", required= False, type=list, default=['input_s2s_tunnel_down'])

args = parser.parse_args()


class kafka_producer:
    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Serializer schema registry
    schema_config = dict(config_parser['schema'])
    schema_registry_info = {'url': schema_config["schema.registry.url"],'basic.auth.user.info':schema_config["basic.auth.user.info"]}
    schema_registry_client = SchemaRegistryClient(schema_registry_info)


    string_serializer = StringSerializer('utf_8')

    input_directory = args.input_dir
    input_files     = args.file
    key             = args.kafka_key
    log_file        = args.log

    string_serializer = StringSerializer('utf_8')

    # Kafka instance
    producer = Producer(config) # Create producer

    # Write into log
    logging.basicConfig(filename=log_file, encoding='utf-8', level=logging.DEBUG)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(self, err, msg):
        if err:
            logging.error('ERROR: Message failed delivery: {}'.format(err))
        else:
            #logging.debug('User record {} successfully produced to {} [{}] at offset {}'.format(
              #                  msg.key(), msg.topic(), msg.partition(), msg.offset()))
            logging.debug("Produced event to topic {topic}:  key = {key:12}, value = {value:12}".format(topic=msg.topic(), key=msg.key().decode('utf-8'),  value=msg.value().decode('utf-8')))


    def produce(self):
        # Check if target file and field file exists
        for file in self.input_files:
            if(not os.path.isfile(f'{self.input_directory}{file}.json')):
                logging.error(f"Sample json file does not exist: {self.input_directory}{file}.json")
                exit(1)

            if(not os.path.isfile(f'{self.input_directory}{file}.json')):
                logging.error(f"Sample json file does not exist: {self.input_directory}{file}_field.json")
                exit(1)


        # Read json target file
        try:
            for target in self.input_files:
                file = open(f'{self.input_directory}{target}.json')
                data = json.load(file)
                file.close
                for values in data.values():
                    for record in values:
                        topic = record.get("topic")
                        value = record.get("value")
                        str_value = json.dumps(value)
                        serialized_key  = self.string_serializer(self.key)
                        serialized_value = self.string_serializer(str_value)
                        self.producer.produce(topic, key = serialized_key, value = serialized_value, callback = self.delivery_callback)
                        sleep(1)
                
                # Block until the messages are sent.
                self.producer.poll(10000)
                self.producer.flush()


        except Exception as err:    
            logging.error(f'Something is wrong during opening JSON file: {err}')


kafka_producer = kafka_producer()
kafka_producer.produce()