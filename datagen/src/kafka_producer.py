import os.path
import json
from argparse import ArgumentParser, FileType
from uuid import uuid4

from configparser import ConfigParser
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer



schema_str = """{
  "$id": "http://example.com/myURI.schema.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "additionalProperties": false,
  "description": "Sample schema to help you get started.",
  "properties": {
    "myField1": {
      "description": "The integer type is used for integral numbers.",
      "type": "integer"
    },
    "myField2": {
      "description": "The number type is used for any numeric type, either integers or floating point numbers.",
      "type": "number"
    },
    "myField3": {
      "description": "The string type is used for strings of text.",
      "type": "string"
    }
  },
  "title": "SampleRecord",
  "type": "object"
}"""

# Parse the command line.
parser = ArgumentParser()
parser.add_argument('-c', '--config_file', type=FileType('r'), required= False, default="./kafka_config.ini")
parser.add_argument('-id', '--input_dir', help="Generated json file directory", required= False, type=str, default="../data/output/")
parser.add_argument('-f', '--file', help="Name of json file to be written into topic", required= False, type=list, default=['nqa-raw','TCI','Zabbix-events'])

argument = parser.parse_args()

# Parse the configuration.
# See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
config_parser = ConfigParser()
config_parser.read_file(argument.config_file)
config = dict(config_parser['default'])

# Serializer schema registry
schema_config = dict(config_parser['schema'])
schema_registry_info = {'url': schema_config["schema.registry.url"],'basic.auth.user.info':schema_config["basic.auth.user.info"]}
schema_registry_client = SchemaRegistryClient(schema_registry_info)

string_serializer = StringSerializer('utf_8')
json_serializer = JSONSerializer(schema_str,schema_registry_client)

input_directory = argument.input_dir
input_files     = argument.file


# Kafka instance
producer = Producer(config) # Create producer
topic = "event_test"        # Topic to be written into

# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).
def delivery_callback(err, msg):
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


# Check if target file and field file exists
for file in input_files:
    if(not os.path.isfile(f'{input_directory}{file}.json')):
        print(f"Mimic target json file does not exist: {input_directory}{file}.json")
        exit(1)

    if(not os.path.isfile(f'{input_directory}{file}.json')):
        print(f"Filed value json file does not exist: {input_directory}{file}_field.json\n * File name needs to be {file}_field.json")
        exit(1)


# Read json target file
json_file = []
try:
    for target in input_files:
        file = open(f'{input_directory}{target}.json')
        data = json.load(file)
        json_file.append(data)
        file.close
except Exception as err:    
    print(f'Something is wrong during opening JSON file: {err}')


# Producer produce event
# Value string is encoded in utf-8
for file in json_file:
    for value in file.values():
        serialized_ = json.dumps(value)
        producer.produce(topic, key = string_serializer(str(uuid4())), value = json_serializer(value, SerializationContext(topic, MessageField.VALUE)), callback = delivery_callback)

# Block until the messages are sent.
producer.poll(10000)
producer.flush()