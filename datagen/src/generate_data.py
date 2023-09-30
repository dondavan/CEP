import json
from faker import Faker
from faker.providers import DynamicProvider
import os.path
import argparse

# CLi tool get argument
parser = argparse.ArgumentParser(description='Mimic data generator')
parser.add_argument('-td', '--target_dir', help="Specify target file directory", required= False, type=str, default="../data/target/")
parser.add_argument('-od', '--output_dir', help="Specify output directory", required= False, type=str, default="../data/output/")
parser.add_argument('-a', '--amount', help="How much data will be generated", required= False, type=int, default=1)
parser.add_argument('-f', '--file', help="Target file name", required= False, type=list, default=['nqa_raw','TCI','Zabbix_events'])

argument = parser.parse_args()

target_files = argument.file
amount = argument.amount
target_directory = argument.target_dir
output_directory = argument.output_dir

# Map Json field value to faker provider name
JSON_FAKER_NAME = {'RAND_TIME_MILLI':'unix_time', 'RAND_TIME_ISO8601':'iso8601'}


# Check if target file and field file exists
for file in target_files:
    if(not os.path.isfile(f'{target_directory}{file}.json')):
        print(f"Mimic target json file does not exist: {target_directory}{file}.json")
        exit(1)

    if(not os.path.isfile(f'{target_directory}{file}.json')):
        print(f"Filed value json file does not exist: {target_directory}{file}_field.json\n * File name needs to be {file}_field.json")
        exit(1)


# Read json target file
targets_data = []
fields_data  = []
try:
    for target in target_files:
        file = open(f'{target_directory}{target}.json')
        data = json.load(file)
        targets_data.append(data)
        file.close
    for target in target_files:
        file = open(f'{target_directory}{target}_field.json')
        data = json.load(file)
        fields_data.append(data)
        file.close
except Exception as err:    
    print(f'Something is wrong during opening JSON file: {err}')


# Add related field into faker provider.
# If field sepecified in ABC_field.json file, related value will be generated.
# Otherwise, value remain the same as from ABC.json file.
fake = Faker('en_US')
faker_field  = []   #list of field will be generate by faker
for field in fields_data:
    for item in field:
        # To differentiate whether if sepecified field value is provided or will be generate by faker
        # Field either be a list or reserved Key word
        if type(field[item])!=list :
            faker_field.append(item)
            # TBA : check if provided RAND value is correct
        else :
            new_provider = DynamicProvider(
                provider_name=item,
                elements=field[item],
            )
            fake.add_provider(new_provider)


# TBA: The logic to generate data can be changed -> generate data at once, and be selected into JSON file when outputting 
# Iterate all json target file to genreate data
for i in range(len(targets_data)):
    output_list = []
    for count in range(amount):
        new_dict = dict()
        for item_key in targets_data[i]:
            if(item_key in fields_data[i]):
                # This will be generate by faker
                if(item_key in faker_field):
                    # Get provider name in faker
                    faker_function_name = JSON_FAKER_NAME[fields_data[i][item_key]]
                    func = getattr(fake,faker_function_name) 
                    value = func()
                    new_dict[item_key] = value
                # This will be generate through provided value
                else:
                    func = getattr(fake,item_key)
                    value = func()
                    new_dict[item_key] = value
            else:
                # This fields value stay original
                new_dict[item_key] = targets_data[i][item_key]
        output_list.append(new_dict)

    output_dict = {f'{target_files[i]}_generated':output_list}        
    # write to output file
    try:
        output_file = open(f'{output_directory}{target_files[i]}.json', 'w')
        json.dump(output_dict, output_file, ensure_ascii=False)
        output_file.close()
    except Exception as err:    
        print(f'Something is wrong when writting output JSON file: {err}')
            
