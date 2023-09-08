import pandas as pd
import json
from faker import Faker
from faker.providers import DynamicProvider

# These will be later ported to cli tool
target_files = ['nqa-raw','TCI','Zabbix-events']
amount = 3
# Map Json field value to faker provider name
JSON_FAKER_NAME = {'RAND_TIME_MILLI':'unix_time', 'RAND_TIME_ISO8601':'iso8601'}


# Check if target file and field file exists
#TBA


# Read json target file from '../data/target/'
targets_data = []
fields_data  = []
try:
    for target in target_files:
        file = open(f'../data/target/{target}.json')
        data = json.load(file)
        targets_data.append(data)
        file.close
    for target in target_files:
        file = open(f'../data/target/{target}_field.json')
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
        output_file = open(f'../data/output/{target_files[i]}.json', 'w')
        json.dump(output_dict, output_file, ensure_ascii=False)
        output_file.close()
    except Exception as err:    
        print(f'Something is wrong when writting output JSON file: {err}')
            
