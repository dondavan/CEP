import pandas as pd
import json
from faker import Faker


target_files = ['nqa-raw','TCI','Zabbix-events']

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


# If field sepecified in ABC_field.json file, related value will be generated.
# Otherwise, value remain the same as from ABC.json file.
for target in targets_data:
    for item in target:
        print(target[item])
        
fake = Faker('en_US')
for _ in range(5):
    print(fake.name())