import json
from configparser import ConfigParser

'''
splitInput : Function to partition the input file
1. Read & Preprocess the input file to get clean data
2. Write the cleaned data to disk so Spark can use it directly : {'Spark_Input.txt'}
3. Equally split the data into { N = #mappers } partitions
4. Write N partitioned files to the input directory
'''
def splitInput(input_dir, worker_num=2, input_file='HOLMES.txt'):
    input_path = "".join((input_dir, input_file))
    input_file = open(input_path, 'r')
    data = input_file.read().strip()
    input_file.close()
    data = clean_data(data)
    clean_data_file = open(input_dir + 'Spark_Input.txt', "w+")
    json.dump(data, clean_data_file)
    clean_data_file.close()
    tokens = data.split(" ")
    size = int(len(tokens)/worker_num)+1
    partition_locations = []
    for idx in range(worker_num):
      input_split = " ".join(tokens[idx*size : (idx+1)*size])
      partition_loc = input_dir + 'input_partition' + str(idx) + '.txt'
      partition_locations.append(partition_loc)
      with open(partition_loc, 'w') as f:
        json.dump(input_split, f)
    return partition_locations


# loadConfig() : loads settings from user specified config file
def loadConfig(config_path='./config.ini'):
    config = ConfigParser()
    config.read(config_path)
    conf = config['DEFAULT']
    project_dir = conf['ProjectDir']
    input_dir = "".join((conf['ProjectDir'], conf['InputDir']))
    interm_dir = "".join((conf['ProjectDir'], conf['IntermediateDir']))
    out_dir = "".join((conf['ProjectDir'], conf['OutputDir']))
    worker_num = int(conf['WorkerNum'])
    bool_fault = conf['fault']
    fault_type = conf['faultType']
    return project_dir, input_dir, interm_dir, out_dir, worker_num, bool_fault, fault_type


'''
clean_data() : 
1. cleans the input data to remove any extra spaces, 
2. correct punctuations, 
3. replace characters encoded in different settings
4. remove special characters
'''
def clean_data(data):
    data = data.lower()
    punc = '''!()' '-[]{};:'"\,<>./''?@#$%^&*_~"'''
    for char in punc:
        data=data.replace(char,' ')
    data=data.replace('\n',' ')
    data = data.replace(b'\xe2\x80\x94'.decode('utf-8'), " ")
    numbers = '''0123456789'''
    for char in numbers:
        data=data.replace(char,'')
    data = data.replace('"', '')
    data = " ".join(data.split())
    data = data.strip()
    return data