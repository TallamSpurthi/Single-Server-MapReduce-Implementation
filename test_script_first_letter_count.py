import master_helper
import os, random, shutil, json
from master import Master as ms
#from pyspark import SparkContext

def main():
    print("STARTING THE TEST FOR THE FIRST LETTER COUNT PROGRAM")
    proj_dir, input_dir, interm_dir, out_dir, worker_num, bool_fault, fault_type = master_helper.loadConfig()
    partition_locations = master_helper.splitInput(input_dir, worker_num, input_file='HOLMES.txt')

    # Delete any previous intermediate, output directories
    shutil.rmtree(interm_dir, ignore_errors = True)
    shutil.rmtree(out_dir, ignore_errors = True)
    if not os.path.isdir(interm_dir):
        print("Intermediate directory has been deleted.")
    else:
        print("Intermediate directory is present. This is wrong!!!")

    if not os.path.isdir(out_dir):
        print("Output directory has been deleted.")
    else:
        print("Output directory is present. This is wrong!!!")

    #set faulty_mapper to -1 at the beginning. If we simulate the fault scenario and faulty_type is mapper, then it's value will change.
    faulty_worker = -1
    #if the fault case is being simulated, randomly pick a worker which will be faulty.
    if bool_fault == "True":
        #generate a random number to choose the faulty worker.
        #this process will wait indefinitely and won't complete.
        faulty_worker = random.randint(0,worker_num - 1)
    master_word_lenght = ms(interm_dir, out_dir, worker_number=worker_num, faulty_worker=faulty_worker, fault_type=fault_type)
    master_word_lenght.setProgram(partition_locations, program_type=3)
    master_word_lenght.run()

    #combined_output will hold all the output files produced by reducers
    combined_output = None
    print("Combining output files...")
    #combine output files
    #iterate over files in the output directory
    count_file = 0
    for filename in os.listdir(out_dir):
        output_file_path = os.path.join(out_dir, filename)
        #check if it is really a file
        if os.path.isfile(output_file_path):
            #read the output file
            output_file = open(output_file_path, "r")
            #add the output file to the array that holds all the output files
            if count_file == 0:
                combined_output = json.load(output_file)
            else:
                combined_output = {**combined_output, **json.load(output_file)}
            output_file.close()
            count_file += 1

    print("Output file are combined.")
    combined_output_file = open(out_dir + 'Combined_Results.txt', "w+")
    sorted_output_tuples = sorted(combined_output.items(), key=lambda item: item[1],reverse=True)
    sorted_output_dict = {k: v for k, v in sorted_output_tuples}
    json.dump(sorted_output_dict, combined_output_file, indent=4)
    output_file.close()
    print("MAP REDUCE PROGRAM Completed. Output files can be seen in the output directory.")
    print("Starting SPARK...")
    '''
    sc = SparkContext("local","Spark Program")
    input_text_path = "".join((input_dir, 'Spark_Input.txt'))
    output_spark_dir = proj_dir + "Spark_Output/"
    shutil.rmtree(output_spark_dir, ignore_errors = True)
    #check if the directories are deleted or not.
    if not os.path.isdir(output_spark_dir):
    	print("Spark output directory has been deleted.")
    else:
    	print("Spark output directory is present. This is wrong!!!")
    #read input
    text = sc.textFile(input_text_path)
    #split the text
    processed_text = text.flatMap(lambda line: line.split(" "))
    #map the processed text to intermediate data
    intermediate_data = processed_text.map(lambda word:(word[0], 1))
    #process the intermediate data and reduce
    word_first_letter_freq = intermediate_data.reduceByKey(lambda x,z:x+z)
    #switch positions of key and value and sort the words in decreasing order by frequency
    word_first_letter_freq_sorted = word_first_letter_freq.map(lambda word:(word[1], word[0])).sortByKey(False)
    #Now we need to switch positions of key and value one more time to have the following format: word, frequency
    word_first_letter_freq_sorted = word_first_letter_freq_sorted.map(lambda word:(word[1], word[0]))
    # create the output
    word_first_letter_freq_sorted.saveAsTextFile(output_spark_dir)
    sc.stop()
    print("SPARK completed and output generated under Spark_Output directory.")
    print("Comparing SPARK and MAP REDUCE outputs...")
    spark_output_file = open("".join((output_spark_dir, 'part-00000')), 'r') #encoding="utf-8")
    #read the spark output and remove blank lines
    spark_output_data = spark_output_file.read().strip()
    spark_output_file.close()
    spark_output_tokens = spark_output_data.split("\n")
    unwanted_chars = ''''''()' '''
    '''#count the number of keys that have the same count in both Spark and Map Reduce outputs.
    count_matched = 0
    #count the number of keys that do not have the same count in both Spark and Map Reduce outputs.
    count_unmatched = 0
    for token in spark_output_tokens:
        for char in unwanted_chars:
            token=token.replace(char,'')
        #if this key exists in the map reduce output
        key = token.split(',')[0]
        #if the key is 0, it means an empty line skip to next iteration.
        if key == '0':
            continue
        value = int(token.split(',')[1])
        if key in sorted_output_dict and value == sorted_output_dict[key]:
            count_matched += 1
        #key does not exist in the map reduce output
        else:
            count_unmatched += 1
    print("NUMBER OF MATCHED KEYS: " + str(count_matched))
    print("NUMBER OF UNMATCHED KEYS: " + str(count_unmatched))
    matched_percentage = count_matched / (count_matched + count_unmatched) * 100
    print("PERCENTAGE OF MATCHED KEYS: " + str(matched_percentage) + "%")
    print("TEST FIRST LETTER COUNT PROGRAM SUCCESFULLY COMPLETED!!!")
    '''

if __name__ == '__main__':
  main()
