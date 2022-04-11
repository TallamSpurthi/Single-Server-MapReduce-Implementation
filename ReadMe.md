# Project 1: Final Submission

Single Server MapReduce: Multiple processes with single fault tolerance

## Description

This contains 3 UDFs & test scripts to run them + 1 configuration file: <br>
1. master.py: a Master class that will create workers (mappers, reducers) and execute MapReduce with 1 fault tolerance.<br>
2. udf_wordCount.py: UDF1 which will count the freq of each word in a given text file.<br>
3. udf_wordLength.py: UDF2 which will find the number of words of each word length in a given text file.<br>
4. udf_firstLetterCount.py: UDF3 which will find the number of first letter of each word in a given text file.<br>
5. test_script(s).py: read the input and project directory from the configuration file and call Master to run a UDF. There are 3 separate files for each of these usecases<br>
6. "config.ini": contains all the information about input directory path, the project directory path, the # of workers which can be set by the user etc...<br>
7. master_helper.py: helper functions to partition the input data, and load config

## Executing program
1. There are three test scripts. One for each UDF. Choose any one test_script.py file to run.
2. Spark code is included in each test_script.py file - **pyspark installation is required to run the spark code!!**
3. When we run a test script, both Spark and our MapReduce implementation will run the same use case and the outputs are compared automatically. The comparison results are shown as  { #of matched k-v pairs } and  {% of matched k k-v pairs}.
4. In the config file, we can chose to modify few parameters like - no of workers, fault = True/False, fault_type = "mapper/reducer/none"
5. run any test_script_[usecase*].py: It partitions the input, creates a Master program, and invokes mapreduce corresponding to the UDF/usecase.
6. user has to specify which UDF they want to run in a Master class. <br>
   default setting: program_type 1 --> word counting program<br>
                    program_type 2 --> word length program<br>
                    program_type 3 --> first letter counting program)<br>
7. UDFs: Read the input from the input directory, write the results after a Map job to the intermediate directory, and save the final ouput after Reduce is done in the output directory.<br>
8. **IMPORTANT - Input Directory ("Input_Files") with input data (hamlet.txt, holmes.txt) needs to be created if not present in the project directory's root folder in the first place**


## Authors
Oguz Celik <br>
Wanyun Huang <br>
Spurthi Tallam <br>
