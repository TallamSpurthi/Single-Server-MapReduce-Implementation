import json
import os
import time
import hashlib

"""
#test case: first letter count
#description: find the number of first letter of each word in a given text file
"""

def map(partition, R, intermediateDir, q, fault=False):
    """
        outputs a list of key-value pairs: key - first letter of the word; value - 1 if this first letter appears once
        expected outputs: [(firstletter1, 1), (firstletter2, 1), ... (firstlettern, 1)]
        
    """
    # each mapper is assigned one partition of the input file
    
    pid = os.getpid()
    words = json.load(open(partition)).split(" ")
    occurences = {}
    for word in words:
        #each word is given a unique idx across processes
        if word != "" and word[0] in "abcdefghijklmnopqrstuvwxyz": 
            hashed_string = hashlib.sha256(word[0].encode())
            val = int.from_bytes(hashed_string.digest(), 'big')
            idx = val % R
        #append (k,v) pairs to the dic
            if idx not in occurences:
                occurences[idx] = []
            occurences[idx].append((word[0], 1))#in this case, we use the first letter of each word as the key
    #if it is faulty process, then sleep for a very long time to simulate a behaviour similar to not responding indefinitely.
    if fault == True:
        time.sleep(1000)
        
    for idx in occurences:
        # create the intermediate file and partition it into R pieces based on each word's index
        # each reducer will only work on one piece of data from this intremediate file
        # this is to make sure that each key will only be assgined to one reducer
        filename = intermediateDir + "Mapper_" + str(pid) + '_intermediate_file_'+str(idx)+'.txt'
        os.makedirs(intermediateDir, exist_ok=True)
        fp = open(filename, "w")
        json.dump(occurences[idx], fp)
        fp.close()
        q.put([pid, filename])

def reduce(files, outputDir,fault=False):
    """
        Outputs a single value together with the provided key:
            key - the word length; value - n, the number of words of this word length in a given text file
        expected outputs: [(wordlength1, m_1), (wordlength2, m_2), ... (wordlengthn,m_n)]
    """
    pid = os.getpid()
    output = {}
    #if it is faulty process, then sleep for a very long time to simulate a behaviour similar to not responding indefinitely.
    if fault == True:
        time.sleep(1000)
    for filename in files:
        fp = open(filename, "r")
        words = json.load(fp)
        fp.close()
        #tally the number of first letter of each word length and return this dic as the final output
        for letter,count in words:
            if letter not in output:
                output[letter] = 0
            output[letter] += 1
    os.makedirs(outputDir, exist_ok=True)
    output_file = open(outputDir + "Reducer_" + str(pid) + '_output.txt', "w+")
    json.dump(output, output_file)
    output_file.close()
