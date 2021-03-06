from multiprocessing import Process, Queue
import udf_wordCount, udf_wordLength, udf_firstLetterCount

class Master():
    def __init__(self, interm_dir, out_dir, worker_number, faulty_worker=-1, fault_type=None):
        self.interm_dir = interm_dir
        self.out_dir = out_dir
        self.no_of_mappers = worker_number
        self.no_of_reducers = worker_number
        self.faulty_worker = faulty_worker
        self.fault_type = fault_type
        print("Master has been initialized.")

    '''
    setProgram() : To set different mapReduce programs
	program_type = 1 : word count
	program_type = 2 : word length
	program_type = 3 : first letter
	'''
    def setProgram(self, partition_locations, program_type=1):
        if program_type == 1:
            self.mapper = udf_wordCount.map
            self.reducer = udf_wordCount.reduce
        elif program_type == 2:
            self.mapper = udf_wordLength.map
            self.reducer = udf_wordLength.reduce
        else:
            self.mapper = udf_firstLetterCount.map
            self.reducer = udf_firstLetterCount.reduce
        self.input_partitions = partition_locations

    def run(self):
        q = Queue()
        mapper_processes = []
        reducer_processes = []
        new_mapper = None
        new_reducer = None
        
        print("Map phase began.")
		#Generate a faulty process, if fault_type is a mapper. Else a normal process.
        for i in range(self.no_of_mappers):
            if self.fault_type != None and self.fault_type == "mapper" and i == self.faulty_worker:
                mapper = Process(target=self.mapper, args=(self.input_partitions[i],self.no_of_reducers,self.interm_dir,q,True,))
            else:
                mapper = Process(target=self.mapper, args=(self.input_partitions[i],self.no_of_reducers,self.interm_dir,q,False,))
            mapper_processes.append(mapper)
            mapper.start()

        '''
		Master polls the processes to check their status
		1. Wait for atmost 10 seconds for a mapper process to terminate, if it takes more than 10 seconds, it means this process could be faulty.
		2. Then, get mapper's exitcode. If exitcode = None, the process is still running, and it will run indefinitely.
		3. Terminate and Launch a new mapper process.
		'''
        count_faulty_mappers = 0
        for mapper_process in mapper_processes:
            mapper_process.join(timeout=10)
            if mapper_process.exitcode == None:
                count_faulty_mappers += 1
                mapper_process.terminate()
                print("A faulty Mapper didn't join. I am starting a new mapper process.")
                faulty_mapper_index = mapper_processes.index(mapper_process)
                new_mapper = Process(target=self.mapper, args=(self.input_partitions[faulty_mapper_index],self.no_of_reducers,self.interm_dir,q,False))
                new_mapper.start()

        if count_faulty_mappers > 1:
            print("CAUTION!! More than 1 faulty mapper. This shouldn't happen!!")

        if new_mapper != None:
            new_mapper.join(timeout=10)
            if new_mapper.exitcode == 0:
                print("There was a faulty mapper but all mappers joined and Map Phase completed")
            else:
                print("New mapper also failed to join. This shouldn't happen!!")
        else:
            print("There wasn't any faulty mapper. All mappers joined and Map Phase completed.")

        # To assign each reducer with one piece of data from the interemediate files generated by each mapper
        no_of_files = self.no_of_mappers * self.no_of_reducers
        self.lists = {}
        while(no_of_files):
            pid, filename = q.get()
            idx = int(filename.strip(".txt").split("file_")[1])
            if idx not in self.lists:
                self.lists[idx] = []
            self.lists[idx].append(filename)
            no_of_files -= 1

        print("Reduce phase began.")
		#Generate a faulty process, if fault_type is a reducer. Else a normal process.
        for i in range(self.no_of_reducers):
            if self.fault_type != None and self.fault_type == "reducer" and i == self.faulty_worker:
                reducer = Process(target=self.reducer, args=(self.lists[i],self.out_dir,True,))
            else:
                reducer = Process(target=self.reducer, args=(self.lists[i],self.out_dir,False,))
            reducer_processes.append(reducer)
            reducer.start()
			
        '''
		Master polls the processes to check their status
		1. Wait for atmost 10 seconds for a reducer process to terminate, if it takes more than 10 seconds, it means this process could be faulty.
		2. Then, get reducer's exitcode. If exitcode = None, the process is still running, and it will run indefinitely.
		3. Terminate and Launch a new reducer process.
		'''
        count_faulty_reducers = 0
        for reducer_process in reducer_processes:
            reducer_process.join(timeout=10)
            if reducer_process.exitcode == None:
                count_faulty_reducers += 1
                reducer_process.terminate()
                print("A faulty Reducer didn't join. I am starting a new reducer process.")
                faulty_reducer_index = reducer_processes.index(reducer_process)
                new_reducer = reducer = Process(target=self.reducer, args=(self.lists[faulty_reducer_index],self.out_dir,False,))
                new_reducer.start()

        if count_faulty_reducers > 1:
            print("CAUTION!! More than 1 faulty reducer. This shouldn't happen!!")
        if new_reducer != None:
            new_reducer.join(timeout=10)
            if new_reducer.exitcode == 0:
                print("There was a faulty reducer but all reducers joined and Reduce Phase Completed")
            else:
                print("New reducer also failed to join. This shouldn't happen!!")
        else:
            print("There wasn't any faulty reducer. All reducers joined and Reduce Phase Completed.")
