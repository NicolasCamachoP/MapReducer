# MapReducer 
Three MapReduce style algorithms for querying statistics related to ingested logs. The aim was to implement various alternatives and comparing threads and processes, considering various types of communications such as pipes and files.

## Usage

* Clone the repository `git clone https://github.com/NicolasCamachoP/MapReducerThreads.git`
* Move to the source files directory `cd /MapReducerThreads/MapReducer`
* Compile using the makefile included with `make`

Run it with the following arguments:

1. Name of the file for data ingest.
2. Number of logs that the file contains.
3. Number desired of Mappers.
4. Number desired of reducers.
5. Number of intermediaries. (Just for analogp)

The file provided must be in the same structure as the log file located in the same folder.

* **analogh** Provides a MapReduce implementation based on the use of the fork() method to create processes that can be mappers, intermediaries and reducers. All communication between processes is supported by the use of buffer files, and each step of the query processing is done sequentially.
* **analogh** Provides a MapReduce implementation based on the use of the pthread library to create threads that can be mappers and reducers. All intra-thread communication is supported by the use of shared memory buffers, and each step of the query processing is done sequentially.
* **analogh2** Provides a MapReduce implementation based on the use of the pthread library to create threads that can be mappers and reducers. All intra-thread communication is supported by the use of shared memory buffers and each step of the query processing is managed with the use of semaphore operations using the semaphore.h library, allowing non-sequential processing.

## Authors 

* [Nicolás Camacho](https://github.com/NicolasCamachoP)
* Jhonnier Coronado
* Brayan González

## License 
[MIT](https://github.com/NicolasCamachoP/MapReducerThreads/blob/master/LICENSE)
