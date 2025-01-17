# An Engine for Efficient Data Stream Summarization using Kafka and Kafka Streams Microservices
# Table of Contents
1. [Abstract](#abstract)
2. [Configuration SaaMS before execution](#configuration-saams-before-execution)
3. [List of Available Synopsis](#list-of-available-synopsis)
4. [User Interface Application](#user-interface-application)
    - [Data Message format](#data-message-format)
    - [Request message for adding a new Synopsis](#request-message-for-adding-a-new-synopsis)
    - [Request message for query a Synopsis](#request-message-for-query-a-synopsis)
    - [Output Message](#output-message)
5. [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [How Execute SaaMS](#how-execute-saams)
        - [Step 1: Git Clone](#step-1-git-clone)
        - [Step 2: Build the JAR](#step-2-build-the-jar)
        - [Step 3: Run the SaaMS JAR](#step-3-run-the-saams-jar)
        - [Step 4: Run the User interface JAR](#step-4-run-the-user-interface-jar)
6. [Annex1: How to use transform data script](#annex-how-to-use-transform-data-script)
7. [Contact](#contact)
# SaaMS : Synopses as a Microservice
## Abstract
In this work, we introduce a novel stream summary maintenance paradigm in the form of distributed microservices, namely Synopses as a MicroService, and we implement this paradigm on top of Apache Kafka and Kafka Streams Microservices. SaaMS is designed for real-time stream summarization and analysis over rapid data streams. SaaMS also contains a built-in library with Synopses that is used for producing stream summaries but remains extensible and customizable to new Synopses techniques. In that, (a) it contributes an innovative architecture to gain scalability dynamically based on the necessary computation requirements, (b) maintains a large volume of Synopses, concurrently with high throughput and fault-tolerance, (c) provides an extensible Synopsis library for real-time analysis (d) experimental evaluation provided using real financial data. SaaMS manages large-scale stream processing and analysis because it enables (i) horizontal scalability, i.e., taking advantage of complicated mechanisms that Kafka has for distributing the workload, achieving maximum throughput and minimum latency (ii) vertical scalability, i.e., the ability to scale the computation with the number of processed streams (iii) federated scalability, i.e., data can be processed across multiple distributed environments even in case they are geographically dispersed.
# Configuration SaaMS before execution:
For the application to function properly, the following parameters must be set in the config.properties file. 

1. Following is an example of the config.properties file as must implement in case of running the application in a Windows environment.
    ```properties
    # Determine the Kafka Broker
    BOOTSTRAP_SERVERS = localhost:9092,localhost:9093,localhost:9094
    
    # Determine the path for saving synopsis instance files
    SAVE_FILE_PATH_PREFIX = C:\\dataset\\StoreSynopses\\stored_
    
    # Determine the path for saving Kafka Streams files
    KAFKA_STREAM_DIR = C:\\dataset\\tmp\\kafka-streams\\
    
    # Determine the Zookeeper server
    ZOOKEEPER_BOOTSTRAP_SERVERS = localhost:2181
    
    # Determine the number of parallel threads in Router Microservice
    PARALLEL_DEGREE = 8
    
    # Determine the replication factor of Kafka topics
    REPLICATION_FACTOR = 3
    
    # Determine the time (sec) for batching messages of data to add this in synopsis
    Batching_Time = 5
    # Determine the REQUEST_PATH and DATA_PATH
    # which is responsible for producing the request and data messages to Request and Data Kafka topics in SaaMS
    REQUEST_TOPIC_PATH = C:\\Request_small.json
    DATA_TOPIC_PATH = C:\\dataset\\ProduceDataToDataTopic
    ```
2. Following is an example of the config.properties file as must implement in case of running the application in a Linux environment.
    ```properties
    # Determine the Kafka Broker
    BOOTSTRAP_SERVERS = localhost:9092,localhost:9093,localhost:9094
    
    # Determine the path for saving synopsis instance files
    SAVE_FILE_PATH_PREFIX = /home/user1/dataset/StoreSynopses/stored_
    
    # Determine the path for saving Kafka Streams files
    KAFKA_STREAM_DIR = /home/user1/dataset/tmp/kafka-streams/
    
    # Determine the Zookeeper server
    ZOOKEEPER_BOOTSTRAP_SERVERS = localhost:2181
    
    # Determine the number of parallel threads in Router Microservice
    PARALLEL_DEGREE = 8
    
    # Determine the replication factor of Kafka topics
    REPLICATION_FACTOR = 3
    
    # Determine the time (sec) for batching messages of data to add this in synopsis
    Batching_Time = 5
    
    # Determine the REQUEST_PATH and DATA_PATH
    # which is responsible for producing the request and data messages to Request and Data Kafka topics in SaaMS
    REQUEST_TOPIC_PATH = /home/user1/Request_small.json
    DATA_TOPIC_PATH = /home/user1/dataset/ProduceDataToDataTopic
    ```
3. In case of running the application in a cluster environment, the config.properties file must be set in all the nodes of the cluster.

4. In case of config.properties file is not found, the application will use default values as describe in  config.properties.

# List of Available Synopsis
| Synopsis ID | Synopsis                | Estimate                         | Mostly Used       | Parameters                                                    |
|-------------|-------------------------|----------------------------------|-------------------|---------------------------------------------------------------|
| 1           | CountMin                | Count                            | Frequent Itemsets | epsilon, confidence, seed                                     |
| 2           | HyperLogLog             | Cardinality                      | Cardinality       | rsd (relative standard deviation)                             |
| 3           | BloomFilter             | Member of a Set                  | Membership        | numberOfElements, maxFalsePositive                            |
| 4           | DFT                     | Fourier Coefficients             | Spectral Analysis | intervalSec, basicWindowSize, slidingWindowSize, coefficients |
| 5           | LossyCounting           | Count, FrequentItems             | Frequent Itemsets | epsilon (the maximum error)                                   |
| 6           | StickySampling          | Count, FrequentItems             | Frequent Itemsets | support, epsilon, probabilityOfFailure                        |
| 7           | AMS                     | L2 norm, Count                   | Frequent Itemsets | depth, buckets                                                |
| 8           | GKQuantiles             | Quantile                         | Quantiles         | epsilon (the maximum error)                                   |
| 9           | LSH                     | Binary Representation of a Set   | Correlation       | slidingwindow(W), compresion(D), workersnum(B)                |
| 10          | WindowSketch Quantiles  | Quantile                         | Quantiles         | epsilon (the maximum error), windowSize                       |


# User Interface Application
In SaaMS, interaction is carried out through a user interface application. The user interface application is a Java application can produce messages in Kafka topics. There are two main types of messages can handle: **Data Messages** and **Request Messages**.
## Data Message format
```json
{
    "streamID":"EURTRY",
    "objectID":"EURTRY_0",
    "dataSetKey":"Forex",
    "date":"01/02/2019",
    "time":"00:00:01",
    "price":6.0654,
    "volume":1   
}
```
## Data Message Structure Explanation:

- **streamID (String):** In a financial example, it represents the stock name.
- **objectID (String):** A unique identifier for each `streamID`.
- **dataSetKey (String):** In a financial example, it represents the Stock Market name.
- **date & time (String):** The timestamp at which the `streamID` was captured.
- **price (Double):** The current price at the specified date and time.
- **volume (Integer):** The quantity of current trades for this `streamID` at the specified date and time.


## Request message for adding a new Synopsis
```json
{
    "streamID": "EURTRY", 
    "synopsisID": 1,
    "requestID": 1,
    "dataSetKey": "Forex",
    "param": ["CountMin", "price", "NotQueryable", 0.001, 0.99, 12345],
    "noOfP": 5,
    "uid": 1001
}
```

## Request message for query a Synopsis
```json
{
    "streamID" : "EURTRY",
    "synopsisID" : 1,
    "requestID" : 2,
    "dataSetKey" : "Forex",
    "param" : [ 6.05736, "price", "Queryable", "Continues", 0.001, 0.99, 12345 ],
    "noOfP" : 5,
    "uid" : 1002
}  
```
## Request Message Structure Explanation:

- **streamID (String):** Symbolizes the item name that needs to build up a Synopsis. If the `streamID` is empty the synopsis built or queried for the whole streamIDs in the `dataSetKey`.
- **synopsisID (Integer):** Defines the synopsis type based on the previous Table.
- **requestID (Integer):** Unique identifier to distinguish each request.
- **dataSetKey (String):** Symbolizes the source where we get the streamID.
- **param (Object[]):** A table that contains different parameters necessary to build up or query a Synopsis. The structure depends on the context:

    - **If it is not a Query:**
        - Example: `["CountMin", "price", "NotQueryable", 0.001, 0.99, 12345]`
        - Breakdown:
            1. **Synopsis Type (String):** Specifies the type of Synopsis algorithm (e.g., CountMin).
            2. **Field Name (String):** Represents the field used to build the Synopsis (e.g., "price", "volume").
            3. **Request Status (String):** Specifies the type of Request (e.g., "NotQueryable").
            4. **Synopsis Parameter (Variable Type):** Each synopsis has different parameters determined by the previous Table. For example, CountMin has epsilon, confidence, and seed.

    - **If it is a Query:**
        - Example: `[6.05736, "price", "Queryable", "Continues", 0.001, 0.99, 12345]`
        - Breakdown:
            1. **Synopsis Type (String):** Specifies the value to do estimation (e.g., 6.05736).
            2. **Field Name (String):** Represents the field used to build the Synopsis (e.g., "price", "volume").
            3. **Request Status (String):** Specifies the type of Request (e.g., "Queryable").
            4. **Query Status (String):** Specifies the type of Query Request (e.g., "Continues" or "Ad-hoc").
            5. **Synopsis Parameter (Variable Type):** Each synopsis has different parameters determined by the previous Table. For example, CountMin has epsilon, confidence, and seed.

- **noOfP (Integer):** Defines the parallelization level of Synopsis Topics and Synopsis Microservice.
- **uid (Integer):** [Optional - not used in the provided example.]

An example of how to use for each type of Synopsis the query and not query Request Message exist in the `RequestExamples` file.
## Load Request Message from a file
The functionality of loading a saved Synopsis from disc, in practice, can be implemented using the following request:
```json
{
  "param" : [ "LOAD_REQUEST", "PathToLoadSynopsis\\stored_CountMin.ser" ]
}
```
The `LOAD_REQUEST` is a keyword that indicates that the request is for loading a Synopsis from disc. The `PathToLoadSynopsis` is the path where the serialized file of the Synopsis is stored.
## Output Message
The result of this estimation is written on an output Kafka topic with the name `OutputTopicSynopsis_(the synopsisid)`.
The representation of the messages that the output topic can contain is presented below:
```console
For Stock EURTRY and Dataset Forex
Estimate Count in the price field of value: 6.05959
Count Min Result is: 37
```

# Getting Started
## Prerequisites:
* Java 19
* Maven 3.6.3
* Kafka & Kafka Streams 3.3.1
* Zookeeper 3.9.0

The project was developed and tested on the following versions, but it should works on other versions as well.

Before proceeding to the execution, ensure that the computer has run a Kafka and a Zookeeper Server.

## How Execute SaaMS:
### Step 1: Git Clone
```
git clone https://github.com/geok1999/Synopses-as-a-MicroService-SaaMS.git
```
### Step 2: Build the JAR
```
mvn clean package
```
### Step 3: Run the SaaMS JAR
```
java -DconfigFilePath=C:\dataset\Configuration\config.properties -jar target/SaaMS_APP-1.0-SNAPSHOT-jar-with-dependencies.jar
```
### Step 4: Run the User interface JAR
```
java -DconfigFilePath=C:\dataset\Configuration\config.properties -jar target/Producing-TO-REQUEST-TOPIC-jar-with-dependencies.jar
```
```
java -DconfigFilePath=C:\dataset\Configuration\config.properties -jar target/Producing-TO-DATA-TOPIC-jar-with-dependencies.jar
```
The `-DconfigFilePath=C:\dataset\Configuration\config.properties` is optional and the file path `C:\dataset\Configuration\config.properties` used as example. If it is not set, the application will use the default values to configure the App.

# Annex 1: How to use transform data script
The `transform_data.jar` is a Java application that can be used to transform a txt file to a JSON file. The JSON file can be used to produce data messages to the Data Kafka topic in SaaMS.
## How to use the transform data script
### Step 1: Must have a txt file with the following file name format:
```
\<Market Type\>·\<Unique Identifier\>·\<Suffix\>
```
This is necessary to extract:
- **streamID**: Extracted from the second component (the unique identifier).
- **dataSetKey**: Extracted from the first component (the market type).
### Step 2: Each line of the txt file must have the following format:
```
\<Date\>,\<Time\>,\<Price\>,\<Volume\>
```
### Step 3: Run the transform data script and provide the path of stock market where the txt file is located
```
java -DconfigFilePath="C:\\custom\\dataset\\path\\" -jar target/Transform-TXT-Data-To-Json.jar 
```
# Annex 2: How to establish a Kafka Cluster and a Zookeeper Server which is necessary for execute SaaMS
In this project provides two methods to establish a Kafka Cluster and a Zookeeper Server:
1. Method 1: Using a Script
   1. Download and install the Kafka and Zookeeper from the official website 
   2. In project Script folder, there are two scripts:
      - `RunAllScripts.cmd` for Windows
      - `RunAllScripts.sh` for Linux
   3. Before execute the scripts check if the file paths that you install kafka are the same in the scripts.
   4. Execute the script.
2. Method 2: Using Docker
   1. In the project, there is a docker-compose file that can be used to establish a Kafka Cluster and a Zookeeper Server.
   2. Run the following command:
      ``` shell
      docker-compose up
      ```
   3. To stop the Kafka Cluster and Zookeeper Server, run the following command:
      ``` shell
      docker-compose down
      ```
In both methods, the Kafka Cluster and Zookeeper Server will be established in the following ports:
- Kafka Broker: 9092, 9093, 9094
- Zookeeper: 2181

# Contact
If you have any questions, please contact us at:
* gkalfakis@tuc.gr
* ngiatrakos@tuc.gr


