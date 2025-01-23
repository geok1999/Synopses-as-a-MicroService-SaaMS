# An Engine for Efficient Data Stream Summarization using Kafka and Kafka Streams Microservices
# Table of Contents
1. [Abstract](#abstract)
2. [Configuration SaaMS before execution](#configuration-saams-before-execution)
3. [List of Available Synopsis](#list-of-available-synopsis)
4. [User Interface Application](#user-interface-application)
    - [Data Message format](#data-message-format)
    - [Request message for adding a new Synopsis](#request-message-for-adding-a-new-synopsis)
    - [Request message for query a Synopsis](#request-message-for-query-a-synopsis)
    - [Load Request Message from a file](#load-request-message-from-a-file)
    - [Delete Request Message from a file](#delete-request-message-from-a-file)
    - [Output Message](#output-message)
5. [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [How Execute SaaMS](#how-execute-saams)
        - [Step 1: Git Clone](#step-1-git-clone)
        - [Step 2: Build the JAR](#step-2-build-the-jar)
        - [Step 3: Run the SaaMS JAR](#step-3-run-the-saams-jar)
        - [Step 4: Run the User interface JAR](#step-4-run-the-user-interface-jar)
6. [Annex1: How to use transform data script](#annex-how-to-use-transform-data-script)
    - [Step 1: Must have a txt file with the following file name format](#step-1-must-have-a-txt-file-with-the-following-file-name-format)
    - [Step 2: Each line of the txt file must have the following format](#step-2-each-line-of-the-txt-file-must-have-the-following-format)
    - [Step 3: Run the transform data script and provide the path of stock market where the txt file is located](#step-3-run-the-transform-data-script-and-provide-the-path-of-stock-market-where-the-txt-file-is-located)
7. [Annex2: How to establish a Kafka Cluster and a Zookeeper Server which is necessary for execute SaaMS](#annex-2-how-to-establish-a-kafka-cluster-and-a-zookeeper-server-which-is-necessary-for-execute-saams)
    - [Method 1: Using a Script](#method-1-using-a-script)
    - [Method 2: Using Docker](#method-2-using-docker)
8. [Annex3: How to use the Metrics User Interface](#annex-3-how-to-use-the-metrics-user-interface)
    - [How to use the Metrics Interface](#how-to-use-the-metrics-interface)
    - [Configuring the Metrics User Interface](#configuring-the-metrics-user-interface)
    - [Executing SaaMS with the Metrics User Interface](#executing-saams-with-the-metrics-user-interface)
9. [Contact](#contact)
# SaaMS : Synopses as a Microservice
## Abstract
The use of data synopses in Big streaming Data analytics can offer 3 types of scalability: (i) horizontal scalability, for scaling with the volume and velocity of Big streaming Data, (ii) vertical scalability,
for scaling with the number of processed streams, and (iii) federated scalability, i.e. reducing
the communication cost for performing global analytics across a number of geo-distributed data centers
or devices in IoT settings. Despite the aforementioned virtues of synopses, no state-of-the-art Big Data framework or IoT platform
provides a native API for stream synopses supporting all three types of required scalability. In this work, we
fill this gap by introducing a novel system and architectural paradigm, namely  Synopses-as-a-MicroService (SaaMS), for both parallel and geo-distributed stream summarization at scale. SaaMS is developed on Apache Kafka and Kafka Streams and can provide all the required types of scalability together with (i) the ability to seamlessly perform adaptive resource allocation with zero downtime for the running analytics and (ii) the ability to run both across powerful computer clusters and Java-enabled IoT devices. Therefore, SaaMS is directly deployable from applications that either operate on powerful clouds or across the cloud to edge continuum.

# Configuration SaaMS before execution:
For the application to function properly, the following parameters must be set in the config.properties file. 

1. Following is an example of the config.properties file as must implement in case of running the application in a Windows environment.
    ```properties
    # Determine the Kafka Broker
    BOOTSTRAP_SERVERS = localhost:9092,localhost:9093,localhost:9094
    
    # Determine the path for saving synopsis instance files
    SAVE_FILE_PATH_PREFIX = C:\\StoreSynopses\\stored_
    
    # Determine the path for saving Kafka Streams files
    KAFKA_STREAM_DIR = C:\\tmp\\kafka-streams\\
    
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
    REQUEST_TOPIC_PATH = C:\\RequestExamples\\Request_small.json
    DATA_TOPIC_PATH = C:\\DataExamples\\ProduceDataToDataTopic
    ```
2. Following is an example of the config.properties file as must implement in case of running the application in a Linux environment.
    ```properties
    # Determine the Kafka Broker
    BOOTSTRAP_SERVERS = localhost:9092,localhost:9093,localhost:9094
    
    # Determine the path for saving synopsis instance files
    SAVE_FILE_PATH_PREFIX = /home/user1/StoreSynopses/stored_
    
    # Determine the path for saving Kafka Streams files
    KAFKA_STREAM_DIR = /home/user1/tmp/kafka-streams/
    
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
    REQUEST_TOPIC_PATH = /home/user1/RequestExamples/Request_small.json
    DATA_TOPIC_PATH = /home/user1/DataExamples/ProduceDataToDataTopic
    ```
3. In case of running the application in a cluster environment, the config.properties file must be set in all the nodes of the cluster.

4. In case of config.properties file is not found, the application will use default values as in config.properties file.

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
    "dataSetKey":"Forex",
    "date":"01/02/2019",
    "time":"00:00:01",
    "price":6.0654,
    "volume":1   
}
```
## Data Message Structure Explanation:

- **streamID (String):** In a financial example, it represents the stock name.
- **dataSetKey (String):** In a financial example, it represents the Stock Market name.
- **date & time (String):** The timestamp at which the `streamID` was captured.
- **price (Double):** The current price at the specified date and time.
- **volume (Integer):** The quantity of current trades for this `streamID` at the specified date and time.


## Request message for adding a new Synopsis
```json
{
    "streamID": "EURTRY", 
    "synopsisID": 1,
    "dataSetKey": "Forex",
    "param": ["CountMin", "price", "NotQueryable", 0.001, 0.99, 12345],
    "noOfP": 5
}
```

## Request message for query a Synopsis
```json
{
    "streamID" : "EURTRY",
    "synopsisID" : 1,
    "dataSetKey" : "Forex",
    "param" : [ 6.05736, "price", "Queryable", "Continues"],
    "noOfP" : 5
}  
```
## Request Message Structure Explanation:

- **streamID (String):** Symbolizes the item name that needs to build up a Synopsis. If the `streamID` is empty the synopsis built or queried for the whole streamIDs in the `dataSetKey`.
- **synopsisID (Integer):** Defines the synopsis type based on the previous Table.
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

An example of how to use for each type of Synopsis the query and not query Request Message exist in the `RequestExamples` file.
## Load Request Message from a file
The functionality of loading a saved Synopsis from disc, in practice, can be implemented using the following request:
```json
{
  "param" : [ "LOAD_REQUEST", "PathToLoadSynopsis\\stored_CountMin.ser" ]
}
```
The `LOAD_REQUEST` is a keyword that indicates that the request is for loading a Synopsis from disc. The `PathToLoadSynopsis` is the path where the serialized file of the Synopsis is stored.
## Delete Request Message from a file
The functionality of deleting a maintained Synopsis in SaaMS, in practice, can be implemented using the following request:
```json
{
  "streamID" : "EURTRY",
  "synopsisID" : 1,
  "dataSetKey" : "Forex",
  "param" : [ "DELETE_REQUEST", "price"],
  "noOfP" : 6
}
```
The `DELETE_REQUEST` is a keyword that indicates that the request is for deleting a maintained Synopsis. The other filed are the same as the [Request message for adding a new Synopsis](#request-message-for-adding-a-new-synopsis).
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

The project was developed and tested on the following versions, but it could also work on other versions.

Before proceeding to the execution, ensure that the computer has run a Kafka and a Zookeeper Server. More details can be found in [Annex 2](#annex-2-how-to-establish-a-kafka-cluster-and-a-zookeeper-server-which-is-necessary-for-execute-saams).

## How to Execute SaaMS:

### Step 1: Git Clone
#### Windows/Linux:
```bash
git clone https://github.com/geok1999/Synopses-as-a-MicroService-SaaMS.git
```

### Step 2: Build the JAR
#### Windows/Linux:
```bash
mvn clean package
```

### Step 3: Run the SaaMS JAR
#### Windows:
```bash
java -DconfigFilePath=.\Configuration\config.properties -jar target/SaaMS_APP-1.0-SNAPSHOT-jar-with-dependencies.jar
```

#### Linux:
```bash
java -DconfigFilePath=./Configuration/config.properties -jar target/SaaMS_APP-1.0-SNAPSHOT-jar-with-dependencies.jar
```
* The `-DconfigFilePath` isn't the exact path of the config.properties file, but it is an example. The user can set the path of the config.properties file according to the location of the file in the computer.
### Step 4: Run the User Interface JAR
For producing messages to the Request and Data Kafka topics in SaaMS, the following commands must be executed:

#### Produce messages to the Request Kafka Topic
##### Windows:
```bash
java -DconfigFilePath=.\Configuration\config.properties -jar target/Producing-TO-REQUEST-TOPIC-jar-with-dependencies.jar
```
##### Linux:
```bash
java -DconfigFilePath=./Configuration/config.properties -jar target/Producing-TO-REQUEST-TOPIC-jar-with-dependencies.jar
```

#### Produce messages to the Data Kafka Topic
##### Windows:
```bash
java -DconfigFilePath=.\Configuration\config.properties -jar target/Producing-TO-DATA-TOPIC-jar-with-dependencies.jar
```
##### Linux:
```bash
java -DconfigFilePath=./Configuration/config.properties -jar target/Producing-TO-DATA-TOPIC-jar-with-dependencies.jar
```

The `-DconfigFilePath` argument is optional, and the example file path `.\Configuration\config.properties` (Windows) or `./Configuration/config.properties` (Linux) is provided for guidance. If it is not set, the application will use the default values to configure the app.

# Annex 1: How to use transform data script
The `Transform-TXT-Data-To-Json.jar` is a Java application that can be used to transform a CSV file to a JSON file. The JSON file can be used to produce data messages to the Data Kafka topic in SaaMS.

## Step 1: Must have a txt file with the following file name format:
```
\<Market Type\>·\<Unique Identifier\>·\<Suffix\>
```
This is necessary to extract:
- **streamID**: Extracted from the second component (the unique identifier).
- **dataSetKey**: Extracted from the first component (the market type).
## Step 2: Each line of the txt file must have the following format:
```
\<Date\>,\<Time\>,\<Price\>,\<Volume\>
```
## Step 3: Run the transform data script and provide the path of stock market where the txt file is located
### Windows:
```
java -DconfigFilePath=.\DataExamples\Forex·EURTRY·NoExpiry.txt -jar target/Transform-TXT-Data-To-Json.jar 
```
### Linux:
```
java -DconfigFilePath=./DataExamples/Forex·EURTRY·NoExpiry.txt -jar target/Transform-TXT-Data-To-Json.jar 
```

# Annex 2: How to establish a Kafka Cluster and a Zookeeper Server which is necessary to execute SaaMS
In this project provides two methods to establish a Kafka Cluster and a Zookeeper Server:
## Method 1: Using a Script
1. Download and install the Kafka and Zookeeper from the official website 
2. In project Script folder, there are two scripts:
   - `RunAllScripts.cmd` for Windows
   - `RunAllScripts.sh` for Linux
3. Before execute the scripts check if the file paths that you install kafka are the same in the scripts.
4. Execute the script.
## 2. Method 2: Using Docker
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
# Annex 3: How to use the Metrics User Interface
The Metrics User Interface is a Java application that used for implement metrics in kafka streams. It uses the JMX to get the metrics from the Kafka Streams. The Metrics User Interface which adopt in SaaMS_2.0 can be used to get the metrics for Throughput and Communication Cost.
## How to use the Metrics Interface
### Step 1: Run the Metrics User Interface JAR
#### Windows:
```
java -DconfigMetricsFilePath=.\Configuration\MetricsConfig.properties -jar target/Metrics-User-Interface.jar
```
#### Linux:
```
java -DconfigMetricsFilePath=./Configuration/MetricsConfig.properties -jar target/Metrics-User-Interface.jar
```

The `-DconfigMetricsFilePath` argument is optional, and the example file path `.\Configuration\MetricsConfig.properties` (Windows) or `./Configuration/MetricsConfig.properties` (Linux) is provided for guidance. If it is not set, the application will use the default values to configure the app. 


### Step 2: Options in the Metrics User Interface
When the Metrics User Interface is launched, you will see the following options:

```
1. Get Throughput
2. Get Communication Cost
3. Exit
```
* Throughput: The results of the throughput metrics will be stored periodically in the file as defined in the `MetricsConfig.properties` file.
* Communication Cost: The results of the communication cost metrics will be shown in the console each time user choose it. 
### Step 3: Select an Option
Choose one of the available options to fetch the desired metrics
* In case of choosing the option 2 except the [Request message for adding a new Synopsis](#request-message-for-adding-a-new-synopsis) it's obligate to send for each one a RawData Request with the same fields as [Request message for adding a new Synopsis](#request-message-for-adding-a-new-synopsis). Following an example of RawData Request
```json
{
    "streamID": "EURTRY", 
    "synopsisID": 0,
    "requestID": 1,
    "dataSetKey": "Forex",
    "param": ["RawData", "price", "NotQueryable"],
    "noOfP": 5
}
```
Important note all RawData Request must have ```"synopsisID": 0```
### Step 4: Fetch and Store Metrics
After selecting an option, the Metrics User Interface retrieves the metrics from Kafka Streams and saves them to a specified file or displays them in the console.

## Configuring the Metrics User Interface

The Metrics User Interface uses a configuration file (`MetricsConfig.properties`) to determine its behavior. Below is an example configuration:

### Example Configuration (Windows):
```properties
# The `JMX_URLS` is the URL of the JMX that the Metrics User Interface will connect to get the metrics
JMX_URLS=service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi
# The `OUTPUT_FILENAME` is the path where the metrics will be stored
OUTPUT_FILENAME=C:\MetricsResults\test1
# The `PERIOD` is the time interval (in seconds) for fetching the metrics
PERIOD=5
#Enable the communication cost calculation
COMMUNICATION_COST_ENABLE=true
```

### Example Configuration (Linux):
```properties
# The `JMX_URLS` is the URL of the JMX that the Metrics User Interface will connect to get the metrics
JMX_URLS=service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi
# The `OUTPUT_FILENAME` is the path where the metrics will be stored
OUTPUT_FILENAME=/home/user1/MetricsResults/test1
# The `PERIOD` is the time interval (in seconds) for fetching the metrics
PERIOD=5
#Enable the communication cost calculation
COMMUNICATION_COST_ENABLE=true
```

### Cluster Environment Configuration:
In a cluster environment, set the `JMX_URLS` property to include multiple nodes:
```properties
JMX_URLS=service:jmx:rmi:///jndi/rmi://<node1>:9999/jmxrmi,service:jmx:rmi:///jndi/rmi://<node2>:9999/jmxrmi,service:jmx:rmi:///jndi/rmi://<node3>:9999/jmxrmi
```

## Executing SaaMS with the Metrics User Interface

To ensure proper functionality, execute SaaMS with the following command:

### Command for Windows:
```bash
java -Dcom.sun.management.jmxremote \
     -Dcom.sun.management.jmxremote.port=9999 \
     -Dcom.sun.management.jmxremote.ssl=false \
     -Dcom.sun.management.jmxremote.authenticate=false \
     -DconfigFilePath=.\Configuration\config.properties -jar target/SaaMS_APP-1.0-SNAPSHOT-jar-with-dependencies.jar
```
### Command for Linux:
```bash
java -Dcom.sun.management.jmxremote \
     -Dcom.sun.management.jmxremote.port=9999 \
     -Dcom.sun.management.jmxremote.ssl=false \
     -Dcom.sun.management.jmxremote.authenticate=false \
     -DconfigFilePath=./Configuration/config.properties -jar target/SaaMS_APP-1.0-SNAPSHOT-jar-with-dependencies.jar
```

# Contact
If you have any questions, please contact us at:
* gkalfakis@tuc.gr
* ngiatrakos@tuc.gr


