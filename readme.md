## Data Pipeline - Approach and Assumptions

### 1. Overview:
----
Data Pipeline is designed to capture, process and process clickstream data from web applications in real time. The pipeline follows specified requirements, using Apache Kafka for data access, Panda for data processing, and Elasticsearch for data indexing and searching. The purpose of the pipeline is to store clickstream data in a data warehouse and perform periodic data operations to collect and analyze clickstream metrics by URL and country

### 2. Data Ingestion:
----
The data capture component manages clickstream data from a Kafka topic, clickstream_topic. The Kafka client receives messages from the broker, processes each message, and extracts the relevant clickstream data. This function assumes that the clickstream data is sent to Kafka in serialized format(JSON), and the process_kafka_message function is responsible for extracting the fields needed to deserialize the message

### 3. Data Processing:
----
The data processing part uses pandas, to perform data processing tasks. Assuming the process_clickstream_data function retrieves the data from the data store through the cassandra-driver as I am not sure about the data storage we will use. Understanding usage collects data by URL and country, counting clicks, unique users, and the average time spent by users from each country on each URL

### 4. Data Indexing:
-----
The data indexing component is responsible for indexing the clickstream data processed in Elasticsearch for efficient query analysis. The index_processed_data service connects to the Elasticsearch cluster and loads each index from the processed data as a separate document in an index called clickstream_index.

### 5. Assumptions:

The data schema for the clickstream events includes fields like 
```
1. row_key
2. user_id
3. timestamp
4. url
5. country
6. city
7. browser
8. os
9. device
```
The clickstream data is sent to Kafka in a pre-processed format, and the process_kafka_message function handles the deserialization. The data store used for storing clickstream data follows the schema mentioned in the requirements. In this implementation, I assume the use of Cassandra as the data store as there is no information about data store. I had no idea about Cassandra until I **Googled** it. I assume the availability of resources like Kafka, Elasticsearch, and the data store with the required configurations and data connectivity.


## Note
As it is a high-level pipeline that we need to build. So, I mentioned the above points that were carried out to make it successful. Also, I wrote the code for how all the processes will work. I don't have enough resources or data, So I wrote the code based on my knowledge and research