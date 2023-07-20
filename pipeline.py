import json
import pandas as pd
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from cassandra.cluster import Cluster


def process_kafka_message(message):
    """Processing the Kafka message and extracting clickstream data"""
    try:
        # Assuming message contains clickstream data in JSON format
        payload = json.loads(message.value.decode("utf-8"))

        # Extracting fields from the payload
        row_key = payload.get("row_key", None)
        user_id = payload.get("user_id", None)
        timestamp = payload.get("timestamp", None)
        url = payload.get("url", None)
        country = payload.get("country", None)
        city = payload.get("city", None)
        browser = payload.get("browser", None)
        os = payload.get("os", None)
        device = payload.get("device", None)

        if (
            row_key is not None
            and user_id is not None
            and timestamp is not None
            and url is not None
            and country is not None
        ):
            clickstream_data = {
                "row_key": row_key,
                "user_id": user_id,
                "timestamp": timestamp,
                "url": url,
                "country": country,
                "city": city,
                "browser": browser,
                "os": os,
                "device": device,
            }

            return clickstream_data
        else:
            return None

    except Exception as e:
        return e


clickstream_data_store = []


def store_clickstream_data(clickstream_data):
    """Storing click stream data"""
    global clickstream_data_store
    clickstream_data_store.append(clickstream_data)


# Data Ingestion
def ingest_clickstream_data(kafka_broker, kafka_topic):
    """Processing kafka message and extracting clickstream data to store"""
    consumer = KafkaConsumer(kafka_topic, bootstrap_servers=[kafka_broker])
    for message in consumer:
        clickstream_data = process_kafka_message(message)
        if clickstream_data:
            store_clickstream_data(clickstream_data)


def process_clickstream_data():
    """Collecting data from the source & Processing it using pandas"""
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect("clickstream_keyspace")
    # Fetching data from the Cassandra table into a pandas DataFrame
    rows = session.execute("SELECT * FROM clickstream_table")
    df = pd.DataFrame(list(rows))

    # Grouping it by URL and country and calculating aggregates
    aggregated_data = (
        df.groupby(["url", "country"])
        .agg(
            click_count=pd.NamedAgg(column="row_key", aggfunc="count"),
            unique_users=pd.NamedAgg(column="user_id", aggfunc=pd.Series.nunique),
            avg_time_spent=pd.NamedAgg(column="time_spent", aggfunc="mean"),
        )
        .reset_index()
    )

    # Returning processed data in a list of dictionaries format
    processed_data = aggregated_data.to_dict(orient="records")
    return processed_data


# Data Indexing
def index_processed_data(processed_data):
    """Connecting to Elasticsearch cluster & indexing the data"""
    es = Elasticsearch(["http://localhost:9200"])

    # Indexing processed_data into Elasticsearch
    for record in processed_data:
        # Creating an Elasticsearch document for each record
        document = {
            "url": record["url"],
            "country": record["country"],
            "click_count": record["click_count"],
            "unique_users": record["unique_users"],
            "avg_time_spent": record["avg_time_spent"],
        }

        es.index(index="clickstream_index", doc_type="_doc", body=document)


if __name__ == "__main__":
    # Defining Kafka configurations
    kafka_broker = "localhost:9092"
    kafka_topic = "clickstream_topic"

    # Data Ingestion
    ingest_clickstream_data(kafka_broker, kafka_topic)

    # Data Processing
    processed_data = process_clickstream_data()

    # Data Indexing
    index_processed_data(processed_data)
