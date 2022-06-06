from google.cloud import pubsub_v1
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from concurrent.futures import TimeoutError
import json
import os
from time import sleep

# Set environment variable
if ("GOOGLE_APPLICATION_CREDENTIALS" not in os.environ):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "project-credentials.json"

# Load config
with open("config.json", "r") as fd:
    config = json.loads(fd.read())

my_id = config["my_id"]
project_id = config["project_id"]
service_account_json = config["service_account_json"]

# Pubsub parameters
subscription_id = "sub_iot_devices"
topic_id = f"inondation_{my_id}"
timeout = 5.0

# BigQuery parameters
dataset = "dataset_pipeline_meteo"


def create_bq_table(table, schema):
    bq_client = bigquery.Client()
    table_id = bigquery.Table.from_string(f"{project_id}.{dataset}.{table}")
    try:
        bq_client.get_table(table_id)
    except NotFound :
        print(f"Creating table : {table_id}...")
        table = bigquery.Table(table_id, schema=schema)
        response = bq_client.create_table(table)
        print(f"Table created : {response}")


def insert_data(table, data):
    bq_client = bigquery.Client()
    table_id = bigquery.Table.from_string(f"{project_id}.{dataset}.{table}")
    rows_to_insert = [data]
    errors = bq_client.insert_rows_json(table_id, rows_to_insert)
    if not errors:
        print("New rows have been added in table.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))


def process_payload(message):
    message.ack()
    print(f"Received message payload : {message.data}")
    print("Processing data...")
    if message.attributes["deviceId"].startswith("station-meteo-"):
        table = f"station-meteo-{my_id}"
        # Create table (if not exist)
        schema = [bigquery.SchemaField("device_id", "STRING", mode="REQUIRED"),
                  bigquery.SchemaField("humidity", "FLOAT", mode="REQUIRED"),
                  bigquery.SchemaField("temperature", "FLOAT", mode="REQUIRED"),
                  bigquery.SchemaField("water_level", "INTEGER", mode="REQUIRED"),
                  bigquery.SchemaField("timestamp", "STRING", mode="REQUIRED")]
        create_bq_table(table, schema)
        # Insert data in table
        data = json.loads(message.data.decode("utf-8"))
        insert_data(table, data)


def consume_payload(project_id, subscription_id):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=process_payload)
    with subscriber:
        try:
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()


def init_pubsub():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    topic_path = subscriber.topic_path(project_id, topic_id)
    # subscription_path = f"projects/{project_id}/subscriptions/{subscription_id}"
    try:
        subscriber.get_subscription(request={"subscription": subscription_path})
    except NotFound:
        subscriber.create_subscription(request={"name": subscription_path, "topic": topic_path})
    return subscription_path


def main():
    subscription_path = init_pubsub()
    print(f"Listening for messages on {subscription_path}...")
    while True:
        consume_payload(project_id, subscription_id)
        sleep(10)


if __name__ == '__main__':
  main()

