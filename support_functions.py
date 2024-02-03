# importing libraries for Azure Blob
import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# importing libraries for data handling
from io import StringIO
import csv

# # importing libraries for couchbase DB
from datetime import timedelta

# needed for any cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster

# needed for options -- cluster, timeout, SQL++ (N1QL) query, etc.
from couchbase.options import ClusterOptions, ClusterTimeoutOptions, QueryOptions
import json

config = json.load(open("config.json"))


# azure blob connection
def azure_blob_conn():
    try:
        # connection string
        connection_string = config["azure_blob_config"]["connection_string"]

        # creating connections
        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )

        print("Azure blob connection established successfully!!")
        return blob_service_client

    except Exception as e:
        print(e)


def couchbase_conn():

    try:
        # connection config
        username = config["couchbase_config"]["user_name"]
        password = config["couchbase_config"]["password"]
        bucket_name = config["couchbase_config"]["bucket_name"]
        scope_name = config["couchbase_config"]["scope_name"]
        connection_url = config["couchbase_config"]["connection_url"]

        # user auth
        auth = PasswordAuthenticator(username, password)

        # making connection with cluster
        cluster = Cluster(connection_url, ClusterOptions(auth))

        # Wait until the cluster is ready for use.
        cluster.wait_until_ready(timedelta(seconds=5))

        print("CouchBase connection established successfully!!")

        return cluster
    except Exception as e:
        print(e)


def container_client_config(blob_service_client):

    try:
        # configuring container client
        input_container = blob_service_client.get_container_client(
            container=config["azure_blob_config"]["input_container"]
        )

        output_container = blob_service_client.get_container_client(
            container=config["azure_blob_config"]["output_container"]
        )

        discard_container = blob_service_client.get_container_client(
            container=config["azure_blob_config"]["discard_container"]
        )

        print("Container-Client connection established successfully!!")
        return input_container, output_container, discard_container

    except Exception as e:
        print(e)


# function to read files from blob storage and convert into a dictionery object
def csv_extractor(input_container):
    try:
        # getting filenames in input container
        blob_list = [file.name for file in input_container.list_blobs()]

        # getting file data in stringIO format
        file_objs = [
            StringIO(input_container.download_blob(file_name).readall().decode("utf-8"))
            for file_name in blob_list
        ]

        # creating Dictionary Object using CSV DictReader
        dict_objs = []
        for data in file_objs:
            data_obj = list(csv.DictReader(data))
            dict_objs.append(data_obj)

        print("CSV Extraction process done successfully!!")
        return blob_list, dict_objs

    except Exception as e:
        print(e)


def dump_to_couchbase(cluster_obj, data_list, file_list):

    try:

        bucket_name = config["couchbase_config"]["bucket_name"]
        scope_name = config["couchbase_config"]["scope_name"]

        # creating bucket Object from bucket_name
        bucket = cluster_obj.bucket(bucket_name)

        # correcting file_list

        file_list = [
            "collection_"
            + file_name.replace(".", "_")
            .translate({ord(c): " " for c in "!@#$%^&*()[]{};:,./<>?\|`~-=+"})
            .strip()
            .replace(" ", "_")
            for file_name in file_list
        ]

        # configuring target to dump data bucket-->scope--->collection

        for collection_name, data in zip(file_list, data_list):

            # Creating a collection as out filename
            result = cluster_obj.query(
                "CREATE COLLECTION `{}`.{}.{}".format(
                    bucket_name, scope_name, collection_name
                )
            )
            result.execute()

            # Setting the target to our collection to dump data
            target = bucket.scope(scope_name).collection(collection_name)

            # preparing data to store as multi insert
            temp_dict = {}
            for num, rec in enumerate(data):
                temp_dict[str(num)] = rec

            # inserting data into couchbase
            result = target.insert_multi(temp_dict)
        print("CSV files dumped into Couchbase Successfully!!")

    except Exception as e:
        print(e)
