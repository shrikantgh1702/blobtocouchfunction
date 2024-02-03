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

from support_functions import (
    azure_blob_conn,
    couchbase_conn,
    container_client_config,
    csv_extractor,
    dump_to_couchbase,
)

import json


def blob_to_couchbase():
    # creating azure blob client
    blob_service_client = azure_blob_conn()

    # creating couch base connection
    cluster = couchbase_conn()

    # container-client configuration
    input_container, output_container, discard_container = container_client_config(
        blob_service_client
    )

    # getting file name and the Data in a list of Dictionary format
    file_list, data_list = csv_extractor(input_container)

    # Dump data to couchbase
    dump_to_couchbase(cluster_obj=cluster, data_list=data_list, file_list=file_list)


if __name__ == "__main__":
    blob_to_couchbase()
