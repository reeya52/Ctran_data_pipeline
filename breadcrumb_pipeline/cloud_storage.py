from google.cloud import storage
from datetime import datetime
import json
import zlib
import binascii

def create_bucket_class_location(storage_client, bucket_name):
    """
    Create a new bucket in the US region with the coldline storage
    class
    """

    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "COLDLINE"
    new_bucket = storage_client.create_bucket(bucket, location="us")

    print(
        "Created bucket {} in {} with storage class {}".format(
            new_bucket.name, new_bucket.location, new_bucket.storage_class
        )
    )
    return new_bucket

def upload_blob_file(storage_client, bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

def upload_blob_memory(storage_client, bucket_name, contents, destination_blob_name):
    """Uploads content from memory to the bucket"""

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(contents)

    print(
        f"{destination_blob_name} with contents of  uploaded to {bucket_name}."
    )

def compress_data(file_path):
    
    # file_path = "/home/ajaybabu/consumed_data/2022-05-26.json"

    with open(file_path, 'r') as f:
        data = json.load(f)

    # print(type(data))

    # for i in range(len(data)):
    #     data[i] = json.dumps(data[i], indent=2).encode('utf-8')

    # data_bytes = json.dumps(data, indent=2).encode('utf-8')
    print(data[0])
    # data_bytes = bytes(data)
    # print(type(data_string))
    compressed_data = zlib.compress(json.dumps(data).encode("utf-8"), 2)
    print("compresses successfully")

    return str(compressed_data)

def check_if_bucket_exists(storage_client, bucket_name):
    buckets = storage_client.list_buckets()

    for each_bucket in buckets:
        if(bucket_name==each_bucket.name):
            return True
    return False

def main():
    # create_bucket_class_location("sample_archive")

    storage_client = storage.Client.from_service_account_json(json_credentials_path='/home/reeya/DE_project/DE_Project_key.json')

    bucket_name = "sample_archive"
    # The path to your file to upload
    source_file_name = "/home/reeya/DE_project/breadcrumb_pipeline/sample.json"
    # The ID of your GCS object
    # destination_blob_name = "sample_json_file"

    date = datetime.today().strftime('%Y-%m-%d')
    destination_blob_name = date + str(".json")

    compressed_data = compress_data(source_file_name)
    
    # create_bucket_class_location(storage_client, bucket_name)
    # # upload_blob(storage_client, bucket_name, source_file_name, destination_blob_name)
    # upload_blob_memory(storage_client, bucket_name, compressed_data, destination_blob_name)

    if(check_if_bucket_exists(storage_client, bucket_name)!=True):
        create_bucket_class_location(storage_client, bucket_name)
    upload_blob_memory(storage_client, bucket_name, compressed_data, destination_blob_name)


if __name__ == "__main__":
    main()
