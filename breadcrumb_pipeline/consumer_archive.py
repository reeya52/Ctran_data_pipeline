#!/usr/bin/env python

from confluent_kafka import Consumer
import json
import ccloud_lib
from datetime import datetime
import os
import ast
from cloud_storage import *


# current_file_path = "/home/reeya/consumed_data"
# directory_path = os.path.join(current_file_path, "Data")



if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'archive_group'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    records_list = list()

    # Process messages
    total_count = 0
    try:
        while True:
            # records_list = list()
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming

                if(len(records_list)!=0):
                    storage_client = storage.Client.from_service_account_json(json_credentials_path='/home/reeya/DE_project/DE_Project_key.json')
                    bucket_name = "breadcrumb_archive"
                    date = datetime.today().strftime('%Y-%m-%d')
                    # destination_blob_name = date + str(".json")
                    destination_blob_name = str('2022-05-24.json')
                    compressed_data = zlib.compress(json.dumps(records_list).encode("utf-8"), 2)
                    compressed_data_string = str(compressed_data)

                    if(check_if_bucket_exists(storage_client, bucket_name)!=True):
                        create_bucket_class_location(storage_client, bucket_name)
                    upload_blob_memory(storage_client, bucket_name, compressed_data, destination_blob_name)

                    print("Data has been archived in buckets\n\n\n")
                    records_list=list()
                print("Waiting for message or event/error in poll()")
                
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                record_key = msg.key()
                record_value = msg.value()
                
    
                if(record_value == b"all records sent"):
                    continue
                else:
                    records_list.append(json.dumps(ast.literal_eval(record_value.decode("UTF-8"))))
                    # print(type(records_list[0]))
                    # break
                    total_count = total_count+1
                    print("Consumed record with key {} and value {}, and updated total count to {}".format(record_key, record_value, total_count))
               
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
