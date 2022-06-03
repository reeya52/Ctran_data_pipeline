#!/usr/bin/env python

from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
from datetime import datetime
import os

# current_file_path = os.getcwd()
current_file_path = "/home/reeya/Data"
# directory_path = os.path.join(current_file_path, "Data")
# date = datetime.today().strftime('%Y-%m-%d')
# file_name = date + str(".json")
file_name = str('2022-05-24.json')

file_path = os.path.join(current_file_path, file_name)
# file_path = "/home/reeya/DE_project/breadcrumb_pipeline/sample.json"
with open(file_path, 'r') as f:
    data = json.load(f)


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer = Producer(producer_conf)

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    #for n in range(10):
    
    for record in data:
        record_key = "alice"
        record_value = json.dumps(record)
        # record_value = record
        print(type(record_value))
        print("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.

        producer.poll(0)
        # break
    
    producer.produce(topic, key=record_key, value = "all records sent", on_delivery=acked)
    producer.poll(0)

    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))
