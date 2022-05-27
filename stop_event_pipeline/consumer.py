#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer
import json
import ccloud_lib
from datetime import datetime
import os
import ast


current_file_path = "/home/reeya/consumed_stop_event"
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
    consumer_conf['group.id'] = 'python_example_group_1'
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

                # date = datetime.today().strftime('%Y-%m-%d')
                # file_name = date + str(".json")
                file_name = str('2022-05-26.json')
                file_path = os.path.join(current_file_path, file_name)

                with open(file_path,'w') as fp:
                    json.dump(records_list, fp, indent=3)
                fp.close()

                print("Data has been consumed and written to file..\n\n\n")
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
                    records_list.append(json.dumps(record_value.decode("UTF-8")))
                    # print(type(records_list[0]))
                    # break
                    total_count = total_count+1
                    print("Consumed record with key {} and value {}, and updated total count to {}".format(record_key, record_value, total_count))
               
                #Main Code
                # Check for Kafka message
                # record_key = msg.key()
                # record_value = msg.value()
                # data = json.loads(record_value)
                # total_count += 1
                # print("Consumed record with key {} and value {}, \
                #       and updated total count to {}"
                #       .format(record_key, record_value, total_count))
                # # record_list.append(data)


    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
