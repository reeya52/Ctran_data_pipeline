import time
import psycopg2
import argparse
import re
import csv
import pandas as pd
import json
from datetime import datetime
import os
import ast
from data_transform import *
import time
import psycopg2.extras as extras


DB_Name = "ctran_db"
DB_User = "reeya"
DB_Password = "reeya"
Create_DB = False
TableName = "stop_data"
def db_connect():
    connection = psycopg2.connect(
        host = "localhost",
        database = DB_Name,
        user = DB_User,
        password = DB_Password,
    )
    connection.autocommit = True
    return connection

def execute_batch_trip(conn, df, table, page_size=100):
    """
    Using psycopg2.extras.execute_batch() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_batch(cursor, query, tuples, page_size)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_batch() done")
    cursor.close()

def return_df():
    # current_file_path = "/home/reeya/consumed_data"
    # date = datetime.today().strftime('%Y-%m-%d')
    # file_name = date + str(".json")
    # file_path = os.path.join(current_file_path, file_name)

    file_path = "/home/reeya/consumed_stop_event/2022-05-28.json"

    with open(file_path, 'r') as f:
        data = json.load(f)

    print("Data Loaded..")

        # print(data[0])
    for i in range(len(data)):
        data_string = ast.literal_eval(data[i])
        data[i] = json.loads(data_string)

    print("Data Transformed..")

    df = pd.read_json(json.dumps(data, indent=15))
    # print(df.head(10))
    print("Returning Data")
    return df

def transform_data(df):
    column_names = ['route_number', 'direction', 'stop_time', 'arrive_time', 'vehicle_number', 'leave_time', 'train', 'dwell', 'location_id', 'door', 'lift', 'ons', 'offs','estimated_load', 'maximum_speed', 'data_source', 'schedule_status', 'trip_id']

    for each_column_name in column_names:
        if(df[each_column_name].isnull().values.any()):
            df[each_column_name] = df[each_column_name].interpolate(method='linear', limit_direction = 'both')
    
    df = df.assign(maximum_speed = 0)
    
    return df

def main():
    df = return_df()
    df_transformed = transform_data(df)

    print(df_transformed.shape)
    print("location_id: ",np.where(pd.isnull(df_transformed['location_id'])))
    print("maximum_speed: ",np.where(pd.isnull(df_transformed['maximum_speed'])))

    conn = db_connect()
    print("Loading into the database")
    execute_batch_trip(conn=conn, df = df_transformed, table=TableName, page_size=100)

if __name__ =="__main__":
    main()