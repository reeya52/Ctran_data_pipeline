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
TableName = "TestTable"
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
    query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s)" % (table, cols)
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

def execute_batch_breadcrumb(conn, df, table, page_size=100):
    """
    Using psycopg2.extras.execute_batch() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s)" % (table, cols)
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

    file_path = "/home/reeya/consumed_data/2022-05-28.json"

    with open(file_path, 'r') as f:
        data = json.load(f)

    print("Data Loaded..")

        # print(data[0])
    for i in range(len(data)):
        data[i] = ast.literal_eval(data[i])

    print("Data Transformed..")

    df = pd.read_json(json.dumps(data, indent=15))
    # print(df.head(10))
    print("Returning Data")
    return df

 
def main():

    df = return_df()
    if not (df.empty):
        print("Data received..")

    # breadcrumb_df = df[['ACT_TIME','GPS_LATITUDE','GPS_LONGITUDE','DIRECTION','VELOCITY','EVENT_NO_TRIP']]
    # trip_df = df[['EVENT_NO_TRIP','VEHICLE_ID']]

    # print(trip_df.head(10))
    print("Changing Timestamp format")
    # Combining ACT_TIME and OPD_DATE
    date_df = pd.DataFrame(columns=['tstamp'])
    date_df['tstamp'] = df['OPD_DATE'].apply(date_to_timestamp)

    time_df = pd.DataFrame(columns=['ACT_TIME'])
    time_df['ACT_TIME'] = df['ACT_TIME'].apply(seconds_to_timestamp)

    # Tranformed timestamp column
    tstamp_df = pd.DataFrame(columns=['tstamp'])
    tstamp_df['tstamp'] = date_df['tstamp']+time_df['ACT_TIME']

    # print(tstamp_df.head(10))

    service_key_df = pd.DataFrame(columns=['service_key'])
    service_key_df['service_key'] = tstamp_df['tstamp'].apply(evaluate_service_key)

    # print(service_key_df.head(10))

    breadcrumb_df = pd.DataFrame(columns=['tstamp','latitude','longitude','direction','speed','trip_id'])
    breadcrumb_df['tstamp'] = tstamp_df['tstamp']
    breadcrumb_df[['latitude','longitude','direction','speed','trip_id']] = df[['GPS_LATITUDE','GPS_LONGITUDE','DIRECTION','VELOCITY','EVENT_NO_TRIP']]

    trip_df = create_trip_df(df=df, service_key_df=service_key_df)

    print(trip_df.shape)

    
    conn = db_connect()
    
    trip_df = trip_df.drop(['route_id','direction'], axis=1)
    breadcrumb_df['direction'] = breadcrumb_df['direction'].replace(to_replace='',value=int(0))
    breadcrumb_df['speed'] = breadcrumb_df['speed'].replace(to_replace='', value=float(0.0))
    breadcrumb_df['latitude'] = breadcrumb_df['latitude'].replace(to_replace='', value=float(0))
    breadcrumb_df['longitude'] = breadcrumb_df['longitude'].replace(to_replace='', value=float(0))


    print("loading data into trip table..")
    execute_batch_trip(conn=conn,df=trip_df,table='trip',page_size=100)
    print("data loaded into trip..")
    
    print("loading data into breadcrumb table..")
    execute_batch_breadcrumb(conn=conn,df=breadcrumb_df,table='breadcrumb',page_size=100)
    print("data loaded into breadcrumb..")

    print("ja bete ja, party karo")



if __name__ == "__main__":
    main()