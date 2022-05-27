from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import re


def seconds_to_timestamp(time_in_seconds):
    return timedelta(seconds=float(time_in_seconds))

def date_to_timestamp(date):
    date_timestamp = datetime.strptime(date,"%d-%b-%y")
    return date_timestamp

def evaluate_service_key(time_stamp):
    day_of_week = time_stamp.weekday()
    
    if(day_of_week<5):
        return "Weekday"
    elif (day_of_week==5):
        return "Saturday"
    else:
        return "Sunday"

def create_trip_df(df, service_key_df):
    print("Creating trip dataframe..")
    df['SERVICE_KEY'] = service_key_df['service_key']
    trip_df = pd.DataFrame(columns=['trip_id','route_id','vehicle_id','service_key','direction'])
    trip_id_lookup = list()
    for index,row in df.iterrows():
        if row['EVENT_NO_TRIP'] in trip_id_lookup:
            continue
        else:
            new_row = {'trip_id':row['EVENT_NO_TRIP'],'route_id':'','vehicle_id':row['VEHICLE_ID'],'service_key':row['SERVICE_KEY'],'direction':''}
            trip_df = trip_df.append(new_row, ignore_index=True)
            trip_id_lookup.append(row['EVENT_NO_TRIP'])
    print('Hold on, returning trip dataframe..')
    return trip_df
            


def main():
    print(seconds_to_timestamp(57212))
    # print(date_to_timestamp("13-OCT-20"))

    example_datetime = "13-OCT-20 15:53:32"
    print(date_to_timestamp(example_datetime))

if __name__ == "__main__":
    main()