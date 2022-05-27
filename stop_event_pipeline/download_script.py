import json
import re
from bs4 import BeautifulSoup as bs
from urllib import request
from datetime import datetime
import os


def extract_text(html_elements_list):
    extracted_list = list()
    for i in range(len(html_elements_list)):
        extracted_list.append(html_elements_list[i].text)

    return extracted_list

def change_dict_value_type(json_dict):
    for key in json_dict:
        if key in ['train_mileage','pattern_distance','location_distance','x_coordinate','y_coordinate']:
            if(json_dict[key]!=''):
                json_dict[key] = float(json_dict[key])
            else:
                json_dict[key] = None
        elif key == 'service_key':
            json_dict[key] = str(json_dict[key])
        else:
            if(json_dict[key]!=''):
                json_dict[key] = int(json_dict[key])
            else:
                json_dict[key] = None

    return json_dict

def extract_trip_id(heading):
    pattern = '(\d+)'
    match = re.search(pattern, heading)

    if match:
        return int(match.group())
    else:
        return None

def download_data():

    url = "http://www.psudataeng.com:8000/getStopEvents"

    html = request.urlopen(url)
    soup = bs(html, 'html.parser')

    headings = soup.find_all('h3')
    tables = soup.find_all('table')

    trip_id = list()

    for each_heading in headings:
        trip_id.append(extract_trip_id(each_heading.text))

    json_array = list()

    for i in range(len(trip_id)):
        

        table_data = tables[i]
        columns = table_data.find_all('th')
        keys_list = extract_text(columns)
        rows = table_data.find_all('tr')
        main_data = rows[1:]

        for each_row in main_data:
            json_object = dict()
            row_data = extract_text(each_row.find_all('td'))
        
            json_object = change_dict_value_type(dict(zip(keys_list, row_data)))
            json_object['trip_id'] = trip_id[i]
            json_array.append(json_object)

    return json_array


def main():
    current_file_path = os.getcwd()
    print(current_file_path)
    print(os.path.abspath(os.getcwd()))

    if (os.path.exists(os.path.join(current_file_path, 'stop_event_data')) == False):

        directory_name = "stop_event_data"
        new_directory_path = os.path.join(current_file_path, directory_name)
        os.mkdir(path = new_directory_path)

        date = datetime.today().strftime('%Y-%m-%d')
        file_name = date + str(".json")
        destination_folder_path = os.path.join(current_file_path, 'stop_event_data')
        file_path = os.path.join(destination_folder_path, file_name)

        json_objects_list = download_data()

        if(json_objects_list):
            print("Wait a min.. data is being written to the file..")
        else:
            print("some error occured")

        with open(file_path, 'w') as fp:
            json.dump(json_objects_list, fp, indent=3)
        fp.close()

        print("stop event data has been written to the file.")

    else:

        date = datetime.today().strftime('%Y-%m-%d')
        file_name = date + str(".json")
        destination_folder_path = os.path.join(current_file_path, 'stop_event_data')
        file_path = os.path.join(destination_folder_path, file_name)

        json_objects_list = download_data()

        if(json_objects_list):
            print("Wait a min.. data is being written to the file..")
        else:
            print("some error occured")

        with open(file_path, 'w') as fp:
            json.dump(json_objects_list, fp, indent=3)
        fp.close()

        print("stop event data has been written to the file.")
        

    print("Data_downloaded!")

if __name__ == "__main__":
    main()



