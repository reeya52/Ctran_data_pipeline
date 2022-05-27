from urllib import request
from datetime import datetime
import os

current_file_path = os.getcwd()

if (os.path.exists(os.path.join(current_file_path, 'Data')) == False):

    directory_name = "Data"
    new_directory_path = os.path.join(current_file_path, directory_name)
    os.mkdir(path = new_directory_path)

    url = "http://www.psudataeng.com:8000/getBreadCrumbData"
    date = datetime.today().strftime('%Y-%m-%d')
    file_name = date + str(".json")
    destination_path = os.path.join(current_file_path, 'Data')
    response = request.urlretrieve(url, os.path.join(destination_path, file_name))

else:

    url = "http://www.psudataeng.com:8000/getBreadCrumbData"
    date = datetime.today().strftime('%Y-%m-%d')
    file_name = date + str(".json")
    destination_path = os.path.join(current_file_path, 'Data')
    response = request.urlretrieve(url, os.path.join(destination_path, file_name))

print("Data_downloaded!")
