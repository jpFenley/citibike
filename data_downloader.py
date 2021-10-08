"""
IMPORTANT: READ

What this file does:
1. Checks if a data folder exists. If it doesnt moves to it.
2. Downloads citibike data from website. Data previous to 2017 and after
are in different formats so they are downloaded using different links.
3. Each downloaded file is a zip file, which can not be read directly.
Each zip file is extracted, and the zip file is removed.
4. Program figures out what all the downloaded csv are. Of these, figure out
which files belong to years before COVID.
5. These files are merged into a single file. As 2013 and 2017 data are different
some changes are made to the data. Additionally, since the headers used for 
different years are different, the headers must be changed so that they are
able to be combined.
6. (Optional) Remove all the smaller csv files.

"""
import os
import urllib.request
import time
import pandas as pd
import zipfile
import csv


def set_path():
    '''
    Creates a data folder if does not already exist and changes current 
    working directory to this folder.
    '''
    data_path = '/data'

    curr_dir = os.getcwd()
    if not os.path.isdir(os.path.join(os.getcwd() + data_path)):
        os.mkdir(os.path.join(curr_dir + data_path))

    os.chdir(os.path.join(curr_dir + data_path))
    print(os.getcwd())


def download_data():
    '''
    Downloads data from Citibike website. Data before and after 2017 are stored
    using different named files. Need two loops to access each data range.
    '''
    zip_dates = pd.date_range('2013-06', '2017-01', freq='M').strftime('%Y%m')
    csv_dates = pd.date_range('2017-01', '2020-01', freq='M').strftime('%Y%m')


    for period in zip_dates:
        link = f"https://s3.amazonaws.com/tripdata/{period}-citibike-tripdata.zip"
        print(link)
        try:
            urllib.request.urlretrieve(link, f'{period}.zip')
            print(f'Downloading {period}')
        except:
            print(f'Error Downloading {period}')
            pass
        finally:
            time.sleep(5)

    for period in csv_dates:
        link = f"https://s3.amazonaws.com/tripdata/{period}-citibike-tripdata.csv.zip"
        print(link)
        try:
            urllib.request.urlretrieve(link, f'{period}.zip')
            print(f'Downloading {period}')
        except:
            print(f'Error Downloading {period}')
            pass
        finally:
            time.sleep(5)


def unzip_items():
    """
    Downloaded data is in zip files. Need to extract each zip file. Removes zip
    files after they are extracted.
    """
    for item in os.listdir(os.getcwd()):
        if item.endswith('.zip'):
            file_name = os.path.abspath(item)  # get full path of files
            zip_ref = zipfile.ZipFile(file_name)  # create zipfile object
            zip_ref.extractall(os.getcwd())  # extract file to dir
            zip_ref.close()  # close file
            os.remove(file_name)  # delete zipped file


def get_files():
    """
    Get the name of all the old files (before 2017) and the new files
    (2017 and onward). Return a list of the file names for each.
    """
    old_files = []
    new_files = []

    for file in os.listdir():
        if file.startswith(('2013', '2014', '2015', '2016')):
            old_files.append(file)
        elif file.startswith(('2017', '2018', '2019', '2020', '2021')):
            new_files.append(file)
    return old_files, new_files


CHUNK_SIZE = 50000
def merge_files(files, output):
    """
    Merges a group of files into an output file. Files must have same columns.
    """
    first_one = True
    for csv_file_name in files:
        print(f'Merging {csv_file_name}')
        if not first_one: # if it is not the first csv file then skip the header row (row 0) of that file
            skip_row = [0]
        else:
            skip_row = []
        chunk_container = pd.read_csv(csv_file_name, chunksize=CHUNK_SIZE, skiprows = skip_row)
        for chunk in chunk_container:
            chunk.to_csv(output, mode="a", index=False)
        first_one = False


def all_merge(inputs, dest_file):
    """
    Merges all files even if they have different columns.
    """

    # Get all possible header values. Use col-trans to replace similarly
    # named columns
    fieldnames = []
    for filename in inputs:
        with open(filename, "r", newline="") as f_in:
            reader = csv.reader(f_in)
            headers = next(reader)
            for h in headers:
                if h not in col_trans:
                    if h not in fieldnames:
                        fieldnames.append(h)
                else:
                    if col_trans[h] not in fieldnames:
                        fieldnames.append(col_trans[h])
    print(fieldnames)

    # merges data
    with open(dest_file, "w", newline="") as f_out:   # Comment 2 below
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        for filename in inputs:
            print(f'Joining {filename}')
            with open(filename, "r", newline="") as f_in:
                reader = csv.DictReader(f_in)  # Uses the field names in this file
                for line in reader:
                    for h in line.copy():
                        if h in col_trans:
                            line[col_trans[h]] = line.pop(h)
                    if 'Birth Year' in line:
                        if not line['Birth Year'].isdigit():
                            line['Birth Year'] == ''
                    if 'Start Station ID' in line:
                        if not line['Birth Year'].isdigit():
                            line['Birth Year'] == -1
                    if 'End Station ID' in line:
                        if not line['Birth Year'].isdigit():
                            line['Birth Year'] == -1
                    writer.writerow(line)

col_trans = {'tripduration': 'Trip Duration',
            'starttime': 'Start Time',
            'stoptime': 'Stop Time',
            'start station id': 'Start Station ID',
            'start station name': 'Start Station Name',
            'start station latitude': 'Start Station Latitude',
            'start station longitude': 'Start Station Longitude',
            'end station id': 'End Station ID',
            'end station name': 'End Station Name',
            'end station latitude': 'End Station Latitude',
            'end station longitude': 'End Station Longitude',
            'bikeid': 'Bike ID',
            'usertype': 'User Type',
            'birth year': 'Birth Year',
            'gender': 'Gender',
            'started_at': 'Start Time',
            'ended_at': 'Stop Time',
            'start_station_name': 'Start Station Name',
            'start_station_id': 'Start Station ID',
            'end_station_name': 'End Station Name',
            'end_station_id': 'End Station ID',
            'start_lat':'Start Station Latitude',
            'start_lng':'Start Station Longitude',
            'end_lat':'End Station Latitude',
            'end_lng':'End Station Longitude'}


def remove_files(files):
    """
    Removes a list of files. Can be used to remove the csv files after
    combining them.
    """
    for file in files:
        os.remove(file)


set_path() # Set data directory
# download_data() # Download data: only run this once. Otherwise it will download each time.
# unzip_items() # Unzip downloaded zip files
old_files, new_files = get_files() # Get list of files downloaded
pre_covid_files = [] 
# Create list of dates before COVID
for file in old_files + new_files:
    if not file.startswith('2020') and not file.startswith('2021'):
        pre_covid_files.append(file)

all_merge(pre_covid_files, 'merge_pre_covid.csv')# Create merged file

#remove_files(old_files + new_files) # Remove files after merging (optional)

