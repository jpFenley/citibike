import os
import urllib.request
import time
import pandas as pd
import zipfile

data_path = '/data'

curr_dir = os.getcwd()
if not os.path.isdir(os.path.join(os.getcwd() + data_path)):
    os.mkdir(os.path.join(curr_dir + data_path))

os.chdir(os.path.join(curr_dir + data_path))
print(os.getcwd())

zip_dates = pd.date_range('2013-06', '2017-01', freq='M').strftime('%Y%m')
csv_dates = pd.date_range('2017-01', '2021-09', freq='M').strftime('%Y%m')

Change to .csv.zip fo4 >= 2017
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

for item in os.listdir(os.getcwd()):
    if item.endswith('.zip'):
        file_name = os.path.abspath(item)  # get full path of files
        zip_ref = zipfile.ZipFile(file_name)  # create zipfile object
        zip_ref.extractall(os.getcwd())  # extract file to dir
        zip_ref.close()  # close file
        os.remove(file_name)  # delete zipped file

old_files = []
new_files = []

for file in os.listdir():
    if file.startswith(('2013', '2014', '2015', '2016')):
        old_files.append(file)
    elif file.startswith(('2017', '2018', '2019', '2020', '2021')):
        new_files.append(file)

CHUNK_SIZE = 50000
def merge_files(files, output):
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


if 'merged2013-2016.csv' not in os.listdir():
    merge_files(old_files, './merged2013-2016.csv')
if 'merged2017-2020.csv' not in os.listdir():
    merge_files(new_files, './merged2017-2021.csv')

# for file in old_files + new_files:
#     os.remove(file)
