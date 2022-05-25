# -*- coding: utf-8 -*-
"""
This module houses the script to download the data from appeears
"""

# %%
import os
import cgi
import requests
from multiprocess import multiprocessor
import pandas as pd
import multiprocessing
import os

rawdata_dir_a4 = "../data/MCD43A4/raw/"
task_done_path = "../data/task_done.csv"
file_done_path = "../data/file_done.csv"
task_done_df=pd.read_csv(task_done_path)
file_done_df=pd.read_csv(file_done_path)
task_done=task_done_df.iloc[:,0].tolist()
file_done=file_done_df.iloc[:,0].tolist()

response = requests.post('https://appeears.earthdatacloud.nasa.gov/api/login', auth=(os.environ['app_username'], os.environ['app_password']))
token_response = response.json()
token = token_response['token']
response = requests.get(
    'https://appeears.earthdatacloud.nasa.gov/api/task', 
    headers={'Authorization': 'Bearer {0}'.format(token)})
task_response = response.json()
task_ids=[res['task_id'] for res in task_response if res['task_name']=='kenya_mod09gq' and res['task_id'] not in task_done and res['status']=='done']
file_ids=[]
for task_id in task_ids:
    response=requests.get(f'https://appeears.earthdatacloud.nasa.gov/api/bundle/{task_id}').json()['files']
    for file in response:
        file_ids.append(f"{task_id}/{file['file_id']}")

class downloader(multiprocessor):
    def process(self,index):
        if index not in file_done:
            # get a stream to the bundle file
            response = requests.get(
                f'https://appeears.earthdatacloud.nasa.gov/api/bundle/{index}',
                stream=True)

            # parse the filename from the Content-Disposition header
            content_disposition = cgi.parse_header(response.headers['Content-Disposition'])[1]
            filename = os.path.basename(content_disposition['filename'])

            # create a destination directory to store the file in
            filepath = os.path.join(rawdata_dir_a4, filename)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            # write the file to the destination directory
            if filename not in os.listdir(os.path.dirname(filepath)):
                with open(filepath, 'wb') as f:
                    for data in response.iter_content(chunk_size=8192):
                        f.write(data)
            pd.DataFrame([index],columns=['task_id']).to_csv(file_done_path,mode='a',header=False,index=False)
        self.completed[index]=1

data_downloader=downloader(file_ids,multiprocessing.cpu_count()-2)

# %%
if __name__ == '__main__':
    data_downloader.do_task()
    pd.DataFrame(task_ids,columns=['task_id']).to_csv(task_done_path,mode='a',header=False,index=False)