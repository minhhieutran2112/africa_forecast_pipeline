# %%
import rasterio
import numpy as np
import os
import geopandas as gpd
import warnings
import pandas as pd
from datetime import datetime, timedelta
import re
import gc
from rasterio import RasterioIOError
import multiprocessing
from multiprocess import multiprocessor
warnings.filterwarnings("ignore")

rawdata_dir_a4 = "../data/MCD43A4/raw/"
processeddata_dir_a4 = "../data/MCD43A4/processed/"

filenames_a4=[filename for filename in os.listdir(rawdata_dir_a4) if filename.endswith('.tif')]
date_a4=[re.findall(r'doy\d+',filename)[0] for filename in filenames_a4]
filenames_a4_df=pd.DataFrame([
    (i.replace('doy',''),
    f'MCD43A4.006_BRDF_Albedo_Band_Mandatory_Quality_Band1_{i}_aid0001.tif',
    f'MCD43A4.006_BRDF_Albedo_Band_Mandatory_Quality_Band2_{i}_aid0001.tif',
    f'MCD43A4.006_Nadir_Reflectance_Band1_{i}_aid0001.tif',
    f'MCD43A4.006_Nadir_Reflectance_Band2_{i}_aid0001.tif') for i in date_a4
], columns=['date','qa1','qa2','band1','band2'])

# get all days (including days that don't have data)
full_days_df=pd.DataFrame(
    [datetime.strftime(datetime.strptime(filenames_a4_df.date.min(),'%Y%j')+timedelta(i),'%Y%j') for i in range((datetime.strptime(filenames_a4_df.date.max(),'%Y%j')-datetime.strptime(filenames_a4_df.date.min(),'%Y%j')).days+1)],
    columns=['date']
)

filenames_a4_df.drop_duplicates(inplace=True)
filenames_a4_df=pd.merge(filenames_a4_df,full_days_df,how='outer')
filenames_a4_df=filenames_a4_df.sort_values('date').reset_index(drop=True)
filenames_a4_df['weekday']=filenames_a4_df.date.apply(lambda x:datetime.strftime(datetime.strptime(x,'%Y%j'),'%w'))

# get profile for later writing
dataset=rasterio.open(rawdata_dir_a4+filenames_a4_df.band1.iloc[-1])
profile=dataset.profile
profile.update(
    dtype=rasterio.float32,
    count=1,
    nodata=-9999,
    compress='lzw'
)

# get task ids
task_ids=[]
for i in range(filenames_a4_df.shape[0]):
    if f"{filenames_a4_df.loc[i,'date']}.tif" not in os.listdir(processeddata_dir_a4+'NDVI/') and filenames_a4_df.loc[i,:].isna().sum() == 0:
        task_ids.append(i)

class NDVI_calculator(multiprocessor):
    def __init__(self,files_df,task_ids,num_cpu):
        super(NDVI_calculator,self).__init__(task_ids,num_cpu)
        self.filenames_a4_df=files_df


    def read_a4_file(self,filename,fill_value):
        dataset=rasterio.open(filename)
        data=dataset.read(1)
        data=np.where(data==fill_value,np.nan,data)
        dataset.close()
        return data

    def get_ndvi(self,row):
        band2=self.read_a4_file(rawdata_dir_a4+row['band2'],32767)
        band1=self.read_a4_file(rawdata_dir_a4+row['band1'],32767)
        qa2=self.read_a4_file(rawdata_dir_a4+row['qa2'],255)
        qa1=self.read_a4_file(rawdata_dir_a4+row['qa1'],255)
        band2=np.where(qa2>1,np.nan,band2)
        band1=np.where(qa1>1,np.nan,band1)
        NDVI=(band2 - band1) / (band2 + band1)
        final_NDVI=np.where((NDVI <= 0) | (NDVI >= 1),np.nan,NDVI)
        return final_NDVI

    def write_tif(self,data,path):
        with rasterio.Env():
            with rasterio.open(path, 'w', **profile) as dst:
                dst.write(data.astype(rasterio.float32), 1)
    
    def process(self,index):
        row=self.filenames_a4_df.iloc[index,:]
        if f"{row['date']}.tif" not in os.listdir(processeddata_dir_a4+'NDVI/') and row.isna().sum() == 0:
            try:
                NDVI=self.get_ndvi(row)
                self.write_tif(NDVI,processeddata_dir_a4+f"NDVI/{row['date']}.tif")
                del NDVI
            except RasterioIOError:
                print(f"{row['date']}.tif")
        gc.collect()
        self.completed[index] = 1

NDVI_calculate=NDVI_calculator(filenames_a4_df,task_ids,multiprocessing.cpu_count()-2)

if __name__ == '__main__':
    print('Start calculating NDVI')
    NDVI_calculate.do_task()
    print('Done')