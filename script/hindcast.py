# %%
import rasterio
from rasterio import plot
from rasterio import mask
import numpy as np
import os
import geopandas as gpd
import json
import multiprocessing
import warnings
import pandas as pd
from tqdm.auto import tqdm
from datetime import datetime, timedelta
import re
import gc
from queue import Empty
import time
import h5py as h5
import glob
from rasterio import RasterioIOError
import sys
from tqdm.auto import tqdm
import GaussianProcesses_12w as GaussianProcesses
sys.path.append('/research/geog/data1/mt601/script/forecast_pipeline/')
warnings.filterwarnings("ignore")

db_path='../data/processed/Database/'
hdf_file = h5.File((db_path+'FinalSubCountyVCI.h5'),'a')

def hindcast(hdf_file):
    dataset_names = list(hdf_file.keys())
    data_length = int(len(hdf_file[dataset_names[0]][:,0]))
    halfway_point = int(data_length/2)

    for dataset_no,dataset in enumerate(tqdm(dataset_names)):
        the_data = hdf_file[dataset]
        hindcast_results = np.full((data_length-halfway_point+12,12),0.0)
        for run_counter,hindcast_counter in enumerate(tqdm(range(halfway_point,data_length),leave=False)):
            try:
                dataset_array = np.array(the_data,dtype=float)[:hindcast_counter]

                dates = [datetime(int(str(date)[:4]),1,1) +
                            timedelta(int(str(date)[4:7])-1) for
                            date in dataset_array[:,0]]


                days = np.array([(date-dates[0]).days for date in dates])
                dates = np.array(dates)
                nan_mask = np.isnan(dataset_array[:,2])

                VCI = dataset_array[:,2][~nan_mask]

                days = days[~nan_mask]

                dates = dates[~nan_mask]
                predicted_days,predicted_VCI3M = GaussianProcesses.forecast(days,VCI)

                final_VCI3M =np.empty((len(predicted_VCI3M)))

                final_VCI3M[:12] = np.nan

                for i in range(13-1,len(predicted_VCI3M)):

                    final_VCI3M[i] = \
                        np.nanmean(predicted_VCI3M[i-12:i])
            except:
                final_VCI3M[:]=np.nan

            for save_counter in range(0,12):
                hindcast_results[run_counter+save_counter,save_counter] = \
                    final_VCI3M[-12+save_counter]


        the_data.resize(len(the_data)+12,axis=0)
        the_data.resize(16,axis=1)
        the_data.attrs['Column_Names'] = ['Date',
                                          'NDVI','VCI',
                                          'VCI3M','0 lag time',
                                          '7 day lag time',
                                          '14 day lag time','21 day lag time',
                                          '28 day lag time','35 day lag time',
                                          '42 day lag time','49 day lag time',
                                          '56 day lag time','63 day lag time',
                                          '70 day lag time','77 day lag time']

        the_data[halfway_point:,4:] = hindcast_results
        print('Dataset {} is done'.format(dataset))
    return None

if __name__=='__main__':
    hindcast(hdf_file)