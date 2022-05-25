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
from rasterio import RasterioIOError
from merge_NDVI import *
warnings.filterwarnings("ignore")

## aggregating
NDVI_dir = processeddata_dir_a4+"NDVI_merged/"
VCI_dir = processeddata_dir_a4+"VCI/"
filenames=[filename for filename in os.listdir(NDVI_dir)]

# max and min NDVI
max_NDVI_ds=rasterio.open(processeddata_dir_a4+'max_NDVI.tif')
min_NDVI_ds=rasterio.open(processeddata_dir_a4+'min_NDVI.tif')
max_NDVI=max_NDVI_ds.read(1)
min_NDVI=min_NDVI_ds.read(1)
min_NDVI[(min_NDVI==min_NDVI_ds.nodata) & (min_NDVI==-9999)]=np.nan
max_NDVI[(max_NDVI==max_NDVI_ds.nodata) & (max_NDVI==-9999)]=np.nan
max_NDVI_ds.close()
min_NDVI_ds.close()
range_NDVI=max_NDVI-min_NDVI
range_NDVI=np.where(range_NDVI==0,np.nan,range_NDVI)

def main():
    for i,filename in enumerate(tqdm(filenames)):
        if filename not in os.listdir(VCI_dir):
            dataset=rasterio.open(NDVI_dir+filename)
            NDVI=dataset.read(1)
            VCI=(NDVI-min_NDVI) / range_NDVI * 100
            write_tif(VCI,VCI_dir+filename)

if __name__=='__main__':
    print('Start geting VCI from merged NDVI')
    main()
    print('Done getting VCI from merged NDVI')