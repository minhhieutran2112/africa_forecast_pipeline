# %%
import rasterio
import numpy as np
import os
import warnings
from tqdm.auto import tqdm
from datetime import datetime, timedelta
from rasterio import RasterioIOError
from get_NDVI import *
warnings.filterwarnings("ignore")

## aggregating
print('Start getting merged data')

def write_tif(data,path):
    with rasterio.Env():
        with rasterio.open(path, 'w', **profile) as dst:
            dst.write(data.astype(rasterio.float32), 1)

def get_merged_NDVI(renormalise=False):
    cnt=0
    for i,row in tqdm(filenames_a4_df.iterrows(),total=filenames_a4_df.shape[0]):
        if f"{row['date']}.tif" in os.listdir(processeddata_dir_a4+'NDVI_merged/') or row['weekday']!='6':
            continue
        lower=datetime.strftime(datetime.strptime(row['date'],'%Y%j')-timedelta(days=6),'%Y%j')
        upper=row['date']
        merged_NDVI=[]
        for filename in filenames_a4_df.query('date>=@lower and date<=@upper').dropna().loc[:,'date']:
            tmp_dataset=rasterio.open(processeddata_dir_a4+f"NDVI/{filename}.tif")
            merged_NDVI.append(tmp_dataset.read(1))
        if len(merged_NDVI)==0:
            continue
        merged_NDVI=np.nanmean(merged_NDVI,axis=0)
        write_tif(merged_NDVI,processeddata_dir_a4+f"NDVI_merged/{row['date']}.tif")
        
        if renormalise:
            if cnt==0:
                if 'min_NDVI.tif' not in os.listdir(processeddata_dir_a4):
                    min_NDVI=merged_NDVI.copy()
                else:
                    min_NDVI_ds=rasterio.open(processeddata_dir_a4+'min_NDVI.tif')
                    min_NDVI=min_NDVI_ds.read(1)
                    min_NDVI[(min_NDVI==min_NDVI_ds.nodata) & (min_NDVI==-9999)]=np.nan
                    min_NDVI_ds.close()
                if 'max_NDVI.tif' not in os.listdir(processeddata_dir_a4):
                    max_NDVI=merged_NDVI.copy()
                else:
                    max_NDVI_ds=rasterio.open(processeddata_dir_a4+'max_NDVI.tif')
                    max_NDVI=max_NDVI_ds.read(1)
                    max_NDVI[(max_NDVI==max_NDVI_ds.nodata) & (max_NDVI==-9999)]=np.nan
                    max_NDVI_ds.close()
                cnt+=1
            else:
                min_NDVI=np.fmin(min_NDVI,merged_NDVI)
                max_NDVI=np.fmax(max_NDVI,merged_NDVI)

    write_tif(max_NDVI,processeddata_dir_a4+'max_NDVI.tif')
    write_tif(min_NDVI,processeddata_dir_a4+'min_NDVI.tif')

if __name__=="__main__":
    print('Start obtaining merged NDVI')
    get_merged_NDVI()
    print('Done obtaining merged NDVI')