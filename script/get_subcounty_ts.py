# %%
import rasterio
from rasterio import plot
from rasterio import mask
import numpy as np
import geopandas as gpd
import json
import multiprocessing
import warnings
import pandas as pd
from tqdm.auto import tqdm
import gc
import h5py as h5
from rasterio import RasterioIOError
from get_VCI import *
warnings.filterwarnings("ignore")

# getting shapefile
shapefile_data1=gpd.read_file('../data/shapefile/africa_processed/africa_level1.shp')
shapefile_data2=gpd.read_file('../data/shapefile/africa_processed/africa_level2.shp')
shapefile_data1_filtered=shapefile_data1[~shapefile_data1[['NAME_0','NAME_1']].fillna('nan').agg('-'.join,axis=1).isin(set(shapefile_data2[['NAME_0','NAME_1']].fillna('nan').agg('-'.join,axis=1)))].reset_index(drop=True)
datasets = [slash.replace(' ', '_').replace('/','|') for slash in
                        shapefile_data1_filtered[['NAME_0','NAME_1']].fillna('nan').agg('-'.join, axis=1)]
shapefile_data1_filtered['joined_name']=datasets
datasets = [slash.replace(' ', '_').replace('/','|') for slash in
                        shapefile_data2[['NAME_0', 'NAME_1', 'NAME_2']].fillna('nan').agg('-'.join, axis=1)]
shapefile_data2['joined_name']=datasets
shapefile_data=pd.concat([shapefile_data1_filtered,shapefile_data2])
shapefile_data.reset_index(drop=True,inplace=True)

calendar=filenames_a4_df.query('date>="2001000" and weekday=="6"')[['date']].rename({'date':'Date'},axis=1).astype(int)

class subcounty_ts_aggregator(multiprocessor):
    """
    aggregate data into series for each subcounty, filter is the pixels' MCD12Q1 values to remove (i.e. if filter=1, all pixel with MCD12Q1 data = 1 will be removed)
    """
    def __init__(self,task_ids,num_cpu,filter=[]):
        super(NDVI_calculator,self).__init__(task_ids,num_cpu)
        self.filter=filter
        self.filename=''
        self.subcounty_ts = self.manager.dict()

    def process(self,index):
        # getting data
        VCI=rasterio.open(VCI_dir+self.filename)
        NDVI=rasterio.open(NDVI_dir+self.filename)

        # # filter pixels based on MCD12Q1 data
        # year=self.filename[:4]
        # landcover=rasterio.open(processeddata_dir_a4+'pixels_to_remove.tif')
        # has_landcover=0

        # get geometry for masking
        subcounty_name=shapefile_data.loc[index,'joined_name']
        geometry_dict=json.loads(shapefile_data.iloc[index:index+1].to_json())
        geometry=[feature["geometry"] for feature in geometry_dict["features"]]

        try:
            masked_VCI,_=mask.mask(VCI,geometry,crop=True)
            masked_NDVI,_=mask.mask(NDVI,geometry,crop=True)
            # masked_landcover,_=mask.mask(landcover,geometry,crop=True)
        except ValueError:
            self.completed[index] = 1
            return

        # if has_landcover==1:
        #     landcover_cond=np.isin(masked_landcover,self.filter)
        # else:
        #     landcover_cond=masked_landcover==1

        # if len(self.filter)==0:
        #     masked_VCI = np.where((masked_VCI==VCI.nodata) | (landcover_cond),np.nan,masked_VCI)
        #     masked_NDVI = np.where((masked_NDVI==NDVI.nodata) | (landcover_cond),np.nan,masked_NDVI)
        # else:
        #     masked_VCI = np.where((masked_VCI==VCI.nodata),np.nan,masked_VCI)
        #     masked_NDVI = np.where((masked_NDVI==NDVI.nodata),np.nan,masked_NDVI)
        
        masked_VCI = np.where((masked_VCI==VCI.nodata),np.nan,masked_VCI)
        masked_NDVI = np.where((masked_NDVI==NDVI.nodata),np.nan,masked_NDVI)

        # write results
        self.subcounty_ts[subcounty_name]=self.subcounty_ts.get(subcounty_name,manager.dict())
        self.subcounty_ts[subcounty_name]['Date']=self.subcounty_ts[subcounty_name].get('Date',[]) + [int(self.filename.replace('.tif',''))]
        self.subcounty_ts[subcounty_name]['NDVI']=self.subcounty_ts[subcounty_name].get('NDVI',[]) + [float(np.nanmean(masked_NDVI[0]))]
        self.subcounty_ts[subcounty_name]['VCI']=self.subcounty_ts[subcounty_name].get('VCI',[]) + [float(np.nanmean(masked_VCI[0]))]
        
        # cleaning up
        VCI.close()
        NDVI.close()
        del masked_VCI, masked_NDVI
        gc.collect()

        self.completed[index] = 1

    def aggregate_all(self):
        for filename in tqdm(filenames):
            self.filename=filename
            self.do_task()
            for i in range(shapefile_data.shape[0]):
                self.queue.put(i)

ts_aggregator=NDVI_calculator(filenames_a4_df,task_ids,multiprocessing.cpu_count()-2)

if __name__=='__main__':
    ts_aggregator.aggregate_all()
    db_path='../data/processed/Database/'
    if 'FinalSubCountyVCI.h5' not in os.listdir(db_path):
        storage_file = h5.File((db_path+'FinalSubCountyVCI.h5'), 'w') ## create database if not available
        append=False
    else:
        storage_file = h5.File((db_path+'FinalSubCountyVCI.h5'), 'a') ## connect otherwise
        append=True

    subcounties=ts_aggregator.subcounty_ts.keys()
    for subcounty in tqdm(subcounties):
        data=pd.DataFrame.from_dict(dict(ts_aggregator.subcounty_ts[subcounty])).merge(calendar,how='right')
        data=data[['Date','NDVI','VCI']].sort_values('Date')
        data['VCI']=np.where(data.NDVI.isna(),np.nan,data.VCI)
        VCI3M=data.VCI.rolling(12,1).mean()
        VCI3M.iloc[:11]=np.nan
        data['VCI3M']=VCI3M
        subcounty=subcounty.replace('/','|')
        if append is False:
            storage_file.create_dataset(
                subcounty,
                data=data.values,
                compression='lzf',
                maxshape=(None,None))
            storage_file[subcounty].attrs['Column_Names'] = np.string_(['Date','NDVI','VCI','VCI3M'])
        elif append is True:
            the_data=storage_file[subcounty]
            the_data.resize(len(the_data)+data.shape[0],axis=0)
            the_data[len(the_data):,:]=data.values