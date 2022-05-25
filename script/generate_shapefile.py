"""
generate shapefile. If admin level 2 boundary is not available for a country -> roll back to level 1
"""

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
from get_NDVI import dataset
sys.path.append('/research/geog/data1/mt601/script/forecast_pipeline/')
warnings.filterwarnings("ignore")

shapefile_dir = "../data/shapefile/gadm36_levels/"

# level 2
shapefile_data = gpd.read_file(shapefile_dir+'gadm36_2.shp')
datasets = [slash.replace(' ', '_').replace('/','|') for slash in
                        shapefile_data[['NAME_0', 'NAME_1', 'NAME_2']].fillna('nan').agg('-'.join, axis=1)]
shapefile_data['joined_name']=datasets
level2_african_counties=[]
for geometry_index in tqdm(range(shapefile_data.shape[0])):
    try:
        geometry_dict=json.loads(shapefile_data.iloc[geometry_index:geometry_index+1].to_json())
        geometry=[feature["geometry"] for feature in geometry_dict["features"]]
        test=mask.mask(dataset,geometry,crop=True)
        level2_african_counties.append(shapefile_data.iloc[geometry_index,:]['joined_name'])
    except ValueError:
        continue
shapefile_data[shapefile_data.joined_name.isin(level2_african_counties)].to_file('../data/shapefile/africa_processed/africa_level2.shp',encoding='utf-8')

# level 1
shapefile_data = gpd.read_file(shapefile_dir+'gadm36_1.shp')
datasets = [slash.replace(' ', '_').replace('/','|') for slash in
                        shapefile_data[['NAME_0', 'NAME_1']].fillna('nan').agg('-'.join, axis=1)]
shapefile_data['joined_name']=datasets
level1_african_counties=[]
for geometry_index in tqdm(range(shapefile_data.shape[0])):
    try:
        geometry_dict=json.loads(shapefile_data.iloc[geometry_index:geometry_index+1].to_json())
        geometry=[feature["geometry"] for feature in geometry_dict["features"]]
        test=mask.mask(dataset,geometry,crop=True)
        level1_african_counties.append(shapefile_data.iloc[geometry_index,:]['joined_name'])
    except ValueError:
        continue
shapefile_data[shapefile_data.joined_name.isin(level1_african_counties)].to_file('../data/shapefile/africa_processed/africa_level1.shp',encoding='utf-8')