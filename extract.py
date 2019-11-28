#!/usr/bin/python
# -*- coding: <encoding name> -*-
import pandas as pd
import re
import numpy as np
import tarfile
from sqlalchemy import create_engine
import glob
from urllib.request import urlretrieve
from geopy.geocoders import GoogleV3
from geopy.exc import GeocoderTimedOut
from etl_util import EtlUtil
from time import sleep
from progress.bar import PixelBar
from etl_loader import LoaderDB

# xtract
# Assign url of file: url
url = 'https://s3.amazonaws.com/dev.etl.python/datasets/data_points.tar.gz'

# Save file locally
urlretrieve(url, 'data_points.tar.gz')

localization = {"Latitude": [], "Longitude": [],"Rua": [], "Numero": [], "Bairro": [], "Cidade": [], "Cep": [], "Estado": [], "Pais": []}

tar_file = tarfile.open('data_points.tar.gz', mode='r:gz')
tar_file.extractall("data_etl")

# transform - clean data
files = [f for f in glob.glob("data_etl" + "**/*.txt", recursive=True)]
for filename in files:
    with open(filename, 'r', encoding='utf-8') as f:
        lines = f.readlines()

        for idx, item in enumerate(lines):
            # for Latitude
            latitude_field = re.search("Latitude: .*", item)
            if ((idx+1)<len(lines)):
                longitude_field_nx = re.search("Longitude: .*", lines[idx+1])
            else:
                longitude_field_nx = None

            if latitude_field is not None and longitude_field_nx is not None:
                localization["Latitude"].append(
                    re.search('\-\d.*|\+\d.*', latitude_field.group()).group())
            # for Longitude
            longitude_field = re.search("Longitude: .*", item)

            if longitude_field is not None:
                latitude_field = re.search("Latitude: .*", lines[idx-1])
                if latitude_field is not None:
                    localization["Longitude"].append(
                        re.search('\-\d.*|\+\d.*', longitude_field.group()).group())
                    #localization["Latitude"].append(np.nan)
# loading
gmaps = GoogleV3(api_key='AIzaSyA45cYSHCf_AVjUsIYz5t3WwBDGmU4FBdk')
etl_parser = EtlUtil()

def reverse_geocode(position):
    try:
        return gmaps.reverse(position)
    except GeocoderTimedOut:
        return reverse_geocode(position)


bar = PixelBar("Loading...", max=len(localization['Latitude']))
for index, row in enumerate(localization['Latitude']):
    g = reverse_geocode([row, localization['Longitude'][index]])

    rua = etl_parser.get_component(g, 'route')
    number = etl_parser.get_component(g, 'street_number')
    bairro = etl_parser.get_component(g, 'sublocality')
    cidade = etl_parser.get_component(g, 'administrative_area_level_2')
    cep = etl_parser.get_component(g, 'postal_code')
    estado = etl_parser.get_component(g, 'administrative_area_level_1')
    pais = etl_parser.get_component(g, 'country')

    if(rua == None) or (number == None)\
        or (bairro == None) or (cidade == None)\
            or (cep == None) or (estado == None)\
                or (pais == None):
        localization['Rua'].append(np.nan)
        localization['Numero'].append(np.nan)
        localization['Bairro'].append(np.nan)
        localization['Cidade'].append(np.nan)
        localization['Cep'].append(np.nan)
        localization['Estado'].append(np.nan)
        localization['Pais'].append(np.nan)                 
    else:
        localization['Rua'].append(rua)
        localization['Numero'].append(number)
        localization['Bairro'].append(bairro)
        localization['Cidade'].append(cidade)
        localization['Cep'].append(cep)
        localization['Estado'].append(estado)
        localization['Pais'].append(pais)
             

    bar.next()
    #df.set_value(index, 'Street Address', g.address)

df = pd.DataFrame(localization)
df.dropna(inplace=True)
#df.drop(columns=['Distance', 'Bearing'])
print(df)
# load into database
loader_db = LoaderDB()
loader_db.load_etl_data(df)
