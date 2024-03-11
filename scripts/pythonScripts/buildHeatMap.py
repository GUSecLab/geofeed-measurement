#!/usr/bin/python3

import pandas as pd, numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import json,ipaddress
import pycountry
import country_converter as coco
from netaddr import IPSet as netaddr_IPSet, IPRange as netaddr_IPRange
from datetime import datetime, timedelta
from matplotlib import colors as mpl_colors

import pdb



def plot_geo_heatmap(dataFile,columns,mapCol,labelVal,savePath):
    dataDF = pd.read_csv(dataFile, index_col=0,usecols=columns)

    #init countryConverter and read in shapes file
    cc = coco.CountryConverter()
    SHAPEFILE = 'Data/raw_data/ne_10m_admin_0_countries/ne_10m_admin_0_countries.shp'
    geoDF = gpd.read_file(SHAPEFILE)[['ADMIN', 'ADM0_A3', 'geometry']]
    geoDF.columns = ['country', 'country_code', 'geometry']
    geoDF = geoDF.where(geoDF.loc[:,'country'].apply(lambda x: x != 'Antarctica')).dropna(how='all')

    #convert to iso2 country codes for easier matching and swap out coountry code col
    gdfIso2Codes = cc.pandas_convert(series=geoDF.loc[:,'country_code'], to='ISO2', not_found=np.nan)
    geoDF.drop(columns='country_code',inplace=True)
    geoDF.insert(2,'country_iso2',gdfIso2Codes)
    geoDF = geoDF.where(geoDF.loc[:,'country_iso2'].notna()).dropna(how='all')
    mergedDF = pd.merge(left=geoDF, how='left',right=dataDF, left_on='country_iso2',right_on='country_iso_code')
    mergedDF.drop(columns=['country'],inplace=True)

    #init graph
    fig, ax = plt.subplots(1, figsize=(20, 8))
    ax.axis('off')
    mergedDF.plot(column=mapCol, ax=ax, edgecolor='0.8',linewidth=1,cmap='magma')

    #add colorbar legend
    sm = plt.cm.ScalarMappable(norm=plt.Normalize(vmin=dataDF.loc[:,mapCol].min(), vmax=dataDF.loc[:,mapCol].max()), cmap='magma')
    cbaxes = fig.add_axes([0.15, 0.25, 0.01, 0.4])
    fig.colorbar(sm,label=labelVal,cax=cbaxes,location='left')#,shrink=0.75)
    fig.savefig(savePath, transparent=False, format='pdf', bbox_inches="tight")




    #Similar but use log scale to determine coloring
def plot_alt_geo_heatmap(dataFile,columns,mapCol,labelVal,savePath):
    dataDF = pd.read_csv(dataFile, index_col=0,usecols=columns)

    #init countryConverter and read in shapes file
    cc = coco.CountryConverter()
    SHAPEFILE = 'Data/raw_data/ne_10m_admin_0_countries/ne_10m_admin_0_countries.shp'
    geoDF = gpd.read_file(SHAPEFILE)[['ADMIN', 'ADM0_A3', 'geometry']]
    geoDF.columns = ['country', 'country_code', 'geometry']
    geoDF = geoDF.where(geoDF.loc[:,'country'].apply(lambda x: x != 'Antarctica')).dropna(how='all')

    #convert to iso2 country codes for easier matching and swap out coountry code col
    gdfIso2Codes = cc.pandas_convert(series=geoDF.loc[:,'country'], to='ISO2', not_found=np.nan)
    geoDF.drop(columns='country_code',inplace=True)
    geoDF.insert(2,'country_iso2',gdfIso2Codes)
    geoDF = geoDF.where(geoDF.loc[:,'country_iso2'].notna()).dropna(how='all')
    mergedDF = pd.merge(left=geoDF, how='left',right=dataDF, left_on='country_iso2',right_on='country_iso_code')
    mergedDF.drop(columns=['country'],inplace=True)

    #init graph
    fig, ax = plt.subplots(1, figsize=(20, 8))
    ax.axis('off')
    mergedDF.plot(column=mapCol, ax=ax, edgecolor='0.8',linewidth=1,norm=mpl_colors.LogNorm(vmin=mergedDF.loc[:,mapCol].min(), vmax=mergedDF.loc[:,mapCol].max()), cmap='magma')
    pdb.set_trace()
    #add colorbar legend
    sm = plt.cm.ScalarMappable(norm=mpl_colors.LogNorm(vmin=dataDF.loc[:,mapCol].min(), vmax=dataDF.loc[:,mapCol].max()), cmap='magma')
    cbaxes = fig.add_axes([0.15, 0.25, 0.01, 0.4])
    cbar = fig.colorbar(sm,cax=cbaxes,label=labelVal)#,location='left',shrink=0.75)
    fig.savefig(savePath, transparent=False, format='pdf', bbox_inches="tight")
