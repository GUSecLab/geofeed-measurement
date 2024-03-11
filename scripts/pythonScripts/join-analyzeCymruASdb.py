#!/usr/bin/python3

import sys,pprint,math
import itertools
import json
import pdb#debugging
import pandas as pd, numpy as np
import pycountry
from os import scandir as os_scandir, mkdir as os_mkdir
from argparse import ArgumentParser
from datetime import datetime,timedelta
from pprint import pprint








#Preprocessing functions before joining:


# Preprocess ASdbFrame:
def findEmptyCols(ASdbFrame):
    emptyCols = []
    for key in ASdbFrame.keys():
        if ASdbFrame.loc[:,key].isna().all():
            emptyCols.append(key)
    return emptyCols





#Preprocess asnMetrics

#Fields that were sets before the DF was saved as a CSV (namely 'geofeed_ASNs', 'console_ASNs', etc.)
# are consistently read-in as a single str. This helper is meant to deal with that.
def parse_setStr(entry):
    entryList = entry[1:-1].split(', ')
    retList = [int(item) for item in entryList if item.isnumeric()]
    return set(retList)


#Helper to find all AS entries categorized as a given Layer 1 category (i.e. catVal)
def get_linewse_topCat(entry,catVal):
    return catVal in entry.unique()

#Read-in list of categories and group/sort ASes by Layer-1 category
#Iterate through the list and build a dict of groupings by layer-1 category
def build_category_dict(gfeedAsnTypes):
    categoryFrame = pd.read_csv ('Data/raw_data/ASdb/NAICSlite.csv')
    topCatFilter = categoryFrame.loc[:,'Layer'].apply(lambda y: y==1)
    topCatFrame = categoryFrame.where(topCatFilter).dropna(how='all')
    topCats = topCatFrame.loc[:,'Category Name'].to_list()
    catGroupings = dict()
    altCatGrpings = dict()
    for cat in topCats:
        categoryFilter = gfeedAsnTypes.apply(lambda x: get_linewse_topCat(x,cat),axis=1)
        categoryFrame = gfeedAsnTypes.where(categoryFilter).dropna(how='all')
        catGroupings[cat] = categoryFrame.index
        altCatGrpings[cat] = categoryFrame.index.to_list()
    return catGroupings,altCatGrpings

# work in progress: 
# find all columns that return the category we're interested in within a grouping: still determining whether to whittle returned values to category numbers only
def find_Cat_Columns(rowVal,catVal,topCats):
     topFilter = rowVal[topCats].apply(lambda x: isinstance(x,str) and x==catVal)
     retVal = rowVal.where(topFilter).dropna().index.to_series()
     return retVal.apply(lambda y: y.split(' - ')[0]).to_list()

#Helper for lambda function to identify the top layer category columns in gfeedAsnTypes
def topCatChecker(entry):
    return isinstance(entry,str) and entry.split(' - ')[-1]=='Layer 1'



def categoryMapFxn(val):
    return [val+' - Layer 1',val+' - Layer 2']



# The ASdb frame's column names are of the form Category 1 - Layer 1, Category 1 - Layer 2, Category 2 - Layer 1, etc.
# Where Layer 1 indicates the higher-level category (e.g. 'Computer and Information Technology')
# and Layer 2 indicates the subcategory (e.g. 'Internet Service Provider (ISP)')
def get_category_dist(key,catGroup,topCats):
    keyCategories = catGroup.apply(lambda x: find_Cat_Columns(x,key,topCats),axis=1)
    keyCategories = keyCategories.explode().reset_index()
    keyCategories[0] = keyCategories.loc[:,0].apply(lambda a: a+' - Layer 2' if isinstance(a,str) else np.nan)#Here the value for 'Category X - Layer 1' is going to be the value stored in key
    keyCatIndices = keyCategories.apply(lambda b: [int(b['index']),b[0]],axis=1)
    return keyCatIndices.apply(lambda x: catGroup.loc[x[0],x[1]] if x[1] in catGroup.keys() else np.nan).dropna()




def get_countrywise_dist(key,catGroup,topCats):
    bigCatGroup = catGroup.explode('Country-Code')
    countryGrps = bigCatGroup.groupby('Country-Code')
    countrywiseDist = dict()
    for name,group in countryGrps:
        valueList = get_category_dist(key,group,topCats)
        countrywiseDist[name] = valueList.value_counts()
    return countrywiseDist



def get_category_dists(gfeedAsnTypes,catGroupings,topCats):
    subcategoryDists = dict()
    catDistByCntry = dict()
    # For each pair, ID Category cols where grouping item is located and determine the distribution of the groups' subcategory values.
    for key,vals in catGroupings.items():
        catGroup = gfeedAsnTypes.loc[vals]
        valueList = get_category_dist(key,catGroup,topCats) 
        subcategoryDists[key] = valueList.value_counts()
        catDistByCntry[key] = get_countrywise_dist(key,catGroup,topCats)
    return subcategoryDists,catDistByCntry




def find_multiple_regs(cymruAsNumGrps):
    multipleRegs = dict()
    for name,group in cymruAsNumGrps:
        countryDist = group.loc[:,'CC'].value_counts()
        if countryDist.size > 1:
            multipleRegs[name] = group.loc[:,'CC'].unique().tolist()
    return multipleRegs





def get_country_name(cc):
    countryObj = pycountry.countries.get(alpha_2=cc)
    if countryObj is None:
        return cc
    try:
        ret = countryObj.common_name
    except AttributeError:
        return countryObj.name
    else:
         return countryObj.common_name






#TODO: Set up params and associated help text
def parse_inputs():
    pass#placeholder







def main():
    ASdbFile = 'Data/raw_data/ASdb/2023-05_categorized_ases.csv'
    asnMetricFile = 'Data/cleaned_data/geofeed-srcs-withCIDRs/gfeed_asn_metrics/04.02.2022-11.15.2023_consoleGfeed_AsnComparison.csv'
    asnOutFile = 'Data/cleaned_data/geofeed-srcs-withCIDRs/gfeed_asn_metrics/ASdb_breakdown/11.10.2023-AsnBreakdown/gfeedAsnFrame.csv'
    categoryOutFile = 'Data/cleaned_data/geofeed-srcs-withCIDRs/gfeed_asn_metrics/ASdb_breakdown/11.10.2023-AsnBreakdown/categoryDict.csv'
    cymruAsnFile = 'Data/cleaned_data/geofeed-srcs-withCIDRs/asn_matched/11.10.2023-gfeedCymru-asn.csv'
    ASdbFrame = pd.read_csv (ASdbFile)
    asdbEmpties = findEmptyCols(ASdbFrame)
    ASdbFrame.drop(columns=asdbEmpties, inplace=True)
    ASdbFrame['ASN'] = ASdbFrame.loc[:,'ASN'].apply(lambda x: int(x[2:]))
    
    #read-in asnMetrics frame:
    asnMetrics = pd.read_csv(asnMetricFile, index_col=0)
    asnMetrics['geofeed_ASNs'] = asnMetrics.loc[:,'geofeed_ASNs'].apply(lambda y: parse_setStr(y))

    #TODO: replace hard-coded index w/programmatic index determination
    filterSet = asnMetrics.loc[28,'geofeed_ASNs']
    asnFilter = ASdbFrame.loc[:,'ASN'].apply(lambda z: z in filterSet)
    gfeedAsnTypes = ASdbFrame.where(asnFilter).dropna(how='all')
    gfdAsnEmpties = findEmptyCols(gfeedAsnTypes)
    gfeedAsnTypes.drop(columns=gfdAsnEmpties,inplace=True)

    #Get the country-wise distribution of the ASNs labelled as being ISPs
    cymruAsnFrame = pd.read_csv(cymruAsnFile,sep=';')
    cymruASFilter = cymruAsnFrame.loc[:,'AS'].apply(lambda z: z in gfeedAsnTypes.loc[:,'ASN'])
    cymruIntrSectFrame = cymruAsnFrame.where(cymruASFilter).dropna(how='all')
    multipleRegs = find_multiple_regs(cymruIntrSectFrame.groupby('AS'))
    cymruSmallFilter = cymruIntrSectFrame.loc[:,'AS'].apply(lambda b: b not in multipleRegs.keys())
    cymruSmallDF = cymruIntrSectFrame.where(cymruSmallFilter).dropna(how='all')

    oneCntryASNs = set(gfeedAsnTypes.loc[:,'ASN'].to_list()).intersection(set(cymruSmallDF.loc[:,'AS'].unique().tolist()))
    singCntryFilter = gfeedAsnTypes.loc[:,'ASN'].apply(lambda c: c in oneCntryASNs)
    oneCntryASNs = gfeedAsnTypes.where(singCntryFilter).dropna(how='all')
    manyCntryASNs = gfeedAsnTypes.where(singCntryFilter.apply(lambda d: not(d))).dropna(how='all')

    #get mapping btw ASN and country (CC) in cymruSmallDF, find ASNs in oneCntryCompIt IDed as being ISPs & map them using the ASN/Country mapping
    asnToCCMapper = cymruSmallDF.loc[:,['AS','CC']].set_index('AS').squeeze().to_dict()
    gfdAsOneCntrs = oneCntryASNs.loc[:,'ASN'].map(asnToCCMapper)
    gfdAsManyCntrs = manyCntryASNs.loc[:,'ASN'].map(multipleRegs)

    oneCntryASNs.insert(1,'Country-Code',gfdAsOneCntrs)
    manyCntryASNs.insert(1,'Country-Code',gfdAsManyCntrs)
    #gfeedAsnTypes = pd.concat([oneCntryASNs,manyCntryASNs])#temporarily holding off on this assignment so helper functions don't break

    #Read-in list of categories and group/sort ASes by Layer-1 category
    #Iterate through the list and build a dict of groupings by layer-1 category
    catGroupings,altCatGrpings = build_category_dict(gfeedAsnTypes) 

    #Save to disk lest anything breaks
    gfeedAsnTypes.to_csv(asnOutFile)
    pd.Series(altCatGrpings).to_csv(categoryOutFile)#To load it in as a dict from resulting file: catDict = pd.read_csv(categoryOutFile,index_col=0,squeeze=True)).to_dict(); altCatDict = dict();for key in catDict: altCatDict[key] = pd.Index(catDict[key])
    
    gfeedAsnTypes = pd.concat([oneCntryASNs,manyCntryASNs])

    #find cols w/Layer 1 categories
    catColNames = gfeedAsnTypes.keys().to_series()
    topCatFilter = catColNames.apply(lambda z: topCatChecker(z))
    topCats = catColNames.where(topCatFilter).dropna().to_list()
    
    subcategories,subcatsByCntry = get_category_dists(gfeedAsnTypes,catGroupings,topCats)

    categoryDirMap = {
            'Computer and Information Technology': 'compIt',
            'Media, Publishing, and Broadcasting': 'mediaPubBrdcast',
            'Finance and Insurance': 'financeInsurance',
            'Education and Research': 'edRsrch',
            'Service': 'service',
            'Agriculture, Mining, and Refineries (Farming, Greenhouses, Mining, Forestry, and Animal Farming)': 'agMiningRefine',
            'Community Groups and Nonprofits': 'cgrpsNonProfit',
            'Construction and Real Estate': 'constructREstate',
            'Museums, Libraries, and Entertainment' : 'museumLibEnt',
            'Utilities (Excluding Internet Service)' : 'non-isp-util',
            'Health Care Services' : 'healthcare',
            'Travel and Accommodation' : 'travelAccomm',
            'Freight, Shipment, and Postal Services' : 'freightShipPost',
            'Government and Public Administration' : 'government',
            'Retail Stores, Wholesale, and E-commerce Sites' : 'salesCommerce',
            'Manufacturing' : 'manufacturing',
            'Other' : 'other',
            'Unknown' : 'unknown'
            }
    basedir = 'Data/cleaned_data/geofeed-srcs-withCIDRs/gfeed_asn_metrics/ASdb_breakdown/11.10.2023-AsnBreakdown'
    for category in subcategories:
        categoryDir = basedir+'/breakdown-byCategory/'+categoryDirMap[category]
        try:
            os_mkdir(categoryDir)
        except FileExistsError:
            pass
        finally:
            subcategories[category].to_csv('/'.join([categoryDir,'global_breakdown.csv']))
            countryBreakdown = pd.DataFrame.from_dict(subcatsByCntry[category],orient='index').reset_index(names='Country-Code')
            countryNames = countryBreakdown.loc[:,'Country-Code'].apply(lambda v: get_country_name(v))
            countryBreakdown.insert(1,'Country-Name',countryNames)
            countryBreakdown.to_csv('/'.join([categoryDir,'breakdown-byCountry.csv']))
            breakdowns = countryBreakdown.loc[:,'Country-Code'].map(subcatsByCntry[category])
            for row in pd.concat([countryNames,breakdowns],axis=1,keys=['Country-Name','breakdown']).iterrows():
                pprint(f'{row[1]["Country-Name"]}\n {row[1]["breakdown"]}')
                print(f'\n\n')


if __name__== "__main__":
    main()
