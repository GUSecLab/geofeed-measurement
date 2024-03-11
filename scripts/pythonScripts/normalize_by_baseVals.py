#!/usr/bin/python3

import ipaddress
from argparse import ArgumentParser
from datetime import datetime,timedelta

import pandas as pd, numpy as np
import matplotlib.pyplot as plt
import pycountry
import country_converter as coco

from netaddr import IPSet as netaddr_IPSet, IPRange as netaddr_IPRange
from pyxdameraulevenshtein import damerau_levenshtein_distance as DL_dist, normalized_damerau_levenshtein_distance as normalized_DL_dist
from thefuzz import fuzz



import pdb#debugging





# Parse NRO delegated stats file to get count of allocated or assigned IPv4 addresses per country
def get_nro_ipv4_breakdown():
    nroFrame = pd.read_csv('Data/raw_data/nro-delegated-stats-oct15_2023',sep='|', names=['registry','country-code','type','start','value','date','status','opaque-id','extensions'], skiprows=5)
    ipv4Filter = nroFrame.loc[:,'type'].apply(lambda x: isinstance(x,str) and x.lower()=='ipv4')
    useFilter = nroFrame.loc[:,'status'].apply(lambda y: isinstance(y,str) and y.lower() in ['allocated','assigned'])
    addressFilter = ipv4Filter & useFilter
    IPv4AddrFrame = nroFrame.where(addressFilter).dropna(how='all')
    countryGrps = IPv4AddrFrame.groupby('country-code')
    groupVals = dict()
    for name,grp in countryGrps:
        groupVals[name] = grp.loc[:,'value'].sum()
    return groupVals




# function to normalize by number of IPv4 addresses allocated to each country 
def normalize_by_ipv4_allocation(infile,pullDates,firstPull,lastPull,lastDate):
    countryIPv4Dict = get_nro_ipv4_breakdown()
    gfeedGrowth = pd.read_csv(infile,index_col=0)
    countryIsoFilter = gfeedGrowth.loc[:,'country_iso_code'].apply(lambda x: x in countryIPv4Dict.keys())
    compVals = gfeedGrowth.where(countryIsoFilter).dropna(how='all')
    allocatedAddrs = compVals.loc[:,'country_iso_code'].map(countryIPv4Dict)
    compVals.insert(11,'Oct-15-2023-allocIPs',allocatedAddrs)

    #because of how allocatedAddrs is generated its index is named 'country_iso_code' by default
    ipWeightedgeofeedPres = pd.concat([compVals.loc[:,f'num_ips_{lastDate}'],allocatedAddrs],axis=1).apply(lambda y: 100*y[f'num_ips_{lastDate}']/y['country_iso_code'],axis=1)
    compVals.insert(12,f'{lastDate}-pcent-alloc-IPs',ipWeightedgeofeedPres)
    sortedbyIPRepPcent = compVals.sort_values(f'{lastDate}-pcent-alloc-IPs',axis=0, ascending=False,kind='stable')
    continentGrps = sortedbyIPRepPcent.groupby('continent')
    continentPcentMetrics = dict()
    for name,group in continentGrps:
        continentPcentMetrics[name] = {
            'max': group.loc[:,f'{lastDate}-pcent-alloc-IPs'].max(),
            'min': group.loc[:,f'{lastDate}-pcent-alloc-IPs'].min(),
            'median':group.loc[:,f'{lastDate}-pcent-alloc-IPs'].median(),
            'mean': group.loc[:,f'{lastDate}-pcent-alloc-IPs'].mean()
        }
    pcentMetFrame = pd.DataFrame(continentPcentMetrics)
    pcentMetFrame.to_csv(f'Data/cleaned_data/gfeed_ipv4_metrics/{pullDates}_gfeedChangeMets_normByIPAlloc.csv')
    sortedbyIPRepPcent.to_csv(f'Data/cleaned_data/gfeed_ipv4_metrics/{pullDates}_gfeedGrowth_normByIPAlloc.csv')
    return (compVals,sortedbyIPRepPcent,continentGrps,continentPcentMetrics)




def fuzzily_match_internet_pop(ciafactBFrame, gfeedGrowth):
     countryPopMap = dict()
     cc = coco.CountryConverter()
     ciafactBFrame['value'] = ciafactBFrame.loc[:,'value'].apply(lambda x: float(x.replace(',' , '')) if isinstance(x,str) else x)
     gfeedGrowth['country_name']= cc.pandas_convert(series=gfeedGrowth.loc[:,'country_name'],to='name_short')
     matches = set(gfeedGrowth.loc[:,'country_name'].to_list()).intersection(set(ciafactBFrame.loc[:,'name'].to_list()))
     matchFilter = gfeedGrowth.loc[:,'country_name'].apply(lambda w: w in matches)
     misfitFilter = matchFilter.apply(lambda z: not(z))
     countryMatches = gfeedGrowth.where(matchFilter).dropna(how='all')
     countryMisfits = gfeedGrowth.where(misfitFilter).dropna(how='all')
     popMapping = ciafactBFrame.loc[:,['name','value']].set_index('name').to_dict()['value']
     matchVals = countryMatches.loc[:,'country_name'].map(popMapping)
     countryMatches.insert(11,'num_Internet_users',matchVals)
     ciaMisfitFilter = ciafactBFrame.loc[:,'name'].apply(lambda v: v not in matches)
     ciaMisfits = ciafactBFrame.where(ciaMisfitFilter).dropna(how='all')
     ciaAltNames = cc.pandas_convert(series=ciaMisfits.loc[:,'name'], to='name_short')
     ciaMisfits.insert(1,'cc_shortName',ciaAltNames)
     groupings = ciaMisfits.groupby('cc_shortName')
     includedCntries = set(ciaMisfits.loc[:,'cc_shortName'].to_list()).intersection(set(countryMisfits.loc[:,'country_name'].to_list()))
     valDict = dict()
     for name,group in groupings:
         if name in includedCntries:
             valDict[name] = group.loc[:,'value'].sum()
     pdb.set_trace()
     misfitPopVals = countryMisfits.loc[:,'country_name'].map(valDict)
     countryMisfits.insert(11,'num_Internet_users',misfitPopVals)
 
     #manually add the correct number for the US Virgin Islands - index #'s were found by checking the relevany DF's using Ipython
     countryMisfits.loc[139,'num_Internet_users']=ciaMisfits.loc[195,'value']
     return pd.concat([countryMatches,countryMisfits],ignore_index=True)


#Prev. version of the function - fuzzy matching approach used below didn't work as well as expected/anticipated
# def fuzzily_match_internet_pop(ciafactBFrame, gfeedGrowth):
#      countryPopMap = dict()
#      for country in gfeedGrowth.loc[:,'country_name']:
#           locDlScores = ciafactBFrame.apply(lambda x: normalized_DL_dist(country,x))
#           locFuzzScores = ciafactBFrame.apply(lambda y: 1-(fuzz.token_sort_ratio(country,y)/100.0))
#           bestDls = [val  for val in locDlScores.nsmallest(n=2).unique() if val<0.5]
#           bestFuzzes = [val for val in locFuzzScores.nsmallest(n=2).unique() if val<0.5]
#           dlFilter = locDlScores.apply(lambda w: w in bestDls)
#           fuzzFilter = locFuzzScores.apply(lambda z: z in bestFuzzes)
#           dlMatches = ciafactBFrame.where(dlFilter).dropna()
#           fuzzMatches = ciafactBFrame.where(fuzzFilter).dropna()
#           fuzzyCountry = ''
#           overlaps = set(dlMatches.loc[:,'name'].to_list()).intersection(set(fuzzMatches.loc[:,'name'].to_list()))
#           if len(overlaps)>0:
#                fuzzyCountry = list(overlaps)[0]
#           else:
#                if len(bestFuzzes) ==1:
#                     fuzzyCountry = fuzzMatches.iloc[0]
#                else:
#                     if len(bestFuzzes)!=0:
#                          altFuzzFilter = locFuzzScores.apply(lambda z: z==min(bestFuzzes))
#                          bestFuzzes = gnameLocs.where(altFuzzFilter).dropna()
#                          fuzzyCountry = bestFuzzes.iloc[0]
#                     else:
#                          continue
#           ciaCountry = ciafactBFrame.query('name'== fuzzyCountry)
#           countryPopMap[country] = ciaCountry['value']
#           print(f'country: {country}, fuzzyCountry: {fuzzyCountry}')
#      return gfeedGrowth.loc[:,'country_name'].map(countryPopMap)

def normalize_by_internet_pop(ciafactBFrame, gfeedGrowth,pullDates,lastDate):
    gfeedGrowthWeight = fuzzily_match_internet_pop(ciafactBFrame,gfeedGrowth)
    gfeedGrowthWeight['num_Internet_users'] = gfeedGrowthWeight.loc[:,'num_Internet_users'].apply(lambda x: float(x.replace(',' , '')) if isinstance(x,str) else x)
    popWeightedGfeedPres = gfeedGrowthWeight.loc[:,[f'num_ips_{lastDate}','num_Internet_users']].apply(lambda y: y[f'num_ips_{lastDate}']/y['num_Internet_users'],axis=1)
    gfeedGrowthWeight.insert(12,'Rep_norm_by_n_Int_Usrs',popWeightedGfeedPres)
    sortedbyNUsers = gfeedGrowthWeight.sort_values('Rep_norm_by_n_Int_Usrs',axis=0, ascending=False,kind='stable')
    sortedbyNUsers.to_csv(f'Data/cleaned_data/gfeed_ipv4_metrics/{pullDates}_gfeedGrowth_normByIntUsers.csv')
    return sortedbyNUsers






def parse_input():
    desc='normalize_by_baseVals.py normalize countrywise geofeed IPv4 address growth data by countries\' respective IPv4 address allocations and their estimated Internet users.'
    parser = ArgumentParser(description=desc)
    parser.add_argument('-g',
            dest='gfeedGrowthFile',
            required=True,
            help='complete or relative filepath to datafile holding the comparison of total IPv4 addresses in the listed within the geofeed as being located in each country represented.'
        )
    parser.add_argument('-c',
            dest='ciafBookFile',
            type=str,
            default='Data/raw_data/ciaFactBook_IntUsersRanking.csv',
            help='complete or relative filepath for cia factbook csv of # Internet users per country (default:\'Data/raw_data/ciaFactBook_IntUsersRanking.csv\')')
    return parser.parse_args()







def main():
    params = parse_input()
    baseFname = params.gfeedGrowthFile.split(sep='/')[-1]
    pullDates = baseFname.split(sep='_')[0]
    [firstPull,lastPull] = pullDates.split('-')
    lastDate = datetime.strptime(lastPull,'%m.%d.%Y').strftime('%b_%d_%Y')
    (compVals,sortedbyIPRepPcent,continentGrps,continentMetrics) = normalize_by_ipv4_allocation(params.gfeedGrowthFile,pullDates,firstPull,lastPull,lastDate)
    ciafactBFrame = pd.read_csv(params.ciafBookFile,sep=';')
    gfeedGrowth = compVals.drop(columns=f'{lastDate}-pcent-alloc-IPs')
    sortedbyNUsers = normalize_by_internet_pop(ciafactBFrame, gfeedGrowth,pullDates,lastDate)



if __name__== "__main__":
    main()
