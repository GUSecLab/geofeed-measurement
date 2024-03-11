#!/usr/bin/python3

import json
import math,random,collections,itertools,ipaddress 
import concurrent.futures 
from os import scandir as os_scandir,cpu_count as os_cpu_count,mkdir as os_mkdir
from argparse import ArgumentParser
from datetime import datetime,timedelta

import pdb#debugging

import netaddr
import pandas as pd, numpy as np




#TODO: 
# (done) setup parameters
# validate parameters passed in
# use date param to determine filename for geofeed data
# use date param to determine folder housing commercial data
# (in progress -debugging) find commercial entries for IP prefixes included in geofeed data
# filter geofeed entries to match those included in commercial dataset
# merge frames together (with proper sorting so rows align)
# calculate which entries agree and which do not
# looking at entries that do not agree compute distance between estimated locations (Likely part of a second script - task will need to be broken down further)
# compute average discrepency over the full dataset (i.e. the full set of overlapping prefixes)



#helper to show which overlapping entries to include from both DF's and how to merge them together
def align_IPSets(mergeCol,gfeedCol):
    overlapMap = dict()
    for cidr in gfeedCol.values:
        entryFilter = mergeCol.apply(lambda z: netaddr.IPSet([z]).intersection(netaddr.IPSet([cidr])).size >0)
        overlapVals = mergeCol.where(entryFilter).dropna(how='all')
        if not overlapVals.empty:
            overlapMap[cidr] = overlapVals.to_list()
    return overlapMap




def find_overlaps(gfeedNets,commercialNets,threadno):
    totRounds = gfeedNets.size
    overlapping = pd.Series()
    gfeedMapping = dict()
    nrounds = 0
    for ind,entry in gfeedNets.items():
        testVal = commercialNets.apply(lambda y: y.overlaps(entry))
        if testVal.any():
            gfeedMapping[str(entry)] = commercialNets.where(testVal).dropna(how='all').apply(lambda x: str(x)).to_list()
        nrounds+=1
        if overlapping.empty:
            overlapping=testVal
        else:
            overlapping=overlapping|testVal
        print(f'Thread {threadno} has completed {nrounds} out of {totRounds} of checks.')
    return overlapping,gfeedMapping




def check_overlap_helper(gfeedCidrs,mapKeys):
    isOK = True
    if not mapKeys.issubset(gfeedCidrs):
        mSuspects = mapKeys - gfeedCidrs
        gSuspects = gfeedCidrs - mapKeys
        for gs in gSuspects:
            testVal = str(ipaddress.ip_interface(gs).network)
            isOK = isOK and testVal in mSuspects
    return isOK


def check_overlap_files(provider,overlapping,nrows,gfeedFile,gfeedMapFile):
    if overlapping is not None:
        if nrows != overlapping.size:
            if provider == 'ipgeolocation-io':
                erStr = '\n'.join([
                    'overlapping series must have the same number of rows as the commercial DB\'s prefix entries.',
                    'Note: to load-in an intermediate DataFrame with ipgeolocation.io data',
                    '(where the \'start_ip\' and \'end_ip\' fields are mapped to corresponding sets of CIDR prefixes)',
                    'the commercialPath parameter must specify the path of the intermediate folder location\.',
                    '(In most cases this path will be Data/cleaned_data/commercial-gfeed-comps/intermediate_data'])
                raise ValueError(erStr)
            raise ValueError('overlapping series must have the same number of rows as the commercial DB\'s prefix entries.')
    gfeedFrame = pd.read_csv(gfeedFile,names=['ip_prefix','country_iso_code','iso_subregion','city_name'],index_col=False)
    if overlapping is not None and gfeedMapFile is not None:
        with open(gfeedMapFile,'r') as mapFile:
            mapDict = json.loads(mapFile.read())
            gfeedCidrs = set(gfeedFrame.loc[:,'ip_prefix'].to_list())
            mapKeys = set(mapDict.keys())
            if not check_overlap_helper(gfeedCidrs,mapKeys):
                print(f'Non-overlapping CIDRs in map file:')
                print(f'{set(mapDict.keys()) - gfeedCidrs}\n None of these CIDRs are found in the geofeed File')
                raise ValueError(f'Mapping specified in geofeed map file: {gfeedMapFile}\n does not align with CIDRs listed in geofeed file: {gfeedFile}')
    return gfeedFrame



def init_maxmind_commercialBlocks(commercialDir,provider):
    providerMap = {
            'maxmind-geoip2': 'GeoIP2-City',
            'maxmind-geolite2':'GeoLite2-City'
            }
    providerBlock = '-'.join([providerMap[provider],'Blocks-IPv4.csv'])
    commercialBlockFile = '/'.join([commercialDir,providerBlock])
    blockCols = [
            'network','geoname_id',
            'latitude','longitude',
            'accuracy_radius'
            ]
    commercialBlocks  = pd.read_csv(commercialBlockFile,usecols=blockCols)#,dtype=blockColTypes)
    commercialBlocks['geoname_id'] = commercialBlocks.loc[:,'geoname_id'].astype(pd.Int64Dtype())
    commercialBlocks['accuracy_radius'] = commercialBlocks.loc[:,'accuracy_radius'].astype(pd.Int64Dtype())
    return commercialBlocks




#Identify maxmind-specific files w/commercial data, check overlap files (if any), read in geofeed and commercial block files 
def get_comparable_maxmind_setup(gfeedFile,commercialDir,provider,overlapping,gfeedMapFile):#commercialBlockFile, commercialLocFile):
    commercialBlocks = init_maxmind_commercialBlocks(commercialDir,provider)
    (nrows,ncol) = commercialBlocks.shape
    gfeedFrame = check_overlap_files(provider,overlapping,nrows,gfeedFile,gfeedMapFile)
    return (commercialBlocks,gfeedFrame)





def get_comparable_maxmind_entries(commercialDir,commercialBlocks,gfeedFrame,overlapping,gfeedMapping,provider):
    providerMap = {
            'maxmind-geoip2': 'GeoIP2-City',
            'maxmind-geolite2':'GeoLite2-City'
            }
    providerLocs = '-'.join([providerMap[provider],'Locations-en.csv'])
    commercialLocFile = '/'.join([commercialDir,providerLocs])
    localeCols = [
            'geoname_id','continent_code',
            'continent_name','country_iso_code',
            'country_name','subdivision_1_iso_code',
            'subdivision_1_name','subdivision_2_iso_code',
            'subdivision_2_name','city_name'
            ]
    localeTypes = {
            'geoname_id': pd.Int64Dtype(),
            'continent_code': str,
            'continent_name':str,
            'country_iso_code':str,
            'country_name':str,
            'subdivision_1_iso_code':str,
            'subdivision_1_name':str,
            'subdivision_2_iso_code':str,
            'subdivision_2_name':str,
            'city_name':str
            }

    commercialLocs = pd.read_csv(commercialLocFile,usecols=localeCols,dtype=localeTypes,index_col='geoname_id')
    # commercialSubregions = commercialLocs.loc[:,['country_iso_code','subdivision_1_iso_code']].apply(lambda x: '-'.join([str(x['country_iso_code']),str(x['subdivision_1_iso_code'])]),axis=1)
    # commercialLocs.insert(5,'iso_subregion',commercialSubregions)
    # commercialLocs.drop(columns='subdivision_1_iso_code',inplace=True)
    
    # testBlocks = commercialBlocks.where(overlapping).dropna(how='all')
    #print('testBlocks shape: ',testBlocks.shape)
    geonameLocales = commercialBlocks.loc[:,'geoname_id'].notna()
    gnameCoords = commercialBlocks.loc[:,'longitude'].notna() & commercialBlocks.loc[:,'latitude'].notna() & commercialBlocks.loc[:,'accuracy_radius'].notna()
    mergeBlocks = commercialBlocks.where(geonameLocales).dropna(how='all')
    nameless = geonameLocales.apply(lambda x: not(x))
    coordBlocks = commercialBlocks.where(gnameCoords & nameless).dropna(how='all')
    existingISOLocs = commercialLocs.where(commercialLocs.loc[:,'country_iso_code'].notna()).dropna(how='all')
    overlapGeonames = existingISOLocs.index.intersection(commercialBlocks.loc[:,'geoname_id'].value_counts().index)
    
    mergeFrame = mergeBlocks.set_index('geoname_id').loc[overlapGeonames].join(existingISOLocs.loc[overlapGeonames],validate='m:1')
    mergeFrame = mergeFrame.reset_index(names='geoname_id')
    #mergeFrame.sort_values(by=['network'],inplace=True)
    mergeFrame = pd.concat([mergeFrame,coordBlocks],sort=True)
    missingEntries = set(commercialBlocks.loc[:,'network'].to_list()) - set(mergeFrame.loc[:,'network'].to_list())
    augments = commercialBlocks.loc[:,'network'].apply(lambda x: x in missingEntries)
    augmentVals = commercialBlocks.where(augments).dropna(how='all')
    mergeFrame = pd.concat([mergeFrame,augmentVals],sort=True)
    #gfeedFrame.sort_values(by=['ip_prefix'],inplace=True)

    #Filter out gfeedFrame entries with no overlapping commercial data entries
    gfeedEntryFilter = gfeedFrame.loc[:,'ip_prefix'].apply(lambda x: x in gfeedMapping.keys())
    gfeedintersectFrame = gfeedFrame.where(gfeedEntryFilter).dropna(how='all')
    return (gfeedintersectFrame,mergeFrame)



def isUsable(cidr):
    return not (cidr.is_private or cidr.is_reserved or cidr.is_link_local or cidr.is_loopback or cidr.is_multicast)



#Helper for converting start and end IPv4 addresses to address range objects for easier comparison w/other data

def ipgeIo_range_builder(entry):
    startAddr = entry.loc['start_ip']
    endAddr = entry.loc['end_ip']
    return [cidr for cidr in ipaddress.summarize_address_range(startAddr,endAddr)]


#Intermediate helper for getting CIDR ranges in ipgeolocation.io entries 
# meant to allow multithreading by calling ipgeIo_range_builder on smaller segments of commercialIps (see main ~lines 690-700)  
def find_ipgeIo_range_segment(segment):
    return segment.apply(ipgeIo_range_builder,axis=1)


#still a work in progress. Do not call yet!
# def ipgeIo_range_helper(entry):
#     for cidr in ipaddress.summarize_address_range(startAddr,endAddr):
#         if isUsable(cidr):
#             retVal.append([
#                     str(cidr),entry.loc[:,'country_id'],entry.loc[:,'state_place_id'],
#                     entry.loc[:,'city_place_id'],entry.loc[:,'latitude'],
#                     entry.loc[:,'longitude'],entry.loc[:,'geoname_id'],
#                     entry.loc[:,'AS_organization']
#                     ])
#     pdb.set_trace()
#     if len(retVal)==0:
#         return np.nan
#     pdb.set_trace()
#     return pd.DataFrame(retVal)



# def ipgeIo_range_helper(entry):
#     retVal=None
#     try:
#         retVal= netaddr.IPSet(netaddr.IPRange(entry.loc['start_ip'],entry.loc['end_ip']))
#     except IndexError:
#         ip_list = list(netaddr.iter_iprange(entry.loc[0,'start_ip'],entry.loc[0,'end_ip']))
#         retVal = netaddr.IPSet(ip_list)
#     finally:
#         return retVal




def init_ipgeolocationIo_commercialBlocks(commercialDir,provider):
    geolocFilePath = '/'.join([commercialDir,'db-ip-geolocation.csv'])
    # countryPath = '/'.join([commercialDir,'db-country.csv'])
    # placePath = '/'.join([commercialDir,'db-place.csv'])

    # See the README.md file in any ipgeolocation.io pull's directory for field descriptions
    # The full header for all fields included in db-ip-geolocation.csv
    geolocFields = [
            'start_ip','end_ip',
            'country_id','state_place_id',
            'district_place_id','city_place_id',
            'zip_code','latitude',
            'longitude','geoname_id',
            'timezone_name','isp',
            'connection_type','AS_organization'
            ]
    #fields from db-ip-geolocation.csv that will actually be used/read in
    geolocUseFields = [
            'start_ip','end_ip',
            'country_id','state_place_id',
            'city_place_id','latitude',
            'longitude','geoname_id',
            'AS_organization'
            ]

    commercialBlocks = pd.read_csv(geolocFilePath,names=geolocFields,index_col=False,usecols=geolocUseFields)
    commercialBlocks['country_id'] = commercialBlocks.loc[:,'country_id'].astype(pd.Int64Dtype())
    commercialBlocks['state_place_id'] = commercialBlocks.loc[:,'state_place_id'].astype(pd.Int64Dtype())
    commercialBlocks['city_place_id'] = commercialBlocks.loc[:,'city_place_id'].astype(pd.Int64Dtype())
    commercialBlocks['geoname_id'] = commercialBlocks.loc[:,'geoname_id'].astype(pd.Int64Dtype())
    return commercialBlocks







#unfortunately maxmind and ipgeolocation.io's csv fields are not uniform and need to be processed differently

def get_comparable_ipgeolocationio_setup(gfeedFile,commercialDir,overlapping,gfeedMapFile,provider):
    commercialBlocks = None
    intermediates = overlapping is not None
    commercialIps=None
    # Use heuristic -
    #       raw data pulls for ipgeolocation.io data
    #       are generally imported to the raw_data directory (see scripts/pull-ipgeolocation-io.sh)
    # So if file is saved within the cleaned_data folder, it's assumed to be an intermediate val from a previous run (or partial run) of this script

    indStr = commercialDir.split(sep='/')[1]
    if intermediates or indStr=='cleaned_data':
        cfileName = '/'.join([commercialDir,'db-ip-geolocation.csv'])
        commercialBlocks = pd.read_csv(cfileName,index_col=0)
        commercialBlocks['country_id'] = commercialBlocks.loc[:,'country_id'].astype(pd.Int64Dtype())
        commercialBlocks['state_place_id'] = commercialBlocks.loc[:,'state_place_id'].astype(pd.Int64Dtype())
        commercialBlocks['city_place_id'] = commercialBlocks.loc[:,'city_place_id'].astype(pd.Int64Dtype())
        commercialBlocks['geoname_id'] = commercialBlocks.loc[:,'geoname_id'].astype(pd.Int64Dtype())
        #pdb.set_trace()#Check format of commercialBlocks - make sure it's up to snuff
    else:
        print('indStr = ',indStr)
        commercialBlocks = init_ipgeolocationIo_commercialBlocks(commercialDir,provider)
        startFilter = commercialBlocks.loc[:,'start_ip'].apply(lambda x: ':' not in x)
        commercialBlocks = commercialBlocks.where(startFilter).dropna(how='all')
        endFilter = commercialBlocks.loc[:,'end_ip'].apply(lambda x: ':' not in x)
        commercialBlocks = commercialBlocks.where(endFilter).dropna(how='all')
        ascendingFilter = commercialBlocks.loc[:,['start_ip','end_ip']].apply(lambda x:ipaddress.IPv4Address(x['start_ip']) <= ipaddress.IPv4Address(x['end_ip']),axis=1) 
        commercialBlocks = commercialBlocks.where(ascendingFilter).dropna(how='all')

        #make formatting more uniform across sources
        startUseFilter = commercialBlocks.loc[:,'start_ip'].apply(lambda x: isUsable(ipaddress.IPv4Address(x)))
        endUseFilter = commercialBlocks.loc[:,'end_ip'].apply(lambda x: isUsable(ipaddress.IPv4Address(x)))
        commercialBlocks = commercialBlocks.where(startUseFilter & endUseFilter).dropna(how='all')
        startAddrs = commercialBlocks.loc[:,'start_ip'].apply(lambda y: ipaddress.IPv4Address(y))
        endAddrs = commercialBlocks.loc[:,'end_ip'].apply(lambda y: ipaddress.IPv4Address(y))
        commercialIps =pd.concat([startAddrs,endAddrs],axis=1)
    (nrows,ncols) = commercialBlocks.shape
    gfeedFrame = check_overlap_files(provider,overlapping,nrows,gfeedFile,gfeedMapFile)
    return (gfeedFrame,commercialBlocks, commercialIps,indStr)











#placeholder for the rest of the function that still needs some refactoring. 
def get_comparable_ipgeolocationio_continuation(commercialDir,commercialBlocks,gfeedFrame,gfeedMapping,provider):
    countryPath = '/'.join([commercialDir,'db-country.csv'])
    placePath = '/'.join([commercialDir,'db-place.csv'])
    dbPlaceFields = [
        'id','name_en',
        'name_de','name_ru',
        'name_ja','name_fr',
        'name_cn','name_es',
        'name_cs','name_it'
        ]
    dbPlaceUseFields = ['id','name_en']
    dbPlaceTypes = {
            'id':'Int32',
            'name_en':str
            }
    ipgeIoPlaces = pd.read_csv(placePath,names=dbPlaceFields,usecols=dbPlaceUseFields,dtype=dbPlaceTypes,index_col='id')
    dbCountryFields = [
            'id','continent_code',
            'continent_name_place_id','country_code_iso_2',
            'country_code_iso_3','country_name_place_id',
            'capital_name_place_id','currency_code',
            'currency_name','currency_symbol','calling_code',
            'tld','languages'
            ]
    dbCountryUseFields = [
            'id','continent_code',
            'continent_name_place_id','country_name_place_id',
            'country_code_iso_2'
            ]
    dbCountryTypes = {
            'id':'Int32',
            'continent_code':str,
            'continent_name_place_id':'Int32',
            'country_name_place_id':'Int32',
            'country_code_iso_2':str
            }
    #Note: due to inconsistent fields in ipgeolocation.io country files, this could break.
    #to avoid this preprocessing is needed to remove characters in languages that go from right to left.
    #pdb.set_trace()
    ipgeIoCountries = pd.read_csv(countryPath,names=dbCountryFields,usecols=dbCountryUseFields,dtype=dbCountryTypes,index_col='id',quotechar='"')
    
    #get mapping for location names (English)
    #start by mapping to country df
    continentNames = ipgeIoCountries.loc[:,'continent_name_place_id'].map(lambda x:ipgeIoPlaces.loc[x,'name_en'])
    ipgeIoCountries.insert(1,'commercial_continent_name',continentNames)
    countryNames = ipgeIoCountries.loc[:,'country_name_place_id'].map(lambda x:ipgeIoPlaces.loc[x,'name_en'])
    ipgeIoCountries.insert(5,'country_name',countryNames)

    #move to geolocation df
    commercialCountryISOs = commercialBlocks.loc[:,'country_id'].map(lambda x: ipgeIoCountries.loc[x,'country_code_iso_2'])
    commercialBlocks.insert(1,'commercial_country_iso_code',commercialCountryISOs)
    commercialCountryNames = commercialBlocks.loc[:,'country_id'].map(lambda x: ipgeIoCountries.loc[x,'country_name'])
    commercialBlocks.insert(2,'commercial_country_name',commercialCountryNames)
    stateNames = commercialBlocks.loc[:,'state_place_id'].map(lambda x: ipgeIoPlaces.loc[x,'name_en'] if not isinstance(x,pd._libs.missing.NAType) else x)
    commercialBlocks.insert(3,'commercial_state/subregion_name',stateNames)
    cityNames = commercialBlocks.loc[:,'city_place_id'].map(lambda x: ipgeIoPlaces.loc[x,'name_en'] if not isinstance(x,pd._libs.missing.NAType) else x)
    commercialBlocks.insert(4,'commercial_city_name',cityNames)
    commercialBlocks.drop(columns=['country_id','state_place_id','city_place_id'],inplace=True)
    
    #Filter out gfeedFrame entries with no overlapping commercial data entries
    gfeedEntryFilter = gfeedFrame.loc[:,'ip_prefix'].apply(lambda x: str(x) in gfeedMapping.keys())
    gfeedintersectFrame = gfeedFrame.where(gfeedEntryFilter).dropna(how='all')
    # pdb.set_trace()#check gfeed key names
    return (commercialBlocks,gfeedintersectFrame)





#Save overlapping filter series and gfeedIPs-commercialIPs mapping to intermediate files
def write_intermediate_files(commercialDir,gfeedFile,outpath,overlapping,gfeedMapping,provider):
    cdate = datetime.strftime(get_commercial_date(commercialDir.split(sep='/')[-1],provider),'%m.%d.%Y')
    gdate = datetime.strftime(get_gfeedFile_date(gfeedFile),'%m.%d.%Y')
    fileIdStr = '-'.join([gdate,cdate,'gfeed'+provider])
    intermedPath = '/'.join([outpath,'intermediate_data'])
    overlapping.to_csv('/'.join([intermedPath,fileIdStr+'-overlaps.csv']))
    with open('/'.join([intermedPath,fileIdStr+'-gfdMap.json']),'w') as fle:
        fle.write(json.dumps(gfeedMapping))
    print('overlapping Value Counts:',overlapping.value_counts())
    return 









#Find set of files or dirs whose collection date is closest to the one passed in (as params.pull_date) 
def find_nearest_date(dataDirObj,inputType,testDate,provider=None):
    deltas = []
    if inputType=='commercial':
        if provider is None:
            raise ValueError('provider must be specified for commercial geolocation providers.')
        ipgeIoStatus = provider=='ipgeolocation-io'
        for entry in dataDirObj:
            if not entry.name.startswith('.') and entry.is_dir():
                datestring = ''
                entrydate=None
                if ipgeIoStatus:
                    [datestring,_] = entry.name.split(sep='-',maxsplit=1)
                else:
                    [_,datestring] = entry.name.split(sep='_',maxsplit=1)
                try:
                    entrydate = datetime.strptime(datestring,'%Y%m%d')
                    delt = testDate - entrydate
                    deltas.append((abs(delt.days),entry.name))
                except ValueError:
                    continue
    else:
        if inputType=='gfeed':
            for entry in dataDirObj:
                if not entry.name.startswith('.') and entry.is_file():
                    [collectDate,ftype] = entry.name.split('-',maxsplit=1)
                    datestring=''
                try:
                    entrydate = datetime.strptime(collectDate,'%m.%d.%Y')
                except ValueError:
                    continue
                else:
                    delt = testDate - entrydate
                    deltas.append((abs(delt.days),entry.name))
    #build up dict of geolocation file dates organized by distance from date passed in
    grouped=collections.defaultdict(list)
    #pdb.set_trace()
    for numDays,dirName in deltas:
        grouped[numDays]+= [dirName]
    return (min(grouped),grouped[min(grouped)])


def get_gfeedFile_date(gfeedFile):
    [datestring,_] = gfeedFile.split(sep='-',maxsplit=1)
    return datetime.strptime(datestring,'%m.%d.%Y')




def get_commercial_date(commercialFile,provider):
    if provider == 'ipgeolocation-io':
        datestring = commercialFile.split(sep='-',maxsplit=1)[0]
    else:
        datestring = commercialFile.split(sep='_',maxsplit=1)[1]
    return datetime.strptime(datestring,'%Y%m%d')



def get_contender_dist(gfeedEntry,commercialEntry,provider):
    gfeedDate = get_gfeedFile_date(gfeedEntry)
    commDate = get_commercial_date(commercialEntry,provider)
    dist = gfeedDate - commDate
    return abs(dist.days)




def find_closest_pair(gfeedCandidates,commercialCandidates,provider):
    (gfeedDist,gfeedContenders) = gfeedCandidates
    (commercialDist,commercContenders) = commercialCandidates
    if len(gfeedContenders)==1 and len(commercContenders)==1:
        dist = get_contender_dist(gfeedContenders[0],commercContenders[0],provider)
        return (dist,(gfeedContenders[0],commercContenders[0]))
    options =[]
    for element in itertools.product(gfeedContenders,commercContenders):
        (gfeedFile,commercialFile) = element
        dist = get_contender_dist(gfeedFile,commercialFile,provider)
        options.append((dist,element))
    return min(options)








def parse_inputs():
    parser = ArgumentParser(description = 'compare_geolocation.py - compares published geofeed information with estimates (for overlapping IPv4 prefixes) in a commercial DB pull')
    parser.add_argument('provider',
                        choices=['maxmind-geoip2','maxmind-geolite2','ipgeolocation-io'],
                        help='Name of the commercial IP-geolocation data source. Currently supported options: [\'maxmind-geoip2\',\'maxmind-geolite2\',\'ipgeolocation-io\']')
    parser.add_argument('-d',
                        dest='pull_date',
                        type=str,
                        default=datetime.today().strftime('%m.%d.%Y'),
                        help='Approximate pull date (of both geofeed and commercial) data to be included given as a string of form MM.DD.YYYY')
    parser.add_argument('-f',
                        dest='feedpath',
                        type=str,
                        default='Data/cleaned_data/ipv4-geofeed-records',
                        help='filepath of the directory housing the IPv4 geofeed-pull results (Default: \'Data/cleaned_data/ipv4-geofeed-records\')')
    parser.add_argument('-c',
                        dest='commercialPath',
                        type=str,
                        default='Data/raw_data/commercial_dbs',
                        help='filepath of the directory housing the IPv4 geofeed-pull results (Default: \'Data/raw_data/commercial_dbs\')')
    parser.add_argument('-o',
                        dest='outpath',
                        type=str,
                        default='Data/cleaned_data/commercial-gfeed-comps',
                        help='filepath for directory into which output will be saved (default: \'Data/cleaned_data/commercial-gfeed-comps\')')
    parser.add_argument('-v',
                        dest='overlapCSV',
                        default=None,
                        help='complete or relative filepath for CSV holding a previously computed series indicating which rows \n of the commercial input contain IP prefixes/CIDRs that overlap with those included in the geofeed data.\n Note: the size of this output must match the number of rows in the commercial block file exactly')
    parser.add_argument('-m',
                        dest= 'intersectMap',
                        type=str,
                        default=None,
                        help='Complete or relative filepath for python dict with the mapping between CIDRs in the geofeed and overlapping CIDRs in the commercial dataFrame (Default: None)')
    return parser.parse_args()






def validate_datadir(dataDir):
    try:
        dataDirObj = os_scandir(dataDir)
    except PermissionError as perr:
        print('You do not have permission to read the dataDir specified (\'%(ddir)\')',{'ddir':dataDir})
        raise
    else:
        return dataDirObj





def validate_inputs():
    params = parse_inputs()
    pullDate = datetime.strptime(params.pull_date,'%m.%d.%Y')
    if pullDate > datetime.today():
        raise ValueError('pull_date parameter (\-d) cannot specify a date in the future.')
    gfeedDataDirObj = validate_datadir(params.feedpath)
    commercialPath = '/'.join([params.commercialPath,params.provider])
    commercialDirObj = validate_datadir(commercialPath)
    overlapping = None
    if params.overlapCSV is not None:
        overlapFilename = params.overlapCSV.split(sep='/')[-1]
        if params.intersectMap is not None:
            intersectMapFname = params.intersectMap.split('/')[-1]
            ovIdStrs = overlapFilename.split(sep='-')[0:2]
            intIdStrs = intersectMapFname.split(sep='-')[0:2]
            if ovIdStrs != intIdStrs:
                print('ovIdStr: ',ovIdStr)#debugging
                print('intIdStr: ',intIdStr)#debugging
                raise ValueError(f'Dates listed in overlapFile ({overlapFilename}) and intersectMapFile ({intersectMapFname}) do not match.\n ')
        overlapping = pd.read_csv(params.overlapCSV,index_col=0).squeeze()
    return (params.provider,params.pull_date,
            params.feedpath,gfeedDataDirObj,
            commercialPath, commercialDirObj,
            params.outpath,overlapping,params.intersectMap)



def expand_mapping(gfeedPrefix,mapping):
    mapVals = mapping[gfeedPrefix]
    return [(gfeedPrefix,entry) for entry in mapVals]



def main():
    (provider,pulldateStr,gfeedpath,gfeedDataDirObj,commercialPath,commercialDirObj,outpath,overlapping,intersectMapFile) = validate_inputs()
    
    #Find files pulled closest to the date specified in the params.
    pulldate = datetime.strptime(pulldateStr,'%m.%d.%Y')
    gfeedCandidates = find_nearest_date(gfeedDataDirObj,'gfeed',pulldate)
    gfeedDataDirObj.close()
    commercialCandidates = find_nearest_date(commercialDirObj,'commercial',pulldate,provider=provider)
    commercialDirObj.close()
    ncpus = os_cpu_count()
    #determine which of the files returned have the closest (respective) pull-dates
    (dist,(gfeedFile,commercialDir)) = find_closest_pair(gfeedCandidates,commercialCandidates,provider)
    gfeedFilePath = '/'.join([gfeedpath,gfeedFile])
    commercialDir = '/'.join([commercialPath,commercialDir])

    #start processing geofeed file:Note geofeed and commercial files are necessary regardless of whether the overlapping series and/or intersectMapFile are specified
    if provider != 'ipgeolocation-io':
        commercialBlocks,gfeedFrame = get_comparable_maxmind_setup(gfeedFilePath,commercialDir,provider,overlapping,intersectMapFile)
        #ncpus = os_cpu_count()
        (nrows,ncol)=commercialBlocks.shape
        chunkEnd = math.floor(nrows / ncpus)
        gfeedNets = gfeedFrame.loc[:,'ip_prefix'].apply(lambda x: ipaddress.ip_interface(x).network)
        if overlapping is not None:
            overlapBlocks = commercialBlocks.where(overlapping).dropna(how='all')
            cNets = overlapBlocks.loc[:,'network'].apply(lambda x: ipaddress.ip_interface(x).network)
            chunkEnd = math.floor(cNets.size / ncpus)
            chunks = [cNets.loc[i* chunkEnd:(i+1)*chunkEnd] for i in range(0,ncpus)]
            chunks.append(cNets.loc[ncpus*chunkEnd:])
            gfeedMapping = None
            if intersectMapFile is None:
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    futureCommercialChunks = {executor.submit(find_overlaps, gfeedNets,chunk,random.randint(0,ncpus+1)): chunk for chunk in chunks}
                    overlapBeta = pd.series()
                    gfeedMapBeta = dict()
                    for future in concurrent.futures.as_completed(futureCommercialChunks):
                        chunk = futureCommercialChunks[future]
                        partialGfeedMap = dict()
                        try:
                            piece,partialGfeedMap = future.result()
                        except Exception:
                            raise
                        else:
                            if overlapBeta.empty:
                                overlapBeta = piece
                            else:
                                overlapBeta = pd.concat([overlapBeta,piece])
                            if len(gfeedMapBeta)==0:
                                gfeedMapBeta = partialGfeedMap
                            else:
                                partKeys = set(partialGfeedMap.keys())
                                bmapKeys = set(gfeedMapBeta.keys())
                                newKeys = partKeys - bmapKeys
                                growthKeys = partKeys.intersection(bmapKeys)
                                for k in partKeys.union(bmapKeys):
                                    if k in newKeys:
                                        gfeedMapBeta[k] = partialGfeedMap[k]
                                    else:
                                        if k in growthKeys:
                                            gfeedMapBeta[k] = list(set(gfeedMapBeta[k]+partialGfeedMap[k]))
                    if not overlapBeta.all():
                        offenders = overlapBeta.apply(lambda x: not(x))
                        print('offending Entries: ')
                        print(overlapBlocks.loc[:,'network'].where(offenders).dropna())
                        raise ValueError('Mismatch between geofeeds or commercial data files used across sessions.')
                    #Since it passed the final check - save the mapping to a file
                    cdate = datetime.strftime(get_commercial_date(commercialDir,provider),'%m.%d.%Y')
                    gdate = datetime.strftime(get_gfeedFile_date(gfeedFile),'%m.%d.%Y')
                    fileIdStr = '-'.join([gdate,cdate,'gfeed'+provider])
                    intermedPath = '/'.join([outpath,'intermediate_data'])
                    # overlapping.to_csv('/'.join([intermedPath,fileIdStr+'-overlaps.csv']))
                    with open('/'.join([intermedPath,fileIdStr+'-gfdMap.json']),'w') as fle:
                        fle.write(json.dumps(gfeedMapBeta))
                    print('overlapping Value Counts:',overlapping.value_counts())
                    gfeedMapping = gfeedMapBeta
                    commercialBlocks = overlapBlocks
                    #Also ensure no duplicate entries in overlappingBeta persist (i.e. same index and value pair)
                    overlapping = overlappingBeta.reset_index().drop_duplicates().set_index('index').squeeze()
            else:
                with open(intersectMapFile,'r') as mapFile:
                    gfeedMapping = json.loads(mapFile.read())
                commercialBlocks = overlapBlocks

        else:
            cNets = commercialBlocks.loc[:,'network'].apply(lambda x: ipaddress.ip_interface(x).network)
            chunks = [cNets.loc[i* chunkEnd:(i+1)*chunkEnd] for i in range(0,ncpus)]
            chunks.append(cNets.loc[ncpus*chunkEnd:])
            with concurrent.futures.ProcessPoolExecutor() as executor:
                futureCommercialChunks = {executor.submit(find_overlaps, gfeedNets,chunk,random.randint(0,ncpus+1)): chunk for chunk in chunks}
                overlapping = pd.Series()
                gfeedMapping = dict()
                for future in concurrent.futures.as_completed(futureCommercialChunks):
                    chunk = futureCommercialChunks[future]
                    partialGfeedMap = dict()
                    try:
                        piece,partialGfeedMap = future.result()
                    except Exception:
                        raise
                    else:
                        if overlapping.empty:
                            overlapping = piece
                        else:
                            overlapping = pd.concat([overlapping,piece])
                        if len(gfeedMapping)==0:
                            gfeedMapping = partialGfeedMap
                        else:
                            newKeys = set(partialGfeedMap.keys()) - set(gfeedMapping.keys())
                            growthKeys = set(partialGfeedMap.keys()).intersection(set(gfeedMapping.keys()))
                            for k in growthKeys:
                                gfeedMapping[k] = list(set(gfeedMapping[k]+partialGfeedMap[k]))
                            for nkey in newKeys:
                                gfeedMapping[nkey] = partialGfeedMap[nkey]
                overlapping.sort_index(inplace=True,kind='stable')
                #Remove any duplicated entries
                overlapping = overlapping.reset_index().drop_duplicates().set_index('index').squeeze()
                write_intermediate_files(commercialDir,gfeedFile,outpath,overlapping,gfeedMapping,provider)#commercialBlocks 
                commercialBlocks = commercialBlocks.where(overlapping).dropna(how='all')
        (gfeedFrame,commercialMergeFrame) = get_comparable_maxmind_entries(commercialDir,commercialBlocks,gfeedFrame,overlapping,gfeedMapping,provider)
        # pdb.set_trace()
        #Now merge the frames using overlapMap as a guide
        gfeedComList = gfeedFrame.loc[:,'ip_prefix'].map(lambda x:expand_mapping(x,gfeedMapping)).to_list()
        flatMappingList  = [val for sublist in gfeedComList for val in sublist]
        mapCols = pd.DataFrame(flatMappingList)
        prefixGfeedFrame = gfeedFrame.set_index('ip_prefix')
        # pdb.set_trace()
        gfeedMapCols = mapCols.loc[:,0].map(lambda x: prefixGfeedFrame.loc[x])
        mergeGfeedFrame = pd.DataFrame(gfeedMapCols.to_list())
        mergeGfeedFrame.reset_index(names='gfeed_ip_prefix',inplace=True)
        reindexedMergeFrame = commercialMergeFrame.set_index('network')
        commercialMapCols = mapCols.loc[:,1].apply(lambda x: reindexedMergeFrame.loc[x])
        commercialMapCols.insert(0,'commercial_ip_prefix',mapCols.loc[:,1])
        comparisonFrame = pd.concat([mergeGfeedFrame,commercialMapCols],axis=1)
        
        #TODO: try commenting out removal of entries that don't have geoname_ids - see if they map to empty geofeed records
        # hasGeoname = comparisonFrame.loc[:,'geoname_id'].notna()
        # comparisonFrame = comparisonFrame.where(hasGeoname).dropna(how='all')
        #pdb.set_trace()
        cdate = datetime.strftime(get_commercial_date(commercialDir.split(sep='/')[-1],provider),'%m.%d.%Y')
        gdate = datetime.strftime(get_gfeedFile_date(gfeedFile),'%m.%d.%Y')
        fileIdStr = '-'.join([gdate,cdate,'gfeed'+provider])
        comparisonFrame.to_csv( '/'.join([outpath,'mergeFrames',fileIdStr+'-mergeFrame.csv']))
    else:
        gfeedMapping = None
        (gfeedFrame,commercialBlocks,commercialIps,indStr) = get_comparable_ipgeolocationio_setup(gfeedFilePath,commercialDir,overlapping,intersectMapFile,provider)
        #use filepath heuristic described in get_comparable_ipgeolocationio_setup to ascertain whether intermediate vals are used.
        if indStr != 'cleaned_data':
            gfeedNets = gfeedFrame.loc[:,'ip_prefix'].apply(lambda x: ipaddress.ip_interface(x).network)
            (nrows,ncol)=commercialBlocks.shape
            chunkEnd = math.floor(nrows / ncpus)

            #set up multithreading for this time-sucka
            chunks = [commercialIps.loc[i* chunkEnd:(i+1)*chunkEnd] for i in range(0,ncpus)]
            chunks.append(commercialIps.loc[ncpus*chunkEnd:])
            # pdb.set_trace()
            with concurrent.futures.ProcessPoolExecutor() as executor:
                combined = pd.DataFrame() 
                futureRanges = {executor.submit(find_ipgeIo_range_segment,chunk): chunk for chunk in chunks}
                for future in concurrent.futures.as_completed(futureRanges):
                    chunk = futureRanges[future]
                    try:
                        piece = future.result()
                    except Exception:
                        raise
                    else:
                        if combined.empty:
                            combined=piece
                        else:
                            combined = pd.concat([combined,piece])
                combined.sort_index(inplace=True,kind='stable')
                combined = combined.reset_index().drop_duplicates(subset=['index']).set_index('index').squeeze()#.apply(lambda x: str(x))
                print('combined type: ', type(combined))
                commercialBlocks.insert(0,'commercial_Cidr',combined)
                commercialBlocks.drop(columns=['start_ip','end_ip'],inplace=True)
                # commercialBlocks = commercialBlocks.explode('commercial_Cidr',ignore_index=True)
            commercialBlocks = commercialBlocks.explode('commercial_Cidr',ignore_index=True)
            intermedPath = '/'.join([outpath,'intermediate_data','ipgeolocation-io'])

            #use similar/same naming convention as original files for compatibility w/date matching functions
            cdate = datetime.strftime(get_commercial_date(commercialDir.split(sep='/')[-1],provider),'%Y%m%d')
            dirName = '-'.join([cdate,'ip','city','isp'])
            try:
                os_mkdir('/'.join([intermedPath,dirName]))
            except FileExistsError:
                pass
            commercialBlocks.to_csv('/'.join([intermedPath,dirName,'db-ip-geolocation.csv']))

            # NOTE: in order to be able to pass in the intermediate table, you will also need to copy the auxiliary table csv files
            #       (i.e. db-country.csv and db-place.csv pulled from the same date) 
        if intersectMapFile is None:
            if overlapping is not None:
                commercialBlocks = commercialBlocks.where(overlapping).dropna(how='all')
            #round 2 of multithreading in the hopes this thing doesn't take forever
            (nrows,ncol)=commercialBlocks.shape
            chunkEnd = math.floor(nrows / ncpus)
            cNets = commercialBlocks.loc[:,'commercial_Cidr']
            intersectChunks = [cNets.loc[i* chunkEnd:(i+1)*chunkEnd] for i in range(0,ncpus)]
            intersectChunks.append(cNets.loc[ncpus*chunkEnd:])
            with concurrent.futures.ProcessPoolExecutor() as executor:
                futureCommercialChunks = {executor.submit(find_overlaps, gfeedNets,chunk,random.randint(0,ncpus+1)): chunk for chunk in intersectChunks}
                overlapping = pd.Series()
                gfeedMapping = dict()
                for future in concurrent.futures.as_completed(futureCommercialChunks):
                    chunk = futureCommercialChunks[future]
                    partialGfeedMap = dict()
                    try:
                        piece,partialGfeedMap = future.result()
                    except Exception:
                        raise
                    else:
                        if overlapping.empty:
                            overlapping = piece
                        else:
                            overlapping = pd.concat([overlapping,piece])
                        if len(gfeedMapping)==0:
                            gfeedMapping = partialGfeedMap
                        else:
                            newKeys = set(partialGfeedMap.keys()) - set(gfeedMapping.keys())
                            growthKeys = set(partialGfeedMap.keys()).intersection(set(gfeedMapping.keys()))
                            for k in growthKeys:
                                gfeedMapping[k] = list(set(gfeedMapping[k]+partialGfeedMap[k]))
                            for nkey in newKeys:
                                gfeedMapping[nkey] = partialGfeedMap[nkey]
                overlapping.sort_index(inplace=True,kind='stable')
                overlapping = overlapping.reset_index().drop_duplicates().set_index('index').squeeze()
                write_intermediate_files(commercialDir,gfeedFile,outpath,overlapping,gfeedMapping,provider)
                commercialBlocks = commercialBlocks.where(overlapping).dropna(how='all')
        else:
            with open(intersectMapFile,'r') as mapFile:
                gfeedMapping = json.loads(mapFile.read()) 
        (commercialMergeFrame,gfeedFrame) = get_comparable_ipgeolocationio_continuation(commercialDir,commercialBlocks,gfeedFrame,gfeedMapping,provider)
        
        #Do not increase indent here: this piece needs to run regardless of whether intermediate pieces passed in
        gfeedComList = gfeedFrame.loc[:,'ip_prefix'].map(lambda x:expand_mapping(x,gfeedMapping)).to_list()
        # pdb.set_trace()
        flatMappingList  = [val for sublist in gfeedComList for val in sublist]
        mapCols = pd.DataFrame(flatMappingList)
        prefixGfeedFrame = gfeedFrame.set_index('ip_prefix')
        gfeedMapCols = mapCols.loc[:,0].map(lambda x: prefixGfeedFrame.loc[x])
        mergeGfeedFrame = pd.DataFrame(gfeedMapCols.to_list())
        mergeGfeedFrame.reset_index(names='gfeed_ip_prefix',inplace=True)
        commercialMergeFrame['commercial_Cidr'] = commercialMergeFrame.loc[:,'commercial_Cidr'].astype(str)
        reindexedMergeFrame = commercialMergeFrame.set_index('commercial_Cidr')
        commercialMapCols = mapCols.loc[:,1].apply(lambda x: reindexedMergeFrame.loc[x])
        commercialMapCols.insert(0,'commercial_ip_prefix',mapCols.loc[:,1])
        comparisonFrame = pd.concat([mergeGfeedFrame,commercialMapCols],axis=1)
        # hasGeoname = comparisonFrame.loc[:,'geoname_id'].notna()
        # comparisonFrame = comparisonFrame.where(hasGeoname).dropna(how='all') 
        cdate = datetime.strftime(get_commercial_date(commercialDir.split(sep='/')[-1],provider),'%m.%d.%Y')
        gdate = datetime.strftime(get_gfeedFile_date(gfeedFile),'%m.%d.%Y')
        fileIdStr = '-'.join([gdate,cdate,'gfeed'+provider])
        comparisonFrame.to_csv( '/'.join([outpath,'mergeFrames',fileIdStr+'-mergeFrame.csv']))

if __name__== "__main__":
    main()
