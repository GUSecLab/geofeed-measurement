#!/usr/bin/python3


import ipaddress, concurrent.futures

from os import scandir as os_scandir,cpu_count as os_cpu_count,mkdir as os_mkdir#TODO: Check which of these you actually need here: only import those
from argparse import ArgumentParser
from datetime import datetime,timedelta

import pandas as pd, numpy as np
from netaddr import IPSet as netaddr_IPSet, IPRange as netaddr_IPRange


'''
calc_commercial_ip_coverage.py iterates through a directory of commercial DB pulls and calculates each pull's IPv4 space coverage (i.e. number of IPv4 addresses).
Results from here will be used later on to compare commercial providers' and geofeeds' respective coverage over time. 
'''




def validate_input(params):
    partial=False
    if params.startDate!='':
        startdate = datetime.strptime(params.startDate,'%m.%d.%Y')
        if startdate > datetime.today():
            raise AttributeError('Invalid startdate {startdate}. startdate cannot be a date in the future.')
        if startdate > datetime.strptime('04.01.2022','%m.%d.%Y'):
            partial=True
    return partial



#pull the timestamp from commercial DB dir names - helper for finding applicable dates and for sorting results by TS date
def get_commercial_date(commercialFile,provider):
    if provider == 'ipgeolocation-io':
        datestring = commercialFile.split(sep='-',maxsplit=1)[0]
    else:
        datestring = commercialFile.split(sep='_',maxsplit=1)[1]
    return datetime.strptime(datestring,'%Y%m%d')



def ipgeIo_sort_helper(entry):
    return get_commercial_date(entry,'ipgeolocation-io')

def maxmind_sort_helper(entry):
    return get_commercial_date(entry,'maxmind')



def find_eligible_db_dirs(inputPath,provider,startdate=None):
    try:
        dataDirObj = os_scandir(inputPath)
    except permissionError as perr:
        print('You do not have permission to read the dataDir specified (\'%(ddir)\')',{'ddir':dataDir})
        raise
    else:
        dbDirs = []
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
                    if startdate is None or startdate <= entrydate:
                        dbDirs.append(entry.name)
                except ValueError:
                    continue
        #sort by date in ascending order
        if ipgeIoStatus:
            dbDirs.sort(key=ipgeIo_sort_helper)
        else:
            dbDirs.sort(key=maxmind_sort_helper)
        return dbDirs




#calculate IPv4 space coverage of a single maxmind DB file (csv)
def calc_maxmind_coverage(commercialDir,provider,dbPath='Data/raw_data/commercial_dbs'):
    entryDate = get_commercial_date(commercialDir,provider)
    providerMap = {
            'maxmind-geoip2': 'GeoIP2-City',
            'maxmind-geolite2':'GeoLite2-City'
            }
    providerBlock = '-'.join([providerMap[provider],'Blocks-IPv4.csv'])
    commercialBlockFile = '/'.join([dbPath,provider,commercialDir,providerBlock])
            
    commercialBlocks  = pd.read_csv(commercialBlockFile,usecols=['network'])#,dtype=blockColTypes)
    coverageSet = netaddr_IPSet(commercialBlocks.loc[:,'network'].to_list())
    return (entryDate,coverageSet.size)


def calc_maxmind_coverages(commercialDirs,provider,dbPath='Data/raw_data/commercial_dbs'):
    retVal=[]
    if isinstance(commercialDirs,list):
        for cdir in commercialDirs:
            retVal.append(calc_maxmind_coverage(cdir,provider,dbPath=dbPath))
    else:
        retVal.append(calc_maxmind_coverage(commercialDirs,provider,dbPath=dbPath))
    return retVal



#for ipgeolocation.io you'll need to pull the intermediate files instead of the raw data files
def calc_ipgeolocationIo_block_coverage(commercialDir,provider,dbPath='Data/cleaned_data/commercial-gfeed-comps/intermediate_data'):
    entryDate = get_commercial_date(commercialDir,'ipgeolocation-io')
    filename = '/'.join([dbPath,provider,commercialDir,'db-ip-geolocation.csv'])
    commercialNets = pd.read_csv(filename,usecols=['commercial_Cidr'])
    coverageSet = netaddr_IPSet(commercialNets.loc[:,'commercial_Cidr'].to_list())
    return (entryDate,coverageSet.size)


def ipgeolocationIo_block_coverages(commercialDirs,provider,dbPath='Data/cleaned_data/commercial-gfeed-comps/intermediate_data'):
    retVal = []
    if isinstance(commercialDirs,list):
        for cdir in commercialDirs:
            retVal.append(calc_ipgeolocationIo_block_coverage(cdir,provider,dbPath=dbPath))
    else:
        retVal.append(calc_ipgeolocationIo_block_coverage(commercialDirs,provider,dbPath=dbPath))
    return retVal


def ipgeIo_range_builder(entry):
    startAddr = entry.loc['start_ip']
    endAddr = entry.loc['end_ip']
    return [cidr for cidr in ipaddress.summarize_address_range(startAddr,endAddr)]



#Intermediate helper for getting CIDR ranges in ipgeolocation.io entries 
# meant to allow multithreading by calling ipgeIo_range_builder on smaller segments of commercialIps (see main ~lines 690-700)  
def find_ipgeIo_range_segment(segment):
    return segment.apply(ipgeIo_range_builder,axis=1)



def calc_ipgeolocationIo_raw_coverage(commercialDir,provider):
    filename = '/'.join([commercialDir,'db-ip-geolocation.csv'])
    return pd.read_csv(filename,usecols=['start_ip','end_ip'])

#Need to add multithreading here so this doesn't take forever
# commercialBlocks = commercialNets.apply(ipgeIo_range_builder


def parse_input():
    desc = "calc_commercial_ip_coverage.py iterates through a directory of commercial DB pulls and calculates each pull's IPv4 space coverage (i.e. number of IPv4 addresses)."
    parser = ArgumentParser(description=desc)
    parser.add_argument('provider',
                       choices=['maxmind-geoip2','maxmind-geolite2','ipgeolocation-io'],
                       help='Name of the commercial IP-geolocation provider to trace. Currently supported options: [\'maxmind-geoip2\',\'maxmind-geolite2\',\'ipgeolocation-io\']')
    parser.add_argument('-s',
                       dest='startDate',
                       type=str,
                       default='',
                       help='Earliest date from which to include data. If not specified all DB pulls from this provider will be included.')
    parser.add_argument('-b',
                       dest='blockFiles',
                       type=bool,
                       default=False,
                       help='For ipgeolocation-io only: Specify whether to use DB files that already specify the results as CIDR blocks. (Default: False) \n If True files will be pulled from \'Data/cleaned_data/commercial-gfeed-comps/intermediate_data/ipgeolocation-io/\' rather than from the default raw data directory.')
    parser.add_argument('-o',
                       dest='outpath',
                       type=str,
                       default='Data/cleaned_data/commercial-gfeed-comps/IPv4Coverage',
                       help='filepath for directory in which to save the output of this script (Default: \'Data/cleaned_data/commercial-gfeed-comps/IPv4Coverage\')')
    return parser.parse_args()






def main():
    params=parse_input()
    partial = validate_input(params)
    commercialDir = '/'.join(['Data/raw_data/commercial_dbs',params.provider])
    ipgeIoStatus = params.provider=='ipgeolocation-io'
    if ipgeIoStatus:
        if not params.blockFiles:
            pass#TODO: Add logic to use concurrency and build up the CIDR blocks, save result to a file under alternate commercialDir location 
        commercialDir = 'Data/cleaned_data/commercial-gfeed-comps/intermediate_data/ipgeolocation-io'
    dirList = []
    if partial:
        dirList = find_eligible_db_dirs(commercialDir,params.provider,startdate=datetime.strptime(params.startDate,'%m.%d.%Y'))
    else:
        dirList = find_eligible_db_dirs(commercialDir,params.provider)
    ncpus = os_cpu_count()
    chunks=[]
    if len(dirList)>ncpus:
        chunkEnd = math.floor(len(dirList)/ncpus)
        chunks = [dirList[i* chunkEnd:(i+1)*chunkEnd] for i in range(0,ncpus)]
        chunks.append(dirList[ncpus*chunkEnd:])
    else:
        chunks = dirList
    if ipgeIoStatus:
        with concurrent.futures.ProcessPoolExecutor() as executor:
            futureCoverageChunks = {executor.submit(ipgeolocationIo_block_coverages,chunk,provider=params.provider): chunk for chunk in chunks}
            metrics = []
            for future in concurrent.futures.as_completed(futureCoverageChunks):
                metricChunk = futureCoverageChunks[future]
                try:
                    listPiece =future.result()
                except Exception:
                    raise
                else:
                    if len(metrics)==0:
                        metrics=[listPiece]
                    else:
                        metrics.append(listPiece)
            flatMetrics = [val for sublist in metrics for val in sublist]
            metricDF = pd.DataFrame(flatMetrics,columns=['db_pull_date(YYYY-MM-DD)','num_IPs'])
            metricDF.sort_values(by='db_pull_date(YYYY-MM-DD)',kind='stable',inplace=True, ignore_index=True)
            metricDF['db_pull_date(YYYY-MM-DD)'] = metricDF.loc[:,'db_pull_date(YYYY-MM-DD)'].apply(lambda x: x.strftime('%Y-%m-%d'))
            try:
                os_mkdir(params.outpath)
            except FileExistsError:
                pass
            (nrows,ncol) = metricDF.shape
            outfileName = '-'.join([metricDF.loc[0,'db_pull_date(YYYY-MM-DD)'],metricDF.loc[nrows-1,'db_pull_date(YYYY-MM-DD)'],params.provider+'_IPv4coverage.csv'])
            metricDF.to_csv('/'.join([params.outpath,outfileName]))
    else:
        with concurrent.futures.ProcessPoolExecutor() as executor:
            futureCoverageChunks = {executor.submit(calc_maxmind_coverages,chunk,provider=params.provider): chunk for chunk in chunks}
            metrics = []
            for future in concurrent.futures.as_completed(futureCoverageChunks):
                metricChunk = futureCoverageChunks[future]
                try:
                    listPiece =future.result()
                except Exception:
                    raise
                else:
                    if len(metrics)==0:
                        metrics=[listPiece]
                    else:
                        metrics.append(listPiece)
            flatMetrics = [val for sublist in metrics for val in sublist]
            metricDF = pd.DataFrame(flatMetrics,columns=['db_pull_date(YYYY-MM-DD)','num_IPs'])
            metricDF.sort_values(by='db_pull_date(YYYY-MM-DD)',kind='stable',inplace=True,ignore_index=True)
            metricDF['db_pull_date(YYYY-MM-DD)'] = metricDF.loc[:,'db_pull_date(YYYY-MM-DD)'].apply(lambda x: x.strftime('%Y-%m-%d'))
            try:
                os_mkdir(params.outpath)
            except FileExistsError:
                pass
            (nrows,ncol) = metricDF.shape
            outfileName = '-'.join([metricDF.loc[0,'db_pull_date(YYYY-MM-DD)'],metricDF.loc[nrows-1,'db_pull_date(YYYY-MM-DD)'],params.provider+'_IPv4coverage.csv'])
            metricDF.to_csv('/'.join([params.outpath,outfileName]))



if __name__== "__main__":
    main()
