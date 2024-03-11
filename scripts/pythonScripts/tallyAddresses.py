#!/usr/bin/python3


from argparse import ArgumentParser 
from datetime import datetime,timedelta

#from ipwhois import IPWhois

import pandas as pd, numpy as np
from os import scandir as os_scandir
# import sys
import pdb#debugging

#classes defined within this repo
from IpCoverageCalculator import IpCoverageCalculator





# Functions moved to separate script b/c they take a long time to run. 
# So we'll load in the result of running this as a csv instead (see queryASNs.py for details). 
# def find_asn(cidr):
#     record = IPWhois(cidr).lookup_whois()
#     return record['asn']
# 
# # For each line in the file:
# # Run whois query on the CIDR's base IP address
# # Extract the ASN from result. (This will be useful for assessing which AS-es/proportion of AS-es that publish (registered) geofeeds.
# def parse_console_file(consolefile):
#     consoleFrame = pd.read_csv(consoleFile,sep=' ',header=None,names=['cidr','geofeed_url'],index_col=False)
#     cidrFrame = consoleFrame['cidr'].str.split('/',expand=True )
#     consoleFrame['asn'] = cidrFrame[0].apply(lambda x: find_asn(x))
#     return consoleFrame



def compare_ip_coverage(consolefile,geoCoverMeter):#,geoTotalAddresses):
    consoleFrame = pd.read_csv(consolefile)
    baseCoverMeter = IpCoverageCalculator(consoleFrame['cidr'].tolist())
    baseIpCover = compute_ranges(baseCoverMeter)
    print('Total IP addresses covered by all participating ASes: ' ,baseIpCover)
    consoleASNs = consoleFrame['asn'].nunique()
    if 'asn_cidr' in consoleFrame.keys():
        baseASNs = consoleFrame.loc[:,('asn','asn_cidr')].drop_duplicates()
        geoIpRanges = pd.Seris(geoCoverMeter.get_ip_ranges())
        geoCidrFrame = geoIpRanges.apply(lambda x:str(x)).str.split('/',expand=True)
        consoleCidrFrame = consoleFrame['asn_cidr'].str.split('/',expand=True)
        #Then will group by prefix length/use this logic to cut down on number of comparisons btw geofeed cidrs and asn cidrs. 
        
        # print('baseASNs (cidrs): \n',baseASNs)
        # print('\n geofeed CIDRS: \n',pd.Series(geoCoverMeter.get_ip_ranges()).apply(lambda x:str(x))

# Calculate the total number of IP addresses included within the geofeed's prefixes
def scan_geofeed_results(cidr_blocks):
    coverMeter = IpCoverageCalculator(cidr_blocks.tolist())
    rangeList = coverMeter.get_ip_ranges()
    totalAddresses = compute_ranges(coverMeter)
    return (coverMeter, rangeList, totalAddresses)


# TODO: Determine/Estimate the total number of used/available IPv4 addresses using routeviews or an acceptable approach/existing measurement.
#       Using that compute the proportion of the IPv4 space is covered within a geofeed-pull's output file. 
def compute_ranges(coverMeter):
    return coverMeter.get_num_addresses()



#map filename to datetime object to allow sorting by date. (Used in filter_by_filetypes)
def sort_helper(entry):
    return datetime.strptime(entry.split('-',1)[0],'%m.%d.%Y')

def infer_infile_dates(dataPath,startDate,endDate,dataDirObj):
    filelist = []
    for entry in dataDirObj:
        if not entry.name.startswith('.') and entry.is_file():
            [collectDate,fsuffix] = entry.name.split('-',1)
            testDate=''
            try:
                testDate = datetime.strptime(collectDate,'%m.%d.%Y')
            except ValueError:
                continue
            else:
                if testDate < startDate or testDate > endDate:
                    continue
                else:
                    filelist.append(entry.name)
    return filelist






def filter_by_filetypes(filelist,filetypes):
    namePatterns = None
    if filetypes=='consoles':
        namePatterns = ['geofeedConsole.csv','geofd-urls.csv']
    elif filetypes == 'geofeeds':
        namePatterns = ['result.csv','ipv4-result.csv']
    else:
        namePatterns = ['geofeedConsole.csv','geofd-urls.csv','result.csv','ipv4-result.csv']
    for entry in filelist:
        suffix = entry.split('-',1)[-1]
        if suffix not in namePatterns:
            filelist.remove(entry)
    if filelist!=[] and filetypes == 'pairs':
        filterFrame = pd.DataFrame([entry.split('-',1) for entry in filelist])
        pairFilterMap = filterFrame[0].value_counts().apply(lambda x:x>1)
        pairFilter = filterFrame[0].map(pairFilterMap)
        filterFrame = filterFrame.where(pairFilter).dropna(how='all')
        filelist = ['-'.join([x,y]) for x,y in zip(filterFrame[0],filterFrame[1])]
    filelist.sort(key=sort_helper)
    return filelist


#For the next three functions: construct the output DF based on the type of filetype(s) passed in/specified

def process_geofeed(datadir,geofeedfiles):
    gfeedBase = []
    for gfile in geofeedfiles:
        pullDate = datetime.strptime(gfile.split('-',1)[0],'%m.%d.%Y')
        filename = datadir+'/'+gfile
        print('filename: ',filename)
        geoFrame = pd.read_csv(filename,names=['CIDR_block', 'country', 'subregion', 'city' ], index_col=False)
        
        #TODO:compute the number of entries w/country-level granularity, subregion-level and city level granularity (?)
        #ipv4 coverage
        (geoCoverMeter, geoRangeList, geoTotalAddresses) = scan_geofeed_results(geoFrame['CIDR_block'])
        entryDict = {
                     'pull-date':pullDate, 'num-CIDRs': len(geoRangeList),
                     'CIDRs':geoRangeList, 'num-ipv4-addresses':geoTotalAddresses
                }
        gfeedBase.append(entryDict)
    return pd.DataFrame(gfeedBase)




def process_gconsole(datadir,consoleFiles):
    consoleBase = []
    for cfile in consoleFiles:
        pullDate = datetime.strptime(cfile.split('-',1)[0],'%m.%d.%Y')
        print(f'cfile: {"/".join([datadir,cfile])}')
        consoleFrame = pd.read_csv(datadir+'/'+cfile,names=['CIDR_block', 'geofeed_url' ], sep=' ', index_col=False)#(consoleCoverMeter, consoleRangeList, consoleTotalAddresses) = scan_geofeed_results(consoleFrame['CIDR_block'])
        if not consoleFrame.loc[:,'geofeed_url'].any():
            consoleFrame = consoleFrame.drop(columns='geofeed_url').rename(columns={'CIDR_block': 'geofeed_url'})
        entryDict = {
                     'pull-date':pullDate, 'num-geofeedURLs': consoleFrame['geofeed_url'].nunique(),
                     'geofeed-urls': consoleFrame['geofeed_url'].unique()
                }
        consoleBase.append(entryDict)
    return pd.DataFrame(consoleBase)

# Currently Omitted fields
# 'num-CIDRs': len(consoleRangeList), 'CIDRs': geoRangeList,
# 'num-ipv4-addresses':consoleTotalAddresses



def process_pairs():
    pass#placeholder





#TODO: Update the script to run on multiple files within the directory passed in.
# Steps to Complete:
# 1.(DONE) Add parameters indicating the input directory (i.e. dir from which files should be read in), start date, end date and filetypes
# 2.(DONE) Update filter_by_filetypes to account for filename suffixes from other directories that may be passed in for analysis.
# 3.(DONE) Split code from main that reads in Dataframe and calls scan_geofeed_results into its own function
#               so that it can be invoked for each file/pair of files read in for a given date.
# Remaining steps/those currently in progress (as of Jan. 30 2023 at 9:40pm EST): 
# 4. Define how functionality will differ based on filetypes specified: namely which data fields will each option's DF include?
# 5. Write (and debug) code that constructs frames determined in (4)
# 6. Update main() to write result to file. 


def parse_inputs():
    parser = ArgumentParser(description = 'tallyAddresses.py - Assesses the input\'s coverage of the IPv4 address space currently in use.')
    parser.add_argument('-f',
                        dest='filetypes',
                        choices=['consoles','geofeeds','pairs'],
                        required=True,
                        help='Types of files to be considered: Options are \'consoles\', \'geofeeds\', \'pairs\''
                        )
    parser.add_argument('-d',
                       dest='datadir',
                       type=str,
                       default='Data/cleaned_data/ipv4-geofeed-records',
                       help='Complete or relative path to directory holding input datafiles to be processed (Default: Data/cleaned_data/ipv4-geofeed-records)'
                       )
    parser.add_argument('-s',
                       dest='startdate',
                       type=str,
                       default='04.02.2022',
                       help='String indicating the earliest date from which to include data of form MM.DD.YYYY (Default: \'01.01.2022\')')
    parser.add_argument('-e',
                        dest='enddate',
                        type=str,
                        default=datetime.today().strftime('%m.%d.%Y'),
                        help='String of form MM.DD.YYYY indicating latest date from which input data should be considered (Defaults to today\'s date)')
    parser.add_argument('-o',
                        dest='outdir',
                        type=str,
                        default='Data/cleaned_data/gfeed_ipv4_metrics',
                        help='(Path to) file directory in which to write output data files.')
    return parser.parse_args()




def validate_inputs():
    params = parse_inputs()
    #date validation
    startdate = datetime.strptime(params.startdate,'%m.%d.%Y')
    if startdate < datetime(2022,1,1):
        raise ValueError('startdate falls before this project began. Ensure startdate falls after January 01, 2022.')
    enddate = datetime.strptime(params.enddate,'%m.%d.%Y')
    if enddate > datetime.today():
        raise ValueError('enddate parameter (\-e) cannot specify a date in the future.')

    if startdate > enddate:
        raise ValueError('enddate cannot be before startdate.')

    #Ensure datadir and outdir exist and are accessible
    try:
        dataDirObj = os_scandir(params.datadir)
    except PermissionError as perr:
        print('You do not have permission to read the dataDir specified (\'%(ddir)\')',{'ddir':params.dataDir})
        raise
    else:
        return (startdate,enddate,params.datadir,dataDirObj,params.outdir,params.filetypes)





def main():
    (startdate,enddate,datadir,dataDirObj,outdir,filetypes) = validate_inputs()
    filelist = infer_infile_dates(datadir,startdate,enddate,dataDirObj)
    dataDirObj.close()
    filelist = filter_by_filetypes(filelist,filetypes)
    print('filetypes: ',filetypes)
    if filetypes=='consoles':
        gconsoleFrame = process_gconsole(datadir,filelist)
        outfilename = '-'.join([startdate.strftime('%m.%d.%Y'),enddate.strftime('%m.%d.%Y'),'gconsoleOrgs.csv'])
        gconsoleFrame.to_csv('/'.join([outdir,outfilename]))
    elif filetypes=='geofeeds':
        gfeedFrame = process_geofeed(datadir,filelist)
        # pdb.set_trace()
        outfilename = '-'.join([startdate.strftime('%m.%d.%Y'),enddate.strftime('%m.%d.%Y'),'geofeedIPv4Coverage.csv'])
        
        gfeedFrame.to_csv('/'.join([outdir,outfilename]))
    else:
        pass#placeholder




if __name__ == "__main__":
    main()
