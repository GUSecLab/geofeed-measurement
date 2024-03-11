#!/usr/bin/python3

import sys,pprint,math
import itertools
import pdb#debugging
import pandas as pd, numpy as np
from os import scandir as os_scandir
from argparse import ArgumentParser
from datetime import datetime,timedelta
# from netaddr import IPNetwork




def parse_inputs():
    parser = ArgumentParser(description = 'processCymruASNs.py counts and compares ASN records across outputs from Cymru IP to ASN Mapping tool')
    parser.add_argument('filetypes',
                        choices=['consoles','geofeeds','pairs'],
                        help='The types of files across which sets of ASNs should be compared. The options are: \'consoles\':geofeed console files only, \'geofeeds\':geofeed output files only, \'pairs\': pairs geofeed output and console files arranged by their date of collection. \n Note: collection dates are determined by the dates given in the filenames.')
    parser.add_argument('-s',
                        dest='startdate',
                        type=str,
                        default='04.02.2022',
                        help='Earliest date of input file to include. Date should be of form MM.DD.YYYY')#Default is date of 1st geofeed data collection
    parser.add_argument('-e',
                        dest='enddate',
                        default=datetime.today().strftime('%m.%d.%Y'),
                        help='Latest date of input file to include. Date should be of form MM.DD.YYYY')
    # parser.add_argument('-c',
    #                     dest='consolefile',
    #                     type=str,
    #                     required=True,
    #                     help='(Path to) file holding bulk ASN query outputs for CIDRs/IPv4 addresses listed within geofeed console')
    parser.add_argument('-d',
                        dest='dataDir',
                        type=str,
                        default='Data/cleaned_data/geofeed-srcs-withCIDRs/asn_matched',
                        help='(Path to) file directory in which to look for input data files.')
    parser.add_argument('-o',
                        dest='outdir',
                        type=str,
                        default='Data/cleaned_data/geofeed-srcs-withCIDRs/gfeed_asn_metrics',
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
        dataDirObj = os_scandir(params.dataDir)
    except PermissionError as perr:
        print('You do not have permission to read the dataDir specified (\'%(ddir)\')',{'ddir':params.dataDir})
        raise

    try:
        outDirObj = os_scandir(params.outdir)
    except PermissionError as perr:
        print('You do not have permission to access the output directory specified (\'%(odir)\')',{'odir':params.outdir})
        raise
    else:
        outDirObj.close()
        return (startdate,enddate,params.dataDir,dataDirObj,params.outdir,params.filetypes)





def tally_asns(asnFrame):
    totalCount = asnFrame['AS'].nunique()
    includedASes = set(asnFrame['AS'].unique().tolist())
    return (totalCount, includedASes)



#map filename to datetime object to allow sorting by date. (Used in filter_by_filetypes)
def sort_helper(entry):
    return datetime.strptime(entry.split('-',1)[0],'%m.%d.%Y')




def infer_infile_dates(dataPath,startDate,endDate,dataDirObj):
    filelist = []
    for entry in dataDirObj:
        if not entry.name.startswith('.') and entry.is_file():
            [collectDate,ftype,_] = entry.name.split('-')
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



# Based on filetypes parameter (choices= ['consoles','geofeeds','pairs'])
#   ensure all entries within list of files to be read-in are of the type specified.
# As hinted at in the code below geofeed contents and console info with their associated ASNs
#  should all be based in the dataDir parameter 
#  (or, in most cases: Data/cleaned_data/geofeed-srcs-withCIDRs/asn_matched). 

def filter_by_filetypes(filelist,filetypes):
    namePatterns = None
    if filetypes=='consoles':
        namePatterns = ['gconsoleCymru-asn.csv']
    elif filetypes == 'geofeeds':
        namePatterns = ['gfeedCymru-asn.csv']
    else:
        namePatterns = ['gconsoleCymru-asn.csv','gfeedCymru-asn.csv']
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






def process_pairs(filelist,dataPath):
    retList = []
    splitFileList =pd.DataFrame([entry.split('-',1) for entry in filelist])
    for key,group in splitFileList.groupby(0,sort=False):
        entry = {'pull_date':key}
        consoleAsnFrame=None
        gfeedAsnFrame=None
        for member in group.values.tolist():
            fullPath = '/'.join([dataPath, '-'.join(member)])
            dtypes = {
                    'AS':'Int32','IP':str,
                    'BGP Prefix':str, 'CC':str,
                    'Registry':str, 'Allocated;Info':str,
                    'AS Name':str
                    }
            if member[1]=='gconsoleCymru-asn.csv':
                print(f'Line 163: fullPath: {fullPath}')
                consoleAsnFrame = pd.read_csv(fullPath,sep=';',header=0,dtype=dtypes,index_col=False)
                (consoleAsnCount,consoleASNs) = tally_asns(consoleAsnFrame)
                entry['console_ASNs'] = consoleASNs
                entry['console_ASN_count'] = consoleAsnCount
            else:
                gfeedAsnFrame = pd.read_csv(fullPath,sep=';',header=0,dtype=dtypes,index_col=False)
                (gfeedAsnCount,gfeedASNs) = tally_asns(gfeedAsnFrame)
                entry['geofeed_ASNs'] = gfeedASNs
                entry['geofeed_ASN_count'] = gfeedAsnCount
        entry['console_-_gfeed_ASNs'] = entry['console_ASNs'] - entry['geofeed_ASNs']
        entry['console_-_gfeed_ASN_Count'] = len(entry['console_ASNs'] - entry['geofeed_ASNs'])
        entry['gfeed_-_console_ASNs'] = entry['geofeed_ASNs'] - entry['console_ASNs']
        entry['gfeed_-_console_ASN_count'] = len(entry['geofeed_ASNs'] - entry['console_ASNs'])
        retList.append(entry)
    return pd.DataFrame(retList)
        



def process_singles(filelist,dataPath,filetypes):
    retList = []
    for fle in filelist:
        fullPath = '/'.join([dataPath,fle])
        dtypes = {
                    'AS':'Int32','IP':str,
                    'BGP Prefix':str, 'CC':str,
                    'Registry':str, 'Allocated;Info':str,
                    'AS Name':str
                    }
        entryFrame = pd.read_csv(fullPath,sep=';',header=0,dtype=dtypes,index_col=False)
        (entryAsnCount,entryASNs)  = tally_asns(entryFrame)
        filetypes = filetypes[:-1]
        entry = {
                'pull_date': fle.split('-',1)[0],
                '_'.join([filetypes,'ASNs']): entryASNs,
                '_'.join([filetypes,'ASN','Count']):entryAsnCount
                }
        retList.append(entry)
        return pd.DataFrame(retList)





def main():
    (startdate,enddate,dataPath,dataDirObj,outdir,filetypes) = validate_inputs()
    filelist = infer_infile_dates(dataPath,startdate,enddate,dataDirObj)
    dataDirObj.close()
    filelist = filter_by_filetypes(filelist,filetypes)
    
    if filetypes=='pairs':
        resFrame = process_pairs(filelist,dataPath)
        # pdb.set_trace()
        if startdate==enddate:
            resFrame.to_csv('/'.join([outdir,startdate.strftime('%m.%d.%Y') + '_consoleGfeed_AsnComparison.csv']))
        else:
            resFrame.to_csv('/'.join([outdir,startdate.strftime('%m.%d.%Y') + '-' + enddate.strftime('%m.%d.%Y') + '_consoleGfeed_AsnComparison.csv']))
    else:
        resFrame = process_singles(filelist,dataPath,filetypes)
        # pdb.set_trace()
        if startdate == enddate:
            resFrame.to_csv('/'.join([outdir,'_'.join([startdate.strftime('%m,%d.%Y'), filetypes[:-1] + 'ASNs.csv'])]))
        resFrame.to_csv('/'.join([outdir,'_'.join([startdate.strftime('%m.%d.%Y') + '-' + enddate.strftime('%m.%d.%Y'), filetypes + 'ASNs.csv'])]))
    


if __name__== "__main__":
    main()
