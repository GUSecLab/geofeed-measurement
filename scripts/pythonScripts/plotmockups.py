#!/usr/bin/python3



from argparse import ArgumentParser 
from datetime import datetime,timedelta
from os import mkdir as os_mkdir,scandir as os_scandir

import pdb#debugging

import matplotlib.pyplot as plt
import pandas as pd, numpy as np
import pycountry


#Axis formatting functions
def numString(x,pos):
    if x>= 1e6:
        s = '{:1.0f} million'.format(x*1e-6)
    elif x >= 1e3:
        s = '{:1.0f} thousand'.format(x*1e-3)
    else:
        s = '{:1.0f}'.format(x)
    return s


#percentages (e.g. for CDFs)
def pcent(x,pos):
    if x <= 1:
        s = '{:1.1f}%'.format(x*100)
    else:
        s =x
    return s



#function for quick bar graph mockup
def mockup_bar_plot(infilePath,cols,title,xAxis,yAxis,savePath):
    fig, ax = plt.subplots(figsize=(7,3.5),layout='constrained')
    ipHistMapFrame = pd.read_csv(infilePath,usecols=cols,index_col=False)#'Data/cleaned_data/gfeed_ipv4_metrics/01.01.2022-02.01.2023-geofeedIPv4Coverage.csv',usecols=['pull-date','num-ipv4-addresses'],index_col=False)
    ax.bar(ipHistMapFrame.loc[:,cols[0]], ipHistMapFrame.loc[:,cols[1]])#ax.bar(ipHistMapFrame.loc[:,'pull-date'], ipHistMapFrame.loc[:,'num-ipv4-addresses'])
    # ax.set_title(title,fontsize='xx-large',fontweight='semibold')
    ax.set_xlabel(xAxis,fontsize='large')
    ax.set_ylabel(yAxis,fontsize='large')
    labels = ax.get_xticklabels()
    plt.setp(labels, rotation=40, horizontalalignment='right',fontsize='small')
    ax.yaxis.set_major_formatter(numString)
    ax.margins(0.01,0.05)
    fig.savefig(savePath, transparent=False, format='pdf', bbox_inches="tight")



def sort_helper(ser):
    return ser.apply(lambda x: datetime.strptime(x,'%m.%d.%Y'))





def build_birds_eye_metFrame(dirPath,provider):
    try:
        dataDirObj = os_scandir(dirPath)
    except PermissionError as perr:
        print(f'You do not have permission to read directory at {dirPath}')
        raise
    else:
        retFrame = pd.DataFrame()
        dateList = []
        for entry in dataDirObj:
            if not entry.name.startswith('.') and entry.is_file():
                tokens = entry.name.split(sep='-')[:-1]
                if 'gfeed'+provider=='-'.join(tokens[2:]):
                    [gfeedDate,commercialDate] = tokens[:2]
                    dateList.append((gfeedDate,commercialDate))
                    miniFrame =  pd.read_csv(entry.path,index_col=0)
                    if retFrame.empty:
                        retFrame = miniFrame
                    else:
                        retFrame = pd.concat([retFrame,miniFrame],ignore_index=True)
        dataDirObj.close()
        if len(dateList) > 0:
            (nrows,ncol) = retFrame.shape
            if nrows==len(dateList):
                dateFrame = pd.DataFrame(dateList,columns=['gfeedPullDate','commercialPullDate'])
                retFrame.insert(0,'gfeedPullDate',dateFrame.loc[:,'gfeedPullDate'])
                retFrame.insert(1,'commercialPullDate',dateFrame.loc[:,'commercialPullDate'])
                retFrame.sort_values(by='commercialPullDate',inplace=True,kind='stable',key=sort_helper)
                return retFrame.set_index(['gfeedPullDate','commercialPullDate'])
        return retFrame




def date_help_fxn(entry):
    return '/\n'.join([entry['gfeedPullDate'],entry['commercialPullDate']])




#plot stacked line graph w/total overlapping IPs,matching geoloc ips, stale IPs
def plot_matchStales(birdsEyeFrame,provider,savePath):
    providerMap = {
            'maxmind-geoip2': 'Maxmind GeoIP2',
            'maxmind-geolite2': 'Maxmind GeoLite2',
            'ipgeolocation-io': 'Ipgeolocation.io'
            }
    fig,ax = plt.subplots(figsize=(6.25,3),layout='constrained')
    plotDates = ['('+partA+',\n'+partB +')' for partA,partB in birdsEyeFrame.index.values.tolist()]
    birdFieldList = [
        birdsEyeFrame.loc[:,'matching_geoloc_ips(error<=5km)'].to_list(),
        birdsEyeFrame.loc[:,'mismatching_ips(error >5km)'].to_list()]
        #birdsEyeFrame.loc[:,'stale_ips'].to_list()]
    ax.stackplot(plotDates,birdFieldList,
            labels=['matching_geoloc_ips(error<=5km)','mismatching_ips(error >5km)'], alpha=0.8)#'stale_ips'
    ax.legend(loc='lower right')#try if you don't like where legend is automatically placed
    #ax.set_title(f'{providerMap[provider]} Coverage and Accuracy',fontsize='xx-large')
    ax.set_xlabel('Collection Dates (geofeed,commercial)')
    ax.set_ylabel('Total IPv4 Addresses')
    labels = ax.get_xticklabels()
    plt.setp(labels, rotation=40, horizontalalignment='right',fontsize='small')
    ax.yaxis.set_major_formatter(numString)
    ax.margins(0,0.05)
    fig.savefig('/'.join([savePath,provider+'_geoloc_accuracy_stackPlt.pdf']), transparent=False, dpi=80, format='pdf', bbox_inches="tight")





def plot_CountryMatches(birdsEyeFrame,provider,savePath):
    providerMap = {
            'maxmind-geoip2': 'Maxmind GeoIP2',
            'maxmind-geolite2': 'Maxmind GeoLite2',
            'ipgeolocation-io': 'Ipgeolocation.io'
            }
    matches = birdsEyeFrame.loc[:,['matching_geoloc_ips(error<=5km)','total_overlapping_ips']].apply(lambda x: x['matching_geoloc_ips(error<=5km)']/x['total_overlapping_ips'],axis=1)
    mismatchPcent = birdsEyeFrame.loc[:,['total_overlapping_ips','mismatching_ips(error >5km)']].apply(lambda w: w['mismatching_ips(error >5km)']/w['total_overlapping_ips'],axis=1)
    ctryMismatchPcent = birdsEyeFrame.loc[:,['total_overlapping_ips','country_level_matched_ips']].\
            apply(lambda x: (x['total_overlapping_ips']-x['country_level_matched_ips'])/x['total_overlapping_ips'],axis=1)
    nonCtryMismatchPcent = pd.concat([mismatchPcent,ctryMismatchPcent],axis=1).apply(lambda z: z[0] -z[1],axis=1)
    fieldList = [
            ctryMismatchPcent.to_list(),
            nonCtryMismatchPcent.to_list(),
            ]
    fig,ax = plt.subplots(figsize=(7,3.2),layout='constrained')
    plotDates = birdsEyeFrame.reset_index().loc[:,['gfeedPullDate','commercialPullDate']].apply(lambda x: date_help_fxn(x),axis=1)
    ax.stackplot(plotDates,fieldList,labels=['mismatched country','country match/city mismatch','fully match'],alpha=0.8)
    ax.legend(loc='lower right')
    #ax.set_title(f'Normalized Accuracy/Inaccuracy Breakdown \n of {providerMap[provider]}\'s Estimates')
    ax.set_xlabel('Collection Dates: Geofeed (MM.DD.YYYY)/\n Commercial DB (MM.DD.YYYY)')
    ax.set_ylabel('Prop. Overlapping IPv4 Addresses')
    labels = ax.get_xticklabels()
    plt.setp(labels, rotation=50, horizontalalignment='right',fontsize='small')
    ax.legend(loc='center left')#try automatic and if need be fix it
    ax.margins(0,0.05)
    ax.yaxis.set_major_formatter(pcent)
    fig.savefig('/'.join([savePath,provider+'accuracy_byCountry_stackPlt.pdf']), transparent=False, dpi=80, format='pdf', bbox_inches="tight")




def expand_birds_eye_mets(dirPath,provider):
    birdsEyeFrame = build_birds_eye_metFrame(dirPath,provider)
    if birdsEyeFrame.empty:
        return
    ctryLvlAccuracy = birdsEyeFrame.loc[:,['country_level_matched_ips','total_overlapping_ips']].apply(lambda x: x['country_level_matched_ips']/x['total_overlapping_ips'],axis=1)
    locBasedAccuracy = birdsEyeFrame.loc[:,['matching_geoloc_ips(error<=5km)','total_overlapping_ips']].apply(lambda x: x['matching_geoloc_ips(error<=5km)']/x['total_overlapping_ips'],axis=1)
    inaccuracy = birdsEyeFrame.loc[:,['mismatching_ips(error >5km)','total_overlapping_ips']].apply(lambda x: x['mismatching_ips(error >5km)']/x['total_overlapping_ips'],axis=1)
    birdsEyeFrame.insert(3,'%_geoloc_match_ips',locBasedAccuracy)
    birdsEyeFrame.insert(5,'%_country_lvl_matches',ctryLvlAccuracy)
    staleFractions = birdsEyeFrame.loc[:,['stale_ips','total_overlapping_ips']].apply(lambda x: x['stale_ips']/x['total_overlapping_ips'],axis=1)
    birdsEyeFrame.insert(9,'%_stale_ips',staleFractions)
    birdsEyeFrame.to_csv('/'.join([dirPath,provider +'-birdsEyeMetricsDf.csv']))
    return birdsEyeFrame






def plot_layered_cdf(dfPathA,dfPathB,provider,outDir):
    monthMap = {
            1:'January',2:'February',
            3:'March', 4:'April',
            5:'May', 6:'June',
            7:'July', 8: 'August',
            9:'September',10:'October',
            11: 'November', 12: 'December'}
    dfFileNmA = dfPathA.split(sep='/')[-1]
    datePairA = dfFileNmA.split(sep='-')[:2]
    monthValsA = [date.split(sep='.')[0] for date in datePairA]
    monthA = monthMap[int(min(monthValsA))]
    datePairA = dfFileNmA.split(sep='-')[:2]
    dfFileNmB = dfPathB.split(sep='/')[-1]
    datePairB = dfFileNmB.split(sep='-')[:2]
    monthValsB = [date.split(sep='.')[0] for date in datePairB]
    monthB = monthMap[int(min(monthValsB))]
    
    dataFrameA = pd.read_csv(dfPathA,index_col=0)
    fig,ax = plt.subplots(figsize=(5,3),layout='constrained')
    n, bins, patches = ax.hist(dataFrameA.loc[:,'estimated_error(km)'],600,density=True,histtype='step',cumulative=True, label=f'observed_incidence:{monthA} 2023')

    dataFrameB = pd.read_csv(dfPathB,index_col=0)
    ax.hist(dataFrameB.loc[:,'estimated_error(km)'],bins=bins, density=True, histtype='step', cumulative=True,label=f'observed_incidence: {monthB} 2023')
    ax.set_xscale("log")
    ax.legend()#loc='lower right')
    #ax.set_title(f'Cumulative Error Distributions for {provider}')
    ax.set_xlabel('Estimated Error (km)')
    ax.set_ylabel('Cumulative incidence')
    ax.margins(0.01,0.05)
    saveName = '-'.join([monthA,monthB,provider,'combinedErrorDistCDFsLogScale.pdf'])
    savePath = '/'.join([outDir,saveName])
    fig.savefig(savePath, transparent=False, dpi=80, format='pdf', bbox_inches="tight")





def plot_cdf(dfPath,outDir):
    dataFrame = pd.read_csv(dfPath,index_col=0)
    fig,ax = plt.subplots(figsize=(5,3),layout='constrained')
    n, bins, patches = ax.hist(dataFrame.loc[:,'estimated_error(km)'],600,density=True,histtype='stepfilled',cumulative=True, label='observed_incidence')
    ax.set_title('Cumulative Error Distribution')
    ax.set_xlabel('Estimated Error (km)')
    ax.set_ylabel('Cumulative incidence')
    ax.margins(0.01,0.05)
    fname = dfPath.split(sep='/')[-2]#Since the last folder always contains the metadata info use that instead of the filename itself
    baseTokens = fname.split(sep='-')[:-1]
    saveName = '-'.join(baseTokens+['errorDistanceCDF.pdf'])
    savePath = '/'.join([outDir,saveName])
    fig.savefig(savePath, transparent=False, dpi=80, format='pdf', bbox_inches="tight")




def plot_misloc_error(dfPath,provider,savePath):
    providerMap = {
            'maxmind-geoip2': 'Maxmind GeoIP2',
            'maxmind-geolite2': 'Maxmind GeoLite2',
            'ipgeolocation-io': 'Ipgeolocation.io'
            }
    providerName = provider
    if provider in providerMap.keys():
        providerName = providerMap[provider]
    dataFrame = pd.read_csv(dfPath)
    fig,ax = plt.subplots(figsize=(5,3),layout='constrained')
    altDF = dataFrame.set_index(keys=['gfeedPullDate','commercialPullDate'])
    plotDates = ['('+partA+',\n'+partB +')' for partA,partB in altDF.index.values.tolist()]
    errorMarks = altDF.loc[:,'mean_misloc_error_dist'].to_list()
    ax.stackplot(plotDates,[errorMarks])
    ax.set_title(f'Mean Mislocation Error for {providerName}')
    ax.set_xlabel('Collection Dates: Geofeed (MM.DD.YYYY)/\n Commercial DB (MM.DD.YYYY)')
    ax.set_ylabel('Mislocation Error (km)')
    ax.margins(0.01,0.05)
    labels = ax.get_xticklabels()
    plt.setp(labels, rotation=30, horizontalalignment='right',fontsize='small')
    ax.yaxis.set_major_formatter(numString)
    fig.savefig(savePath, transparent=False, dpi=80, format='pdf', bbox_inches="tight")





#Scan <dirPath> and find all elements of filetype <targetType> (i.e. 'dir' or 'file') and describing provider <provider>
def find_target_dirs(dirPath,provider,targetType='dir'):
    try:
        dataDirObj = os_scandir(dirPath)
    except PermissionError as perr:
        print(f'You do not have permission to read directory at {dirPath}')
        raise
    else:
        retVal = []
        for entry in dataDirObj:
            checkMap = {
                'dir': entry.is_dir(),
                'file': entry.is_file()
                }
            if not entry.name.startswith('.') and checkMap[targetType]:
                nameComps = entry.name.split(sep='-')
                if len(nameComps)==5:
                    [gfeedPullDate,commercialPullDate,comProvider,comProdType,stub] = entry.name.split(sep='-')
                    if 'gfeed'+provider != '-'.join([comProvider,comProdType]):
                        continue
                elif len(nameComps) !=1:
                    continue
                retVal.append(entry.name)
        dataDirObj.close()
        return retVal






#Automatically generate a latex table from the bad guess DF, write the result to outFilePath
def make_bad_guess_table(fpath,provider,outPath):
    mapping = {
            'maxmind-geoip2':'country_iso_code.1',
            'maxmind-geolite2': 'country_iso_code.1',
            'ipgeolocation-io': 'commercial_country_iso_code'
            }
    tokens = fpath.split(sep='/')
    fname = tokens[-1]
    baseTokens = tokens[-2].split(sep='-')[:-1]
    outFileBase = '/'.join([outPath,baseTokens[0]])
    outFilePath = '-'.join([outFileBase] + baseTokens[1:] + ['badGuessCountries.tex'])

    badGuessFrame =  pd.read_csv(fpath,index_col=0)
    countryNames = badGuessFrame.loc[:,mapping[provider]].apply(lambda x: pycountry.countries.get(alpha_2=x).name if pycountry.countries.get(alpha_2=x) is not None else x)
    badGuessFrame.insert(0,provider+' guessed country',countryNames)
    badGuessFrame.rename(columns={'num_ips': 'Mislocated IPv4 Addresses'},inplace=True)
    badGuessFrame.drop(columns=mapping[provider],inplace=True)
    with open(outFilePath,'w+') as ofile:
        print(badGuessFrame.style.to_latex(),file=ofile,flush=True)
    return





#Automatically generate a latex table from the bad guess DF, write the result to outFilePath
def make_mislocated_countries_table(fpath,provider,outPath):
    mapping = {
            'maxmind-geoip2':'country_iso_code',
            'maxmind-geolite2': 'country_iso_code',
            'ipgeolocation-io': 'gfeed_country_iso_code'
            }
    tokens = fpath.split(sep='/')
    fname = tokens[-1]
    baseTokens = tokens[-2].split(sep='-')[:-1]
    outFileBase = '/'.join([outPath,baseTokens[0]])
    outFilePath = '-'.join([outFileBase] + baseTokens[1:] + ['mislocatedCntries.tex'])
    badGuessFrame =  pd.read_csv(fpath,usecols=[mapping[provider],'total_ips','mislocated_ctry_ips','error_fraction'])
    countryNames = badGuessFrame.loc[:,mapping[provider]].apply(lambda x: pycountry.countries.get(alpha_2=x).name)
    badGuessFrame.insert(0,'Country (as given by geofeed)' ,countryNames)
    badGuessFrame.rename(columns={'num_ips': 'Mislocated IPv4 Addresses'},inplace=True)
    badGuessFrame.drop(columns=mapping[provider],inplace=True)
    with open(outFilePath,'w') as ofile:
        print(badGuessFrame.style.to_latex(),file=ofile,flush=True)
    return












def fname_series_group_mapper(entry):
    retVal = entry.split(sep='/')[-1]
    if '-' in retVal:
        return retVal.split(sep='-')[-1]
    return retVal





def parse_inputs():
    desc = 'plotmockups.py - plots relevant DF\'s and saves the figures'
    parser = ArgumentParser(description = desc)
    parser.add_argument('provider',
                         choices=['maxmind-geoip2','maxmind-geolite2','ipgeolocation-io'],
                        help='Name of the commercial IP-geolocation data source. Currently supported options: [\'maxmind-geoip2\',\'maxmind-geolite2\',\'ipgeolocation-io\']')
    parser.add_argument('-d',
                        dest='dataDir',
                        type=str,
                        default='Data/cleaned_data/commercial-gfeed-comps/metrics',
                        help='Full or relative path for directory holding the data files to be plotted')
    parser.add_argument('-o',
                        dest='outPath',
                        type=str,
                        default = 'figures/mockups',
                        help='Complete or relative path to directory in which the generated figures (and tables) should be stored (default: figures/mockups)')
    # parser.add_argument()
    # parser.add_argument()
    return parser.parse_args()






def main():
    params = parse_inputs()
    birdsEyeDir = '/'.join([params.dataDir,'top_level_metrics'])
    errorDir = '/'.join([params.dataDir,'mislocation_error'])
    staleDir = '/'.join([params.dataDir,'stale_locations'])#Not sure whether I'll need this one
    tableOPath = '/'.join([params.outPath,'tables'])
    compErrDirs = find_target_dirs(errorDir, params.provider)
    longFleList = []
    for dirVal in compErrDirs:
        fleList = find_target_dirs('/'.join([errorDir,dirVal]),params.provider,targetType='file')
        longFleList += ['/'.join([errorDir,dirVal,entry]) for entry in fleList]
    lFileSeries = pd.Series(longFleList)
    #group results by the type of DF (i.e. what the DF provides a breakdown of)
    fleTypeGrps = lFileSeries.value_counts().groupby(fname_series_group_mapper)
    dfSubjectMapper = {
            'badCountryGuessOverview.csv': lambda x: make_bad_guess_table(x,params.provider,'/'.join([tableOPath,'bad_geoloc_estimates'])),
            'errorDistCDF.csv': lambda y: plot_cdf(y,params.outPath),
            'countryMisLocs.csv': lambda z: make_mislocated_countries_table(z,params.provider,'/'.join([tableOPath,'mislocated_countries'])),
            'error/badCountryGuessOverview.csv':lambda w: make_bad_guess_table(w,params.provider,'/'.join([tableOPath,'mislocated_countries']))
            }#This should be a map btw group name and the fx'n to be called
    #Generate all of the figures and tables (:-D)
    for name,group in fleTypeGrps:
        group.reset_index().loc[:,'index'].apply(dfSubjectMapper[name])
    #TODO Somehow determine how that maps to where these files should be stored to enable figure/table creation in bulk
    birdsEyeFrame = expand_birds_eye_mets(birdsEyeDir,params.provider)
    plt.rcParams.update({'figure.autolayout': True})
    plot_matchStales(birdsEyeFrame,params.provider,params.outPath+'/overall-metrics/commercial-gfeed-comps')
    plot_CountryMatches(birdsEyeFrame,params.provider,params.outPath +'/overall-metrics/commercial-gfeed-comps')
    







if __name__== "__main__":
    main()
