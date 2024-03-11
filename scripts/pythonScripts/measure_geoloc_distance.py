#!/usr/bin/python3

import ipaddress,collections#,json #not sure json is needed
from argparse import ArgumentParser 
from datetime import datetime,timedelta
from os import cpu_count as os_cpu_count, mkdir as os_mkdir,scandir as os_scandir#determine whether you actually need the last one
import pdb#debugging

import pandas as pd, numpy as np
import pycountry
import country_converter as coco
from netaddr import IPSet as netaddr_IPSet, IPRange as netaddr_IPRange
from pyxdameraulevenshtein import damerau_levenshtein_distance as DL_dist, normalized_damerau_levenshtein_distance as normalized_DL_dist
from thefuzz import fuzz
from geopy.distance import distance as flat_distance#dist metrics assume all locations are at sea level (i.e. elev. of 0)
from deep_translator import GoogleTranslator






#determine the number of CIDR block prefixes and IPv4 addresses for each country within a geofeed 
def count_gfeed_ips_by_country(gfeedFrame):
    countryGrps = gfeedFrame.groupby('country_iso_code')
    countryCounts = dict()
    prefixes = dict()
    for name,group in countryGrps:
        ipv4Set = netaddr_IPSet(group.loc[:,'ip_prefix'].to_list())
        nrows,ncol=group.shape
        prefixes[name]=nrows
        countryCounts[name] = ipv4Set.size
    countryIpFrame = pd.DataFrame(sorted(countryCounts.items(), key=lambda item: item[1],reverse=True),columns=['country_iso_code','num_ips'])
    countryIpFrame = countryIpFrame.set_index('country_iso_code')
    prefixFrame = pd.DataFrame(sorted(prefixes.items(), key=lambda item: item[1],reverse=True),columns=['country_iso_code','num_prefixes'])
    prefixFrame = prefixFrame.set_index('country_iso_code')
    prefixFrame.insert(1,'num_ips',countryIpFrame.loc[:,'num_ips'])
    return prefixFrame



# using count_gfeed_ips_by_country(), build table comparing makeup of the first and last geofeed pulls
# Note: This function (and the helper above) were both run within the Ipython console, and are included for completeness.
# Therefore some of the values are hard-coded, and it is not actually called by main()
def build_gfeed_countrywise_comparison(lastFile,firstFile='Data/cleaned_data/ipv4-geofeed-records/04.03.2022-ipv4-result.csv'):
    firstGfeed = pd.read_csv(firstFile,
            names=['ip_prefix','country_iso_code','subregion_iso_code','city_name','blank'],
            usecols=['ip_prefix','country_iso_code','subregion_iso_code','city_name'])
    firstGfeedMetrics = count_gfeed_ips_by_country(firstGfeed)
    lastGfeed = pd.read_csv(lastFile,
            names=['ip_prefix','country_iso_code','subregion_iso_code','city_name','blank'],
            usecols=['ip_prefix','country_iso_code','subregion_iso_code','city_name'])
    lastGfeedMetrics = count_gfeed_ips_by_country(lastGfeed)
 
    firstFname = firstFile.split('/')[-1]
    lastFname = lastFile.split('/')[-1]
 
    firstDate = datetime.strptime(firstFname.split('-')[0],'%m.%d.%Y').strftime('%b_%d_%Y')
    lastDate = datetime.strptime(lastFname.split('-')[0],'%m.%d.%Y').strftime('%b_%d_%Y')
 
    # Since first gfeed has more countries represented than the last one,
    # we will insert cols of lastGfeedMetrics into firstGfeedMetrics
    firstGfeedMetrics.rename(columns={'num_ips':f'num_ips_{firstDate}','num_prefixes':f'num_prefixes_{firstDate}'},inplace=True)
    firstGfeedMetrics.insert(1,f'num_prefixes_{lastDate}',lastGfeedMetrics.loc[:,'num_prefixes'])
    firstGfeedMetrics[f'num_prefixes_{lastDate}']  = firstGfeedMetrics.loc[:,f'num_prefixes_{lastDate}'].fillna(value=0)
    firstGfeedMetrics.insert(3,f'num_ips_{lastDate}',lastGfeedMetrics.loc[:,'num_ips'])
    firstGfeedMetrics[f'num_ips_{lastDate}']  = firstGfeedMetrics.loc[:,f'num_ips_{lastDate}'].fillna(value=0)
 
    #calculate the total change of num_prefixes and num_ips
    prefixChange = firstGfeedMetrics.loc[:,[f'num_prefixes_{firstDate}',f'num_prefixes_{lastDate}']].apply(lambda x: x[f'num_prefixes_{lastDate}']-x[f'num_prefixes_{firstDate}'],axis=1)
    firstGfeedMetrics.insert(2,'change_num_prefixes',prefixChange)
    ipChange = firstGfeedMetrics.loc[:,[f'num_ips_{lastDate}',f'num_ips_{firstDate}']].apply(lambda x: x[f'num_ips_{lastDate}']-x[f'num_ips_{firstDate}'],axis=1)
    firstGfeedMetrics.insert(5,'change_num_ips',ipChange)
    firstGfeedMetrics = firstGfeedMetrics.reset_index(names='country_iso_code')
 
    #determine country names from ISO 3166-2 digit country codes
    countryNames = firstGfeedMetrics.loc[:,'country_iso_code'].apply(lambda v: pycountry.countries.get(alpha_2=v).name if pycountry.countries.get(alpha_2=v) is not None else v)
    firstGfeedMetrics.insert(1,'country_name',countryNames)
    cc = coco.CountryConverter()
    continents = cc.pandas_convert(series = firstGfeedMetrics.loc[:,'country_name'], to='Continent')
    firstGfeedMetrics.insert(2,'continent',continents)
    firstTotal = firstGfeedMetrics.loc[:,f'num_ips_{firstDate}'].sum()
    lastTotal = firstGfeedMetrics.loc[:,f'num_ips_{lastDate}'].sum()
    firstPcents = firstGfeedMetrics.loc[:,f'num_ips_{firstDate}'].apply(lambda b: (b*100)/firstTotal)
    lastPcents = firstGfeedMetrics.loc[:,f'num_ips_{lastDate}'].apply(lambda c: (c*100)/lastTotal)
    firstGfeedMetrics.insert(9,f'{firstDate}-gfeed-pcent',firstPcents)
    firstGfeedMetrics.insert(10,f'{lastDate}-gfeed-pcent',lastPcents)
    firstGfeedMetrics.sort_values(by=f'num_ips_{lastDate}',ascending=False,inplace=True,kind='stable',ignore_index=True)
    altFirstDate = firstFname.split('-')[0]
    altLastDate = lastFname.split('-')[0]
    firstGfeedMetrics.to_csv(f'Data/cleaned_data/gfeed_ipv4_metrics/{altFirstDate}-{altLastDate}_gfeedChanges_byCountry.csv')
    return firstGfeedMetrics






#calculates Pearson correlation coefficients between # of geofeed IPs allocated to a given country (country x)
# and commercial provider's rate of incorrectly estimating/locating IPs allocated to other countries to country x.
def calc_correlation_coefficients(provider):
    outDir = 'Data/cleaned_data/commercial-gfeed-comps/metrics/mislocation_error/gfeedCov-V-badGuess'
    if provider=='ipgeolocation-io':
        corrCoefficients = dict()
        fnames = [
                '02.01.2023-01.31.2023-gfeedipgeolocation-io-error',
                '02.28.2023-02.28.2023-gfeedipgeolocation-io-error',
                '03.13.2023-03.14.2023-gfeedipgeolocation-io-error',
                '04.13.2023-04.11.2023-gfeedipgeolocation-io-error',
                '05.13.2023-05.23.2023-gfeedipgeolocation-io-error',
                '05.28.2023-05.23.2023-gfeedipgeolocation-io-error'
                ]

        baseNames = [
                 '02.01.2023_gfeed_country_makeup.csv',
                 '02.28.2023_gfeed_country_makeup.csv',
                 '03.13.2023_gfeed_country_makeup.csv',
                 '04.13.2023_gfeed_country_makeup.csv',
                 '05.13.2023_gfeed_country_makeup.csv',
                 '05.28.2023_gfeed_country_makeup.csv'
                 ]
        for fname,baseName in zip(fnames,baseNames):
            fComps = fname.split(sep='-')[:-1]
            ipgeIoMisLocFile = '-'.join(fComps+['badCountryGuessOverview.csv'])
            ipgeIoMisLocs = pd.read_csv('/'.join([fbase,fname,ipgeIoMisLocFile]),index_col=0).set_index('commercial_country_iso_code')
            gfeedCntrys = pd.read_csv('/'.join([baseDir,baseName]),index_col=0).set_index('country_iso_code')
            gfeedCntrys.insert(3,'num_bad_guesses',ipgeIoMisLocs.loc[:,'num_ips'])
            gfeedCntrys['num_bad_guesses']=gfeedCntrys.loc[:,'num_bad_guesses'].fillna(value=0)
            corrCoefficient = gfeedCntrys.loc[:,'total_IPv4_addrs'].corr(gfeedCntrys.loc[:,'num_bad_guesses'])
            dates = '-'.join(fComps[:2])
            corrCoefficients[dates] = corrCoefficient
            gfeedCntrys.to_csv('/'.join([outDir,dates+'-gfeedCov-ipgeIoBadGuesses.csv']))
        pd.Series(corrCoefficients).to_csv('/'.join([outDir,provider+'_badGuess_corr_coefficients.csv']))
    else:
        if provider !='maxmind-geoip2':
            return
        corrCoefficients = dict()
        fnames = [
                '01.13.2023-01.13.2023-gfeedmaxmind-geoip2-error',
                '02.28.2023-02.28.2023-gfeedmaxmind-geoip2-error',
                '03.28.2023-03.28.2023-gfeedmaxmind-geoip2-error',
                '04.28.2023-04.25.2023-gfeedmaxmind-geoip2-error',
                '05.13.2023-05.16.2023-gfeedmaxmind-geoip2-error']
        
        baseNames = [
                '01.13.2023_gfeed_country_makeup.csv',
                '02.28.2023_gfeed_country_makeup.csv',
                '03.28.2023_gfeed_country_makeup.csv',
                '04.28.2023_gfeed_country_makeup.csv',
                '05.13.2023_gfeed_country_makeup.csv'
                ]
        for fname,baseName in zip(fnames,baseNames2):
            fComps = fname.split(sep='-')[:-1]
            misLocFile = 'badCountryGuessOverview.csv'
            misLocs = pd.read_csv('/'.join([fbase,fname,misLocFile]),index_col=0).set_index('country_iso_code.1')
            gfeedCntrys = pd.read_csv('/'.join([baseDir,baseName]),index_col=0).set_index('country_iso_code')
            gfeedCntrys.insert(3,'num_bad_guesses',misLocs.loc[:,'num_ips'])
            gfeedCntrys['num_bad_guesses']=gfeedCntrys.loc[:,'num_bad_guesses'].fillna(value=0)
            corrCoefficient = gfeedCntrys.loc[:,'total_IPv4_addrs'].corr(gfeedCntrys.loc[:,'num_bad_guesses'])
            dates = '-'.join(fComps[:2])
            corrCoefficients[dates] = corrCoefficient
            gfeedCntrys.to_csv('/'.join([outDir,dates+'-gfeedCov-maxmindBadGuesses.csv']))
        pd.Series(corrCoefficients).to_csv('/'.join([outDir,provider+'_badGuess_corr_coefficients.csv']))








#TODO: generalize function to work for both maxmind and ipgeolocation.io
# Current version only works for Maxmind (due to hard-coded vals)
def compare_misLoc_top20():
    Jan13BadCtrs = pd.read_csv('Data/cleaned_data/commercial-gfeed-comps/metrics/mislocation_error/01.13.2023-01.13.2023-gfeedmaxmind-geoip2-error/badCountryGuessOverview.csv',index_col=0)
    may13BadCtrs = pd.read_csv('Data/cleaned_data/commercial-gfeed-comps/metrics/mislocation_error/05.13.2023-05.16.2023-gfeedmaxmind-geoip2-error/badCountryGuessOverview.csv',index_col=0)
    
    #Normalize to compare proportions
    totalJan13 = Jan13BadCtrs.loc[:,'num_ips'].sum()
    totalMay13 = may13BadCtrs.loc[:,'num_ips'].sum()
    janMayNumComparison = Jan13BadCtrs.join(may13BadCtrs.set_index('country_iso_code.1'),on='country_iso_code.1',how='inner',lsuffix='_Jan.13',rsuffix='_May.13')
    jan13Proportions = Jan13BadCtrs.set_index('country_iso_code.1').loc[:,'num_ips'].apply(lambda x: x/totalJan13).reset_index()
    may13Proportions = may13BadCtrs.set_index('country_iso_code.1').loc[:,'num_ips'].apply(lambda y: y/totalMay13)
    janMayPropComparison = jan13Proportions.join(may13Proportions,on='country_iso_code.1',how='inner',lsuffix='_Jan.13',rsuffix='_May.13')
    janMayComparison= janMayNumComparison.join(janMayPropComparison.set_index('country_iso_code.1'),on='country_iso_code.1',lsuffix='_number',rsuffix='_proportion')
    janMayComps = janMayComparison.iloc[:20]
    countryNames = janMayComps.loc[:,'country_iso_code.1'].apply(lambda v: pycountry.countries.get(alpha_2=v).name if pycountry.countries.get(alpha_2=v) is not None else v)
    janMayComps.insert(0,'Incorrectly_guessed_country',countryNames)
    janMayComps = janMayComps.rename(columns={'num_ips_Jan.13_number':'Jan_13_num_IPs','num_ips_May.13_number':'May13_num_IPs','num_ips_Jan.13_proportion':'Jan_13_IP_proportion','num_ips_May.13_proportion':'May_13_IP_proportion'})
    janMayComps.to_csv('Data/cleaned_data/commercial-gfeed-comps/metrics/mislocation_error/01.13.2023-vs-05.16.2023-badCtryGuess-top-20Alt.csv')



#Additional analysis function that was run in ipython to get the breakdown of the extent to which countries and different continents were represented within the geofeeds.
#One of the cited issues with commercial DB's is that their records seem to disproportionately represent major developed countries (e.g. the US) - and that WHOIS records also seemed to share this problem. 
# (See Poesse et al.)
# 
def get_gfeed_country_rep_breakdown():
    dataDirObj = os_scandir('Data/cleaned_data/ipv4-geofeed-records/')
    for entry in dataDirObj:
        if not entry.name.startswith('.') and entry.is_file():
            nameChunks = entry.name.split(sep='-')
            if nameChunks[1:]!= ['ipv4','result.csv']:
                continue
            else:
                entryDate = nameChunks[0]
                dfVal = pd.read_csv(entry.path,index_col=False,names=['ip_prefix','country_iso_code','iso_subregion','city_name'])
                ctryGrps = dfVal.groupby('country_iso_code')
                ctryDist = dict()
                for key,grp in ctryGrps:
                    numIps = grp.loc[:,'ip_prefix'].apply(lambda x: netaddr_IPSet([x]).size)
                    ctryDist[key] = numIps.sum()
                ctryDistFrame = pd.Series(ctryDist).reset_index().rename(columns={'index':'country_iso_code',0:'total_IPv4_addrs'})
                ctryBlockDist = dfVal.loc[:,'country_iso_code'].value_counts()
                countryDistFrame = ctryDistFrame.set_index('country_iso_code').join(ctryBlockDist)
                # pdb.set_trace()
                countryDistFrame = countryDistFrame.sort_values(by='total_IPv4_addrs',ascending=False).rename(columns={'country_iso_code':'num_prefix_blocks'})
                countryNames = countryDistFrame.reset_index().loc[:,'country_iso_code'].apply(lambda y: pycountry.countries.get(alpha_2=y).name if pycountry.countries.get(alpha_2=y) is not None else y)
                countryDistFrame = countryDistFrame.reset_index()
                countryDistFrame.insert(1,'country_name',countryNames)
                pdb.set_trace()
                fname = entryDate+'_gfeed_country_makeup.csv'
                countryDistFrame.to_csv('/'.join(['Data/cleaned_data/gfeed_ipv4_metrics/country_coverage',fname]))
    dataDirObj.close()





#Remove overlapping gfeed IP prefix entries from the mergeframe
def deduplicate(mergeFrame,provider):
    maxmindKeys = {
         'gfeedCountryIso':'country_iso_code',
         'gfeedIsoSubreg': 'iso_subregion',
         'gfeedCityName': 'city_name'
         }
    ipgeIoKeys = {
         'gfeedCountryIso':'gfeed_country_iso_code',
         'gfeedIsoSubreg': 'gfeed_iso_subregion',
         'gfeedCityName': 'gfeed_city_name'
         }
    keyMap = None
    if provider in ['maxmind-geoip2','maxmind-geolite2']:
        keyMap = maxmindKeys
    else:
        if provider == 'ipgeolocation-io':
            keyMap = ipgeIoKeys
    mergeGroups = mergeFrame.groupby('commercial_ip_prefix')
    replacementEntries = []
    dropList = []
    for name,group in mergeGroups:
        nrows,ncol = group.shape
        if nrows==1:
            continue
        # largeGroup = nrows > 3
        #print(f' name: {name},\n group: {group}')
        basePrefixList = group.loc[:,'gfeed_ip_prefix'].unique().tolist()
        #pdb.set_trace()
        #Is the intersection of the IPSet(all other suspected overlapping IP prefixes) and the prefix in question empty? (If so, no other gfeed prefix overlaps it
        # If not: There is at least one other prefix in the suspected prefixes that overlaps w/the one in question
        overlaps = group.loc[:,'gfeed_ip_prefix'].apply(lambda x: netaddr_IPSet([x]).intersection(netaddr_IPSet([y for y in basePrefixList if y!=x])))
        duplicateCounts = overlaps.apply(lambda x: x.size)
        copyCounts = group.loc[:,'gfeed_ip_prefix'].value_counts()
        copyFilter = copyCounts.apply(lambda x: x > 1)
        copyVals = copyCounts.where(copyFilter).dropna().index.to_list()
        mainCopyFilter = group.loc[:,'gfeed_ip_prefix'].apply(lambda z: z in copyVals)
        overlapFilter = duplicateCounts.apply(lambda y: y>0)
        duplicateFilter = mainCopyFilter | overlapFilter
        duplicates = group.where(duplicateFilter).dropna(how='all')#.drop_duplicates(subset=['gfeed_ip_prefix','commercial_ip_prefix'])
        if duplicates.empty:
            continue
        gfeedMaskszs = duplicates.loc[:,'gfeed_ip_prefix'].apply(lambda x: int(x[-2:]))
        testMaskszs = gfeedMaskszs.sort_values(kind='stable').unique().tolist()
        # pdb.set_trace()
        replacementCandidates = []
        replacementCandidateVals = []#(see followup checks to ensure replacementEntries are unique/don't overlap w/each other) 
        dropVals = []
        #subtract one b/c last item will have largest mask and therefore refer to smallest block
        nrounds = len(testMaskszs)
        for ind in range(nrounds):
            biggestBlock = testMaskszs[ind]
            subsetInds = gfeedMaskszs.apply(lambda y: y > biggestBlock)
            #subsetInds = subsetInds & subsetInds.index.to_series().apply(lambda b: b in nonDuplicates.index)
            subsetEntries = duplicates.where(subsetInds).dropna(how='all')
            otherInds = gfeedMaskszs.apply(lambda z: z == biggestBlock)#Explicitly remove bigger blocks we've already checked from consideration
            otherEntries = duplicates.where(otherInds).dropna(how='all').drop_duplicates(subset=['gfeed_ip_prefix',keyMap['gfeedCountryIso'],keyMap['gfeedIsoSubreg'],'commercial_ip_prefix'])
            for entry in otherEntries.itertuples():
                entryIpSet = netaddr_IPSet([entry.gfeed_ip_prefix])
                subsetLocIDVals = subsetEntries.loc[:,[keyMap['gfeedCountryIso'],keyMap['gfeedIsoSubreg'],keyMap['gfeedCityName']]].apply(lambda x: [x[keyMap['gfeedCountryIso']],x[keyMap['gfeedIsoSubreg']],x[keyMap['gfeedCityName']]],axis=1)
                locNonMatchFilter = None
                if provider in  ['maxmind-geoip2','maxmind-geolite2']:
                    locNonMatchFilter = subsetLocIDVals.apply(lambda y: y!= [entry.country_iso_code, entry.iso_subregion,entry.city_name])
                else:
                    locNonMatchFilter = subsetLocIDVals.apply(lambda y: y!= [entry.gfeed_country_iso_code, entry.gfeed_iso_subregion,entry.gfeed_city_name])
                subsetCompEntries = subsetEntries.where(locNonMatchFilter).dropna(how='all')
                nonMatchOverlapFilter = subsetCompEntries.loc[:,'gfeed_ip_prefix'].apply(lambda z: netaddr_IPSet([z]).issubset(entryIpSet))
                nonMatchOverlaps = subsetCompEntries.where(nonMatchOverlapFilter).dropna(how='all')
                if not nonMatchOverlaps.empty:
                    dropVals.append(entry.Index)
                    resSet = entryIpSet - netaddr_IPSet(nonMatchOverlaps.loc[:,'gfeed_ip_prefix'].to_list())
                    resVal = resSet.intersection(netaddr_IPSet([entry.commercial_ip_prefix]))
                    cidrStrings = [str(cidr) for cidr in resVal.iter_cidrs()]
                    for cidr in cidrStrings:
                        if cidr in group.loc[:,'gfeed_ip_prefix'].to_list() or netaddr_IPSet(replacementCandidateVals).issuperset(netaddr_IPSet([cidr])):
                            continue
                        testAddrSet = netaddr_IPSet([cidr])
                        for setCidr in replacementCandidateVals:
                            if testAddrSet.issuperset(netaddr_IPSet([setCidr])):
                                replacementCandidateVals.remove(setCidr)
                        entryCopy = mergeFrame.loc[[entry.Index],mergeFrame.keys()[1:]]
                        entryCopy.insert(0,'gfeed_ip_prefix',cidr)
                        replacementCandidates.append(entryCopy)
                        replacementCandidateVals.append(cidr)
                    if len(replacementCandidates) > 1:
                        rCandidates = pd.concat(replacementCandidates)
                        repeatFilter = rCandidates.loc[:,'gfeed_ip_prefix'].apply(lambda x: x in replacementCandidateVals)
                        replacementCandidates = [rCandidates.where(repeatFilter).dropna(how='all')]
        if len(dropVals)>0:
            dropList += dropVals#print(f'dropVals: {dropVals}')
            replacementEntries += replacementCandidates
            duplicates = pd.concat([duplicates.drop(index=dropVals)] + replacementCandidates)
            biggestBlock = gfeedMaskszs.min()
            smallestBlock = gfeedMaskszs.max()
        survivingSubGrps = duplicates.groupby([keyMap['gfeedCountryIso'],keyMap['gfeedIsoSubreg'],keyMap['gfeedCityName']])
        if len(survivingSubGrps)==1:
            gfeedMaskszs = duplicates.loc[:,'gfeed_ip_prefix'].apply(lambda x: int(x[-2:]))
            testMaskszs = gfeedMaskszs.sort_values(kind='stable').unique().tolist()
            nrounds = len(testMaskszs)
            for ind in range(nrounds):
                biggestBlock = testMaskszs[ind]
                subnetInds = gfeedMaskszs.apply(lambda y: y >= biggestBlock)
                supersetInds = gfeedMaskszs.apply(lambda z: z < biggestBlock)
                superNets = duplicates.where(supersetInds).dropna(how='all')
                subnetCandidates = duplicates.where(subnetInds).dropna(how='all')
                for entryInd,entry in superNets.loc[:,'gfeed_ip_prefix'].items():
                    subsetCheck = subnetCandidates.loc[:,'gfeed_ip_prefix'].apply(lambda b: netaddr_IPSet([b]).issubset(netaddr_IPSet([entry])))
                    subMatches = subnetCandidates.where(subsetCheck).dropna(how='all')
                    dropList += subMatches.index.to_list()
                    # print(f'dropping: {subMatches.index.to_list()}')
        else:
            for locName, subgroup in survivingSubGrps:
                gfeedMaskszs = subgroup.loc[:,'gfeed_ip_prefix'].apply(lambda x: int(x[-2:]))
                testMaskszs = gfeedMaskszs.sort_values(kind='stable').unique().tolist()
                nrounds = len(testMaskszs)
                for ind in range(nrounds):
                    biggestBlock = testMaskszs[ind]
                    subnetInds = gfeedMaskszs.apply(lambda y: y > biggestBlock)
                    supersetInds = gfeedMaskszs.apply(lambda z: z == biggestBlock)
                    superNets = subgroup.where(supersetInds).dropna(how='all')
                    subnetCandidates = subgroup.where(subnetInds).dropna(how='all')
                    for entryInd,entry in superNets.loc[:,'gfeed_ip_prefix'].items():
                        subsetCheck = subnetCandidates.loc[:,'gfeed_ip_prefix'].apply(lambda b: netaddr_IPSet([b]).issubset(netaddr_IPSet([entry])))
                        subMatches = subnetCandidates.where(subsetCheck).dropna(how='all')
                        dropList += subMatches.index.to_list()
                        #print(f'dropping: {subMatches.index.to_list()}')
        #print(f'replacementCandidates: {replacementCandidates}')
    return dropList, replacementEntries





#Calc number of overlapping IPs between the geofeed and commercial IPv4 CIDR prefixes.
# Since each DF row corresponds to a pairing of overlapping geofeed and commercial IPv4 CIDR prefixes,
# the results are natively computed by number of prefixes (or prefix pairs) instead of by the number of IPv4 addresses.
# Therefore, the results from running this function will be used to scale the results
# before ordering them by rate of prevalence (or performing other forms of analysis).

def count_overlapping_ips(entry):
    gfeedIpSet = netaddr_IPSet([entry['gfeed_ip_prefix']])
    commercialIpSet = netaddr_IPSet([entry['commercial_ip_prefix']])
    overlap = gfeedIpSet.intersection(commercialIpSet)
    return overlap.size



def get_overlapping_ips(entry):
    gfeedIpSet = netaddr_IPSet([entry['gfeed_ip_prefix']])
    commercialIpSet = netaddr_IPSet([entry['commercial_ip_prefix']])
    overlap = gfeedIpSet.intersection(commercialIpSet)
    return overlap




def check_match_val(name1, name2):
    if not isinstance(name1,str) or not isinstance(name2,str):
        return 1.0
    return normalized_DL_dist(name1.lower(),name2.lower())


def series_ipSetUnion(ser):
   startVal = netaddr_IPSet()
   for index,val in ser.items():
       startVal = startVal | val
   return startVal

#This is really slow so I'd recommend multithreading and then aggregating if you want to look at the full mergeFrame
def calc_num_overlapping_ips(mergeFrame):
    overlapIpSeries = mergeFrame.apply(lambda x: get_overlapping_ips(x),axis=1)
    unionVal =series_ipSetUnion(overlapIpSeries)
    return unionVal


# See comment on geofeed entries w/o geoloc. info (starts: Per RFC 8805...)

#identify which entries locate overlapping IPs to different place (i.e. where there are discrepancies)
def find_maxmind_mismatches(mergeFrame):
    emptyGeolocs = mergeFrame.loc[:,'country_iso_code'].isna() & mergeFrame.loc[:,'country_iso_code'].isna() & mergeFrame.loc[:,'city_name'].isna()
    emptyGfeedRecs = mergeFrame.where(emptyGeolocs).dropna(how='all')
    staleCoords = emptyGfeedRecs.loc[:,'latitude'].notna() | emptyGfeedRecs.loc[:,'longitude'].notna() |emptyGfeedRecs.loc[:,'accuracy_radius'].notna()
    staleGname = emptyGfeedRecs.loc[:,'geoname_id'].notna()
    staleSubdivs = \
            emptyGfeedRecs.loc[:,'subdivision_1_iso_code'].notna() | emptyGfeedRecs.loc[:,'subdivision_1_name'].notna() |\
            emptyGfeedRecs.loc[:,'subdivision_2_iso_code'].notna() | emptyGfeedRecs.loc[:,'subdivision_2_name'].notna() |\
            emptyGfeedRecs.loc[:,'city_name.1'].notna()
    staleGeoloc = staleCoords | staleGname | staleSubdivs
    staleMismatches = emptyGfeedRecs.where(staleGeoloc).dropna(how='all')
    trulyBlank = staleGeoloc.apply(lambda x: not(x))
    trueEmpties = emptyGfeedRecs.where(trulyBlank).dropna(how='all')
    nonEmpties = emptyGeolocs.apply(lambda x: not(x))
    mergeFrame = mergeFrame.where(nonEmpties).dropna(how='all')

    countryMatches = mergeFrame.loc[:,['country_iso_code','country_iso_code.1']].apply(lambda x: x['country_iso_code']==x['country_iso_code.1'],axis=1)
    ctryMismatch = countryMatches.apply(lambda x: not(x))
    countryMismatches = mergeFrame.where(ctryMismatch).dropna(how='all')
    misMatches = pd.concat([staleMismatches,countryMismatches])
    nextRound = mergeFrame.where(countryMatches).dropna(how='all')
    
    # Round 2: Look at iso subregions:
    # Note: we will need to circle back to find records where non-NAN iso subcodes mismatch but country and city names match
    # Records meeting that criterial will likely need to be analyzed manually.
    subregIsos = nextRound.loc[:,'iso_subregion'].apply(lambda y: y.split('-')[-1]  if isinstance(y,str) else y)
    subregMatchFilter = pd.concat([subregIsos,nextRound.loc[:,'subdivision_1_iso_code']],axis=1).apply(lambda z: z['iso_subregion']==z['subdivision_1_iso_code'],axis=1)
    vacuousSubreg = subregIsos.isna() | nextRound.loc[:,'subdivision_1_iso_code'].isna()
    subregCriterion = subregMatchFilter | vacuousSubreg
    misfit = subregCriterion.apply(lambda a: not(a))
    subregMismatch = nextRound.where(misfit).dropna(how='all')
    misMatches = pd.concat([misMatches,subregMismatch])
    nextRound = nextRound.where(subregCriterion).dropna(how='all')

    #Round 3: Compare city names: NOTE: This is where the comparisons seems to break down.
    cityNans = nextRound.loc[:,'city_name'].isna() | nextRound.loc[:,'city_name.1'].isna()
    cityMatchVals = nextRound.loc[:,['city_name','city_name.1']].apply(lambda b: check_match_val(b['city_name'],b['city_name.1']),axis=1)
    cityMatchVals.rename('normalized_DL_dist',inplace=True)#rename series column val for easier concatenation later on
    fuzzyCityMFilter = cityMatchVals.apply(lambda c: c< 0.5)
    cityMs = fuzzyCityMFilter|cityNans

    #Debugging line for visibility into fuzzy distance values.
    # As given in the pyxdameraulevenshtein docs
    # a lower value for the normalized_damerau_levenshtein_distance indicates a closer match
    (nrows,ncol) = nextRound.shape
    nextRound.insert(ncol,'normalized_DL_dist',cityMatchVals)
    misMs = cityMs.apply(lambda d: not(d))
    cityMismatches = nextRound.where(misMs).dropna(how='all')
    misMatches = pd.concat([misMatches,cityMismatches])
    matches = nextRound.where(cityMs).dropna(how='all')
    return ((matches,trueEmpties),(misMatches,staleMismatches,countryMismatches,subregMismatch,cityMismatches))




#helper for find_ipgeIo_mismatches, map subdivision names and iso3166-2 codes to each other
def get_subdiv(subdivCode):
    record = pycountry.subdivisions.get(code=subdivCode)
    return record


#Still a work in progress - find subdivision code given country name and subdiv name.
def find_subdiv_iso_code(entry):
    subdivName = entry['commercial_state/subregion_name']
    mapping = entry['iso_subregion_map']
    scores=dict()
    for key,(name,translation) in mapping.items():
        score= max(fuzz.token_sort_ratio(name,subdivName),fuzz.token_sort_ratio(translation,subdivName))
        scores[score] = (key,name)
    dist=0
    if len(scores)>0:
        (dist,(key,name)) = max(scores.items())
    if dist >70:
        return key#May need to use fuzzy matching to get this to work
    return None



def find_ipgeIo_mismatches(mergeFrame):
    noGfeedCountry = mergeFrame.loc[:,'gfeed_country_iso_code'].isna()
    noGfeedSubreg = mergeFrame.loc[:,'gfeed_iso_subregion'].isna()
    noGfeedCity = mergeFrame.loc[:,'gfeed_city_name'].isna()
    noGfeedRes = noGfeedCountry & noGfeedSubreg & noGfeedCity

    #Separate off and compare entries where geofeeds have no geoloc. info:
    #  per RFC 8805 section 2.1.2:
    # "Feed publishers may indicate that some IP prefixes should not have any associated geolocation information. 
    #  It may be that some prefixes under their administrative control are reserved, not yet allocated or deployed,
    #  or in the process of being redeployed elsewhere and existing geolocation information can,
    #  from the perspective of the publisher, safely be discarded.
    #  This special case can be indicated by explicitly leaving blank all fields that specify any degree of geolocation information "
    # If commercial DBs contain geoloc. information - indicates records likely stale, or that they may not have checked the current geofeed.
    noGeolocRes = mergeFrame.where(noGfeedRes).dropna(how='all')
    falseCountryCodes = noGeolocRes.loc[:,'commercial_country_iso_code'].notna()
    falseCountryNames = noGeolocRes.loc[:,'commercial_country_name'].notna()
    falseCountries = falseCountryCodes | falseCountryNames
    falseSubregs = noGeolocRes.loc[:,'commercial_state/subregion_name'].notna()
    falseCities = noGeolocRes.loc[:,'commercial_city_name'].notna()
    falseGeolocs = falseCountries | falseSubregs | falseCities
    staleLocs = noGeolocRes.where(falseGeolocs).dropna(how='all')
    trulyBlank = falseGeolocs.apply(lambda x: not(x))
    trueEmpties = noGeolocRes.where(trulyBlank).dropna(how='all')

    gfeedRes = noGfeedRes.apply(lambda x: not(x))
    mergeFrame = mergeFrame.where(gfeedRes).dropna(how='all')
    countryMatches = mergeFrame.loc[:,['gfeed_country_iso_code','commercial_country_iso_code']].apply(lambda x: x['gfeed_country_iso_code']==x['commercial_country_iso_code'],axis=1)
    ctryMismatch = countryMatches.apply(lambda x: not(x))
    countryMismatches = mergeFrame.where(ctryMismatch).dropna(how='all')
    misMatches = pd.concat([staleLocs,countryMismatches])
    nextRound = mergeFrame.where(countryMatches).dropna(how='all')

    # #Mapping iso subregion is a bit more complex since ipgeolocation.io lists the subregion name but not its iso code
    # gfeedSubregIsos = nextRound.loc[:,'gfeed_iso_subregion'].apply(lambda y: y.split('-')[-1]  if isinstance(y,str) else y)
    # #countryFilter = nextRound.loc[:,'commercial_country_iso_code'].apply(lambda x: isinstance(x,str))
    # subdivFilter = nextRound.loc[:,'commercial_state/subregion_name'].apply(lambda x: isinstance(x,str))
    # gfeedIsoSubFilter = nextRound.loc[:,'gfeed_iso_subregion'].notna()

    # #records where we can try to match by subdiv iso code
    # isoSubdivComparable = gfeedIsoSubFilter & subdivFilter & countryFilter
    # isoSubComps = nextRound.where(isoSubdivComparable).dropna(how='all')

    # #records where we'll have to compare the subdivision names
    # noComCountry = countryFilter.apply(lambda x: not(x))
    # nonIsoSubComps = nextRound.where(noComCountry & subdivFilter).dropna(how='all')
    # 
    # #records that don't specify a commercial subdivision but may still specify city name (though it's not super likely)
    # nonSubdivFilter = subdivFilter.apply(lambda x: not(x)) 
    # skippingSubdivs = nextRound.where(nonSubdivFilter & noComCountry).dropna(how='all')

    # #compare the subdiv. codes for all applicable entries:
    # codeSearchKeys = isoSubComps.loc[:,['commercial_country_iso_code','commercial_state/subregion_name']]
    # countryCodes = pd.Series(codeSearchKeys.loc[:,'commercial_country_iso_code'].unique())
    # subregions = countryCodes.apply(lambda x: pycountry.subdivisions.get(country_code=x))
    # subregions.rename('subdivision',inplace=True)
    # subregDF = pd.concat([countryCodes,subregions],axis=1).set_index(0)
    # translator = GoogleTranslator(source='auto',target='en')
    # translatedSubreg = subregDF.loc[:,'subdivision'].apply(lambda x: dict([(div.code,(div.name,translator.translate(div.name))) for div in x]))
    # countrySubregs = codeSearchKeys.loc[:,'commercial_country_iso_code'].apply(lambda x: translatedSubreg.loc[x])
    # codeSearchKeys.insert(2,'iso_subregion_map',countrySubregs)
    # commercialSubdivIsos = codeSearchKeys.apply(lambda x: find_subdiv_iso_code(x),axis=1)
    # commercialSubdivIsos = commercialSubdivIsos.str.split('-',expand=True).loc[:,1]
    # commercialSubdivIsos.rename('subdivision_1_iso_code',inplace=True)

    # compGfeedSubregIsos = gfeedSubregIsos.where(isoSubdivComparable).dropna()
    # #pd.concat and compare
    # subregPairCheck =  pd.concat([compGfeedSubregIsos,commercialSubdivIsos],axis=1)
    # subregIsoMatchFilter = subregPairCheck.apply(lambda z: z['gfeed_iso_subregion']==z['subdivision_1_iso_code'],axis=1)
    # noComparison = subregPairCheck.apply(lambda x: isinstance(x['gfeed_iso_subregion'],type(None)) or isinstance(x['subdivision_1_iso_code'],type(None)),axis=1)
    # pdb.set_trace()
    # subdivIsoMatches = isoSubComps.where(subregIsoMatchFilter|noComparison).dropna(how='all')
    # subdivIsoMismatches = isoSubComps.where(subregIsoMatchFilter.apply(lambda a: not(a))).dropna(how='all')
    # pdb.set_trace()#check values of matches and mismatches: Do they look correct?

    # #comparison by subregion name
    # gfeedLookupIsos = gfeedSubregIsos.where(noComCountry & subdivFilter).dropna()
    # gfeedSubdivNames = gfeedLookupIsos.apply(lambda x: get_subdiv_name(x))
    # pdb.set_trace()
    # matchScores = pd.concat([gfeedSubdivNames,nonIsoSubComps.loc[:,'commercial_state/subregion_name']],axis=1)
    # passingScore = matchScores.apply(lambda b: b < 0.5)
    # failingScore = matchScores.apply(lambda c: c >= 0.5) 
    # subdivNameMatches = nonIsoSubComps.where(passingScore).dropna(how='all')
    # subdivNameMismatches = nonIsoSubComps.where(failingScore).dropna(how='all')
    # pdb.set_trace()#manually check values of fuzzy matches/mismatches - do they pass muster?
    # 
    # subregMatches = pd.concat([subdivIsoMatches,subdivNameMatches])
    # subregMismatches = pd.concat([subdivIsoMismatches,subdivNameMismatches])
    # misMatches = pd.concat([misMatches,subregMismatches])
    # nextRound = pd.concat([skippingSubdivs,subregMatches])

    #Round 3: Compare city names: 
    cityNans = nextRound.loc[:,'gfeed_city_name'].isna() | nextRound.loc[:,'commercial_city_name'].isna()
    cityMatchVals = nextRound.loc[:,['gfeed_city_name','commercial_city_name']].apply(lambda b: check_match_val(b['gfeed_city_name'],b['commercial_city_name']),axis=1)
    cityMatchVals.rename('normalized_DL_dist',inplace=True)#rename series column val for easier concatenation later on
    fuzzyCityMFilter = cityMatchVals.apply(lambda c: c< 0.5)
    cityMs = fuzzyCityMFilter|cityNans

    #Debugging line for visibility into fuzzy distance values.
    # As given in the pyxdameraulevenshtein docs
    # a lower value for the normalized_damerau_levenshtein_distance indicates a closer match
    (nrows,ncol) = nextRound.shape
    nextRound.insert(ncol,'normalized_DL_dist',cityMatchVals)
    misMs = cityMs.apply(lambda d: not(d))
    cityMismatches = nextRound.where(misMs).dropna(how='all')
    misMatches = pd.concat([misMatches,cityMismatches])
    matches = nextRound.where(cityMs).dropna(how='all')
    return ((matches,trueEmpties),(misMatches,staleLocs,countryMismatches,cityMismatches))#subregMismatches







#Helper function for weighting "stale" commercial entries by the number of IPs to get their actual prevalence
def weight_stales_by_numIPs(staleFrame,field):
    uniqueStaleVals = staleFrame.loc[:,field].dropna().unique()
    codeCounts = dict()
    for code in uniqueStaleVals:
        codeFilter = staleFrame.loc[:,field].apply(lambda x: isinstance(x,str) and x==code)
        codeIpCount = staleFrame.loc[:,'num_overlapping_ips'].where(codeFilter).dropna().astype(pd.Int64Dtype()).sum()
        codeCounts[code]=codeIpCount
    return pd.DataFrame(sorted(codeCounts.items(),key=lambda item: item[1],reverse=True),columns=[field,'num_ips'])
    



#looks up the geographic coordinates for geofeed locations to allow dist. btw them to be measured:
# Function is currently customized to only work for maxmind DF field names. 
# See lookup_gfeedipgeIo_location_coords() for ipgeolocation.io-specific implementation.
def lookup_gfeed_location_coords(gnameFrame,mismatches):
    namedCityMismatches = mismatches.where(mismatches.loc[:,'city_name'].notna()).dropna(how='all')
    namedCities = namedCityMismatches.loc[:,'city_name'].apply(lambda x: x.lower())
    namedCityMismatches['city_name']=namedCities
    gnameFrame['location_name']=gnameFrame.loc[:,'location_name'].apply(lambda x: x.lower())
    countryGnameGroups = gnameFrame.groupby('country_iso_code')
    countryMismatchGroups = namedCityMismatches.groupby('country_iso_code')
    missedCityNames = []
    resFrame = pd.DataFrame()
    for name,group in countryMismatchGroups:
        cityList = group.loc[:,'city_name'].unique()
        try:
            gnameGrp = countryGnameGroups.get_group(name)
        except KeyError as k:
            for city in cityList:
                missedCityNames.append((name,city))
            pdb.set_trace()#Which options are at our disposal in this case?
        else:
            gnameLocs = pd.Series(gnameGrp.loc[:,'location_name'].unique())
            gnameLocGrps = gnameGrp.groupby('location_name')
            # pdb.set_trace()
            cityList = group.loc[:,'city_name'].unique()
            cityCoords = dict()
            for city in cityList:
                try:
                    cityGnameGrp = gnameLocGrps.get_group(city)
                except KeyError as k:
                    missedCityNames.append((name,city))
                    locDlScores = gnameLocs.apply(lambda x: normalized_DL_dist(city,x))
                    locFuzzScores = gnameLocs.apply(lambda y: 1-(fuzz.token_sort_ratio(city,y)/100.0))
                    bestDls = [val  for val in locDlScores.nsmallest(n=2).unique() if val<0.5]
                    bestFuzzes = [val for val in locFuzzScores.nsmallest(n=2).unique() if val<0.5]
                    dlFilter = locDlScores.apply(lambda w: w in bestDls)
                    fuzzFilter = locFuzzScores.apply(lambda z: z in bestFuzzes)
                    dlMatches = gnameLocs.where(dlFilter).dropna()
                    fuzzMatches = gnameLocs.where(fuzzFilter).dropna()
                    fuzzyCity = ''
                    overlaps = set(dlMatches.to_list()).intersection(set(fuzzMatches.to_list()))
                    if len(overlaps)>0:
                        fuzzyCity = list(overlaps)[0]
                    else:
                        if len(bestFuzzes) ==1:
                            fuzzyCity = fuzzMatches.iloc[0]
                        else:
                            if len(bestFuzzes)!=0:
                                altFuzzFilter = locFuzzScores.apply(lambda z: z==min(bestFuzzes))
                                bestFuzzes = gnameLocs.where(altFuzzFilter).dropna()
                                fuzzyCity = bestFuzzes.iloc[0]
                            else:
                                continue
                    cityGnameGrp = gnameLocGrps.get_group(fuzzyCity)
                    cityEntry = cityGnameGrp.head(n=1)
                    cityCoords[city]=(cityEntry['latitude'].to_list()[0],cityEntry['longitude'].to_list()[0])
                else:
                    cityEntry = cityGnameGrp.head(n=1)
                    cityCoords[city]=(cityEntry['latitude'].to_list()[0],cityEntry['longitude'].to_list()[0])
            placementFilter = group.loc[:,'city_name'].apply(lambda x: x in cityCoords.keys())
            resEntries = group.where(placementFilter).dropna(how='all')
            coordVals = resEntries.loc[:,'city_name'].map(cityCoords)
            lats = coordVals.apply(lambda x: x[0])
            longs = coordVals.apply(lambda y: y[1])
            resEntries.insert(5,'gfeed_latitude',lats)
            resEntries.insert(6,'gfeed_longitude',longs)
            if resFrame.empty:
                resFrame = resEntries
            else:
                resFrame = pd.concat([resFrame,resEntries])
    return (resFrame,missedCityNames)



#Lookups for geocoding geofeed records where the country iso code is specified
# but the subdivision iso code (3166-2) and city name fields are omitted.
#Note: The geonames dataset did not seem to consistently contain country names. 
#      Therefore, country entries are handled in gname_frame_lookup() where their centroids are estimated
#      by taking the means of the coordinates (for latitude and longitude respectively).
def country_lookup_helper(code, gnameFrame):
    countryEntry = pycountry.countries.get(alpha_2 = code)
    nameMatchFilter = gnameFrame.loc[:,'location_name'].apply(lambda x: isinstance(x,str) and  x==countryEntry.name.lower())
    matchTest = nameMatchFilter.value_counts()
    return gnameFrame.where(nameMatchFilter).dropna(how='all')



#Geocoding (for both providers) lookups for geofeed records where the country iso code (3166-1) and iso-subregion (3166-2) 
# are specified 
def gnameFrame_lookup_subreg_helper(entry,gnameFrame):
    try:
        countryGroup = gnameFrame.groupby('country_iso_code').get_group(entry.country_code)
    except KeyError as k:
        print("nothinig to return")
        return
    else:
        nameMatchScores = countryGroup.loc[:,'location_name'].apply(lambda x: 1-(fuzz.token_sort_ratio(entry.name.lower(),x)/100.0) if isinstance(x,str) else np.nan )
        #nameMatchFilter = gnameFrame.loc[:,'location_name'].apply(lambda x: isinstance(x,str) and x.lower()==entry.name.lower())
        minScores = nameMatchScores.nsmallest(n=2,keep='all')
        minScores = minScores.where(minScores.apply(lambda x: x<=0.4)).dropna()# minScores are given in ascending order with their original index vals
        minScoreVals = minScores.unique()
        if len(minScoreVals) > 0:
            indVals = minScores.index
            nameMatches = gnameFrame.loc[indVals]#where(nameMatchScores.apply(lambda z: z in minScores)).dropna(how='all')
            if len(minScoreVals > 1):
                minScores = minScores.nsmallest(n=1,keep='all')#gnameFrame.where(nameMatchScores.apply(lambda z: z ==minScore)).dropna()
                nameMatches = nameMatches.loc[minScores.index]
            if entry.country_code=='US':
                subregCode = entry.code.split(sep='-')[-1]
                subregFilter = nameMatches.loc[:,'admin1_code'].apply(lambda z: isinstance(z,str) and z==subregCode)
                retVal = nameMatches.where(subregFilter).dropna(how='all')
                if retVal.empty:
                    territories = {
                            'puerto rico': 'PR',
                            'american samoa':'AS',
                            'guam': 'GU',
                            'republic of palau':'PW',
                            'northern mariana islands': 'MP',
                            'marshall islands':'MH'
                            }
                    if entry.name.lower() in territories.keys():
                        nameFilter = gnameFrame.loc[:,'location_name'].apply(lambda x: isinstance(x,str) and x==entry.name.lower())
                        countryFilter = gnameFrame.loc[:,'country_iso_code'].apply(lambda y: isinstance(y,str) and y==territories[entry.name.lower()])
                        #pdb.set_trace()
                        return gnameFrame.where(nameFilter & countryFilter).dropna(how='all')
                    else:
                        pdb.set_trace()#Check in case you need to account for alternate names or use fuzzy matching
                        print(f'{entry.name.lower()} did not match any of the names in territories {territories}')
                        return None
                return retVal
            else:
                if nameMatches.empty:
                    pdb.set_trace()
                    print("nothing to return")
                return nameMatches
        else:
            print('none of the scores in nameMatchScores met the threshold of being < 0.4')
            pdb.set_trace()
            territories = { 
                'puerto rico': 'PR',
                'american samoa':'AS',
                'guam': 'GU',
                'republic of palau':'PW',
                'northern mariana islands': 'MP',
                'marshall islands':'MH'
                }   
            if entry.name.lower() in territories.keys():
                nameFilter = gnameFrame.loc[:,'location_name'].apply(lambda x: isinstance(x,str) and x==entry.name.lower())
                countryFilter = gnameFrame.loc[:,'country_iso_code'].apply(lambda y: isinstance(y,str) and y==territories[entry.name.lower()])
                return gnameFrame.where(nameFilter & countryFilter).dropna(how='all')
            else:
                print("Nothing to return")





# (Geocoding) Lookup approx. GPS coordinates of locations where a location is given in the geofeed but a city name is not specified.
def gname_frame_lookup(mismatches,gnameFrame,provider='maxmind'):
    includeSubreg = noSubreg = subregions = None
    if provider == 'maxmind':
        mismatches = mismatches.where(mismatches.loc[:,'country_iso_code'].notna()).dropna(how='all')
        includeSubreg = mismatches.where(mismatches.loc[:,'iso_subregion'].notna()).dropna(how='all')
        noSubreg = mismatches.where(mismatches.loc[:,'iso_subregion'].isna()).dropna(how='all')
        subregions = includeSubreg.loc[:,'iso_subregion'].apply(lambda x: pycountry.subdivisions.get(code=x)).dropna()
    else:
        mismatches = mismatches.where(mismatches.loc[:,'gfeed_country_iso_code'].notna()).dropna(how='all')
        includeSubreg = mismatches.where(mismatches.loc[:,'gfeed_iso_subregion'].notna()).dropna(how='all')
        noSubreg = mismatches.where(mismatches.loc[:,'gfeed_iso_subregion'].isna()).dropna(how='all')
        subregions = includeSubreg.loc[:,'gfeed_iso_subregion'].apply(lambda x: pycountry.subdivisions.get(code=x)).dropna()
    unmappedSubregs = []
    if not subregions.empty:
        subregMap = dict()
        for entry in subregions.unique():
            mapEntry = gnameFrame_lookup_subreg_helper(entry,gnameFrame)
            if mapEntry is None or mapEntry.empty: 
                print(f'mapEntry for {entry.name} is empty')
                pdb.set_trace()
                unmappedSubregs.append(entry)
            else:
                (nrows,ncol) = mapEntry.shape
                if nrows >1:
                    coords =tuple(mapEntry.loc[:,['latitude','longitude']].mean().to_list())
                else:
                    coords = (mapEntry['latitude'].to_list()[0],mapEntry['longitude'].to_list()[0])
                subregMap[entry.name] = coords
        subregNames = subregions.apply(lambda z: z.name)
        included = subregions.apply(lambda x: x not in unmappedSubregs)
        # print(subregions.apply(lambda x: x in unmappedSubregs).value_counts())
        gnameCoords = pd.DataFrame(subregNames.map(subregMap).to_list(),index=subregNames.index,columns=['gfeed_latitude','gfeed_longitude']) 
        includeSubreg.insert(5,'gfeed_latitude',gnameCoords.loc[:,'gfeed_latitude'])
        includeSubreg.insert(6,'gfeed_longitude',gnameCoords.loc[:,'gfeed_longitude'])
        # pdb.set_trace()
        includeSubreg = includeSubreg.where(included).dropna(how='all')
    else:
        if includeSubreg is not None and not includeSubreg.empty:
            for entry in includeSubreg:
                unmappedSubregs.append(entry)
    cntryMap = dict()
    unmappedCtrys = []
    countryCoords=None
    if provider=='maxmind':
        gnameCtryGrps = gnameFrame.groupby('country_iso_code')
        for code in noSubreg.loc[:,'country_iso_code'].unique():
            try:
                ctryLocs = gnameCtryGrps.get_group(code)#country_lookup_helper(code, gnameFrame)
            except KeyError as k:
                unmappedCtrys.append((code,pycountry.countries.get(alpha_2 = code)))
                continue
            else:
                coordEstimate = (ctryLocs.loc[:,'latitude'].mean(),ctryLocs.loc[:,'longitude'].mean())
                cntryMap[code] = coordEstimate
        countryCoords = pd.DataFrame(noSubreg.loc[:,'country_iso_code'].map(cntryMap).to_list(),index=noSubreg.index,columns=['gfeed_latitude','gfeed_longitude'])
    else:
        gnameCtryGrps = gnameFrame.groupby('country_iso_code')
        for code in noSubreg.loc[:,'gfeed_country_iso_code'].unique():
            try:
                ctryLocs = gnameCtryGrps.get_group(code)
            except KeyError as k:
                unmappedCtrys.append((code,pycountry.countries.get(alpha_2 = code)))
                continue
            else:
                coordEstimate = (ctryLocs.loc[:,'latitude'].mean(),ctryLocs.loc[:,'longitude'].mean())
                cntryMap[code] = coordEstimate
        countryCoords = pd.DataFrame(noSubreg.loc[:,'gfeed_country_iso_code'].map(cntryMap).to_list(),index=noSubreg.index,columns=['gfeed_latitude','gfeed_longitude'])
    noSubreg.insert(5,'gfeed_latitude',countryCoords.loc[:,'gfeed_latitude'])
    noSubreg.insert(6,'gfeed_longitude',countryCoords.loc[:,'gfeed_longitude'])
    #pdb.set_trace()
    if not subregions.empty:
        retVal = pd.concat([includeSubreg,noSubreg])
    else:
        retVal = noSubreg
    return retVal, unmappedSubregs,unmappedCtrys




# DF Keys for ipgeolocation.io mergeFrames:
#Index(['gfeed_ip_prefix', 'gfeed_country_iso_code', 'gfeed_iso_subregion',
#        'gfeed_city_name', 'commercial_ip_prefix',
#        'commercial_country_iso_code', 'commercial_country_name',
#        'commercial_state/subregion_name', 'commercial_city_name', 'latitude',
#        'longitude', 'geoname_id', 'AS_organization'],
#       dtype='object')
def lookup_gfeedipgeIo_location_coords(gnameFrame,mismatches):
    namedCityMismatches = mismatches.where(mismatches.loc[:,'gfeed_city_name'].notna()).dropna(how='all')
    namedCities = namedCityMismatches.loc[:,'gfeed_city_name'].apply(lambda x: x.lower())
    namedCityMismatches['gfeed_city_name']=namedCities
    gnameFrame['location_name']=gnameFrame.loc[:,'location_name'].apply(lambda x: x.lower())
    countryGnameGroups = gnameFrame.groupby('country_iso_code')
    countryMismatchGroups = namedCityMismatches.groupby('gfeed_country_iso_code')
    missedCityNames = []
    resFrame = pd.DataFrame()
    for name,group in countryMismatchGroups:
        cityList = group.loc[:,'gfeed_city_name'].unique()
        try:
            gnameGrp = countryGnameGroups.get_group(name)
        except KeyError as k:
            for city in cityList:
                missedCityNames.append((name,city))
            pdb.set_trace()#Which options are at our disposal in this case?
        else:
            gnameLocs = pd.Series(gnameGrp.loc[:,'location_name'].unique())
            gnameLocGrps = gnameGrp.groupby('location_name')
            # pdb.set_trace()
            cityList = group.loc[:,'gfeed_city_name'].unique()
            cityCoords = dict()
            for city in cityList:
                try:
                    cityGnameGrp = gnameLocGrps.get_group(city)
                except KeyError as k:
                    missedCityNames.append((name,city))
                    locDlScores = gnameLocs.apply(lambda x: normalized_DL_dist(city,x))
                    locFuzzScores = gnameLocs.apply(lambda y: 1-(fuzz.token_sort_ratio(city,y)/100.0))
                    bestDls = [val  for val in locDlScores.nsmallest(n=2).unique() if val<0.5]
                    bestFuzzes = [val for val in locFuzzScores.nsmallest(n=2).unique() if val<0.5]
                    dlFilter = locDlScores.apply(lambda w: w in bestDls)
                    fuzzFilter = locFuzzScores.apply(lambda z: z in bestFuzzes)
                    dlMatches = gnameLocs.where(dlFilter).dropna()
                    fuzzMatches = gnameLocs.where(fuzzFilter).dropna()
                    fuzzyCity = ''
                    overlaps = set(dlMatches.to_list()).intersection(set(fuzzMatches.to_list()))
                    if len(overlaps)>0:
                        fuzzyCity = list(overlaps)[0]
                    else:
                        if len(bestFuzzes) ==1:
                            fuzzyCity = fuzzMatches.iloc[0]
                        elif len(bestFuzzes) ==0:
                            continue
                        else:
                            altFuzzFilter = locFuzzScores.apply(lambda z: z==min(bestFuzzes))
                            bestFuzzes = gnameLocs.where(altFuzzFilter).dropna()
                            fuzzyCity = bestFuzzes.iloc[0]
                    cityGnameGrp = gnameLocGrps.get_group(fuzzyCity)
                    cityEntry = cityGnameGrp.head(n=1)
                    cityCoords[city]=(cityEntry['latitude'].to_list()[0],cityEntry['longitude'].to_list()[0])
                else:
                    cityEntry = cityGnameGrp.head(n=1)
                    cityCoords[city]=(cityEntry['latitude'].to_list()[0],cityEntry['longitude'].to_list()[0])
            placementFilter = group.loc[:,'gfeed_city_name'].apply(lambda x: x in cityCoords.keys())
            resEntries = group.where(placementFilter).dropna(how='all')
            coordVals = resEntries.loc[:,'gfeed_city_name'].map(cityCoords)
            lats = coordVals.apply(lambda x: x[0])
            longs = coordVals.apply(lambda y: y[1])
            resEntries.insert(5,'gfeed_latitude',lats)
            resEntries.insert(6,'gfeed_longitude',longs)
            if resFrame.empty:
                resFrame = resEntries
            else:
                resFrame = pd.concat([resFrame,resEntries])
    # pdb.set_trace()
    return (resFrame,missedCityNames)




# calculate distance between geofeed result (as geocoded using geonames) and commercial estimates in km
# insert result into gCodedMismatches (i.e. mergeFrame entries where gfeed and commercial locs didn't match)
# return mean error and augmented mismatch DataFrame
# Note: Since the distance between geofeed entries with no geolocation information 
# (i.e. that only included a prefix) and commercial estimated locations is undefined,
# the shared IPs for which the geofeeds did not provide geolocation information were not included
# in the denominator calculating the mean/average distance by which the commercial records were off/disagreed w/the geofeeds. 
def calc_mean_error_distance(gCodedMismatches,matches):
    gfeedPass = gCodedMismatches.loc[:,'gfeed_latitude'].notna() & gCodedMismatches.loc[:,'gfeed_longitude'].notna()
    commercialPass = gCodedMismatches.loc[:,'latitude'].notna() & gCodedMismatches.loc[:,'longitude'].notna()
    gCodedMismatches = gCodedMismatches.where(gfeedPass & commercialPass).dropna(how='all')
    flatErrorDists = gCodedMismatches.loc[:,['gfeed_latitude','gfeed_longitude','latitude','longitude']].apply(lambda x: flat_distance((x['gfeed_latitude'],x['gfeed_longitude']), (x['latitude'],x['longitude'])).km,axis=1)
    gCodedMismatches.insert(1,'estimated_error(km)',flatErrorDists)
    errorGroups = gCodedMismatches.groupby('estimated_error(km)')
    errDistVal = 0
    totalIps = 0
    errorGroups = gCodedMismatches.groupby('estimated_error(km)')
    for distance,group in errorGroups:
        numIps=group.loc[:,'num_overlapping_ips'].sum()
        totalIps+=numIps
        errDistVal+= distance * numIps
    totalIps+=matches.loc[:,'num_overlapping_ips'].sum()
    meanError = errDistVal/totalIps
    return gCodedMismatches,meanError




# Which countries have the most IPs mismatched to countries that differ from the geofeed result
# & in which countries does the commercial provider estimate these are located.
def get_metrics_by_mismatched_country(countryMismatches,provider):
    if provider=='ipgeolocation-io':
        mismatchRank = weight_stales_by_numIPs(countryMismatches,'gfeed_country_iso_code')
        countryMismatchGrps = countryMismatches.groupby('gfeed_country_iso_code')
        resVals = dict()
        for name,group in countryMismatchGrps:
            resVals[name] = weight_stales_by_numIPs(group,'commercial_country_iso_code')
        return mismatchRank,resVals
    else:
        mismatchRank = weight_stales_by_numIPs(countryMismatches,'country_iso_code')
        countryMismatchGrps = countryMismatches.groupby('country_iso_code')
        resVals = dict()
        for name,group in countryMismatchGrps:
            resVals[name] = weight_stales_by_numIPs(group,'country_iso_code.1')
        return mismatchRank,resVals





# Which countries have the highest proportion of their IPs mislocated?
def get_countryWise_error_rates(nonEmptyMergeFrame,mismatchRank,provider='maxmind'):
    gfeedCtryDist = None
    if provider=='maxmind':
        gfeedCtryDist = weight_stales_by_numIPs(nonEmptyMergeFrame,'country_iso_code')
    else:
        gfeedCtryDist = weight_stales_by_numIPs(nonEmptyMergeFrame,'gfeed_country_iso_code')
    gfeedCtryDist.rename(columns={'num_ips':'total_ips'},inplace=True)
    mismatchRank.rename(columns={'num_ips':'mislocated_ctry_ips'},inplace=True)
    metricFrame = None
    if provider=='maxmind':
        metricFrame = gfeedCtryDist.merge(mismatchRank,on='country_iso_code',validate='1:1')
    else:
        metricFrame = gfeedCtryDist.merge(mismatchRank,on='gfeed_country_iso_code',validate='1:1')
    mislocFractions = metricFrame.apply(lambda x: x['mislocated_ctry_ips']/x['total_ips'],axis=1)
    metricFrame.insert(3,'error_fraction',mislocFractions)
    return metricFrame



#TODO: Move this to the top of the script once you've finished coding it
def validate_inputs(params):
    mappedProvider = params.mergeFramePath.split('/')[-1]
    #expects the bare filename to be of form [gdate]-[cdate]-gfeed[provider]-mergeFrame.csv
    mappedProviderBlocks = mappedProvider.split('-')[2:4]
    mappedProvider = '-'.join([mappedProviderBlocks[0][5:],mappedProviderBlocks[1]])
    if mappedProvider!=params.provider:
        raise ValueError(f'Commercial provider in mergeFrame ({mappedProvider}) does not match specified provider ({params.provider}).')
    pass#placeholder
    #params.provider
    #params.mergeFramePath
    #params.geocodeFilepath



def parse_inputs():
    desc = 'measure_geoloc_distance.py - Measures location disagreement between geofeed geolocation and commercial estimates for corresponding IPv4 addresses.'
    parser = ArgumentParser(description = desc)
    parser.add_argument('-p',
                        dest='provider',
                        required=True,
                        choices=['maxmind-geoip2','maxmind-geolite2','ipgeolocation-io'],
                        help='Name of the commercial IP-geolocation data source. Currently supported options: [\'maxmind-geoip2\',\'maxmind-geolite2\',\'ipgeolocation-io\']')
    parser.add_argument('-m',
                        dest='mergeFramePath',
                        type=str,
                        required=True,
                        help='complete or relative filepath to a DataFrame (stored as a CSV) with merged geofeed and commercial data (see compare_geolocation.py for details)')
    parser.add_argument('-o',
                        dest='outPath',
                        type=str,
                        default='Data/cleaned_data/commercial-gfeed-comps')
    # parser.add_argument('-c',
    #                     dest='geocodeFilepath',
    #                     type=str,
    #                     default='Data/raw_data/gnames')
    return parser.parse_args()




def main():
    params = parse_inputs()
    validate_inputs(params)
    keyFields = [
        'geonameid','location_name',
        'ascii_name','altNames',
        'latitude','longitude',
        'feature_class','feature_code',
        'country_iso_code','alt_country_codes',
        'admin1_code','admin2_code',
        'admin3_code','admin4_code',
        'population','elevation_in_meters',
        'dem','timezone',
        'last_mod_date'
        ]
    keyUseFields = [
        'geonameid','location_name',
        'latitude','longitude','country_iso_code',
        'admin1_code','admin2_code',
        'admin3_code','admin4_code'
        ]
    keyUseTypes = {
        'geonameid':pd.Int64Dtype(), 'location_name': str,
        'latitude':pd.Float64Dtype(),'longitude': pd.Float64Dtype(),
        'country_iso_code':str, 'admin1_code':str,
        'admin2_code':str,'admin3_code':str,
        'admin4_code':str
        }
    gnameMap = pd.read_csv('Data/raw_data/gnames/3.22.2023-allCountries.txt',names=keyFields,usecols=keyUseFields,dtype=keyUseTypes,sep='\t',index_col=0)
    gnameMap['location_name'] = gnameMap.loc[:,'location_name'].apply(lambda x:x.lower())
    gnameMap.sort_values(by='location_name',inplace=True,kind='stable')
    altNameKeys = [
        'altNameId','geonameid',
        'isolanguage','alt_name',
        'preffered','isShortName',
        'isColloquial','isHistoric'
        ]
    altNameUseKeys = [
        'altNameId','geonameid',
        'alt_name'
        ]
    altNames = pd.read_csv('Data/raw_data/gnames/alternateNames.txt',names=altNameKeys,usecols=altNameUseKeys,sep='\t',index_col=0)
    mergeFrame = pd.read_csv(params.mergeFramePath,index_col=0)
    dropList, replacementEntries = deduplicate(mergeFrame,params.provider)
    tempMFrame = mergeFrame.drop(index=dropList)
    dedupedFrame = pd.concat([tempMFrame]+replacementEntries,ignore_index=True)

    #sanity check tests for correctness of deduplicate()
    
    #1. Total coverage of overlapping (geofeed/commercial) IPs did not change
    totalMergeIPs = netaddr_IPSet(mergeFrame.loc[:,'gfeed_ip_prefix'].to_list()).intersection(netaddr_IPSet(mergeFrame.loc[:,'commercial_ip_prefix'].to_list())).size
    totalDedupedIPs = netaddr_IPSet(dedupedFrame.loc[:,'gfeed_ip_prefix'].to_list()).intersection(netaddr_IPSet(dedupedFrame.loc[:,'commercial_ip_prefix'].to_list())).size
    assert totalMergeIPs == totalDedupedIPs,f'Total overlapping IP addresses given in mergeFrame ({totalMergeIPs}) and dedupedFrame ({totalDedupedIPs}) do not match!'
    
    dropList, replacementEntries = deduplicate(dedupedFrame,params.provider)
    tempMFrame = dedupedFrame.drop(index=dropList)
    dedupedFrame = pd.concat([tempMFrame]+replacementEntries,ignore_index=True)
    #pdb.set_trace()


    # dedupMGroups = dedupedFrame.groupby('commercial_ip_prefix')
    # for name, group in dedupMGroups:
    #     nrows,ncol = group.shape
    #     if nrows > 1:
    #         baseGfeedList = group.loc[:,'gfeed_ip_prefix'].to_list()
    #         overlaps = group.loc[:,'gfeed_ip_prefix'].apply(lambda x: netaddr_IPSet([x]).intersection(netaddr_IPSet([y for y in baseGfeedList if y!=x])).size)
    #         if overlaps.max()>0 or group.loc[:,'gfeed_ip_prefix'].value_counts().max()>1:
    #             print(group)
    #             pdb.set_trace()
    # pdb.set_trace()

    #2. All duplicate entries were actually removed
    linewiseNumIPs = dedupedFrame.apply(lambda x: count_overlapping_ips(x),axis=1)
    # pdb.set_trace()
    #debugging code
    # baseGfeedList = dedupedFrame.loc[:,'gfeed_ip_prefix'].unique().tolist()
    # gfeedOverlaps = dedupedFrame.loc[:,'gfeed_ip_prefix'].apply(lambda x: netaddr_IPSet([x]).intersection(netaddr_IPSet([y for y in baseGfeedList if y!=x])))

    # baseCommercialList = group.loc[:,'commercial_ip_prefix'].unique().tolist()
    # dedupedFrame.loc[:,'commercial_ip_prefix'].apply(lambda x: netaddr_IPSet([x]).intersection(netaddr_IPSet([y for y in baseCommercialList if y!=x])))
    assert totalMergeIPs == linewiseNumIPs.sum(),f'Total IPs computed linewise ({linewiseNumIPs.sum()}) differs from the set operation total ({totalMergeIPs}). Check deduplicate() for bugs.'

    mergeFrame = dedupedFrame
    mergeFrame['geoname_id'] = mergeFrame.loc[:,'geoname_id'].astype(pd.Int64Dtype())

    # pdb.set_trace()
    mergeFrame.insert(0,'num_overlapping_ips',linewiseNumIPs)
    
    #set up file path names for output (since convention will be consistent across all files analyzed)
    metricsPath = '/'.join([params.outPath,'metrics'])
    analysisPath = '/'.join([params.outPath,'analysis'])
    mfileName = params.mergeFramePath.split(sep='/')[-1]#e.g. 02.28.2023-02.28.2023-gfeedmaxmind-geoip2-mergeFrame.csv
    fleTokens = mfileName.split(sep='-')[0:-1]
    fleBaseName = '-'.join(fleTokens)
    staleFilePath = '/'.join([metricsPath,'stale_locations',fleBaseName+'-staleIpCtryDist.csv' ])
    birdsEyeMetFpath = '/'.join([metricsPath,'top_level_metrics'])
    misLocErrPath = '/'.join([metricsPath,'mislocation_error'])
    unmappedSubregs = unmappedCtrys = cfilledMismatches = None# init vars so they stay in scope later
    if params.provider!='ipgeolocation-io':
        (matchTuple,mismatchTuple) = find_maxmind_mismatches(mergeFrame)#,gnameMap)
        (matches,trueEmpties) = matchTuple
        (misMatches,staleMismatches,countryMismatches,subregMismatches,cityMismatches) = mismatchTuple
        staleIpCtryDistDF = weight_stales_by_numIPs(staleMismatches,'country_iso_code.1')
        staleIpCtryDistDF.to_csv(staleFilePath)
        (mismatchLocs,noMatchCtys) = lookup_gfeed_location_coords(gnameMap,misMatches)
        # pdb.set_trace()
        noCityId = misMatches.where(misMatches.loc[:,'city_name'].isna()).dropna(how='all')
        
        #process entries w/no city name
        gCodedNoCity, unmappedSubregs, unmappedCtrys = gname_frame_lookup(noCityId,gnameMap)
        cfilledMismatches = pd.concat([mismatchLocs,gCodedNoCity])
        testFileName = mfileName.split(sep='-')[:-1]
        testFileName.append('cfilledMismatches.csv')
        testFileName = '-'.join(testFileName)
        cfilledMismatches.to_csv('/'.join([metricsPath,testFileName]))
        print("This is a placeholder so I can see the values of Maxmind matches and misMatches")
        checkVar = cfilledMismatches.where(cfilledMismatches.loc[:,'gfeed_longitude'].isna()).dropna(how='all')
        # pdb.set_trace()
        cfilledMismatches = cfilledMismatches.where(cfilledMismatches.loc[:,'gfeed_longitude'].notna()).dropna(how='all')
        cfilledMismatches,meanDistError = calc_mean_error_distance(cfilledMismatches,matches)
        mismatchRank,resVals = get_metrics_by_mismatched_country(countryMismatches,params.provider)
        nonEmptyMergeFrame = mergeFrame.where(mergeFrame.loc[:,'country_iso_code'].notna()).dropna(how='all')
        countryMisLocsbyNum = get_countryWise_error_rates(nonEmptyMergeFrame,mismatchRank)
        countryMisLocs = countryMisLocsbyNum.sort_values(by='error_fraction',ascending=False,kind='stable')
        countryMisLocs = countryMisLocs.reset_index()
        countryMisLocs.rename(columns={'index':'rank_total_mislocated_ips'},inplace=True)
        # pdb.set_trace()#Take inventory of what you've calculated (and where/which vars) and what still needs to be calc'd 
        
        # "Bird's eye view" metrics:
        
        #Total matching IPv4 addresses
        #If the commercial location estimate is in the same country as the gfeed result and its coordinates within 5km (~3.1miles) (geodesic dist) of its estimated coordinates (not accounting for elevation), we'll consider it an effective match
        countryLvlCutoff = cfilledMismatches.loc[:,['country_iso_code','country_iso_code.1']].apply(lambda x: x['country_iso_code'] == x['country_iso_code.1'],axis=1)
        effectiveMatchFilter = cfilledMismatches.loc[:,'estimated_error(km)'].apply(lambda x: x<=5) & countryLvlCutoff
        mismatchFilter = effectiveMatchFilter.apply(lambda x:not(x))
        effectiveMatches = cfilledMismatches.where(effectiveMatchFilter).dropna(how='all')
        efMismatches = cfilledMismatches.where(mismatchFilter).dropna(how='all')
        calcdMatches = matches.loc[:,'num_overlapping_ips'].sum()+effectiveMatches.loc[:,'num_overlapping_ips'].sum()
        ctryLvlMatchFilter = mergeFrame.loc[:,['country_iso_code','country_iso_code.1']].apply(lambda x: x['country_iso_code']==x['country_iso_code.1'],axis=1)
        ctryLvlMatches = mergeFrame.where(ctryLvlMatchFilter).dropna(how='all')

        #bird's eye view metrics file-prep
        birdsEyeMets = {
                'total_overlapping_ips':[mergeFrame.loc[:,'num_overlapping_ips'].sum()],
                'num_distinct_gfeed_countries': [mergeFrame.loc[:,'country_iso_code'].nunique()],
                'matching_geoloc_ips(error<=5km)':[calcdMatches],
                'country_level_matched_ips': [ctryLvlMatches.loc[:,'num_overlapping_ips'].sum()],
                'mismatching_ips(error >5km)':[efMismatches.loc[:,'num_overlapping_ips'].sum()], 
                'mean_misloc_error_dist': [meanDistError],
                'stale_ips': [staleMismatches.loc[:,'num_overlapping_ips'].sum()]
                }
        birdDf = pd.DataFrame(birdsEyeMets)
        birdDf.to_csv('/'.join([birdsEyeMetFpath,fleBaseName+'-birdsEyeMetrics.csv']))

        dirName = '/'.join([misLocErrPath,fleBaseName+'-error'])
        try:
            os_mkdir(dirName)
        except FileExistsError:
            pass

        #DF for CDF of error distance
        errDist = cfilledMismatches.loc[:,'estimated_error(km)'].value_counts().reset_index()
        errDist= errDist.rename(columns={'index':'estimated_error(km)', 'estimated_error(km)':'total_incidence'})
        normErrDist = cfilledMismatches.loc[:,'estimated_error(km)'].value_counts(normalize=True).reset_index()
        normErrDist = normErrDist.rename(columns={'index':'estimated_error(km)','estimated_error(km)':'normalized_incidence'})
        errDist.insert(2,'normalized_incidence',normErrDist.loc[:,'normalized_incidence'])
        errDist.sort_values(by='estimated_error(km)',inplace=True,kind='stable',ignore_index=True)
        errDist.to_csv('/'.join([dirName,'errorDistCDF.csv']))

        #mislocated countries
        countryMisLocs.to_csv('/'.join([dirName,'countryMisLocs.csv']))
        badGuessCountries = weight_stales_by_numIPs(countryMismatches,'country_iso_code.1')
        badGuessCountries.to_csv('/'.join([dirName,fleBaseName+'-badCountryGuessOverview.csv']))
        bdown = '/'.join([dirName,'country_misLoc_breakdown'])
        try:
            os_mkdir(bdown)
        except FileExistsError:
            pass
        try:
            os_mkdir(bdown +'/byNumber')
            os_mkdir(bdown+'/byPercent')
        except FileExistsError:
            pass

        for i in range(0,20):
            mslocCountryName = pycountry.countries.get(alpha_2=countryMisLocs.loc[i,'country_iso_code']).name
            fleName = mslocCountryName+'-'+str(i)+'-most_ByPercent.csv'
            resVals[countryMisLocs.loc[i,'country_iso_code']].to_csv('/'.join([bdown,'byPercent',fleName])) 
            bguessCntry = countryMisLocsbyNum.loc[i,'country_iso_code']
            bguessCntryName = pycountry.countries.get(alpha_2=bguessCntry).name
            bgfleName = bguessCntryName+'-'+str(i)+'-most_ByNumber.csv'
            resVals[bguessCntry].to_csv('/'.join([bdown,'byNumber',bgfleName]))
    else:
        (matchTuple,mismatchTuple) = find_ipgeIo_mismatches(mergeFrame)
        (matches,trueEmpties) = matchTuple
        (misMatches,staleLocs,countryMismatches,cityMismatches) = mismatchTuple#subregMismatches
        staleIpCtryDistDF = weight_stales_by_numIPs(staleLocs,'commercial_country_iso_code')
        staleIpCtryDistDF.to_csv(staleFilePath)
        (mismatchLocs,noMatchCtys) = lookup_gfeedipgeIo_location_coords(gnameMap,misMatches)
        noCityId = misMatches.where(misMatches.loc[:,'gfeed_city_name'].isna()).dropna(how='all')
        gCodedNoCity,unmappedSubregs,unmappedCtrys = gname_frame_lookup(noCityId,gnameMap,provider='ipgeolocation-io')
        cfilledMismatches = pd.concat([mismatchLocs,gCodedNoCity])
        # pdb.set_trace()
        testFileName = mfileName.split(sep='-')[:-1]
        testFileName.append('cfilledMismatches.csv')
        testFileName = '-'.join(testFileName)
        cfilledMismatches.to_csv('/'.join([metricsPath,testFileName]))
        # pdb.set_trace()
        print("This is a placeholder so I can see the values of ipgelocation.io matches and misMatches")
        cfilledMismatches,meanDistError = calc_mean_error_distance(cfilledMismatches,matches)
        # pdb.set_trace()#Are there any coordinate entries in cfilledMismatches that are NaNs?
        mismatchRank,resVals = get_metrics_by_mismatched_country(countryMismatches,params.provider)
        nonEmptyMergeFrame = mergeFrame.where(mergeFrame.loc[:,'gfeed_country_iso_code'].notna()).dropna(how='all')
        countryMisLocsbyNum = get_countryWise_error_rates(nonEmptyMergeFrame,mismatchRank,provider=params.provider)
        countryMisLocs = countryMisLocsbyNum.sort_values(by='error_fraction',ascending=False,kind='stable')
        countryMisLocs.reset_index()
        countryMisLocs.rename(columns={'index':'rank_total_mislocated_ips'},inplace=True)
        # pdb.set_trace()

        # "Bird's eye view" metrics:

        #Total matching IPv4 addresses
        #If the commercial location estimate is in the same country as the gfeed result and its coordinates within 5km (~3.1miles) (geodesic dist) of its estimated coordinates (not accounting for elevation), we'll consider it an effective match
        countryLvlCutoff = cfilledMismatches.loc[:,['gfeed_country_iso_code','commercial_country_iso_code']].apply(lambda x: x['gfeed_country_iso_code'] == x['commercial_country_iso_code'],axis=1)
        effectiveMatchFilter = cfilledMismatches.loc[:,'estimated_error(km)'].apply(lambda x: x<=5) & countryLvlCutoff
        mismatchFilter = effectiveMatchFilter.apply(lambda x:not(x))
        effectiveMatches = cfilledMismatches.where(effectiveMatchFilter).dropna(how='all')
        efMismatches = cfilledMismatches.where(mismatchFilter).dropna(how='all')
        calcdMatches = matches.loc[:,'num_overlapping_ips'].sum()+effectiveMatches.loc[:,'num_overlapping_ips'].sum()
        ctryLvlMatchFilter = mergeFrame.loc[:,['gfeed_country_iso_code','commercial_country_iso_code']].apply(lambda x: x['gfeed_country_iso_code']==x['commercial_country_iso_code'],axis=1)
        ctryLvlMatches = mergeFrame.where(ctryLvlMatchFilter).dropna(how='all')

        #bird's eye view metrics file-prep
        birdsEyeMets = {
                'total_overlapping_ips':[mergeFrame.loc[:,'num_overlapping_ips'].sum()],
                'num_distinct_gfeed_countries': [mergeFrame.loc[:,'gfeed_country_iso_code'].nunique()],
                'matching_geoloc_ips(error<=5km)':[calcdMatches],
                'country_level_matched_ips': [ctryLvlMatches.loc[:,'num_overlapping_ips'].sum()],
                'mismatching_ips(error >5km)':[efMismatches.loc[:,'num_overlapping_ips'].sum()],
                'mean_misloc_error_dist': [meanDistError],
                'stale_ips': [staleLocs.loc[:,'num_overlapping_ips'].sum()]
                }
        birdDf = pd.DataFrame(birdsEyeMets)
        birdDf.to_csv('/'.join([birdsEyeMetFpath,fleBaseName+'-birdsEyeMetrics.csv']))

        dirName = '/'.join([misLocErrPath,fleBaseName+'-error'])
        try:
            os_mkdir(dirName)
        except FileExistsError:
            pass

        #DF for CDF of error distance
        errDist = cfilledMismatches.loc[:,'estimated_error(km)'].value_counts().reset_index()
        errDist= errDist.rename(columns={'index':'estimated_error(km)', 'estimated_error(km)':'total_incidence'})
        normErrDist = cfilledMismatches.loc[:,'estimated_error(km)'].value_counts(normalize=True).reset_index()
        normErrDist = normErrDist.rename(columns={'index':'estimated_error(km)','estimated_error(km)':'normalized_incidence'})
        errDist.insert(2,'normalized_incidence',normErrDist.loc[:,'normalized_incidence'])
        errDist.sort_values(by='estimated_error(km)',inplace=True,kind='stable',ignore_index=True)
        errDist.to_csv('/'.join([dirName,'errorDistCDF.csv']))

        #mislocated countries
        countryMisLocs.to_csv('/'.join([dirName,'countryMisLocs.csv']))
        badGuessCountries = weight_stales_by_numIPs(countryMismatches,'commercial_country_iso_code')
        badGuessCountries.to_csv('/'.join([dirName,fleBaseName+'-badCountryGuessOverview.csv']))
        bdown = '/'.join([dirName,'country_misLoc_breakdown'])
        try:
            os_mkdir(bdown)
        except FileExistsError:
            pass
        try:
            os_mkdir(bdown +'/byNumber')
            os_mkdir(bdown+'/byPercent')
        except FileExistsError:
            pass

        for i in range(0,20):
            mslocCountryName = pycountry.countries.get(alpha_2=countryMisLocs.loc[i,'gfeed_country_iso_code']).name
            fleName = mslocCountryName+'-'+str(i)+'-most_ByPercent.csv'
            resVals[countryMisLocs.loc[i,'gfeed_country_iso_code']].to_csv('/'.join([bdown,'byPercent',fleName]))
            bguessCntry = countryMisLocsbyNum.loc[i,'gfeed_country_iso_code']
            bguessCntryName = pycountry.countries.get(alpha_2=bguessCntry).name
            bgfleName = bguessCntryName+'-'+str(i)+'-most_ByNumber.csv'
            resVals[bguessCntry].to_csv('/'.join([bdown,'byNumber',bgfleName]))


if __name__== "__main__":
    main()
