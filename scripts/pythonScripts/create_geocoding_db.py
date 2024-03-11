#!/usr/bin/python3



import pandas as pd,numpy as np
from sqlalchemy import create_engine,URL
from argparse import ArgumentParser

import pdb#debugging




def build_db(filepath,altnamesFname,gnamesFname,password):
    (gnameFrame, altNames, admin1Codes, admin2Codes) = read_data(filepath,altnamesFname,gnamesFname)
    urlObj = URL.create(
            drivername="mysql",
            username="geonames",
            password=password,
            host="localhost",
            port=3306,
            database="geocoding"
            )
    engine = create_engine(urlObj)
    gnameFrame.to_sql('Geocodes',con=engine,if_exists='replace',index_label='id',chunksize=100)
    altNames.to_sql('AlternateNames',con=engine,if_exists='replace',index_label='id',chunksize=100)
    admin1Codes.to_sql('Admin_1_Codes',con=engine,if_exists='replace',index_label='id',chunksize=100)
    admin2Codes.to_sql('Admin_2_Codes',con=engine,if_exists='replace',index_label='id',chunksize=100)



def read_data(filepath,altnamesFname,gnamesFname):
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
    altNamefPath = '/'.join([filepath,altnamesFname])
    altNames = pd.read_csv(altNamefPath,names=altNameKeys,usecols=altNameUseKeys,sep='\t',index_col=0)
    keyFields = [
            'geonameid','location_name',
            'ascii_name','altNames',
            'longitude','latitude',
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
            'longitude','latitude','country_iso_code',
            'admin1_code','admin2_code',
            'admin3_code','admin4_code'
            ]
    keyUseTypes = {
            'geonameid':pd.Int64Dtype(), 'location_name': str,
            'longitude':pd.Float64Dtype(),'latitude': pd.Float64Dtype(),
            'country_iso_code':str, 'admin1_code':str,
            'admin2_code':str,'admin3_code':str,
            'admin4_code':str
            }
    gnamefPath = '/'.join([filepath,gnamesFname])
    gnameFrame = pd.read_csv(gnamefPath,names=keyFields,usecols=keyUseFields,dtype=keyUseTypes,sep='\t',index_col=0)
    admin1fPath = '/'.join([filepath,'admin1CodesASCII.txt '])
    admin1Codes = pd.read_csv(admin1fPath,names=['code','name','name_ASCII','geonameid'],sep='\t')
    admin2fPath = '/'.join([filepath,'admin2Codes.txt'])
    admin2Codes = pd.read_csv(admin2fPath,names=['concatenatedCodes','name','name_ASCII','geonameid'],sep='\t')

    return (gnameFrame, altNames, admin1Codes, admin2Codes)


def parse_inputs():
    desc = 'create_geocoding.py populates tables to create a geocoding database.'
    parser = ArgumentParser(description=desc)
    parser.add_argument('-p',
                        dest='dbPassword',
                        type=str,
                        required=True,
                        help='Password for connecting to the MySQL (geocoding) DB')
    parser.add_argument('-f',
                        dest='filepath',
                        type=str,
                        default='Data/raw_data/gnames/',
                        help='Path to directory holding pulled data you want to input into the DB')
    parser.add_argument('-g',
                        dest='geonamesFileName',
                        type=str,
                        default = '3.22.2023-allCountries.txt',
                        help='Name of file holding the main geonames table data')
    parser.add_argument('-a',
                        dest='altNamesFilename',
                        type=str,
                        default='alternateNames.txt',
                        help='filename of txt file holding the \'Alternate_Names\' table data')
    return parser.parse_args()





def main():
    params = parse_inputs()
    build_db(params.filepath,params.altNamesFilename,params.geonamesFileName,params.dbPassword)



if __name__== "__main__":
    main()
