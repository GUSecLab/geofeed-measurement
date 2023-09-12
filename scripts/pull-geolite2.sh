#!/usr/bin/env bash

DATE=$(date +"%Y%m%d")
LICENSEKEY="<Add-your-license-key-here>"

pushd ~/geofeed-measurement/Data/raw_data/commercial_dbs/maxmind-geolite2


curl -o GeoLite2-City-CSV_${DATE}.zip "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City-CSV&license_key=${LICENSEKEY}&suffix=zip"
curl -o GeoLite2-City-CSV_${DATE}.zip.sha256 "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City-CSV&license_key=${LICENSEKEY}&suffix=zip.sha256"

sha256sum GeoLite2-City-CSV_${DATE}.zip 

sha256sum --status -c GeoLite2-City-CSV_${DATE}.zip.sha256 
if [ $(echo "$?") = "0" ]; then
	unzip GeoLite2-City-CSV_${DATE}.zip 
else
	rm GeoLite2-City-CSV_${DATE}.zip
fi

curl -o GeoLite2-ASN_${DATE}.zip "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-ASN&license_key=${LICENSEKEY}&suffix=zip"
curl -o GeoLite2-ASN_${DATE}.zip.sha256 "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-ASN&license_key=${LICENSEKEY}&suffix=zip.sha256"

sha256sum GeoLite2-ASN_${DATE}.zip

sha256sum --status -c GeoLite2-ASN_${DATE}.zip.sha256
if [ $(echo "$?") = "0" ]; then
        unzip GeoLite2-ASN_${DATE}.zip
else
        rm GeoLite2-ASN_${DATE}.zip
fi

popd


pushd ~/geofeed-measurement/Data/raw_data/commercial_dbs/maxmind-geoip2

curl -o GeoIP2-City-CSV_${DATE}.zip "https://download.maxmind.com/app/geoip_download?edition_id=GeoIP2-City-CSV&license_key=${LICENSEKEY}&suffix=zip"
curl -o GeoIP2-City-CSV_${DATE}.zip.sha256 "https://download.maxmind.com/app/geoip_download?edition_id=GeoIP2-City-CSV&license_key=${LICENSEKEY}&suffix=zip.sha256"

sha256sum GeoIP2-City-CSV_${DATE}.zip

sha256sum --status -c GeoIP2-City-CSV_${DATE}.zip.sha256
if [ $(echo "$?") = "0" ]; then
        unzip GeoIP2-City-CSV_${DATE}.zip
else
        rm GeoIP2-City-CSV_${DATE}.zip
fi



popd



