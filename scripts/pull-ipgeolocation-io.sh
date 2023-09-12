#!/usr/bin/env bash


DATE=$(date +"%Y%m%d")
APIKEY="<Your-API-Key>"

pushd ~/geofeed-measurement/Data/raw_data/commercial_dbs/ipgeolocation-io
LASTUPDATE='' && [ -f lastupdate.txt ] && LASTUPDATE="$(< lastupdate.txt)"
echo "Last update: ${LASTUPDATE}"



#check last update
STATUS=$(curl https://database.ipgeolocation.io/status |jq '.ipToCityAndISPDatabase.lastWeeklyUpdate') #|sed 's/ / +/g')
#date --date=$(${LASTUPDATE}|sed 's/ / +/g') 
if [[ -z ${LASTUPDATE} ]] || [[ ${LASTUPDATE} < ${STATUS} ]] #[ $(echo "$?") = "0" ] &&
then
	curl "https://database.ipgeolocation.io/download/ipToCityAndISPDatabase?apiKey=${APIKEY}" --output ${DATE}-ip-city-isp.zip
        unzip ${DATE}-ip-city-isp.zip -d ${DATE}-ip-city-isp
	pushd ${DATE}-ip-city-isp
        find . \! -name "*.md5" -type f|md5sum -c db-ip-city-isp.md5
	popd
        if [ $(echo "$?") = "0" ]
        then
        	ZFILES=$(find ${DATE}-ip-city-isp -name "*.gz" -type f)
        	gunzip -N ${ZFILES}
        	echo ${STATUS}>lastupdate.txt
        else
        	rm ${DATE}-ip-city-isp.zip
        	rm -r ${DATE}-ip-city-isp
        fi
fi

popd

