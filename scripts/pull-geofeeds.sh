#!/usr/bin/env bash



DATE=$(date +"%m.%d.%Y")

pushd ~/geofeed-measurement

./scripts/geofeed-finder-linux-x64 -u -o Data/raw_data/geofeed-pulls/${DATE}-result.csv > Data/raw_data/geofeed-pulls/${DATE}-geofeedConsole.csv


less Data/raw_data/geofeed-pulls/${DATE}-result.csv |grep -v :: |grep -v : > Data/cleaned_data/ipv4-geofeed-records/${DATE}-ipv4-result.csv

less Data/cleaned_data/ipv4-geofeed-records/${DATE}-ipv4-result.csv |
	sort |uniq |
	sed 's/[A-Z,\s]*$//g'|
	sed 's/[A-Z][a-z]*//g'|
	sed 's/,[-]*//g'|
	sed 's/\/[0-9]*//g'|
	sed 's/[a-z]//g' > Data/cleaned_data/cymru-ipInputs/${DATE}-geofeed-ipv4-addrs.txt



# less Data/raw_data/geofeed-pulls/${DATE}-geofeedConsole.csv |
# 	grep ^inetnum: |
# 	grep -v :: |
#        	sort | uniq |
# 	sed 's/^inetnum: //g' |
# 	sed 's/\[cache\]//g' |
# 	sed 's/\[download\]//g' > Data/cleaned_data/geofeed-srcs-withCIDRs/geofeed-urls/${DATE}-geofd-urls.csv


less Data/raw_data/geofeed-pulls/${DATE}-geofeedConsole.csv |
       	sort | uniq |
       	grep -v Error: |
       	grep -v http:// |
	grep -v Cannot |
	sed 's/[A-Z,\s]*$//g'|
       	sed 's/[A-Z][a-z]*//g'|
       	sed 's/,[-]*//g' |
       	sed 's/\[.\+\].*//g' > Data/cleaned_data/geofeed-srcs-withCIDRs/geofeed-urls/${DATE}-geofd-urls.csv 




# less Data/cleaned_data/geofeed-srcs-withCIDRs/geofeed-urls/${DATE}-geofd-urls.csv |
# 	sed -e 's/https:\/\/[a-zA-Z0-9._\/\?%=#~&-]*//g'|
# 	sed -e 's/http:\/\/[a-zA-Z0-9._\/\?%=#~&-]*//g'|
#         sed 's/\/[0-9]* //g'|
# 	sed 's/[a-z]//g' > Data/cleaned_data/cymru-ipInputs/${DATE}-gConsole-ipv4-addrs.txt


source bin/activate

python scripts/pythonScripts/cymru-asnmap.py -f Data/cleaned_data/cymru-ipInputs/${DATE}-geofeed-ipv4-addrs.txt -o Data/cleaned_data/geofeed-srcs-withCIDRs/asn_matched/${DATE}-gfeedCymru-asn.csv

# python scripts/pythonScripts/cymru-asnmap.py -f Data/cleaned_data/cymru-ipInputs/${DATE}-gConsole-ipv4-addrs.txt -o Data/cleaned_data/geofeed-srcs-withCIDRs/asn_matched/${DATE}-gconsoleCymru-asn.csv

popd
