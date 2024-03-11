# Scripts directory overview:

(Folders are denoted in **bold** and script files are given as `inline snippets`)

* **Scripts**:_ All bash files placed directly in this folder are run as cron jobs for regular data collection_ 
	- `pull-geofeeds.sh`: pulls geofeed data using a geofeed-finder [https://github.com/massimocandela/geofeed-finder] designed as prescribed by RFC9092[https://datatracker.ietf.org/doc/draft-ietf-opsawg-finding-geofeeds/]. Performs basic cleaning of the data as well. 
	- `pull-geolite2.sh`: pulls new releases of the Maxmind Geolite2 and GeoIP2 IP to City database contents as .csv files
	- `pull-ipgeolocation-io.sh`: pulls new releases of the ipgeolocation.io ip-city-isp database contents as both .csv and .mmdb files

	- _geofeed-finder-linux-x64_ (Note: This file is not part of the repo but is necessary for pulling geofeeds - and the bash scripts assume it is located in this directory.)
					For this project I used the Linux executable from release v1.5.0 for geofeed collections pulled between April 2022 - January 2023, release v1.7.0 from January 2023-June 2023, release 1.10 from June 2023-August 2023, release 1.11 from September 2023 - December 2023 and release 1.11.1 from December 2023 onward.

	- **pythonScripts** :  Though all written in Python the scripts in this subfolder can be grouped by functionality as follows:
		- Data Collection scripts: _ These were generally invoked by the bash scripts (see above)_
			- `cymru-asnmap.py`: An updated version of the script originally written by Team Cymru[https://github.com/0xc0da/cymru-asnmap]. Used to perform bulk ASN mapping queries for IP (prefixes) given within geofeed results files and console output.
		- Data Analysis scripts:
			- `processCymruASNs.py`: compile IP prefix-ASN mappings given by `cymru-asnmap.py` outputs for geofeeds and console outputs within a pre-specified date range, calculate the number of unique ASNs in each file assessed and output dataFrame of results for easy graphing.
			- `IpCoverageCalculator.py`: Python class - supports calculation of a geofeed's IPv4 space coverage
			- `calc_commercial_ip_coverage.py`: calculates the IPv4 address space coverage of each of the DB pulls collected for the commercial provider specified.

                        - `tallyAddresses.py`:  Calculates IPv4 space coverage of geofeed results over time. 
			- `compare_geolocation.py`: Finds which IPv4 prefixes are shared between a single geofeed output and a commercial IP-geolocation update/dataset pulled at roughly the same time. Assesses level of agreement between geolocation information given by geofeed and commercial dataset for each of the overlapping IP ranges.

                        -  `measure_geoloc_distance.py`: Removes duplicate overlapping geofeed/commercial CIDR pairings, computes the (estimated) pairwise geodesic distance between disagreeing/mismatching location names 

			- `normalize_by_baseVals.py`: Normalizes Country-wise geofeed opt-in in IPv4 addresses by the number or Internet users as given by the 2023 CIA World Factbook and by Country-wise Pv4 address allocation.
			- `calc_commercial_ip_coverage.py`: Calculates IPv4 address coverage for a specific Maxmind GeoIP2 or Ipgeolocation.io DB pull.

			- `join-analyzeCymruASdb.py`: Maps Cymru ASN mappings to overlapping classifications from the Stanford ASDB. After joining gets Country-wise AS distributions by category. 

  
		- Data Visualization/graphing:
			- `plotmockups.py`: Synthesizes results frame written by `measure_geoloc_distance.py` into a bird's eye view of the comparison metrics over time, graphs the results. This script also contains several additional graphing functions which were run through the ipython augmented interactive console.
			- `buildHeatMap.py`: Functions to read in country-wise datasets and to plot them as geographic heatmaps. Note that this script does not have a `main()` function and will therefore need to be run in an interactive python console.


		- Unused/Deprecated Scripts: _scripts that are not needed in the repo and will likely be removed at a later revision
			- `parseMaxMindASNs.py`
			- `queryASNs.py`: Previous iteration/attempt to get ASN information for geofeed data. Uses the IPWhois package which (as of this writing) is not actively maintained.
                        - `plotCDF.py`
                        - `clean_gnames_data.py`
                        - `create_geocoding_db.py`


## Notes: Running the scripts:
 ### compare\_geolocation.py
 Note that this script is very computationally intensive as it is used to compare commercial datasets with 8 million+ entries to geofeed output. 
			     When actually running the comparisons this script was run on a high-compute capacity VM setup (e2-custom-16-20480) with 16 VCPUs in lieu of its typical one (e2-highmem-2) and still took roughly 17+ hours to finish when run. (These runs therefore save the results of the heaviest computation to output files before running subsequent analysis, and provide the option for passing these in for subsequent runs.) 
			     
For that reason it also saves files to house some of its intermediate processing steps. (We describe these below.) 			     
The basic usage is (given through the `parse_inputs()` function help) as follows:
			     
	
	usage: compare_geolocation.py [-h] [-d PULL_DATE] [-f FEEDPATH] [-c COMMERCIALPATH] [-o OUTPATH] [-v OVERLAPCSV] [-m INTERSECTMAP] {maxmind-geoip2,maxmind-geolite2,ipgeolocation-io}

	compare_geolocation.py - compares published geofeed information with estimates (for overlapping IPv4 prefixes) in a commercial DB pull

	positional arguments:
 	 {maxmind-geoip2,maxmind-geolite2,ipgeolocation-io}
                        Name of the commercial IP-geolocation data source. 
			Currently supported options: ['maxmind-geoip2','maxmind-geolite2','ipgeolocation-io']

	optional arguments:
	  -h, --help            show this help message and exit
	  -d PULL_DATE          Approximate pull date (of both geofeed and commercial) data to be included given as a string of form MM.DD.YYYY
	  -f FEEDPATH           filepath of the directory housing the IPv4 geofeed-pull results (Default: 'Data/cleaned_data/ipv4-geofeed-records')
	  -c COMMERCIALPATH     filepath of the directory housing the IPv4 geofeed-pull results (Default: 'Data/raw_data/commercial_dbs')
	  -o OUTPATH            filepath for directory into which output will be saved (default: 'Data/cleaned_data/commercial-gfeed-comparisons')
	  -v OVERLAPCSV         complete or relative filepath for CSV holding a previously computed series indicating which rows of the commercial input 				 contain IP prefixes/CIDRs that overlap with those included in the geofeed data. 
                                Note: the size of this output must match the number of rows in the commercial block file exactly
 	 -m INTERSECTMAP       Complete or relative filepath for python dict with the mapping between CIDRs in the geofeed and overlapping CIDRs in the 	commercial dataFrame (Default: None)
	  ```
	  
 * Running the script from scratch: Potentially the simplest command to enter, running the script from the raw/minimally preprocessed input data files   
   is done using the default values for all of optional parameters except -p PULL_DATE. 
   e.g. To use geofeed and the maxmind-geoip2 commercial dataset pulled closest to March 13, 2023
	 	 ```
	 	 python scripts/pythonScripts/compare_geolocation.py maxmind-geoip2 -d 03.13.2023
	 	 ```
	  
   As noted above, running the preprocessing and record matching from scratch is very computationally intensive, heavily utilizes multithreading and therefore works best in multicore machines. (Even with a server-type machine with ~ 40 CPU's available, running the example command given above took roughly 18 hours to complete.) For this reason, it saves the results of the most computationally intensive functions - whose files can subsequently be passed in should the full computation fail to complete for any reason.
	
	Intermediate result storage fields, descriptions and file locations:
		- `overlapping`: A boolean pandas Series denoting which commercial CIDRs/IP prefixes overlap with those listed within the geofeed result(s) collected at roughly the same date. This value is stored as a csv file with the following name format:
			
			
			[OUTPATH]/intermediate_data/[gdate]-[cdate]-gfeed[provider]-overlaps.csv
			
			
	where `[OUTPATH]` is the directory specified by the `-o OUTPATH` parameter, 
	- `[gdate]` is a string of form MM.DD.YYYY denoting the date when the geofeed used in the comparison was pulled,
	- `[cdate]` is a string of form MM.DD.YYYY denoting the date when the commercial dataset used in the comparison was pulled,
	- `[provider]` is the commercial ip-geolocation provider specified with the first positional parameter. 
	e.g. Using the example from scratch input given above this filepath would be:
			
		    Data/cleaned_data/commercial-gfeed-comparisons/intermediate_data/03.13.2023-03.14.2023-gfeedmaxmind-geoip2-overlaps.csv
				
	- `gfeedMapping`: python dict (stored as a json object) holding the mapping between IPv4 prefixes in the geofeed, and those within the commercial dataset that contain at least one IPv4 address within the geofeed's prefix set. This is stored in filepath given as:
			
		[OUTPATH]/intermediate_data/[gdate]-[cdate]-gfeed[provider]-gfdMap.json
	
	  following the original example, this file would be stored at:
			
	  	```
		Data/cleaned_data/commercial-gfeed-comparisons/intermideate_data/03.13.2023-03.14.2023-gfeedmaxmind-geoip2-gfdMap.json
		```
			
	-  `commercialBlocks`: (ipgeolocation-io only)  A pandas DataFrame (stored as csv file) holding the cleaned commercial data - i.e. where 'start_ip' and 'end_ip' fields have been replaced by the CIDR prefixes contained within the ranges originally specified in these fields. (In cases where the range could not be specified as a single CIDR, the rest of the row was duplicated for each CIDR within the originally specified IP range.) This file is stored in the directory specified as:
			
			[OUTPATH]/intermediate_data/ipgeolocation-io/[PULLDATE]-ip-city-isp/db-ip-geolocation.csv
			
		where 
		- `[OUTPATH]` is the directory specified by the `-o OUTPATH` parameter
		- `[PULLDATE]` is a string of form `YYYYMMDD` denoting the date when the commercial dataset was pulled
	 	e.g. If in the original example we had compared an ipgeolocation-io dataset to the geofeed data, this file would likely be stored in:
		   
		   	```
			Data/cleaned_data/commercial-gfeed-comparisons/intermediate_data/ipgeolocation-io/20230313-ip-city-isp/db-ip-geolocation.csv
			```
	
* Running the script using the intermediate values:
	- maxmind-geoip2 and maxmind-geolite2:
	 Here you will (only) need to specify the -v OVERLAPCSV and -m INTERSECTMAP parameters
	 e.g. If re-running the script using intermediate values stored from the first example's "from scratch" run, you would enter:
	      
	       python scripts/pythonScripts/compare_geolocation.py maxmind-geoip2 -d 03.13.2023 
	       -v Data/cleaned_data/commercial-gfeed-comparisons/intermediate_data/03.13.2023-03.14.2023-gfeedmaxmind-geoip2-overlaps.csv 
	       -m Data/cleaned_data/commercial-gfeed-comparisons/intermediate_data/03.13.2023-03.14.2023-gfeedmaxmind-geoip2-gfdMap.json
              
		
	- ipgeolocation.io (ipgeolocation-io):
	  Given the additional step taken to clean the `commercialBlocks` DataFrame, you will need to perform a few additional steps before
	  you can run the script using the intermediate values.
		1. Copy the supplementary table csv files from the ipgeolocation.io data's original (raw) data directory to the intermediate location. 
		```
		 cp Data/raw_data/commercial_dbs/ipgeolocation-io/20230313-ip-city-isp/db-country.csv Data/cleaned_data/commercial-gfeed-comparisons/intermediate_data/ipgeolocation-io/20230313-ip-city-isp/
		 cp Data/raw_data/commercial_dbs/ipgeolocation-io/20230313-ip-city-isp/db-place.csv Data/cleaned_data/commercial-gfeed-comparisons/intermediate_data/ipgeolocation-io/20230313-ip-city-isp/
		```
			
		2. When calling the function explicitly specify the `-c COMMERCIALPATH` parameter as `'Data/cleaned_data/commercial-gfeed-comparisons/intermediate_data'`
		3. When calling the function explicitly specify the `-v OVERLAPCSV` and `-m INTERSECTMAP` parameters as well.
			
	    Following steps b. and c. the function call for ipgeolocation.io data collected on March 13, 2023 would look as follows:
			
			
			python scripts/pythonScripts/compare_geolocation.py ipgeolocation-io -d 03.13.2023
			-c Data/cleaned_data/commercial-gfeed-comparisons/intermediate_data 
			-v Data/cleaned_data/commercial-gfeed-comparisons/intermediate_data/03.13.2023-03.14.2023-gfeedipgeolocation-io-overlaps.csv
			-m Data/cleaned_data/commercial-gfeed-comparisons/intermediate_data/03.13.2023-03.14.2023-gfeedipgeolocation-io-gfdMap.json
			
