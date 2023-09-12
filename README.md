# rahel-dissertation-research
Dissertation research about geofeeds, commercial IP-geolocation (and possibly geoblocking too). 


## Repository File Structure ##
* __Data__ : Data files pulled from geofeeds and commercial IP-geolocation service databases (maintained using LFS- see below for details)
  * __raw\_data__
    * __geofeed-pulls__: geographic location information & console output pulled w/the geofeed finder
    * __commercial_dbs__: data from commercial datasets (maxmind, ipgeolocation.io) pulled as CSV files
    * __gnames__: geonames geocoding data pulled  March 22, 2023
  * __cleaned\_data__:
    * __cleaned\_geofeed\_data__:
       * __geofeed-srcs-withCIDRS__: Text files of (partial) Inetnum records for IPv4 address prefixes.
       * __IPv4-geofeed-records__: Cleaned geofeed pull entries (IPv4 address records only).
       * __cymru_ipInputs__: IPv4 addresses originally taken from geofeed data formatted to allow bulk WHOIS lookups  using Cymru.
       * __gfeed_ipv4_metrics__: DataFrame tables (csv files) w/metrics of geofeeds' coverage over time.
       * __commercial_gfeed_comparisons__: DataFrame tables (csv files) for comparing geolocation info from geofeeds w/estimates given by commercial providers. (See folder's Readme for more details) 
* __scripts__: Houses scripts used for data collection, cleaning and analysis
  * scripts housed directly in this folder (i.e. not within a subfolder) are bash scripts used for data collection. These are then fired off periodically as cron jobs
  * __python\_scripts__: Python scripts used for data preprocessing (i.e. cleaning) and analysis.
* __Figures__: Graphs and Tables visualizing the processed data (see __Data__ directory)
  * __mockups__:
      * __cdfComps__: CDFs of estimated geodesic error of commercial DB records compared to those in geofeeds pulled at roughly the same time. 
      * __overall-metrics__: 
          * __commercial-gfeed-comps__: Additional comparisons of commercial DB and geofeed records (none of which are CDFs)
          * __geofeeds__: Graphs charting the expansion of geofeeds as measured over time
      * __tables__: Data focused on mislocation trends as Latex tables (`\tabular` objects)
          * __bad\_geoloc\_estimates__:
          * __mislocated\_countries__:     

## Software Requirements ##
This project uses:
* Python 3.8 
* git-lfs
* geofeed-finder (see https://github.com/massimocandela/geofeed-finder)
	* versioning info used for data collection: 
                * Used geofeed finder v 1.5.0 until Feb. 01, 2022
		* v. 1.7.0 Feb. 01, 2023 - June 16, 2023
		* v. 1.10 June 16, 2023 - August 28, 2023
		* v. 1.11 September 2023 - (Current version) 


## Setting up your workspace/getting started:
* Install and initialize git-lfs (see [Instructions below](#large-file-management))
* (If missing also install Python 3)
* Clone repository and install required packages:
 ```
 >>> git clone git@github.com:GUSecLab/rahel-dissertation-research.git

 # Configure virtual environment:
 >>> python3 -m venv rahel-dissertation-research/
 
  >>> cd rahel-dissertation-research/
 
 # activate venv and install Python package requirements:
 >>> source bin/activate
 
 >>> pip install -r requirements.txt
 ```


## Large File management
This project uses Git lfs to manage large dataset files.
 More information about this tool can be found here:
https://git-lfs.github.com/

Documentation for Git lfs can be found at:
https://github.com/git-lfs/git-lfs/blob/master/docs/man/

### LFS setup:
#### On Mac 
git-lfs can be installed using Homebrew or MacPorts:
 * Homebrew:  `brew install git-lfs`
 * MacPorts: `port install git-lfs`
 
#### For Debian/Ubuntu Linux distributions
`curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | sudo bash`
and then 
`apt-get install git-lfs`


