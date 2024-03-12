# IPGeolocation Database (IP to City+ISP)

This document includes the complete details of the database files included in the archive to be downloaded.  

This database provides geolocation information in the following languages: English, German, Russian, Japanese, French, Chinese (Simplified), Spanish, Czech, and Italian.  

This database includes 3 database files named “db-place”, “db-country” and “db-ip-geolocation” in the g-zipped CSV format.  

This archive also includes checksum file “db-ip-city-isp.md5” which includes MD5 checksum of all the files included in the downloaded archive. You can use this checksum file to verify all the files against any changes.  

The details of the fields in each database file is given below. The order of the fields in each database file same as they are described here.  

## db-place.csv.gz

| Field | Description |
|-------|-------------|
| id | IP address, for which the security information has been provided here |
| name_en | Name of the place in English |
| name_de | Name of the place in German |
| name_ru | Name of the place in Russian |
| name_ja | Name of the place in Japanese |
| name_fr | Name of the place in French |
| name_cn | Name of the place in Chinese (Simplified) |
| name_es | Name of the place in Spanish |
| name_cs | Name of the place in Czech |
| name_it | Name of the place in Italian |

## db-country.csv.gz

| Field | Description |
|-------|-------------|
| id | Unique ID of the country in the countries database |
| continent_code | ISO 2-letters continent code |
| continent_name_place_id | Place ID to get multi-language continent name from places database |
| country_code_iso_2 | ISO 2-letters country code |
| country_code_iso_3 | ISO 3-letters country code |
| country_name_place_id | Place ID to get multi-language country name from places database |
| capital_name_place_id | Place ID to get multi-language capital name from places database |
| currency_code | Currency code of the country |
| currency_name | Currency name of the country |
| calling_code | Country’s calling code |
| tld | Top level domain of the country |
| languages | Codes of the languages spoken in the country |

## db-ip-geolocation.csv.gz

| Field | Description |
|-------|-------------|
| start_ip | Starting IP address of the range |
| end_ip | Starting IP address of the range |
| country_id | Country ID for the country information from countries database |
| state_place_id | Place ID for the state name of the geolocation from places database |
| district_place_id | Place ID for the district name of the geolocation from places database |
| city_place_id | Place ID for the city name of the geolocation from places database |
| zip_code | Zip code of the location |
| latitude | Latitude of the location |
| longitude | Longitude of the location |
| geoname_id | Geoname ID of the city |
| timezone_name | Standard time zone name of the geolocation’s region |
| isp | Organization which, currently, holds the IP range |
| connection_type | Connection type of the IP range. Note: It is experimental. |
| organization | AS Organization or the parent organization of the current holder of the IP range|

Note: The state, district, connection_type, and organization might be empty in response to some of the queries. Make sure to have flexibility in your implementation for such cases.