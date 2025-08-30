# Parcel-Level-Flood-Risk-NYC
The purpose of this project is to model and score flood risk in New York City



[NYC Parcel Data](https://data.cityofnewyork.us/City-Government/Primary-Land-Use-Tax-Lot-Output-PLUTO-/64uk-42ks/about_data)
```
poetry run python load_csv_to_mongo.py --csv [filename].csv --db nyc --collection parcels --normalize-keys
```

[NYC FEMA Firm Data](https://data-dathere.dataops.dathere.com/bs/dataset/nyc-fema-flood-insurance-rate-map/resource/1fc6953f-9bf4-4c54-8f42-038540fe2c48?utm_source=chatgpt.com)
```
poetry run python load_geojson_to_mongo.py --input [filename].geojson --db nyc --collection fema-firm
```

[NYC Flood Vulnerability Index](https://data.cityofnewyork.us/Environment/New-York-City-s-Flood-Vulnerability-Index/mrjc-v9pm/about_data?utm_source=chatgpt.com)
```
poetry run python load_wkt_csv_to_mongo.py   --csv [filename].csv --db nyc --collection flood-index --crs-in EPSG:2263 --crs-out EPSG:4326 --create-index
```

[NYC Hurricane Evacuation Zone](https://www.nyc.gov/content/planning/pages/resources/datasets/hurricane-evacuation-zones?utm_source=chatgpt.com)
```
poetry run python load_geojson_to_mongo.py --input [filename].geojson --db nyc --collection fema-firm
```
