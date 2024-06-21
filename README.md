## GeoSearch

A tool for offline querying of the geonames database.

This is a pure Python tool, which uses Whoosh instead of Lucene as a search engine.

This is a pure Python tool, which uses Whoosh instead of Lucene as a search engine.

Our tool allows you to search by name or geographic position, returning attributes related to location. Furthermore, it simplifies interpretation by including details of higher hierarchical levels and description of region codes.

## Instalation

    pip install geosearch


## Database installation

	from geosearch import Geosearch
	
	\# To download the complete database, it takes ~12Gb and can be slow 
	Geosearch.download() 
	
	\# Or, for a single country, use the GeoNames partition url, as in the example:
	Geosearch.download("https://download.geonames.org/export/dump/BR.zip")


## Basic Usage

    from geosearch import Geosearch
	
	
    gsearch = Geosearch()
	res = gsearch.query("São Paulo")
	print(res)


## Search by geographic coordinate

	res = gsearch.query_coord(-23.223,-45.894) \# Lat, lon positions
	print(res)
	
	
## Terminal search

	python -m Geosearch.geosearch --query "São Paulo"
	python -m Geosearch.geosearch --query_coord "-23.223,-45.894"
