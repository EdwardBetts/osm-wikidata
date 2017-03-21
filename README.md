### Introduction

This tool is running live using the Wikidata SPARQL query service and the OSM
Overpass and Nominatim APIs. It works best with a city, island or
administrative area.

The index page displays list of existing matches and a search box.
If you pick an existing result you'll see Wikidata items within the given
area, for each item there is a list of candidate matching OSM items.

### Matching process

The matching process takes a few minutes. It downloads potential OSM objects
from Overpass and loads them into PostgreSQL with osm2pgsql. The interface
provides status updates while the matching process is running.

A large relation will trigger an Overpass timeout and the match will
fail.

Once the matching process is complete a 'view match candidates' link will
appear. The matching is based on names and English Wikipedia categories.

### Development

* Code: <https://github.com/EdwardBetts/osm-wikidata>
* Tasks: <https://github.com/EdwardBetts/osm-wikidata/issues>
