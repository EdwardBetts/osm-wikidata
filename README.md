### Introduction

This tool is running live using the Wikidata SPARQL query service and the OSM
Overpass and Nominatim APIs. It works best with a city, island or
administrative area.

The index page displays list of existing matches and a search box.
If you pick an existing result you'll see Wikidata items within the given
area, for each item there is a list of candidate matching OSM items.

### Matching process interface

The matching process takes a few minutes. It downloads potential OSM objects
from Overpass and loads them into PostgreSQL with osm2pgsql. The interface
provides status updates while the matching process is running.

A large relation will trigger an Overpass timeout and the match will
fail.

Once the matching process is complete a 'view match candidates' link will
appear. The matching is based on names and English Wikipedia categories.

### English language Wikipedia categories are used for matching

The matching system makes use of categories on Wikipedia because the
information on Wikidata is incomplete. Wikidata includes an import of all
Wikipedia including the coordinates, but for a lot of items the 'instance of'
property is not set.

Only Wikidata items with a page on English language Wikipedia are included in
the match. In the future I'll add support for matching items without a linked
Wikipedia article.

There is a mapping from Wikipedia category to possible OSM tags. For example if
a Wikipedia article has the word 'station' in any category then railway=station
is added to the list of possible tags.

### Name matching

Each Wikidata name is compared with each name in OSM. Wikidata names are pulled
from the item name, alias and sitelinks in every language. Most name fields on
the OSM side are considered, old\_name and a few others are ignored.

The name comparison includes some normalisation, punctuation is removed.

### Items that are unlikely to match

- Radio stations: not mapped on OpenStreetMap
- Streets: the matcher ignores streets to minimise the Overpass data download
- Sites of Special Scientific Interest: not tagged as such on OSM
- Rivers: river relations are not multipolygons so osm2pgsql ignores them
- Things that no longer exist: Wikidata items that existed in the past
- Organisations: Small organisations within office buildings are not in OSM
- Events and festivals: Not part of OSM

### Development

* Code: <https://github.com/EdwardBetts/osm-wikidata>
* Tasks: <https://github.com/EdwardBetts/osm-wikidata/issues>
