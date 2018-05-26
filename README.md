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

A large area will trigger an Overpass timeout and the match will fail.

Once the matching process is complete a 'view match candidates' link will
appear. The matching is based on names and English Wikipedia categories.

### One-to-one between OSM and Wikidata

With this system the aim is for a single OSM entity to link to one Wikidata
item. Many geographical entities are represented by multiple objects in OSM with
the same name, for example dual carriageway bridges are mapped as two roadways,
or buildings within a large site like a hospital.

For bridges with two roadways the system will look for the outline of the bridge
tagged with man\_made=bridge and for a hospital or other campus the aim is to
tag the way or relation that represents the entire site.

### English language Wikipedia categories and Wikidata are used for matching

The matching system makes use of categories on Wikipedia because the information
on Wikidata is incomplete. Wikidata includes an import of all Wikipedia
including the coordinates, but for a lot of items the 'instance of' property is
not set.

There is a mapping from Wikipedia category to possible OSM tags. For example if
a Wikipedia article has the word 'station' in any category then railway=station
is added to the list of possible tags.

### Name matching

Each Wikidata name is compared with each name in OSM. Wikidata names are pulled
from the item name, alias and sitelinks in every language. Most name fields on
the OSM side are considered, old\_name and a few others are ignored.

The name comparison includes some normalisation, punctuation is removed.

### Existing OSM tags for Wikidata and Wikipedia

Existing Wikipedia tags are ignored. If an OSM entity already has a Wikidata tag
it will be left alone.

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
* All timestamps stored in the database should be in UTC.

<!--- vim: set syntax=markdown tw=80 spell: --->
