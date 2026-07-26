-- osm2pgsql flex output for OWL Places.
--
-- Keep the table and column names used by the matcher while preserving all
-- tagged relations. Relation IDs stay positive because relations have their
-- own table.

local prefix = os.getenv('OWL_PLACES_OSM2PGSQL_PREFIX')
if not prefix or prefix == '' then
    error('OWL_PLACES_OSM2PGSQL_PREFIX is not set')
end

local common_columns = {
    { column = 'name', type = 'text' },
    { column = 'tags', type = 'hstore' },
}

local indexes = {
    { column = 'way', method = 'gist' },
    { column = 'tags', method = 'gin' },
}

local points = osm2pgsql.define_table({
    name = prefix .. '_point',
    ids = { type = 'node', id_column = 'osm_id' },
    indexes = indexes,
    columns = {
        common_columns[1],
        common_columns[2],
        { column = 'way', type = 'point', projection = 3857, not_null = true },
    },
})

local lines = osm2pgsql.define_table({
    name = prefix .. '_line',
    ids = { type = 'way', id_column = 'osm_id' },
    indexes = indexes,
    columns = {
        common_columns[1],
        common_columns[2],
        {
            column = 'way',
            type = 'linestring',
            projection = 3857,
            not_null = true,
        },
    },
})

local polygons = osm2pgsql.define_table({
    name = prefix .. '_polygon',
    ids = { type = 'way', id_column = 'osm_id' },
    indexes = indexes,
    columns = {
        common_columns[1],
        common_columns[2],
        { column = 'way', type = 'polygon', projection = 3857, not_null = true },
    },
})

local relations = osm2pgsql.define_table({
    name = prefix .. '_relation',
    ids = { type = 'relation', id_column = 'osm_id' },
    indexes = indexes,
    columns = {
        common_columns[1],
        common_columns[2],
        { column = 'way', type = 'geometry', projection = 3857, not_null = true },
    },
})

-- This is the set of polygon flags from the old matcher.style. Explicit
-- area=yes/no tagging still takes precedence.
local polygon_keys = {
    ['abandoned:aeroway'] = true,
    ['abandoned:amenity'] = true,
    ['abandoned:building'] = true,
    ['abandoned:landuse'] = true,
    ['abandoned:power'] = true,
    aeroway = true,
    amenity = true,
    area = true,
    ['area:highway'] = true,
    building = true,
    harbour = true,
    historic = true,
    landuse = true,
    leisure = true,
    man_made = true,
    military = true,
    natural = true,
    office = true,
    place = true,
    power = true,
    public_transport = true,
    shop = true,
    sport = true,
    tourism = true,
    water = true,
    waterway = true,
    wetland = true,
}

local function row(object, geometry)
    return {
        name = object.tags.name,
        tags = object.tags,
        way = geometry,
    }
end

local function is_area(object)
    if not object.is_closed or object.tags.area == 'no' then
        return false
    end
    if object.tags.area == 'yes' then
        return true
    end
    for key in pairs(polygon_keys) do
        if object.tags[key] then
            return true
        end
    end
    return false
end

function osm2pgsql.process_node(object)
    points:insert(row(object, object:as_point()))
end

function osm2pgsql.process_way(object)
    if is_area(object) then
        local geom = object:as_polygon()
        if not geom:is_null() then
            polygons:insert(row(object, geom))
            return
        end
    end

    local geom = object:as_linestring()
    if not geom:is_null() then
        lines:insert(row(object, geom))
    end
end

function osm2pgsql.process_relation(object)
    local geom
    if object.tags.type == 'multipolygon' or object.tags.type == 'boundary' then
        geom = object:as_multipolygon()
    end
    if not geom or geom:is_null() then
        geom = object:as_geometrycollection()
    end
    if not geom:is_null() then
        relations:insert(row(object, geom))
    end
end
