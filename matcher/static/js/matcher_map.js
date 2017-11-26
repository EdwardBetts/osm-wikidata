function highlightFeature(e) {
    var layer = e.target;

    layer.setStyle({
        weight: 5,
        fill: false,
        dashArray: '',
    });

    if (!L.Browser.ie && !L.Browser.opera && !L.Browser.edge) {
        layer.bringToFront();
    }
}

function empty_style(chunk) {
  chunk.setStyle({color: '#666'});
}

function resetHighlight(e) {
  var chunk = e.target;
  // layer.resetStyle(chunk);

  layer_id = layer.getLayerId(chunk);
  if(empty_layers.indexOf(layer_id) != -1) {
      empty_style(chunk);
  }
}

function onEachFeature(feature, layer) {
    layer.on({
        mouseover: highlightFeature,
        mouseout: resetHighlight,
    });
}

var map = L.map('mapid');

var layer;

if (chunk_geojson.length == 1) {
  layer = L.geoJSON(chunk_geojson);
} else {
  layer = L.geoJSON(chunk_geojson, {onEachFeature: onEachFeature});
}

layer.addTo(map);
map.fitBounds(layer.getBounds());
var tiles = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png');
tiles.addTo(map);

function add_pin(item) {
  var marker = L.marker([item.lat, item.lon]);

  var label = document.createElement('div');
  var link = document.createElement('a');
  link.setAttribute('href', 'https://www.wikidata.org/wiki/' + item.qid)
  link.appendChild(document.createTextNode(item.label));
  label.appendChild(link)
  label.appendChild(document.createTextNode(" (" + item.qid + ")"))
  marker.bindPopup(label);
  return marker;
}

function load_wikidata_pins() {
  // load wikidata items via AJAX
  // not used: we access via websocket instead
  $.getJSON(wikidata_json_url).done(function(data) {
      $.each(data.items, function(i, item) {
        add_pin(item);
      });
  });
}
