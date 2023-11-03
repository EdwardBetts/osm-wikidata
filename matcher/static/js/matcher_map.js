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

layer = L.geoJSON(chunk_geojson);

layer.addTo(map);
map.fitBounds(layer.getBounds());
var tiles = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
});
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
