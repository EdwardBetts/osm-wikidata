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
  setOsmChunkStateByLayer(chunk, 'empty');
}

var osmChunkStates = {};

function osmChunkStyle(state) {
  var styles = {
    active: {color: '#007bff', weight: 3, opacity: 0.95, fillColor: '#007bff', fillOpacity: 0.10, dashArray: ''},
    done: {color: '#28a745', weight: 2, opacity: 0.85, fillColor: '#28a745', fillOpacity: 0.06, dashArray: ''},
    empty: {color: '#666', weight: 2, opacity: 0.85, fillColor: '#666', fillOpacity: 0.04, dashArray: ''},
    pending: {color: '#3388ff', weight: 3, opacity: 1, fillColor: '#3388ff', fillOpacity: 0.2, dashArray: ''},
  };
  return styles[state] || styles.pending;
}

function bindOsmChunkTooltip(chunkLayer, chunkNum, state) {
  var label = 'OSM chunk ' + (chunkNum + 1);
  if (state === 'active') label += ' (downloading)';
  if (state === 'done') label += ' (complete)';
  if (state === 'empty') label += ' (no Wikidata items)';
  if (chunkLayer.getTooltip()) chunkLayer.unbindTooltip();
  chunkLayer.bindTooltip(label, {sticky: true});
}

function setOsmChunkStateByLayer(chunkLayer, state) {
  var chunkNum = layer.getLayers().indexOf(chunkLayer);
  if (chunkNum === -1) return;
  osmChunkStates[chunkNum] = state;
  chunkLayer.setStyle(osmChunkStyle(state));
  bindOsmChunkTooltip(chunkLayer, chunkNum, state);
}

function setOsmChunkState(chunkNum, state) {
  var chunkLayer = layer.getLayers()[chunkNum];
  if (!chunkLayer) return;
  setOsmChunkStateByLayer(chunkLayer, state);
}

function resetHighlight(e) {
  var chunk = e.target;
  var chunkNum = layer.getLayers().indexOf(chunk);
  chunk.setStyle(osmChunkStyle(osmChunkStates[chunkNum] || 'pending'));
}

function onEachFeature(feature, layer) {
    layer.on({
        mouseover: highlightFeature,
        mouseout: resetHighlight,
    });
}

var map = L.map('mapid');

var layer;
var wikidataChunkLayer = L.layerGroup();
var wikidataChunks = {};
var wikidataSplitLayers = {};

layer = L.geoJSON(chunk_geojson);

layer.addTo(map);
wikidataChunkLayer.addTo(map);
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

function wikidataChunkStyle(state) {
  var styles = {
    active: {color: '#007bff', weight: 3, opacity: 0.95, fillColor: '#007bff', fillOpacity: 0.10, dashArray: ''},
    done: {color: '#28a745', weight: 2, opacity: 0.85, fillColor: '#28a745', fillOpacity: 0.06, dashArray: ''},
    split: {color: '#dc3545', weight: 2, opacity: 0.85, fillColor: '#dc3545', fillOpacity: 0.04, dashArray: '6,4'},
    pending: {color: '#c8973e', weight: 2, opacity: 0.85, fillColor: '#c8973e', fillOpacity: 0.06, dashArray: '4,4'},
  };
  return styles[state] || styles.pending;
}

function bindWikidataChunkTooltip(chunkLayer, chunkNum, state) {
  var label = 'Wikidata chunk ' + chunkNum;
  if (state === 'active') label += ' (requesting)';
  if (state === 'done') label += ' (complete)';
  if (state === 'split') label += ' (split after timeout)';
  if (chunkLayer.getTooltip()) chunkLayer.unbindTooltip();
  chunkLayer.bindTooltip(label, {sticky: true});
}

function addWikidataChunk(feature, state) {
  var chunkNum = feature.properties.chunk_num;
  if (wikidataChunks[chunkNum]) {
    wikidataChunkLayer.removeLayer(wikidataChunks[chunkNum]);
  }
  var chunkLayer = L.geoJSON(feature, {style: wikidataChunkStyle(state)});
  bindWikidataChunkTooltip(chunkLayer, chunkNum, state);
  wikidataChunks[chunkNum] = chunkLayer;
  wikidataChunkLayer.addLayer(chunkLayer);
}

function setWikidataChunkState(chunkNum, state) {
  var chunkLayer = wikidataChunks[chunkNum];
  if (!chunkLayer) return;
  chunkLayer.setStyle(wikidataChunkStyle(state));
  bindWikidataChunkTooltip(chunkLayer, chunkNum, state);
}

function splitWikidataChunk(chunkNum, features) {
  setWikidataChunkState(chunkNum, 'split');
  if (wikidataSplitLayers[chunkNum]) {
    wikidataChunkLayer.removeLayer(wikidataSplitLayers[chunkNum]);
  }
  var splitLayer = L.geoJSON(features, {
    style: wikidataChunkStyle('pending'),
  });
  splitLayer.bindTooltip('New Wikidata sub-chunk', {sticky: true});
  wikidataSplitLayers[chunkNum] = splitLayer;
  wikidataChunkLayer.addLayer(splitLayer);
}
