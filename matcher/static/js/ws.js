'use strict';

var url = ws_scheme + '://' + location.host + '/websocket/matcher/' + osm_type + '/' + osm_id;
var connection = new WebSocket(url);

var messages = document.getElementById('messages');
var current = document.getElementById('current');

connection.onopen = function () {
    console.log('websocket connected');
};

// Log errors
connection.onerror = function (error) {
    console.log('WebSocket Error ' + error);
};

function post_message(msg) {
  var msg_div = document.createElement('div');
  msg_div.appendChild(document.createTextNode(msg));
  messages.appendChild(msg_div);
}

function post_error(msg) {
  var msg_div = document.createElement('div');
  msg_div.appendChild(document.createTextNode(msg));
  msg_div.className = 'error';
  messages.appendChild(msg_div);
}

function empty(data) {
  var empty_count = data.length;
  if (!empty_count)
    return;

  // document.getElementById('empty-msg').style.display = 'block';
  document.getElementById('empty-msg').className = '';
  document.getElementById('chunk-msg').className = 'd-none';

  var span = document.getElementById('empty-count');
  while (span.firstChild) {
      span.removeChild( span.firstChild );
  }
  span.appendChild(document.createTextNode(empty_count));

  var chunk_layers = layer.getLayers();
  $.each(data, function(i, item) {
    var chunk = chunk_layers[item];
    empty_layers.push(layer.getLayerId(chunk));
    empty_style(chunk);
  });
}

// Log messages from the server
connection.onmessage = function (e) {
  var data = JSON.parse(e.data);
  var msg_type = data['type'];

  connection.send('ack');

  var standard = {
    'already_done': 'error: place already ready',
    'get_wikidata_items': 'retrieving items from wikidata',
    'load_cat': 'loading categories from English language Wikipedia',
    'load_cat_done': 'categories loaded',
    'items_saved': 'items saved to database',
    'overpass_done': 'overpass queries complete',
    'overpass_error': 'error retrieving data from overpass',
  }

  if(msg_type in standard) {
      post_message(standard[msg_type]);
      return
  }

  switch(data['type']) {
    case 'ping':
      console.log('ping');
      break;
    case 'msg':
      post_message(data['msg']);
      break;
    case 'error':
      post_error(data['msg']);
      break;
    case 'item':
      current.textContent = data['msg'];
      break;
    case 'done':
      if (connection.readyState === WebSocket.OPEN) {
          connection.close();
      }
      window.location = matcher_done_url;
      break;
    case 'get_chunk':
      post_message('requesting chunk ' + (data['chunk_num'] + 1))
      break;
    case 'chunk_done':
      post_message('chunk ' + (data['chunk_num'] + 1) + ' downloaded')
      break;
    case 'connected':
      post_message('connected to task queue');
      break;
    case 'empty':
      empty(data['empty']);
      break;
    case 'pins':
      var markers = L.markerClusterGroup();
      $.each(data['pins'], function(i, item) {
        var marker = add_pin(item);
        markers.addLayer(marker);
      });
      map.addLayer(markers);
      break;
  }
};
