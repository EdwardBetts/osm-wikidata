'use strict';

var address = location.host;
// var n = address.indexOf(':');
// var host = address.substring(0, n == -1 ? address.length : n);

var connection = new WebSocket('ws://' + address + '/matcher/' + osm_type + '/' + osm_id + '/run');

var messages = document.getElementById('messages');
var current = document.getElementById('current');

connection.onopen = function () {
    connection.send('start');
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

// Log messages from the server
connection.onmessage = function (e) {
  var data = JSON.parse(e.data);
  if (data['type'] == 'match') {
      var num = data['candidate_count'];
      var line = (data['query_label'] + ' (' + data['qid'] + '): ' +
                  num + ' ' +
                  (num == 1 ? 'candidate' : 'candidates') + ' found');
      current.textContent = line;
  }
  if (data['type'] == 'done') {
      current.textContent = 'finished';
      window.location = candidates_url;
  }
  if ('empty' in data) {
    var chunk_layers = layer.getLayers();
    $.each(data['empty'], function(i, item) {
      var chunk = chunk_layers[item];
      empty_layers.push(layer.getLayerId(chunk));
      empty_style(chunk);
    });
  }
  if ('pins' in data) {
    // post_message('pins added to map');
    $.each(data['pins'], function(i, item) {
      add_pin(item);
    });
  }
  if ('msg' in data) {
    post_message(data['msg']);
  }
};
