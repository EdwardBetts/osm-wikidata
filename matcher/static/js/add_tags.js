'use strict';

var current_item = 0;
var changeset_id = null;
var update_count = 0;
var comment;
var end = matches.length;

var url = ws_scheme + '://' + location.host + '/websocket/add_tags/' + osm_type + '/' + osm_id;

var connection;

function send(payload) {
    connection.send(JSON.stringify(payload));
}

function start_upload() {
    comment = $('#comment').val();
    if (!comment) {
        $('#status').text('a comment is required');
        return;
    }

    $('#comment').prop('disabled', true);
    $('#save').prop('disabled', true);
    $('#status').text('opening changeset');

    connection = new WebSocket(url);

    connection.onopen = function () {
        send({'comment': comment, 'matches': matches});
    };

    // Log errors
    connection.onerror = function (error) {
        console.log('WebSocket Error ' + error);
    };

    connection.onmessage = onmessage;
}

function update_progress(num) {
    var progress = (current_item * 100) / end;
    $('#upload-progress').css('width', progress + '%');
}

function onmessage(e) {
    var data = JSON.parse(e.data);
    var msg_type = data['type'];

    switch(msg_type) {
      case 'open':
        changeset_id = data['id'];
        var url = 'https://www.openstreetmap.org/changeset/' + changeset_id;
        $('#changeset-link').prop('href', url);
        $('#notice').hide();
        $('#status').text('uploading tags');
        break;
      case 'changeset-error':
        $('#status').text('error opening changeset');
        break;
      case 'progress':
        var num = data['num'];
        var m = matches[num];
        $('#status').text(m['description']);
        update_progress(num);
        $('#' + data['qid']).addClass('table-active');
        break;
      case 'saved':
        $('#' + data['qid']).removeClass('table-active');
        $('#' + data['qid']).addClass('table-success')
        break;
      case 'changeset-error':
        $('#' + data['qid']).removeClass('table-active');
        $('#' + data['qid']).addClass('table-danger')
        break;
      case 'already_tagged':
      case 'deleted':
        $('#' + data['qid']).removeClass('table-active');
        $('#' + data['qid']).addClass('table-warning');
        break;
      case 'closing':
        $('#status').text('closing changeset');
        break;
      case 'done':
        $('#status').text('upload complete');
        $('#done').show();
        break;
    }
}

$(function() {
    $("#save").click(start_upload);
    $("#done").hide();
});
