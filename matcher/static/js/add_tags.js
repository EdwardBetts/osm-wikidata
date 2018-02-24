"use strict";
var current_item = 0;
var changeset_id = null;
var update_count = 0;


function open_changeset() {
    var comment = $('#comment').val();
    if (!comment) {
        $('#status').text('a comment is required');
        return;
    }
    $('#comment').prop('disabled', true);
    $('#save').prop('disabled', true);
    $('#status').text('opening changeset');
    $.post(open_changeset_url, {'comment': comment}).done(start_upload);
}

function start_upload(data) {
    if (data == 'error') {
        $('#status').text('error opening changeset');
        return
    }
    changeset_id = data;
    var url = 'https://www.openstreetmap.org/changeset/' + changeset_id;
    $('#changeset-link').prop('href', url);

    $('#notice').hide();
    $('#status').text('uploading tags');
    upload_tags();
}

function finished(data) {
    $('#status').text('upload complete');
    $("#done").show();
}

function close_changeset() {
    $('#status').text('closing changeset');
    var comment = $('#comment').val();
    var data = {
        'comment': comment,
        'changeset_id': changeset_id,
        'update_count': update_count,
    }
    $.post(close_changeset_url, data).done(finished);
}

function upload_tags() {
    var end = items.length;

    if(current_item >= end) {
        close_changeset();
        return;
    }

    var item = items[current_item++];
    var progress = (current_item * 100) / end;
    $('#upload-progress').css('width', progress + '%');

    $('#' + item.row_id).addClass('table-active');
    $('#status').text(item.description);

    $.post(item.post_tag_url, {'changeset_id': changeset_id}).done(function(data) {
        $('#' + item.row_id).removeClass('table-active');
        if (data == 'done') {
            $('#' + item.row_id).addClass('table-success')
            update_count += 1;
        } else if (data == 'already tagged') {
            $('#' + item.row_id).addClass('table-warning')
        } else {
            $('#' + item.row_id).addClass('table-danger')
        }

        upload_tags();
    });
};

$(function() {
    $("#save").click(open_changeset);
    $("#done").hide();
});
