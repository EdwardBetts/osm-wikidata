'use strict';

$(function() {

  $('.hide-all-tags').hide();
  $('.bad-match').hide();
  $('.bad-reported').hide();
  $('#oql').hide();


  $('#uncheck-all').click(function(e) {
    e.preventDefault();
    $('input:checkbox').prop('checked', false);
  });

  $('.show-tags-link').click(function(e) {
    var link = $(this);
    var show_all = '[show tags]';
    e.preventDefault();
    $('#candidate-' + link.data('key')).toggle();
    link.text(link.text() == show_all ? '[hide tags]' : show_all);

  });

  $('.bad-match-link').click(function(e) {
    var link = $(this);
    e.preventDefault();
    $('#bad-match-' + link.data('key')).toggle();
  });

  $('#oql-toggle').click(function(e) {
    var link = $(this);
    var show = 'show query';
    e.preventDefault();
    $('#oql').toggle();
    link.text(link.text() == show ? 'hide query' : show);
  });

  $('.bad-match-save').click(function(e) {
    var link = $(this);
    e.preventDefault();
    var comment = $('#comment-' + link.data('key')).val();
    var item_id = link.data('item');
    var osm_id = link.data('osm-id');
    var osm_type = link.data('osm-type');

    var url = '/bad_match/Q' + item_id + '/' + osm_type + '/' + osm_id;

    $.post(url, {'comment': comment}).done(function(data) {
        $('#bad-match-' + link.data('key')).hide();
        $('#reported-' + link.data('key')).show();
        $('#report-link-' + link.data('key')).hide();
    });
  });
});
