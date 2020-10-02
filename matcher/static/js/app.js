'use strict';

$(function() {

  $('.hide-all-tags').hide();
  $('.bad-match').hide();
  $('.bad-reported').hide();
  $('#oql').hide();

  $('#uncheck-all').click(function(e) {
    e.preventDefault();
    $('input:checkbox').prop('checked', false);
    $('input:checkbox').change();
  });

  $('.show-tags-link').click(function(e) {
    var link = $(this);
    var show_all = '[show tags]';
    e.preventDefault();
    e.stopPropagation();
    $('#candidate-' + link.data('key')).toggle();
    link.text(link.text() == show_all ? '[hide tags]' : show_all);

  });

  $('.bad-match-link').click(function(e) {
    var link = $(this);
    e.preventDefault();
    e.stopPropagation();
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

  $('.candidate-item').click(function(e) {
    var item = $(this);
    var label = item.find('.item-label');
    var qid = item.attr('id');
    var checkbox = $('input[value=' + qid + ']');
    checkbox.prop('checked', !checkbox.prop('checked'));

    if (checkbox.prop('checked')) {
        item.addClass('border-success').removeClass('border-danger');
        label.addClass('alert-success').removeClass('alert-danger');
    } else {
        item.addClass('border-danger').removeClass('border-success');
        label.addClass('alert-danger').removeClass('alert-success');
    }

    checkbox.change();
  });

  $('.candidate-item a').click(function(e) {
    e.stopPropagation();
  });

  $('.candidate-item input').click(function(e) {
    e.stopPropagation();
  });

  $('.candidate-item :checkbox').change(function(e) {
    e.stopPropagation();

    var item = $(this).closest('.candidate-item');
    var label = item.find('.item-label');
    if (this.checked) {
        item.addClass('border-success').removeClass('border-danger');
        label.addClass('alert-success').removeClass('alert-danger');
    } else {
        item.addClass('border-danger').removeClass('border-success');
        label.addClass('alert-danger').removeClass('alert-success');
    }
  });

  $('input:checkbox').change();
});
