$(function() {

  $('.all-tags').hide();
  $('.bad-match').hide();
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
});
