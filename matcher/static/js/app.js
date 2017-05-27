$(function() {

  $('.all_tags').hide();

  $('.show_tags_link').click(function(e) {
    var link = $(this);
    var show_all = 'show all tags';
    e.preventDefault();
    $('#candidate' + link.data('key')).toggle();
    link.text(link.text() == show_all ? 'hide tags' : show_all);

  });

  $('#oql').hide();

  $('#oql-toggle').click(function(e) {
    var link = $(this);
    var show = 'show query';
    e.preventDefault();
    $('#oql').toggle();
    link.text(link.text() == show ? 'hide query' : show);

  });
});
