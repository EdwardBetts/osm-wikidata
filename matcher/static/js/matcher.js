var items = [];
var current_item = 0;

window.setInterval(fade, 1100);

function fade() {
   $('.active').fadeOut(500).fadeIn(500);
}

function mark_active(e) {
  // console.log('active:', e.attr('id'));
  e.removeClass('pending').addClass('active');
}

function mark_done(e) {
  // console.log('done:', e.attr('id'));
  e.removeClass('active').addClass('done');
  e.append('<span>&nbsp;<span class="done-label">done</span></span>')
}

function load_wikidata() {
  mark_active($('#load_wikidata'));
  $.post(load_wikidata_url).done (function(data) {
    items = data.item_list;
    $('#item_list_count').text(items.length);
    mark_done($('#load_wikidata'));
    check_overpass();
  });
}

function check_overpass() {
  mark_active($('#load_overpass'));
  $.post(check_overpass_url, function(data) {
    if(data == 'get') {
      trigger_overpass();
    } else {
      mark_done($('#load_overpass'));
      load_osm2pgsql();
    }
  }, 'text');
}

function load_osm2pgsql() {
  mark_active($('#load_osm2pgsql'));
  $.post(load_osm2pgsql_url).done (function(data) {
    if(data == 'done') {
      mark_done($('#load_osm2pgsql'));
      load_match();
    } else { // error
      $('#load_osm2pgsql').removeClass('active');
      $('#osm2pgsql_error_message').text(data);
      $('#osm2pgsql_error').show();
    }
  });
}

function load_match() {
  mark_active($('#load_match'));
  $('#current_item').show();
  load_individual_match();
}

function load_done() {
  $('#current_item').hide();
  $.post(load_ready).done (function() {
    mark_done($('#load_match'));
    $('#candidates-link').show();
  });
}

function load_individual_match() {
  if(current_item >= items.length) {
    load_done();
    return;
  }
  item = items[current_item++];
  $('#item_num').text(current_item);
  $('#item_id').text('Q' + item.id);
  $('#item_name').text(item.name);
  $.post(load_individual_match_url + item.id).done(load_individual_match);
}

$('#candidates-link').hide();
$('#overpass_error').hide();
$('#osm2pgsql_error').hide();
$('#overpass_slow').hide();
$('#current_item').hide();
load_wikidata();

function trigger_overpass() {
  $('#overpass_slow').show();
  $.ajax({
    type: 'POST',
    dataType: 'text',
    contentType: 'text/plain; charset=utf-8',
    url: overpass_url,
    cache: false,
    data: oql,
  })
  .done(function(data) {
    $.ajax({
       type: 'POST',
       dataType: 'text',
       contentType: 'text/xml; charset=utf-8',
       url: post_overpass_url,
       cache: false,
       data: data,
    })
    .done(function(reply) {
       mark_done($('#load_overpass'));
       load_osm2pgsql();
    });
    $('#overpass_slow').hide();
  })
  .fail(function(jqXHR, textStatus, errorThrown) {
    $('#overpass_slow').hide();
    $('#load_overpass').removeClass('active');
    if (errorThrown) {
      $('#overpass_error_message').text(errorThrown);
    } else {
      if(textStatus == 'error') {
        $('#overpass_error_message').text('overpass timeout');
        $.post(overpass_timeout_url);
      } else {
        $('#overpass_error_message').text(textStatus);
      }
    }
    $('#overpass_error').show();
  });
}

