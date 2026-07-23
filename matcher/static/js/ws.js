'use strict';

var url = ws_scheme + '://' + location.host + '/websocket/matcher/' + osm_type + '/' + osm_id;
var connection = new WebSocket(url);

var messageLog    = document.getElementById('message-log');
var activityWrap  = document.getElementById('activity-wrap');
var activityLine  = document.getElementById('activity-line');

var startTime     = Date.now();
var chunksNonEmpty = Math.max(total_chunks, 1);  // at least 1 for node places
var chunksDone    = 0;
var retryTimer    = null;

/* ── Utilities ─────────────────────────────────────────────── */

function elapsed() {
  var s = Math.round((Date.now() - startTime) / 1000);
  var m = Math.floor(s / 60);
  s = s % 60;
  return m > 0 ? m + ':' + (s < 10 ? '0' + s : s) : s + 's';
}

function logMessage(text, extraClass) {
  var entry = document.createElement('div');
  entry.className = 'log-entry' + (extraClass ? ' ' + extraClass : '');

  var t = document.createElement('span');
  t.className = 'log-time';
  t.textContent = elapsed();

  var tx = document.createElement('span');
  tx.textContent = text;

  entry.appendChild(t);
  entry.appendChild(tx);
  messageLog.appendChild(entry);
  messageLog.scrollTop = messageLog.scrollHeight;
}

function setActivity(text) {
  activityLine.textContent = text;
  activityWrap.classList.remove('d-none');
}

function clearActivity() {
  if (retryTimer) {
    window.clearInterval(retryTimer);
    retryTimer = null;
  }
  activityWrap.classList.add('d-none');
  activityLine.textContent = '';
}

function plural(num, singular, pluralText) {
  return num === 1 ? singular : pluralText;
}

function formatSeconds(total) {
  var m = Math.floor(total / 60);
  var s = total % 60;
  if (m <= 0) return total + ' ' + plural(total, 'second', 'seconds');
  return m + ':' + (s < 10 ? '0' + s : s);
}

function startRetryCountdown(data) {
  if (retryTimer) window.clearInterval(retryTimer);

  var remaining = data.delay;
  var service = data.service || 'Service';
  var reason = data.reason || 'temporarily unavailable';
  var attempts = data.attempt + '/' + data.max_attempts;

  function render() {
    setActivity(
      service + ' ' + reason + '; retrying in ' +
      formatSeconds(remaining) + ' (' + attempts + ')'
    );
  }

  render();
  retryTimer = window.setInterval(function() {
    remaining--;
    if (remaining <= 0) {
      window.clearInterval(retryTimer);
      retryTimer = null;
      setActivity(service + ' retrying now\u2026');
      return;
    }
    render();
  }, 1000);
}

/* ── Stage helpers ──────────────────────────────────────────── */

function stageEl(id) { return document.getElementById(id); }

function setStageActive(id) {
  stageEl(id).classList.add('active');
}

function setStageDone(id) {
  var el = stageEl(id);
  el.classList.remove('active');
  el.classList.add('done');
  el.querySelector('.stage-icon').textContent = '\u2713';
}

function setStageFailed(id) {
  var el = stageEl(id);
  if (!el) return;
  el.classList.remove('active');
  el.classList.add('failed');
  el.querySelector('.stage-icon').textContent = '\u00d7';
}

function failActiveStages() {
  $.each($('.stage.active'), function(_i, el) {
    setStageFailed(el.id);
  });
}

function isActive(id) { return stageEl(id).classList.contains('active'); }
function isDone(id)   { return stageEl(id).classList.contains('done'); }

/* ── Overpass chunk progress ────────────────────────────────── */

function showChunkProgress() {
  document.getElementById('overpass-progress').classList.remove('d-none');
}

function updateChunkProgress() {
  var pct = Math.round(chunksDone / chunksNonEmpty * 100);
  document.getElementById('chunk-progress-bar').style.width = pct + '%';
  document.getElementById('chunk-progress-text').textContent =
    chunksDone + '\u202f/\u202f' + chunksNonEmpty + ' chunks';
}

/* ── Matching progress ──────────────────────────────────────── */

var matchingTotal = 0;

function showMatchingProgress() {
  document.getElementById('matching-progress').classList.remove('d-none');
}

function updateMatchingProgress(num, total) {
  var pct = Math.round(num / total * 100);
  document.getElementById('matching-progress-bar').style.width = pct + '%';
  document.getElementById('matching-progress-text').textContent =
    num + '\u202f/\u202f' + total + ' items';
}

/* ── WebSocket handlers ─────────────────────────────────────── */

connection.onopen = function() {
  console.log('websocket connected');
};

connection.onerror = function(error) {
  console.log('WebSocket Error', error);
};

connection.onmessage = function(e) {
  var data = JSON.parse(e.data);
  connection.send('ack');

  switch (data.type) {

    case 'ping':
      break;

    /* ── Wikidata items stage ─────────────────────── */

    case 'get_wikidata_items':
      setStageActive('stage-wikidata');
      logMessage('Fetching Wikidata items\u2026');
      break;

    case 'load_cat':
      logMessage('Loading Wikipedia categories\u2026');
      break;

    case 'load_cat_done':
      /* categories loaded — items_saved follows immediately */
      break;

    case 'items_saved':
      setStageDone('stage-wikidata');
      setStageActive('stage-details');
      logMessage('Items saved to database');
      break;

    /* ── Overpass stage ───────────────────────────── */

    case 'empty':
      /* Arrives before get_chunk; update non-empty count for progress bar */
      chunksNonEmpty = Math.max(total_chunks - data.empty.length, 1);
      var chunk_layers = layer.getLayers();
      $.each(data.empty, function(i, idx) {
        var chunk = chunk_layers[idx];
        empty_layers.push(layer.getLayerId(chunk));
        empty_style(chunk);
      });
      break;

    case 'get_chunk':
      if (!isActive('stage-overpass') && !isDone('stage-overpass')) {
        if (!isDone('stage-details')) setStageDone('stage-details');
        clearActivity();
        setStageActive('stage-overpass');
        showChunkProgress();
        updateChunkProgress();
      }
      break;

    case 'chunk_done':
      clearActivity();
      chunksDone++;
      updateChunkProgress();
      break;

    case 'overpass_done':
      if (!isDone('stage-details'))  setStageDone('stage-details');
      if (!isDone('stage-overpass')) setStageDone('stage-overpass');
      setStageActive('stage-matching');
      clearActivity();
      logMessage('OSM data download complete');
      break;

    /* ── Map pins ─────────────────────────────────── */

    case 'pins':
      var markers = L.markerClusterGroup();
      $.each(data.pins, function(i, item) {
        markers.addLayer(add_pin(item));
      });
      map.addLayer(markers);
      break;

    /* ── Generic messages ─────────────────────────── */

    case 'msg':
      var text = data.msg;
      if (text.indexOf('using existing Wikidata items') !== -1) {
        /* cached — skip straight past both wikidata stages */
        setStageDone('stage-wikidata');
        setStageDone('stage-details');
        logMessage(text);
      } else if (text.indexOf('Overpass server too busy, retrying in') !== -1) {
        /* retry_wait carries structured data and renders the countdown */
      } else if (text.indexOf('rate limited') !== -1) {
        logMessage(text, 'log-warn');
      } else {
        logMessage(text);
      }
      break;

    case 'retry_wait':
      logMessage(
        data.service + ' ' + data.reason + ', retrying in ' +
        data.delay + ' seconds (' + data.attempt + '/' + data.max_attempts + ')',
        'log-warn'
      );
      startRetryCountdown(data);
      break;

    case 'matching_start':
      matchingTotal = data.total;
      showMatchingProgress();
      updateMatchingProgress(0, data.total);
      break;

    case 'matching_progress':
      updateMatchingProgress(data.num, data.total);
      break;

    case 'item':
      /* High-frequency per-item progress — shown in the activity line only */
      setActivity(data.msg);
      break;

    case 'error':
      clearActivity();
      if (data.stage) setStageFailed('stage-' + data.stage);
      logMessage(data.msg, 'log-error');
      break;

    /* ── Terminal states ──────────────────────────── */

    case 'failed':
      clearActivity();
      failActiveStages();
      if (data.msg) logMessage(data.msg, 'log-error');
      if (connection.readyState === WebSocket.OPEN) connection.close();
      break;

    case 'done':
      setStageDone('stage-matching');
      clearActivity();
      if (connection.readyState === WebSocket.OPEN) connection.close();
      window.location = matcher_done_url;
      break;

    case 'already_done':
      logMessage('Place is already matched', 'log-warn');
      break;

    case 'not_found':
      logMessage('Place not found', 'log-error');
      break;

    case 'connected':
      logMessage('Connected to task queue');
      break;
  }
};
