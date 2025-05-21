'use strict';

var map = L.map('map');

function add_geojson_layer(geojson) {
  var mapStyle = {fillOpacity: 0};
  var layer = L.geoJSON(geojson, {style: mapStyle});
  layer.addTo(map);
}

var commons_api_url = 'https://commons.wikimedia.org/w/api.php'

var tiles = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
});
var group = L.featureGroup();
var osm_layer = null;

tiles.addTo(map);
map.addLayer(group);

const pinColour = '#00ccff';

const markerHtmlStyles = `
  background-color: ${pinColour};
  width: 1rem;
  height: 1rem;
  display: block;
  left: -0.5rem;
  top: -0.5rem;
  position: relative;
  border-radius: 1rem 1rem 0;
  transform: rotate(45deg);
  border: 1px solid #FFFFFF`;

const highlightColour = '#ff0000';

const highlightHtmlStyles = `
  background-color: ${highlightColour};
  width: 1rem;
  height: 1rem;
  display: block;
  left: -0.5rem;
  top: -0.5rem;
  position: relative;
  border-radius: 1rem 1rem 0;
  transform: rotate(45deg);
  border: 1px solid #FFFFFF`;

const highlightIcon = L.divIcon({
  className: "my-custom-pin",
  iconAnchor: [0, 24],
  labelAnchor: [-6, 0],
  popupAnchor: [0, -36],
  html: `<span style="${highlightHtmlStyles}" />`
});

const icon = L.divIcon({
  className: "my-custom-pin",
  iconAnchor: [0, 24],
  labelAnchor: [-6, 0],
  popupAnchor: [0, -36],
  html: `<span style="${markerHtmlStyles}" />`
});

function drop_osm_layer() {
  if(!osm_layer) return;
  map.removeLayer(osm_layer);
  osm_layer = null;
}

var app = new Vue({
  el: '#app',
  data: {
      osm_type: null,
      osm_id: null,
      current_highlight: null,
      current_marker: null,
      message: "content goes here",
      isa_facets_per_page: 5,
      isa_facets_pages: 1,
      languages: [],
      items: [],
      item_lookup: {},
      language_lookup: {},
      isa_facets: [],
      isa_filter: [],
      isa_lookup: {},
      matches_loaded: false,
      refresh_matcher_url: null,
      login_url: null,
      authenticated: user_is_authenticated,
      image_filenames: [],
      image_to_item: {}
  },
  computed: {
    isa_facets_end: function() {
      return this.isa_facets_per_page * this.isa_facets_pages;
    },
    unsure_items: function() {
        return this.items.filter(item => item.upload_okay && !item.start_ticked);
    },
    ticked_items: function() {
        return this.items.filter(item => item.start_ticked);
    },
  },
  watch: {
      languages: function(new_lang, old_lang) {
          this.update_language(new_lang);
      },
      isa_filter: function(new_filter) {
          localStorage.setItem('isa_filter', JSON.stringify(new_filter));
      }
  },
  methods: {
      set_refresh_matcher_url: function(url) {
        this.refresh_matcher_url = url;
      },
      user_reorder_language: function () {
          // report language order so it can be saved in the database
          var language_codes = this.languages.map(l => l.code);
          axios.post(document.baseURI + '/languages', {languages: language_codes});
      },
      move_to_top: function(index) {
        this.languages.unshift(this.languages[index]);
        this.languages.splice(index + 1, 1);
        this.user_reorder_language();
      },
      update_language: function(new_lang) {
          if (!new_lang)
            return;
          var language_codes = new_lang.map(l => l.code);

          this.items.forEach(item => {
              for (let language of language_codes) {
                  if(item.labels[language]) {
                      item.best_langauge = language;
                      var label = item.labels[language]['label'];
                      item.marker.setTooltipContent(`${label} (${item.qid})`);
                      break;
                  }
              }

              for (let language of language_codes) {
                    if(item.first_paragraphs[language]) {
                        item.first_paragraph = {
                            'language': language,
                            'extract': item.first_paragraphs[language],
                        }
                        break;
                    }
              }

            item.candidates.forEach(c => {
              if(c.names.name) {
                c.label = c.names.name;
              } else {
                for (let language of language_codes) {
                  if(c.names[language]) {
                    c.label = c.names[language];
                    break;
                  }
                }
              }
            });

          });

          this.isa_facets.forEach(isa => {
              for (let language of language_codes) {
                  if(isa.labels[language]) {
                      isa.label = isa.labels[language]['label']
                      isa.description = isa.labels[language]['description']
                      break;
                  }
              }
          });

          for (const qid in this.isa_lookup) {
              var isa = this.isa_lookup[qid];
              for (let language of language_codes) {
                  if(isa.labels[language]) {
                      isa.label = isa.labels[language]['label']
                      isa.description = isa.labels[language]['description']
                      break
                  }
              }
          }

      },
      filter_matches_item: function (item) {
         return this.isa_filter.length == 0 ||
                item.isa_super_qids.some(qid => this.isa_filter.includes(qid));
      },
      tick_item: function (item) {
        if (this.authenticated && item.upload_okay)
            item.ticked = !item.ticked;
      },
      untick_all: function() {
          if (!this.authenticated)
              return
          this.items.forEach(item => item.ticked = false);
      },
      show_on_map: function (item) {
          var marker = item['marker'];
          var qid = item['qid'];

          if (this.current_marker) this.current_marker.setIcon(icon);

          this.current_highlight = qid;
          item['marker'].setIcon(highlightIcon);
          this.current_marker = item['marker'];

          drop_osm_layer();

          this.get_candidate_geojson(qid);

      },
      get_candidate_geojson: function (qid) {
          var item_candidate_json_url = `../../../item_candidate/${qid}.json`;

          var item = this.item_lookup[qid];
          var marker = item['marker'];

          axios.get(item_candidate_json_url)
               .then(response => {
                  if(response.data.candidates.length == 0) {
                    map.setView(marker.getLatLng(), 18);
                    return;
                  }

                  var mapStyle = {fillOpacity: 0};
                  osm_layer = L.geoJSON(null, {'style': mapStyle}).addTo(map);
                  response.data.candidates.forEach( candidate => {
                    osm_layer.addData(candidate.geojson);
                  });
                  bounds = osm_layer.getBounds();
                  bounds.extend(marker.getLatLng());
                  map.fitBounds(bounds);
               });
      },
      show_all_tags: function() {
          this.items.forEach(item => {
              item.candidates.forEach(c => c.show_tags = true);
          });
      },
      get_images: function() {
        var chunk_size = 50;
        var temparray = this.image_filenames.splice(0, chunk_size);
        if (temparray.length == 0) {
          return;  // done
        }
        var titles = temparray.join('|');

        var params = {
          action: 'query',
          prop: 'imageinfo',
          iiprop: 'url',
          iiurlwidth: '400',
          titles: titles,
          formatversion: '2',
          format: 'json',
          origin: '*',
        };
        axios.get(commons_api_url, {params: params}).then(response => {
          response.data.query.pages.forEach(page => {
            this.image_to_item[page.title].image = page.imageinfo[0].thumburl;
          });
          window.setTimeout(this.get_images, 2000);
        });
      },
  },
  mounted () {
    // Initialize isa_filter from localStorage if present
    var saved_filter = localStorage.getItem('isa_filter');
    if (saved_filter) {
        this.isa_filter = JSON.parse(saved_filter);
    }
    axios.get(candidates_json_url)
         .then(response => {
            this.items = response.data.items;
            this.isa_facets = response.data.isa_facets;
            this.isa_lookup = response.data.isa;
            this.osm_type = response.data.osm_type;
            this.osm_id = response.data.osm_id;

            this.languages = response.data.languages;
            this.languages.forEach(l => {
              this.language_lookup[l['code']] = l['lang'];
            });

            this.items.forEach(item => {
                var qid = item.qid;
                this.item_lookup[qid] = item;

                this.$set(item, 'ticked', item.ticked);
                this.$set(item, 'start_ticked', item.ticked || false);
                this.$set(item, 'notes', item.notes);
                this.$set(item, 'best_langauge', null);
                this.$set(item, 'image', null);

                if(item.image_filenames.length > 0) {
                  var filename = 'File:' + item.image_filenames[0];
                  this.image_filenames.push(filename);
                  this.image_to_item[filename] = item;
                }

                item.candidates.forEach(c => {
                    this.$set(c, 'show_tags', false);
                    this.$set(c, 'show_name_match', false);
                    this.$set(c, 'tag_lookup', Object.fromEntries(c.tags));

                });

                // no longer needed, using isa_super_qids instead
                // item.isa_qids = item.isa_list.map(isa => isa.qid);

                var marker = L.marker([item.lat, item.lon],
                                      {'title': item.label_and_qid, 'icon': icon});

                marker.bindTooltip('');
                marker.addTo(group);
                marker.on('click', e => {
                    drop_osm_layer();
                    this.current_highlight = qid;
                    var card = document.getElementById(qid);
                    card.scrollIntoView();
                    var scrolledY = window.scrollY;
                    if(scrolledY){
                        window.scroll(0, scrolledY - 60);
                    }

                    if (this.current_marker) this.current_marker.setIcon(icon);
                    e.target.setIcon(highlightIcon);
                    this.current_marker = e.target;
                    this.get_candidate_geojson(qid);

                });
                item['marker'] = marker;
            });

            this.update_language(this.languages);

            if (this.items.length > 0)
                map.fitBounds(group.getBounds());
            this.matches_loaded = true;
            this.get_images();
         });
  },
});


