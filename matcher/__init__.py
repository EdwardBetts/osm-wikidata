from flask import Flask, request

user_agent = 'osm-wikidata/0.1 (https://github.com/EdwardBetts/osm-wikidata; edward@4angle.com)'

default_change_comments = {
    'single': 'add wikidata tag',
    'multi': 'add wikidata tags within PLACE',
}

distance_choices = [
    ('local', 'local units for current place'),
    ('km_and_metres', 'kilometres and metres'),
    ('miles_and_feet', 'miles and feet'),
    ('miles_and_metres', 'miles and metres'),
    ('miles_and_yards', 'miles and yards'),
    ('metres', 'just metres'),
    ('km', 'just kilometres'),
]

country_units = {
    'gb': 'miles_and_metres',  # UK
    'us': 'miles_and_feet',    # USA
    'lr': 'miles_and_metres',  # Liberia
    'mm': 'miles_and_metres',  # Myanmar
    'as': 'miles_and_feet',    # American Samoa
    'bs': 'miles_and_metres',  # Bahamas
    'bz': 'miles_and_metres',  # Belize
    'vg': 'miles_and_metres',  # British Virgin Islands
    'ky': 'miles_and_metres',  # Cayman Islands
    'dm': 'miles_and_metres',  # Dominica
    'fk': 'miles_and_metres',  # Falkland Islands
    'gd': 'miles_and_metres',  # Grenada
    'gu': 'miles_and_feet',    # Guam
    'mp': 'miles_and_feet',    # Northern Mariana Islands
    'ws': 'miles_and_metres',  # Samoa
    'lc': 'miles_and_metres',  # Saint Lucia
    'vc': 'miles_and_metres',  # Saint Vincent and the Grenadines
    'sh': 'miles_and_metres',  # Saint Helena
    'kn': 'miles_and_metres',  # Saint Kitts and Nevis
    'tc': 'miles_and_metres',  # Turks and Caicos Islands
    'vi': 'miles_and_feet',    # US Virgin Islands
}

class MatcherFlask(Flask):
    def log_exception(self, exc_info):
        self.logger.error("""
Path:                 %s
HTTP Method:          %s
Client IP Address:    %s
User Agent:           %s
User Platform:        %s
User Browser:         %s
User Browser Version: %s
GET args:             %s
view args:            %s
URL:                  %s
""" % (
            request.path,
            request.method,
            request.remote_addr,
            request.user_agent.string,
            request.user_agent.platform,
            request.user_agent.browser,
            request.user_agent.version,
            dict(request.args),
            request.view_args,
            request.url
        ), exc_info=exc_info)

def user_agent_headers():
    return {'User-Agent': user_agent}
