from flask import current_app, request, has_app_context
from itertools import islice
from . import mail
import os.path
import json
import math
import user_agents
import humanize

def chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())

def flatten(l):
    return [item for sublist in l for item in sublist]

def drop_start(s, start):
    assert s.startswith(start)
    return s[len(start):]

def remove_start(s, start):
    return s[len(start):] if s.startswith(start) else s

def cache_dir():
    return current_app.config['CACHE_DIR']

def cache_filename(filename):
    return os.path.join(cache_dir(), filename)

def load_from_cache(filename):
    return json.load(open(cache_filename(filename)))

def get_radius(default=1000):
    arg_radius = request.args.get('radius')
    return int(arg_radius) if arg_radius and arg_radius.isdigit() else default

def get_int_arg(name):
    if name in request.args and request.args[name].isdigit():
        return int(request.args[name])

def calc_chunk_size(area_in_sq_km, size=22):
    side = math.sqrt(area_in_sq_km)
    return max(1, math.ceil(side / size))

def file_missing_or_empty(filename):
    return (os.path.exists(filename) or
        os.stat(filename).st_size == 0)

def is_bot():
    ua = request.headers.get('User-Agent')
    return ua and user_agents.parse(ua).is_bot

def log_location():
    return current_app.config['LOG_DIR']

def good_location():
    return os.path.join(log_location(), 'complete')

def capfirst(value):
    ''' Uppercase first letter of string, leave rest as is. '''
    return value[0].upper() + value[1:] if value else value

def find_log_file(place):
    start = '{}_'.format(place.place_id)
    for f in os.scandir(good_location()):
        if f.name.startswith(start):
            return f.path

def check_free_space(config=None):
    ''' Check how much disk space is free.
        E-mail admin if free space is low. '''

    if config is None:
        if not has_app_context():
            return
        config = current_app.config

    min_free_space = config.get('MIN_FREE_SPACE')

    if not min_free_space:  # not configured
        return

    s = os.statvfs('/')
    free_space = s.f_bsize * s.f_bavail

    if free_space > min_free_space:
        return
    readable = humanize.naturalsize(free_space)
    subject = 'Low disk space: {} OSM/Wikidata matcher'.format(readable)

    body = '''
Warning

The OSM/Wikidata matcher server is low on space.

There is currently {} available.
'''.format(readable)

    mail.send_mail(subject, body, config=config)
