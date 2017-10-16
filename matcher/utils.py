from flask import current_app, request
from itertools import islice
import os.path
import json
import math

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

def calc_chunk_size(area_in_sq_km):
    side = math.sqrt(area_in_sq_km)
    return math.ceil(side / 32)
