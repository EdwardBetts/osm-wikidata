from flask import current_app
from itertools import islice
import os.path
import json

def chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())

def flatten(l):
    return [item for sublist in l for item in sublist]

def drop_start(s, start):
    assert s.startswith(start)
    return s[len(start):]

def cache_dir():
    return current_app.config['CACHE_DIR']

def cache_filename(filename):
    return os.path.join(cache_dir(), filename)

def load_from_cache(filename):
    return json.load(open(cache_filename(filename)))
