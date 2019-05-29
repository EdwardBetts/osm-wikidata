import requests
import lxml.etree
import urllib.parse
from . import utils

commons_api_url = 'https://tools.wmflabs.org/magnus-toolserver/commonsapi.php'
commons_start = 'http://commons.wikimedia.org/wiki/Special:FilePath/'

def commons_uri_to_filename(uri):
    return urllib.parse.unquote(utils.drop_start(uri, commons_start))

def image_detail(filenames, thumbwidth=None):
    if not isinstance(filenames, list):
        filenames = [filenames]
    params = {'image': '|'.join(filenames)}
    if thumbwidth is not None:
        params['thumbwidth'] = thumbwidth
    r = requests.get(commons_api_url, params=params)
    print(r.url)
    root = lxml.etree.fromstring(r.content)

    images = []
    for image in root:
        if image.tag == 'image':
            file_element = image.find('./file')
        elif image.tag == 'file':
            file_element = image
        else:
            continue
        thumb_element = file_element.find('./urls/thumbnail')

        image = {
            'name': image.get('name'),
            'image': file_element.find('./urls/file').text,
            'height': int(file_element.find('./height').text),
            'width': int(file_element.find('./width').text),
        }

        if thumb_element is not None:
            image['thumbnail'] = thumb_element.text

        images.append(image)

    return images
