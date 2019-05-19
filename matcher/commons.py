import requests
import lxml.etree

commons_api_url = 'https://tools.wmflabs.org/magnus-toolserver/commonsapi.php'
commons_start = 'http://commons.wikimedia.org/wiki/Special:FilePath/'

def image_detail(commons_filename, thumbwidth=None):
    params = {
        'image': commons_filename,
    }
    if thumbwidth is not None:
        params['thumbwidth'] = thumbwidth
    r = requests.get(commons_api_url, params=params)
    root = lxml.etree.fromstring(r.content)
    file_element = root.find('./file')

    return {
        'image': file_element.find('./urls/file').text,
        'height': file_element.find('./height').text,
        'width': file_element.find('./width').text,
        'thumbnail': file_element.find('./urls/thumbnail').text,
    }
