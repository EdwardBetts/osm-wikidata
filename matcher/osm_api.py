import requests
import lxml.etree
import os.path
from . import model, utils
from time import sleep

base = 'https://www.openstreetmap.org/api/0.6/'

def get_changeset(changeset_id):
    changeset_dir = os.path.join(utils.cache_dir(), 'changesets')
    filename = os.path.join(changeset_dir, f'{changeset_id}.xml')
    if os.path.exists(filename):
        return lxml.etree.parse(filename).getroot()

    url = base + f'changeset/{changeset_id}/download'
    r = requests.get(url)
    r.raise_for_status()
    open(filename, 'wb').write(r.content)
    sleep(1)
    return lxml.etree.fromstring(r.content)

def parse_osm_change(root):
    edits = []
    for e in root:
        osm = e[0]
        qid = osm.find('tag[@k="wikidata"]').get('v')
        edit = model.ChangesetEdit(
            changeset_id=int(osm.get('changeset')),
            osm_type=osm.tag,
            osm_id=int(osm.get('id')),
            saved=osm.get('timestamp'),
            item_id=int(qid[1:]),
        )
        edits.append(edit)

    return edits
