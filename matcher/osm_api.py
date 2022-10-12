"""OSM API calls."""

import os.path
from time import sleep

import lxml.etree
import requests

from . import model, utils

base = "https://www.openstreetmap.org/api/0.6/"


def get_changeset(changeset_id: int) -> lxml.etree.Element:
    """Get a changeset from OSM."""
    changeset_dir = os.path.join(utils.cache_dir(), "changesets")
    filename = os.path.join(changeset_dir, f"{changeset_id}.xml")
    if os.path.exists(filename):
        return lxml.etree.parse(filename).getroot()

    url = base + f"changeset/{changeset_id}/download"
    r = requests.get(url)
    r.raise_for_status()
    open(filename, "wb").write(r.content)
    sleep(1)
    return lxml.etree.fromstring(r.content)


def parse_osm_change(root: lxml.etree.Element) -> list[model.ChangesetEdit]:
    """Parse an OSM changeset."""
    edits = []
    for e in root:
        osm = e[0]
        qid = osm.find('tag[@k="wikidata"]').get("v")
        edit = model.ChangesetEdit(
            changeset_id=int(osm.get("changeset")),
            osm_type=osm.tag,
            osm_id=int(osm.get("id")),
            saved=osm.get("timestamp"),
            item_id=int(qid[1:]),
        )
        edits.append(edit)

    return edits
