"""Wikimedia Commons API call."""

import urllib.parse
from typing import Any, Iterable

import requests

from . import CallParams, utils

commons_start = "http://commons.wikimedia.org/wiki/Special:FilePath/"
commons_url = "https://www.wikidata.org/w/api.php"
page_size = 50


def commons_uri_to_filename(uri: str) -> str:
    """Convert commons URI to a filename."""
    return urllib.parse.unquote(utils.drop_start(uri, commons_start))


def api_call(params: CallParams) -> requests.Response:
    """Call the Commons API."""
    call_params: CallParams = {
        "format": "json",
        "formatversion": 2,
        **params,
    }

    return requests.get(commons_url, params=call_params, timeout=5)


def image_detail_params(thumbheight: int | None, thumbwidth: int | None) -> CallParams:
    """Image detail params."""
    params: CallParams = {
        "action": "query",
        "prop": "imageinfo",
        "iiprop": "url",
    }
    if thumbheight is not None:
        params["iiurlheight"] = thumbheight
    if thumbwidth is not None:
        params["iiurlwidth"] = thumbwidth

    return params


def api_image_detail_call(params: CallParams, cur: Iterable[str]) -> requests.Response:
    """Image details API call."""
    call_params = params.copy()
    call_params["titles"] = "|".join(f"File:{f}" for f in cur)

    return api_call(call_params)


def image_detail(
    filenames: list[str], thumbheight: int | None = None, thumbwidth: int | None = None
) -> dict[str, Any]:
    """Get image detail from Wikimedia Commons."""
    params = image_detail_params(thumbheight, thumbwidth)

    images = {}
    for cur in utils.chunk(filenames, page_size):
        r = api_image_detail_call(params, cur)
        for image in r.json()["query"]["pages"]:
            filename = utils.drop_start(image["title"], "File:")
            images[filename] = image["imageinfo"][0] if "imageinfo" in image else None

    return images
