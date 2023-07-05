import collections
import typing

import lxml.etree
import lxml.html
import requests

from . import mail, user_agent_headers
from .utils import chunk, drop_start

page_size = 50
extracts_page_size = 20


Pages = list[dict[str, typing.Any]]


def run_query(
    titles: collections.abc.Collection[str],
    params: dict[str, typing.Any],
    language_code: str = "en",
) -> Pages:
    base: dict[str, str | int] = {
        "format": "json",
        "formatversion": 2,
        "action": "query",
        "continue": "",
        "titles": "|".join(titles),
    }
    p = base.copy()
    p.update(params)

    url = f"https://{language_code}.wikipedia.org/w/api.php"
    r = requests.get(url, params=p, headers=user_agent_headers())
    expect = "application/json; charset=utf-8"
    success = True
    if r.status_code != 200:
        print(f"status code: {r.status_code}")
        success = False
    if r.headers["content-type"] != expect:
        print(f"content-type: {r.headers['content-type']}")
        success = False
    if not success:
        mail.error_mail("wikipedia error", p, r)
    assert success
    json_reply = r.json()
    return typing.cast(Pages, json_reply["query"]["pages"])


class TitleAndCat(typing.TypedDict):
    title: str
    cats: list[str]


def get_cats(
    titles: collections.abc.Collection[str], language_code: str = "en"
) -> list[TitleAndCat]:
    params = {"prop": "categories", "cllimit": "max", "clshow": "!hidden"}
    # filter out redirects from query result
    return [
        {
            "title": page["title"],
            "cats": [
                drop_start(cat["title"], "Category:") for cat in page["categories"]
            ],
        }
        for page in run_query(titles, params, language_code)
        if "categories" in page
    ]


def get_coords(titles: list[str], language_code: str = "en") -> Pages:
    return run_query(titles, {"prop": "coordinates"}, language_code)


def page_category_iter(titles: list[str]) -> typing.Iterator[tuple[str, list[str]]]:
    for cur in chunk(titles, page_size):
        for page in get_cats(cur):
            yield (page["title"], page["cats"])


def add_enwiki_categories(items):
    enwiki_to_item = {v["enwiki"]: v for v in items.values() if "enwiki" in v}

    page_cats = page_category_iter(enwiki_to_item.keys())
    for title, cats in page_cats:
        enwiki_to_item[title]["categories"] = cats


def get_items_with_cats(items):
    assert isinstance(items, dict)
    for cur in chunk(items.keys(), page_size):
        for page in get_cats(cur):
            items[page["title"]]["cats"] = page["cats"]


def html_names(article: str) -> list[str]:
    if not article or article.strip() == "":
        return []
    try:
        root = lxml.html.fromstring(article)
    except lxml.etree.ParserError:
        return []
    # avoid picking pronunciation guide bold text
    # <small title="English pronunciation respelling"><i><b>MAWD</b>-lin</i></small>
    names = [
        b.text_content()
        for b in root.xpath(".//b[not(ancestor::small)][not(ancestor::ul)]")
    ]
    return [n.strip() for n in names if len(n) > 1]


def extracts_query(
    titles: collections.abc.Collection[str], language_code: str = "en"
) -> Pages:
    params = {
        "prop": "extracts",
        "exlimit": extracts_page_size,
        "exintro": "1",
    }
    return run_query(titles, params, language_code)


def get_extracts(
    titles: collections.abc.Collection[str], code: str = "en"
) -> typing.Iterator[tuple[str, str]]:
    for cur in chunk(titles, extracts_page_size):
        for page in extracts_query(cur, language_code=code):
            if "extract" not in page:
                continue
            extract = page["extract"].strip()
            if extract:
                yield (page["title"], page["extract"])
