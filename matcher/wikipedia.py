import collections
import typing

import lxml.etree
import lxml.html
import requests

from . import mail, user_agent_headers, wikidata_oauth
from .utils import chunk, drop_start
from .wikimedia_api_logging import logged_get, logged_request

page_size = 50
extracts_page_size = 20


Pages = list[dict[str, typing.Any]]


class WikipediaQueryError(Exception):
    """A Wikipedia API query failed."""

    def __init__(self, response: requests.Response) -> None:
        self.response = response

    def __str__(self) -> str:
        status = f"{self.response.status_code} {self.response.reason}".strip()
        request_id = self.response.headers.get("x-request-id")
        request_detail = f", request ID {request_id}" if request_id else ""
        excerpt = " ".join(self.response.text.split())
        if len(excerpt) > 300:
            excerpt = excerpt[:297].rstrip() + "..."
        body_detail = f": {excerpt}" if excerpt else ""
        return (
            f"Wikipedia API query failed: HTTP {status}{request_detail}"
            f"{body_detail}"
        )


class WikipediaRateLimited(WikipediaQueryError):
    """Wikipedia rejected a query because the client is rate limited."""


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
    oauth_session = wikidata_oauth.get_request_session()
    if oauth_session is not None:
        r = logged_request(oauth_session, "GET", url, params=p, timeout=10)
    else:
        r = logged_get(
            url, params=p, headers=user_agent_headers(), timeout=10
        )

    if r.status_code == 429:
        raise WikipediaRateLimited(r)

    content_type = r.headers.get("content-type", "").partition(";")[0].lower()
    if r.status_code != 200 or content_type != "application/json":
        mail.error_mail("wikipedia error", p, r)
        raise WikipediaQueryError(r)

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
