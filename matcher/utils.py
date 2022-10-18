"""Various utility functions."""

import json
import math
import os.path
import re
from itertools import islice
from typing import Any, Iterable, Iterator, cast

import flask
import pattern.en
import user_agents

metres_per_mile = 1609.344
feet_per_metre = 3.28084
feet_per_mile = 5280


def chunk(it: Iterable[Any], size: int) -> Iterator[Any]:
    """Split an iterable into chunks of the given size."""
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


def flatten(top_list: list[list[Any]]) -> list[Any]:
    """Flatten a list."""
    return [item for sub_list in top_list for item in sub_list]


def drop_start(s: str, start: str) -> str:
    """Remove string prefix, otherwise throw an error."""
    assert s.startswith(start)
    return s[len(start) :]


def remove_start(s: str, start: str) -> str:
    """Remove a string prefix, if present."""
    return s[len(start) :] if s.startswith(start) else s


def normalize_url(url: str) -> str:
    """Standardize URLs to help in comparison."""
    for start in "http://", "https://", "www.":
        url = remove_start(url, start)
    return url.rstrip("/")


def contains_digit(s: str) -> bool:
    """Check if string contains a digit."""
    return any(c.isdigit() for c in s)


def cache_dir() -> str:
    """Get cache dir location."""
    d: str = flask.current_app.config["CACHE_DIR"]
    return d


def cache_filename(filename: str) -> str:
    """Get absolute path for cache file."""
    return os.path.join(cache_dir(), filename)


def load_from_cache(filename: str) -> Any:
    """Load JSON data from cache."""
    return json.load(open(cache_filename(filename)))


def get_radius(default: int = 1000) -> int | None:
    """Get radius request argument with default."""
    arg_radius = flask.request.args.get("radius")
    return int(arg_radius) if arg_radius and arg_radius.isdigit() else default


def get_int_arg(name: str) -> int | None:
    """Get an request arg and convert to integer."""
    v = flask.request.args.get(name)
    return int(v) if v and v.isdigit() else None


def calc_chunk_size(area_in_sq_km: float, size: int = 22) -> int:
    """Work out the size of a chunk."""
    side = math.sqrt(area_in_sq_km)
    return max(1, math.ceil(side / size))


def file_missing_or_empty(filename: str) -> bool:
    """Check if a file is missing or empty."""
    return os.path.exists(filename) or os.stat(filename).st_size == 0


def is_bot() -> bool:
    """Is the current request from a web robot."""
    ua = flask.request.headers.get("User-Agent")
    return bool(ua and user_agents.parse(ua).is_bot)


def log_location() -> str:
    """Get log location from Flask config."""
    return cast(str, flask.current_app.config["LOG_DIR"])


def capfirst(value: str) -> str:
    """Uppercase first letter of string, leave rest as is."""
    return value[0].upper() + value[1:] if value else value


def any_upper(value: str) -> bool:
    """Check if string contains any uppercase characters."""
    return any(c.isupper() for c in value)


def get_free_space(config: flask.config.Config) -> int:
    """Return the amount of available free space."""
    s = os.statvfs(config["FREE_SPACE_PATH"])
    return s.f_bsize * s.f_bavail


def metric_display_distance(units: str, dist: float) -> str | None:
    """Convert distance from metres to the specified metric units."""
    if units == "km_and_metres":
        units = "km" if dist > 500 else "metres"
    if units == "metres":
        return f"{dist:,.0f} m"
    if units == "km":
        return f"{dist / 1000:,.2f} km"

    return None


def display_distance(units: str, dist: float) -> str | None:
    """Convert distance from metres to the specified units."""
    if units in ("miles_and_feet", "miles_and_yards"):
        total_feet = dist * feet_per_metre
        miles = total_feet / feet_per_mile

        if miles > 0.5:
            return f"{miles:,.2f} miles"
        else:
            return {
                "miles_and_feet": f"{total_feet:,.0f} feet",
                "miles_and_yards": f"{total_feet / 3:,.0f} yards",
            }[units]

    if units == "miles_and_metres":
        miles = dist / metres_per_mile
        return f"{miles:,.2f} miles" if miles > 0.5 else f"{dist:,.0f} metres"

    return metric_display_distance(units, dist)


def is_in_range(address_range: str, address: str) -> bool:
    """Check if an address is within a range."""
    re_range = re.compile(r"\b(\d+) ?(?:to|-) ?(\d+)\b", re.I)
    re_number_list = re.compile(r"\b([\d, ]+) (?:and|&) (\d+)\b", re.I)
    re_number = re.compile(r"^(?:No\.?|Number)? ?(\d+)\b")

    m_number = re_number.match(address)
    if not m_number:
        return False

    m_range = re_range.search(address_range)
    if m_range:
        start, end = int(m_range.group(1)), int(m_range.group(2))
        if re_range.search(address):
            return False
        return start <= int(m_number.group(1)) <= end

    m_list = re_number_list.search(address_range)
    if m_list:
        numbers = {n.strip() for n in m_list.group(1).split(",")} | {m_list.group(2)}
        if re_number_list.search(address):
            return False
        return m_number.group(1) in numbers

    return False


def pluralize_label(label: dict[str, str]) -> str:
    """Get plural of English label, don't try and pluralize non-English labels."""
    text = label["value"]
    if label["language"] != "en":
        return text

    # pattern.en.pluralize has the plural of 'mine' as 'ours'
    if text == "mine":
        return "mines"

    return cast(str, pattern.en.pluralize(text))
