import json

import requests
import pytest

from matcher import wikidata_api


def response(status_code, *, retry_after=None, entities=None):
    r = requests.Response()
    r.status_code = status_code
    r.url = wikidata_api.wikidata_url
    if retry_after is not None:
        r.headers["Retry-After"] = retry_after
    if entities is not None:
        r._content = json.dumps({"entities": entities}).encode()
    return r


def test_entity_iter_retries_rate_limit_and_reports_wait(monkeypatch):
    responses = [
        response(429, retry_after="7"),
        response(200, entities={"Q1": {"id": "Q1"}}),
    ]
    waits = []
    retries = []
    monkeypatch.setattr(wikidata_api, "api_call", lambda params: responses.pop(0))
    monkeypatch.setattr(wikidata_api.time, "sleep", waits.append)

    entities = list(
        wikidata_api.entity_iter(
            {"Q1"}, retry_callback=lambda *args: retries.append(args)
        )
    )

    assert entities == [("Q1", {"id": "Q1"})]
    assert retries == [(7, 1, 5)]
    assert waits == [7]


def test_entity_iter_raises_after_rate_limit_attempts(monkeypatch):
    r = response(429)
    retries = []
    monkeypatch.setattr(wikidata_api, "api_call", lambda params: r)
    monkeypatch.setattr(wikidata_api.time, "sleep", lambda delay: None)

    with pytest.raises(requests.HTTPError):
        list(
            wikidata_api.entity_iter(
                {"Q1"},
                attempts=2,
                retry_callback=lambda *args: retries.append(args),
            )
        )

    assert retries == [(60, 1, 2)]
