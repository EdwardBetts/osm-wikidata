import requests
import pytest

from matcher import job_queue, wikidata_api
from matcher.job_queue import MatcherJob, bbox_geojson_feature


def test_bbox_geojson_feature():
    feature = bbox_geojson_feature((1, 2, 3, 4), 7)

    assert feature == {
        "type": "Feature",
        "properties": {"chunk_num": 7},
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [3.0, 1.0],
                    [4.0, 1.0],
                    [4.0, 2.0],
                    [3.0, 2.0],
                    [3.0, 1.0],
                ]
            ],
        },
    }


def test_wikidata_chunk_splitting_is_bounded(monkeypatch):
    response = requests.Response()
    response.status_code = 502
    response.reason = "Bad Gateway"
    response.url = "https://query.wikidata.org/sparql"

    class TimeoutPlace:
        def bbox_wikidata_items(self, bbox, want_isa):
            raise wikidata_api.QueryTimeout("query", response)

    job = MatcherJob("relation", 1)
    job.place = TimeoutPlace()
    job.send = lambda *args, **kwargs: None
    job.status = lambda msg: None
    monkeypatch.setattr(job_queue, "WIKIDATA_MAX_CHUNK_SPLIT_DEPTH", 1)

    with pytest.raises(wikidata_api.QueryServiceUnavailable):
        job.wikidata_chunked([(1, 2, 3, 4)])
