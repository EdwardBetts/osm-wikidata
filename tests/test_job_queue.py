from matcher.job_queue import bbox_geojson_feature


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
