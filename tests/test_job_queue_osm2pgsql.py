"""Tests for osm2pgsql failure reporting."""

from types import SimpleNamespace

import pytest

from matcher import job_queue


def test_subprocess_error_detail_keeps_output_tail():
    output = "old detail\n" + ("x" * 20) + "\nimportant final error"

    detail = job_queue.subprocess_error_detail(output, max_length=30)

    assert detail.startswith("[earlier output omitted]\n")
    assert detail.endswith("important final error")
    assert "old detail" not in detail


def test_run_osm2pgsql_reports_output_and_failed_stage(monkeypatch):
    messages = []
    job = job_queue.MatcherJob(
        osm_type="relation",
        osm_id=176069,
        status_callback=messages.append,
    )
    job.place = SimpleNamespace(
        prefix="osm_test",
        osm2pgsql_cmd=lambda: ["osm2pgsql", "input.xml"],
    )
    monkeypatch.setitem(job_queue.app.config, "DB_PASS", "secret")
    monkeypatch.setattr(
        job_queue.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1,
            stdout="ERROR: malformed XML near line 42\n",
        ),
    )

    with pytest.raises(job_queue.MatcherJobFailed, match="exit status 1"):
        job.run_osm2pgsql()

    error = next(message for message in messages if message["type"] == "error")
    assert error["stage"] == "matching"
    assert "malformed XML near line 42" in error["msg"]
