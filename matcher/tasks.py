"""Procrastinate tasks."""

import traceback

from procrastinate import RetryStrategy

from .procrastinate_app import procrastinate_app
from .wikidata_api import QueryServiceUnavailable


@procrastinate_app.task(
    name="matcher.run_matcher",
    retry=RetryStrategy(
        max_attempts=3,
        exponential_wait=5,
        retry_exceptions={QueryServiceUnavailable},
    ),
)
def run_matcher_task(
    osm_type: str,
    osm_id: int,
    user_id: int | None = None,
    remote_addr: str | None = None,
    user_agent: str | None = None,
    want_isa: list[str] | None = None,
) -> None:
    """Run the matcher for a given place."""
    from matcher import database, mail
    from matcher.job_queue import MatcherJob, MatcherJobFailed
    from matcher.view import app

    with app.app_context():
        job = MatcherJob(
            osm_type=osm_type,
            osm_id=osm_id,
            user=user_id,
            remote_addr=remote_addr,
            user_agent=user_agent,
            want_isa=set(want_isa) if want_isa else set(),
        )
        try:
            job.run_in_app_context()
        except QueryServiceUnavailable as e:
            database.session.rollback()
            msg = f"{e}; retrying matcher task later"
            print(msg)
            job.status(msg)
            raise
        except MatcherJobFailed as e:
            job.failed(str(e))
        except Exception as e:
            error_str = f"{type(e).__name__}: {e}"
            print(error_str)
            traceback.print_exc()
            job.send("error", msg=error_str)
            job.failed(error_str)
            mail.send_traceback("matcher task", prefix="matcher task")
        finally:
            job.close()
