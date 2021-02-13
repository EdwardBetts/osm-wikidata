from flask import current_app, has_app_context
from datetime import datetime, timedelta
from . import mail, database, model, utils

import humanize


def check_free_space(config=None):
    """Check how much disk space is free.
    E-mail admin if free space is low."""

    if config is None:
        if not has_app_context():
            return
        config = current_app.config

    min_free_space = config.get("MIN_FREE_SPACE")

    if not min_free_space:  # not configured
        return

    free_space = utils.get_free_space(config)

    if free_space > min_free_space:
        return

    one_hour_ago = datetime.utcnow() - timedelta(hours=1)
    recent = model.SpaceWarning.most_recent()
    if recent and recent.timestamp > one_hour_ago:
        return  # already sent an alert within the last hour

    readable = humanize.naturalsize(free_space)
    subject = f"Low disk space: {readable} OSM/Wikidata matcher"

    print(f"low space warning: {readable}")

    body = f"""
Warning

The OSM/Wikidata matcher server is low on space.

There is currently {readable} available.
"""

    mail.send_mail(subject, body, config=config)

    alert = model.SpaceWarning(free_space=free_space)
    database.session.add(alert)
    database.session.commit()
