"""Collection of web forms."""

from flask_wtf import FlaskForm
from wtforms import validators
from wtforms.fields import BooleanField, SelectField, StringField

from . import default_change_comments, distance_choices

multi_help = "PLACE will be replaced by the name of the place."
add_wikipedia_tag = "Add wikipedia tags in addition to wikidata tags"


class AccountSettingsForm(FlaskForm):
    """Account settings form."""

    single = StringField(
        "Single item change comment",
        [validators.InputRequired()],
        default=default_change_comments["single"],
    )
    multi = StringField(
        "Multiple items change comment",
        [validators.InputRequired()],
        description=multi_help,
        default=default_change_comments["multi"],
    )
    units = SelectField("Distance units", choices=distance_choices, default="local")
    wikipedia_tag = BooleanField(add_wikipedia_tag, default=False)
