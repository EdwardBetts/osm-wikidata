from flask_wtf import FlaskForm

from wtforms.fields import StringField, SelectField, BooleanField
from wtforms import validators
from . import default_change_comments, distance_choices

multi_help = "PLACE will be replaced by the name of the place."
add_wikipedia_tag = "Add wikipedia tags in addition to wikidata tags"


class AccountSettingsForm(FlaskForm):
    single = StringField(
        "Single item change comment",
        [validators.required()],
        default=default_change_comments["single"],
    )
    multi = StringField(
        "Multiple items change comment",
        [validators.required()],
        description=multi_help,
        default=default_change_comments["multi"],
    )
    units = SelectField("Distance units", choices=distance_choices, default="local")
    wikipedia_tag = BooleanField(add_wikipedia_tag, default=False)
