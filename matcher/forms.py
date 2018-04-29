from flask_wtf import FlaskForm

from wtforms.fields import StringField, SelectField
from wtforms import validators
from . import default_change_comments, distance_choices, wikipedia_tag_choices

multi_help = 'PLACE will be replaced by the name of the place.'
wikipedia_tag_help = 'Add wikipedia tags in addition to wikidata tags.'

class AccountSettingsForm(FlaskForm):
    single = StringField('Single item change comment',
                         [validators.required()],
                         default=default_change_comments['single'])
    multi = StringField('Multiple items change comment',
                        [validators.required()],
                        description=multi_help,
                        default=default_change_comments['multi'])
    units = SelectField('Distance units',
                       choices=distance_choices,
                       default='local')
    wikipedia_tag = SelectField('Add wikipedia tag to OSM',
                       choices=wikipedia_tag_choices,
                       description=wikipedia_tag_help,
                       default='nothing')
