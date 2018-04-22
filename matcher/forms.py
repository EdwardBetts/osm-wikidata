from flask_wtf import FlaskForm

from wtforms.fields import StringField
from wtforms import validators
from . import default_change_comments

multi_help = 'PLACE will be replaced by the name of the place.'

class AccountSettingsForm(FlaskForm):
    single = StringField('Single item change comment',
                         [validators.required()],
                         default=default_change_comments['single'])
    multi = StringField('Multiple items change comment',
                        [validators.required()],
                        description=multi_help,
                        default=default_change_comments['multi'])
