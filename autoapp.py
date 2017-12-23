from matcher.view import app
from matcher import database
from matcher.error_mail import setup_error_mail

app.config.from_object('config.default')
app.debug = False
database.init_app(app)
setup_error_mail(app)
