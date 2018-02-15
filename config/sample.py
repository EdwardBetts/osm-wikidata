SECRET_KEY = '{{ secret_key }}'
ADMIN_NAME = '{{ admin_name }}'
ADMIN_EMAIL = '{{ admin_email }}'
ADMINS = [ADMIN_EMAIL]

DATA_DIR = '{{ data_dir }}'
CACHE_DIR = '{{ cache_dir }}'
OVERPASS_DIR = '{{ overpass_dir }}'
LOG_DIR = '{{ log_dir }}'
WEBASSET_CACHE = '{{ webasset_cache_dir }}'

DB_URL = 'postgresql://{{ db_user }}:{{ db_pass }}@localhost/{{ db_name }}'

TEMPLATES_AUTO_RELOAD = True

PLACE_MIN_AREA = 1      # km^2
PLACE_MAX_AREA = 90000  # km^2

DB_NAME = '{{ db_name }}'
DB_USER = '{{ db_user }}'
DB_PASS = '{{ db_pass }}'
DB_HOST = 'localhost'

SMTP_HOST = 'localhost'
MAIL_FROM = '{{ mail_from }}'


SOCIAL_AUTH_USER_MODEL = 'matcher.model.User'
SOCIAL_AUTH_CLEAN_USERNAMES = False

SOCIAL_AUTH_PIPELINE = (
    'social.pipeline.social_auth.social_details',
    'social.pipeline.social_auth.social_uid',
    'social.pipeline.social_auth.auth_allowed',
    'social.pipeline.social_auth.social_user',
    'social.pipeline.user.get_username',
    'social.pipeline.user.create_user',
    'social.pipeline.social_auth.associate_user',
    'social.pipeline.social_auth.load_extra_data',
    'social.pipeline.user.user_details',
)

SOCIAL_AUTH_LOGIN_URL = '/'
SOCIAL_AUTH_LOGIN_REDIRECT_URL = '/done/'
SOCIAL_AUTH_AUTHENTICATION_BACKENDS = (
    'social.backends.openstreetmap.OpenStreetMapOAuth',
)

SOCIAL_AUTH_OPENSTREETMAP_KEY = '{{ osm_key }}'
SOCIAL_AUTH_OPENSTREETMAP_SECRET = '{{ osm_secret }}'
