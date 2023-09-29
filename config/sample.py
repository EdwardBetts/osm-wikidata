from datetime import timedelta

SECRET_KEY = "{{ secret_key }}"
ADMIN_NAME = "{{ admin_name }}"
ADMIN_EMAIL = "{{ admin_email }}"
ADMINS = [ADMIN_EMAIL]

DATA_DIR = "{{ data_dir }}"
CACHE_DIR = "{{ cache_dir }}"
OVERPASS_DIR = "{{ overpass_dir }}"
LOG_DIR = "{{ log_dir }}"

DB_URL = "postgresql://{{ db_user }}:{{ db_pass }}@localhost/{{ db_name }}"

TEMPLATES_AUTO_RELOAD = True

PLACE_MIN_AREA = 1  # km^2
PLACE_MAX_AREA = 90000  # km^2

SHOW_TOP_SAVE_CANDIDATES = False

DB_NAME = "{{ db_name }}"
DB_USER = "{{ db_user }}"
DB_PASS = "{{ db_pass }}"
DB_HOST = "localhost"

SMTP_HOST = "localhost"
MAIL_FROM = "{{ mail_from }}"

BROWSE_CACHE_TTL = timedelta(days=1)
