from flask import (Blueprint, abort, url_for, redirect, current_app, render_template,
                   request, flash, session)
from . import database, utils, jobs
from .place import Place, PlaceMatcher
from .model import SiteBanner, BadMatchFilter, User
import flask_login

admin_blueprint = Blueprint('admin', __name__)

admin_pages = [
    ('admin.admin_site_banner', 'Site banner'),
    ('admin.admin_bad_match', 'Bad match'),
    ('admin.admin_demo_mode', 'Demo mode'),
    ('admin.list_users', 'Users'),
    ('admin.list_active_jobs', 'Active jobs'),
    ('admin.list_recent_jobs', 'Recent jobs'),
    ('admin.list_slow_jobs', 'Slowest jobs'),
]

admin_job_lists = [
    ('admin.list_active_jobs', 'Active jobs'),
    ('admin.list_recent_jobs', 'Recent jobs'),
    ('admin.list_slow_jobs', 'Slowest jobs'),
]

def assert_user_is_admin():
    user = flask_login.current_user
    if not (user.is_authenticated and user.is_admin):
        abort(403)

@admin_blueprint.route('/admin/space')
@flask_login.login_required
def space_report():
    rows = database.get_big_table_list()
    items = [{
        'place_id': place_id,
        'size': size,
        'added': added,
        'candidates_url': url_for('candidates', osm_type=osm_type, osm_id=osm_id),
        'display_name': display_name,
        'state': state,
        'changesets': changeset_count,
        'recent': recent,
    } for place_id, osm_type, osm_id, added, size, display_name, state, changeset_count, recent in rows]

    free_space = utils.get_free_space(current_app.config)

    return render_template('space.html', items=items, free_space=free_space)

@admin_blueprint.route('/admin')
@flask_login.login_required
def admin_index():
    assert_user_is_admin()
    return render_template('admin/index.html',
                           admin_pages=admin_pages)

@admin_blueprint.route('/admin/banner')
@flask_login.login_required
def admin_site_banner():
    assert_user_is_admin()

    q = SiteBanner.query.order_by(SiteBanner.start)
    return render_template('admin/banner.html', q=q)

@admin_blueprint.route('/admin/bad_match', methods=['GET', 'POST'])
def admin_bad_match():
    if request.method == 'POST':
        item = BadMatchFilter(wikidata=request.form['wikidata'],
                              osm=request.form['osm'])
        database.session.add(item)
        database.session.commit()
        return redirect(url_for('.admin_bad_match'))
    q = BadMatchFilter.query.order_by(BadMatchFilter.osm, BadMatchFilter.wikidata)
    return render_template('admin/bad_match.html', q=q)

@admin_blueprint.route('/admin/demo', methods=['GET', 'POST'])
@flask_login.login_required
def admin_demo_mode():
    demo_mode = session.get('demo_mode', False)
    if request.method != 'POST':
        return render_template('admin/demo.html', demo_mode=demo_mode)

    session['demo_mode'] = not demo_mode
    flash('demo mode ' + ('activated' if demo_mode else 'deactivated'))
    return redirect(url_for(request.endpoint))

@admin_blueprint.route('/admin/users')
@flask_login.login_required
def list_users():
    assert_user_is_admin()
    q = User.query.order_by(User.sign_up.desc())
    return render_template('admin/users.html', users=q)


@admin_blueprint.route('/admin/jobs')
@flask_login.login_required
def list_active_jobs():
    assert_user_is_admin()
    job_list = jobs.get_jobs()
    return render_template('admin/active_jobs.html',
                           admin_job_lists=admin_job_lists,
                           items=job_list)

@admin_blueprint.route('/admin/stop/<osm_type>/<int:osm_id>', methods=['GET', 'POST'])
@flask_login.login_required
def stop_job(osm_type, osm_id):
    assert_user_is_admin()
    place = Place.get_or_abort(osm_type, osm_id)
    job = jobs.get_job(place)
    job or abort(404)

    if request.method == 'POST':
        name = place.name_for_changeset
        jobs.stop_job(place)
        flash(f'job stopping: {name}')
        return redirect(url_for('.list_active_jobs'))

    return render_template('admin/stop_job.html', job=job, place=place)

@admin_blueprint.route('/admin/log/<osm_type>/<int:osm_id>/<start>')
def view_log(osm_type, osm_id, start):
    assert_user_is_admin()
    start = start.replace('_', ' ')

    matcher_run = PlaceMatcher.query.get((start, osm_type, osm_id))
    log = matcher_run.read_log()

    return render_template('admin/matcher_log.html',
                           place=matcher_run.place,
                           log=log,
                           matcher_run=matcher_run)

@admin_blueprint.route('/admin/jobs/recent')
@flask_login.login_required
def list_recent_jobs():
    assert_user_is_admin()
    jobs = PlaceMatcher.query.order_by(PlaceMatcher.start.desc()).limit(100)

    return render_template('admin/jobs_list.html',
                           title='Past jobs',
                           admin_job_lists=admin_job_lists,
                           items=jobs)

@admin_blueprint.route('/admin/jobs/slowest')
@flask_login.login_required
def list_slow_jobs():
    assert_user_is_admin()
    duration = PlaceMatcher.end - PlaceMatcher.start
    jobs = (PlaceMatcher.query
                        .filter(PlaceMatcher.end.isnot(None))
                        .order_by(duration.desc())
                        .limit(100))

    return render_template('admin/jobs_list.html',
                           title='Slowest jobs',
                           admin_job_lists=admin_job_lists,
                           items=jobs)
