# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import datetime, timedelta
import sys
import traceback

from bndl.util import dash
from flask.blueprints import Blueprint
from flask.templating import render_template
from werkzeug.exceptions import NotFound
import flask


blueprint = Blueprint('execute', __name__,
                      template_folder='templates')


class Status(dash.StatusPanel):
    def render(self):
        return dash.status.OK, render_template('execute/status.html')


class Dash(dash.Dash):
    blueprint = blueprint
    status_panel_cls = Status


@blueprint.app_template_filter('task_stats')
def task_stats(tasks):
    tasks = [task for task in tasks if task.group != 'hidden']
    total = len(tasks)

    started = [task.started_on for task in tasks if task.started_on]
    started_on = min(started) if started else None
    started = len(started)

    stopped = [task.stopped_on for task in tasks if task.stopped_on]
    stopped_on = max(stopped) if stopped else None
    stopped = len(stopped)
    all_stopped = stopped == total
    not_pending = stopped == started

    pending = sum(1 for task in tasks if task.started_on and not task.stopped_on)
    cancelled = sum(1 for task in tasks if task.cancelled)
    failed = sum(1 for task in tasks if task.failed)

    completed = stopped - failed - cancelled
    remaining = 0 if all_stopped else total - stopped - failed
    idle = total - started

    if started:
        if not_pending:
            duration = stopped_on - started_on
        else:
            duration = datetime.now() - started_on
    else:
        duration = None

    if completed and remaining and pending:
        # get durations for tasks stopped and those currently pending
        durations_stopped = [task.duration for task in tasks if task.stopped_on]
        durations_pending = [task.duration for task in tasks if task.started_on and not task.stopped_on]
        # sum them together
        duration_stopped = sum(durations_stopped, timedelta())
        duration_pending = sum(durations_pending, timedelta())
        # for the initial tasks use the duration of the tasks still pending as well
        # it will be an underestimate, but less of an underestimate than using duration_stopped
        if len(durations_pending) > len(durations_stopped):
            avg_duration = duration_stopped / len(durations_stopped) + \
                           duration_pending / len(durations_pending)
        else:
            avg_duration = duration_stopped / len(durations_stopped)
        # use the average duration times remaining to compute the total time remaining
        # divided by no remaining - it indicates the concurrency
        # count the duration pending only half (add a bit of pessimism)
        time_remaining = max(timedelta(), avg_duration * (remaining + 1) / pending
                                          - duration_pending / pending)
    else:
        time_remaining = None

    if not_pending:
        finished_on = stopped_on
    elif time_remaining:
        finished_on = datetime.now() + time_remaining
    else:
        finished_on = ''

    return locals()


@blueprint.app_template_filter('task_status')
def task_status(task):
    if task.failed:
        return 'failed'
    elif task.cancelled:
        return 'cancelled'
    elif task.stopped_on:
        return 'done'
    elif task.started_on:
        return 'pending'
    else:
        return ''


@blueprint.app_template_filter('fmt_exc')
def fmt_exc(exc):
    if sys.version_info >= (3, 5):
        return ''.join(traceback.TracebackException.from_exception(exc).format())
    else:
        parts = []
        while exc:
            etype = type(exc)
            ename = etype.__qualname__
            emod = etype.__module__
            if emod not in ("__main__", "builtins"):
                ename = emod + '.' + ename

            chunks = (
                ['Traceback (most recent call last):\n'] +
                traceback.format_tb(exc.__traceback__) +
                [ename + ':' + str(exc)]
            )
            parts.insert(0, ''.join(chunks))
            exc = exc.__cause__
        return '\n\nThe above exception was the direct cause of the following exception:\n\n'.join(parts)


@blueprint.route('/')
def jobs():
    return render_template('execute/jobs.html')


def job_by_id(job_id):
    job_id = int(job_id)
    for job in flask.g.ctx.jobs:
        if job.id == job_id:
            return job


@blueprint.route('/job/<job_id>/')
def job(job_id):
    job = job_by_id(job_id)
    if job:
        return render_template('execute/job.html', job=job)
    else:
        return NotFound()


@blueprint.route('/job/<job_id>/group/<group_id>')
def group(job_id, group_id):
    job = job_by_id(job_id)
    if job:
        tasks = job.group(int(group_id))
        if tasks:
            return render_template('execute/group.html', job=job, tasks=tasks)
    return NotFound()
