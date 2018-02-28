# -*- coding: utf-8 -*-

"""

    Module :mod:``


    LICENSE: The End User license agreement is located at the entry level.

"""

# ----------- START: Native Imports ---------- #
import time

from datetime import datetime, timedelta
# ----------- END: Native Imports ---------- #

# ----------- START: Third Party Imports ---------- #
import simplejson as json

from apscheduler.jobstores.base import ConflictingIdError

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

from core.scheduler.trigger import OneTimeTrigger, IntervalTrigger, CronTrigger

from apscheduler.events import (
    EVENT_JOBSTORE_ADDED,
    EVENT_JOBSTORE_REMOVED,
    EVENT_SCHEDULER_START,
    EVENT_SCHEDULER_SHUTDOWN,
    EVENT_JOB_ADDED,
    EVENT_JOB_ERROR,
    EVENT_JOB_MISSED,
    EVENT_JOB_REMOVED,
    EVENT_JOB_EXECUTED
)
# ----------- END: Third Party Imports ---------- #

# ----------- START: In-App Imports ---------- #
from core.constants import (
    SCHEDULER_COALESCE,
    SCHEDULER_MAX_INSTANCES,
    SCHEDULER_DEFAULT_DELAY_BY_SECS,
    SCHEDULER_THREAD_POOL_EXECUTOR_COUNT,
    SCHEDULER_PROCESS_POOL_EXECUTOR_COUNT
)

from core.utils.utils import Singleton

from core.utils.environ import get_jobs_db_details
# ----------- START: In-App Imports ---------- #


__all__ = []


class SchedulerManager(object):

    def __init__(self):

        self.is_scheduler_running = False

        jobs_db_path = get_jobs_db_details()['path']

        jobstores = {
            'default': SQLAlchemyJobStore(url='sqlite:///{}'.format(jobs_db_path))
        }

        executors = {
            'default': ThreadPoolExecutor(SCHEDULER_THREAD_POOL_EXECUTOR_COUNT),
            'processpool': ProcessPoolExecutor(SCHEDULER_PROCESS_POOL_EXECUTOR_COUNT)
        }

        job_defaults = {
            'coalesce': SCHEDULER_COALESCE,
            'max_instances': SCHEDULER_MAX_INSTANCES
        }

        self.scheduler = BackgroundScheduler(
            jobstores=jobstores, executors=executors, job_defaults=job_defaults
        )

    def start(self):
        self.scheduler.start()
        self.is_scheduler_running = True

    def stop(self):
        self.scheduler.stop()
        self.is_scheduler_running = False

    def restart(self):
        self.stop()
        time.sleep(2)
        self.start()


def job_trigger_callback(*args, **kwargs):
    print '................. CALLED ............. {}'.format(kwargs)


class TaskScheduler(SchedulerManager):

    """Singleton Task Scheduler."""

    __metaclass__ = Singleton

    def __init__(self):

        super(self.__class__, self).__init__()

        self.scheduler.add_listener(
            self.job_event_listener,
            EVENT_JOB_EXECUTED | EVENT_JOB_MISSED | EVENT_JOB_ERROR
        )

        self.scheduler.add_listener(
            self.sched_event_listener,
            EVENT_SCHEDULER_SHUTDOWN | EVENT_SCHEDULER_START
        )

        self.scheduler.add_listener(
            self.jobstore_event_listener,
            EVENT_JOB_ADDED | EVENT_JOB_REMOVED | EVENT_JOBSTORE_ADDED | EVENT_JOBSTORE_REMOVED
        )

    def __call__(self):
        self.start()

    def job_event_listener(self, event):
        print event, 'JOB EVENT'

    def jobstore_event_listener(self, event):
        print event, 'JOBSTORE EVENT'

    def sched_event_listener(self, event):
        print event, 'SCHEDULER EVENT'

    def process_job(self, payload=None):

        job_id = payload['job_id']

        schedule_type = payload['schedule_type'].lower().replace(' ', '')

        start_date = payload['start_date']

        start_date_str = """{} {}:{}:{}""".format(
            start_date['date'],
            start_date['hour'],
            start_date['minute'],
            start_date['second']
        )

        date_time_object = datetime.strptime(start_date_str, "%Y-%m-%d %H:%M:%S")

        delay_by_seconds = payload.get('delay_by')

        if delay_by_seconds:
            delay_by_seconds = \
                int(delay_by_seconds['hour']) * 60 * 60 + \
                int(delay_by_seconds['minute']) * 60 + \
                int(delay_by_seconds['second'])

        date_time_object += timedelta(
            seconds=delay_by_seconds or SCHEDULER_DEFAULT_DELAY_BY_SECS
        )

        trigger = {
            'onetime': OneTimeTrigger(),
            'daily': IntervalTrigger(),
            'weekly': CronTrigger()
        }[schedule_type]

        job_action = payload.get('job_action')

        if job_action not in ('add', 'update', 'remove', ):
            raise Exception('job action is wrong')

        if job_action == 'add':

            try:
                job = trigger.add_job(
                    self.scheduler,
                    job_id,
                    job_trigger_callback,
                    run_date=date_time_object.strftime('%Y-%m-%d %H:%M:%S'),
                    emit_event='InitiateProcess'
                )
            except ConflictingIdError as error:
                print 'CRITICAL ERROR'

            else:

                next_run_time = job.next_run_time.isoformat()
                print next_run_time

