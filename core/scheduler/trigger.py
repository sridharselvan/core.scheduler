# -*- coding: utf-8 -*-

"""

    Module :mod:``


    LICENSE: The End User license agreement is located at the entry level.

"""

# ----------- START: Native Imports ---------- #
import itertools
# ----------- END: Native Imports ---------- #

# ----------- START: Third Party Imports ---------- #
# ----------- END: Third Party Imports ---------- #

# ----------- START: In-App Imports ---------- #
from core.constants import DATETIME_FORMAT

from core.utils.utils import get_ordinal

from core.utils.environ import get_scheduler_config_details
# ----------- START: In-App Imports ---------- #


__all__ = []


scheduler_config = get_scheduler_config_details()

SCHEDULER_MAX_INSTANCES = scheduler_config['max_instances']
SCHEDULER_MISFIRE_GRACE_TIME_IN_SECS = scheduler_config['misfire_grace_time_in_secs']


class JobTrigger(object):

    def add_job(self, *args, **kwargs):
        raise NotImplementedError

    def remove_job(self, scheduler, job_id):

        if scheduler.get_job(job_id=job_id):
            scheduler.remove_job(job_id=job_id)


class OneTimeTrigger(JobTrigger):

    def add_job(self, scheduler, job_id, callback, *args, **kw):

        job = scheduler.add_job(
            callback,
            trigger='date',
            id=job_id,
            run_date=kw['run_date'].strftime(DATETIME_FORMAT),
            args=list(),
            kwargs=dict(job_id=job_id, event=kw['emit_event'], type='onetime', user_id=kw['user_id']),
            misfire_grace_time=SCHEDULER_MISFIRE_GRACE_TIME_IN_SECS,
            max_instances=SCHEDULER_MAX_INSTANCES,
        )


        return job

    # def update_job(self, scheduler, job_id, callback, *args, **kw):
    #     """."""
    #     self.remove_job(scheduler, job_id=job_id)

    #     self.add_job(scheduler, job_id, callback, *args, **kw)


class IntervalTrigger(JobTrigger):

    def add_job(self, scheduler, job_id, callback, *args, **kw):

        job = scheduler.add_job(
            callback,
            trigger='cron',
            id=job_id,
            start_date=kw['run_date'].strftime(DATETIME_FORMAT),
            day='{}'.format(kw['recurrence']),
            hour=kw['run_date'].hour,
            minute=kw['run_date'].minute,
            second=kw['run_date'].second,
            args=list(),
            kwargs=dict(job_id=job_id, event=kw['emit_event'], user_id=kw['user_id'], type='Daily'),
            misfire_grace_time=SCHEDULER_MISFIRE_GRACE_TIME_IN_SECS,
            max_instances=SCHEDULER_MAX_INSTANCES,
        )


        return job


class CronTrigger(JobTrigger):
    
    def add_job(self, scheduler, job_id, callback, *args, **kw):

        _day = ', '.join(
            ['{} {}'.format(get_ordinal(_day), _week) for _day, _week in itertools.product(kw['recurrence'].split(','), kw['day_of_week'].split(','))
             ]
        )

        job = scheduler.add_job(
            callback,
            trigger='cron',
            id=job_id,
            start_date=kw['run_date'].strftime(DATETIME_FORMAT),
            #week='',# '{}'.format(kw['recurrence']),
            #day_of_week='1st sun, 2nd tue', #'{}'.format(kw['day_of_week']),
            day=_day,
            month='1-12',
            hour=kw['run_date'].hour,
            minute=kw['run_date'].minute,
            second=kw['run_date'].second,
            args=list(),
            kwargs=dict(job_id=job_id, event=kw['emit_event'], user_id=kw['user_id'], type='Weekly'),
            misfire_grace_time=SCHEDULER_MISFIRE_GRACE_TIME_IN_SECS,
            max_instances=SCHEDULER_MAX_INSTANCES,
        )


        return job


