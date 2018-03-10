# -*- coding: utf-8 -*-

"""

    Module :mod:``


    LICENSE: The End User license agreement is located at the entry level.

"""

# ----------- START: Native Imports ---------- #
# ----------- END: Native Imports ---------- #

# ----------- START: Third Party Imports ---------- #
# ----------- END: Third Party Imports ---------- #

# ----------- START: In-App Imports ---------- #
from core.constants import (
    SCHEDULER_MAX_INSTANCES,
    SCHEDULER_MISFIRE_GRACE_TIME_IN_SECS
)
# ----------- START: In-App Imports ---------- #


__all__ = []


class JobTrigger(object):

    def add_job(self, *args, **kwargs):
        raise NotImplementedError

    def update_job(self, *args, **kwargs):
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
            run_date=kw['run_date'],
            args=list(),
            kwargs=dict(job_id=job_id, event=kw['emit_event']),
            misfire_grace_time=SCHEDULER_MISFIRE_GRACE_TIME_IN_SECS,
            max_instances=SCHEDULER_MAX_INSTANCES,
        )


        return job

    def update_job(self, scheduler, job_id, callback, *args, **kw):
        """."""
        self.remove_job(scheduler, job_id=job_id)

        self.add_job(scheduler, job_id, callback, *args, **kw)


class IntervalTrigger(JobTrigger):
    pass


class CronTrigger(JobTrigger):
    pass

