# -*- coding: utf-8 -*-

"""

    Module :mod:``


    LICENSE: The End User license agreement is located at the entry level.

"""

# ----------- START: Native Imports ---------- #
import time

from copy import deepcopy

from datetime import datetime, timedelta
# ----------- END: Native Imports ---------- #

# ----------- START: Third Party Imports ---------- #
import simplejson as json

from apscheduler.jobstores.base import ConflictingIdError

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.jobstores.memory import MemoryJobStore

from core.scheduler.trigger import OneTimeTrigger, IntervalTrigger, CronTrigger

from apscheduler.events import (
    EVENT_JOBSTORE_ADDED,
    EVENT_JOBSTORE_REMOVED,
    EVENT_SCHEDULER_START,
    EVENT_SCHEDULER_SHUTDOWN,
    EVENT_JOB_ADDED,
    EVENT_JOB_MODIFIED,
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
    SCHEDULER_PROCESS_POOL_EXECUTOR_COUNT,

    SCHEDULER_SVC_LOGGER_TPL,
    SCHEDULER_ACCESS_LOGGER_TPL,
    INITIATED,
    MISSED
)

from core.utils.utils import Singleton

from core.utils.environ import get_jobs_db_details

from core.mq import SimpleCentralLogPublisher

from core.scheduler.web import deactivate_completed_onetime_jobs

from core.db.model import (
    JobRunLogModel, CodeStatusModel, JobDetailsModel
)

from core.backend.utils.core_utils import AutoSession
from core.constants.code_message import filled_code_message
# ----------- START: In-App Imports ---------- #


__all__ = []


class SchedulerManager(object):

    def __init__(self):

        self.is_scheduler_running = False

        jobs_db_path = get_jobs_db_details()['path']

        jobstores = {
            #'default': SQLAlchemyJobStore(url='sqlite:///{}'.format(jobs_db_path))
            'default': MemoryJobStore()
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

        _payload = deepcopy(SCHEDULER_SVC_LOGGER_TPL)

        if self.scheduler.running:
            _payload['message'] = filled_code_message('CM0010')

            SimpleCentralLogPublisher().publish(payload=_payload)

            self.is_scheduler_running = True

        else:
            _payload['message'] = filled_code_message('CM0011')
            _payload['log_level'] = 'ERROR'
            _payload['status'] = 'FAILED'

            SimpleCentralLogPublisher().publish(payload=_payload)

    def stop(self):
        self.scheduler.stop()

        _payload = deepcopy(SCHEDULER_SVC_LOGGER_TPL)

        if not self.scheduler.running:
            _payload['message'] = filled_code_message('CM0012')

            SimpleCentralLogPublisher().publish(payload=_payload)

            self.is_scheduler_running = False
        else:
            _payload['message'] = filled_code_message('CM0013')
            _payload['log_level'] = 'ERROR'
            _payload['status'] = 'FAILED'

            SimpleCentralLogPublisher().publish(payload=_payload)

    def restart(self):
        self.stop()
        time.sleep(2)
        self.start()


def job_trigger_callback(*args, **kwargs):

    print '................. CALLED ............. {}'.format(kwargs)

    from core.mq import SimpleSMSPublisher
    from core.utils.environ import get_queue_details

    from core.db.model import JobDetailsModel, UserModel

    queue_details = get_queue_details()

    with AutoSession() as session:

        phone_no = str(
            UserModel.fetch_user_data(
                session, mode='one', user_idn=kwargs['user_id']
            ).phone_no1
        )

        _params = dict(
            message=filled_code_message(
                'CM0021', 
                schedule_type=kwargs['type'], 
                current_datetime=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ),
            number=phone_no
        )

        #
        # Push sms notification
        SimpleSMSPublisher().publish(payload=_params)

        # Inserting into job_run_log table
        _params = {
            'job_id':kwargs['job_id'],
            'status_idn':CodeStatusModel.fetch_status_idn(session, status=INITIATED).status_idn
        }
        JobRunLogModel.create_run_log(session, **_params)

    if 'type' in kwargs and kwargs['type'].lower() == 'onetime':
        deactivate_completed_onetime_jobs(kwargs['job_id'])


class TaskScheduler(SchedulerManager):

    """Singleton Task Scheduler."""

    __metaclass__ = Singleton

    def __init__(self):

        super(self.__class__, self).__init__()

        self.scheduler.add_listener(
            self.callback_job_executed_event,
            EVENT_JOB_EXECUTED
        )

        self.scheduler.add_listener(
            self.callback_job_missed_event,
            EVENT_JOB_MISSED
        )

        self.scheduler.add_listener(
            self.callback_job_error_event,
            EVENT_JOB_ERROR
        )

        self.scheduler.add_listener(
            self.callback_scheduler_start_event,
            EVENT_SCHEDULER_START
        )

        self.scheduler.add_listener(
            self.callback_scheduler_shutdown_event,
            EVENT_SCHEDULER_START
        )

        self.scheduler.add_listener(
            self.callback_job_add_event,
            EVENT_JOB_ADDED
        )

        self.scheduler.add_listener(
            self.callback_job_remove_event,
            EVENT_JOB_REMOVED
        )

        self.scheduler.add_listener(
            self.callback_job_update_event,
            EVENT_JOB_MODIFIED
        )

        self.scheduler.add_listener(
            self.callback_jobstore_event,
            EVENT_JOBSTORE_ADDED | EVENT_JOBSTORE_REMOVED
        )

    def callback_scheduler_start_event(self, event):
        pass

    def callback_scheduler_shutdown_event(self, event):
        pass

    def callback_job_add_event(self, event):
        message = filled_code_message('CM0022', job_id=event.job_id)

        scheduler_access_tpl = deepcopy(SCHEDULER_ACCESS_LOGGER_TPL)
        scheduler_access_tpl['job_id'] = event.job_id
        scheduler_access_tpl['message'] = message

        SimpleCentralLogPublisher().publish(payload=scheduler_access_tpl)

    def callback_job_update_event(self, event):
        message = filled_code_message(
            'CM0023', 
            job_id=event.job_id, 
            next_run_time=event.scheduled_run_time.isoformat()
        )

        scheduler_access_tpl = deepcopy(SCHEDULER_ACCESS_LOGGER_TPL)
        scheduler_access_tpl['job_id'] = event.job_id
        scheduler_access_tpl['message'] = message

        SimpleCentralLogPublisher().publish(payload=scheduler_access_tpl)

    def callback_job_remove_event(self, event):
        message = filled_code_message('CM0024', job_id=event.job_id)

        scheduler_access_tpl = deepcopy(SCHEDULER_ACCESS_LOGGER_TPL)
        scheduler_access_tpl['job_id'] = event.job_id
        scheduler_access_tpl['message'] = message

        SimpleCentralLogPublisher().publish(payload=scheduler_access_tpl)

    def callback_jobstore_event(self, event):
        pass

    def callback_job_executed_event(self, event):
        message = filled_code_message(
            'CM0025', 
            job_id=event.job_id, 
            next_run_time=event.scheduled_run_time.isoformat()
        )

        scheduler_access_tpl = deepcopy(SCHEDULER_ACCESS_LOGGER_TPL)
        scheduler_access_tpl['job_id'] = event.job_id
        scheduler_access_tpl['message'] = message
        job = self.scheduler.get_job(job_id=event.job_id)

        if job:
            with AutoSession() as session:
                _updates = {'next_run_time': job.next_run_time.isoformat()}
                JobDetailsModel.update_jobs(session, where_condition={'job_id':event.job_id}, updates=_updates)

        SimpleCentralLogPublisher().publish(payload=scheduler_access_tpl)

    def callback_job_missed_event(self, event):
        message = filled_code_message(
            'CM0026', 
            job_id=event.job_id, 
            next_run_time=event.scheduled_run_time.isoformat()
        )

        scheduler_access_tpl = deepcopy(SCHEDULER_ACCESS_LOGGER_TPL)
        scheduler_access_tpl['job_id'] = event.job_id
        scheduler_access_tpl['message'] = message

        #Inserting into job_run_log table
        with AutoSession() as session:
            _params = {
                'job_id':event.job_id,
                'status_idn':CodeStatusModel.fetch_status_idn(session, status=MISSED).status_idn
            }
            JobRunLogModel.create_run_log(session, **_params)



        SimpleCentralLogPublisher().publish(payload=scheduler_access_tpl)

    def callback_job_error_event(self, event):
        message = filled_code_message(
            'CM0027', 
            job_id=event.job_id, 
            next_run_time=event.scheduled_run_time.isoformat()
        )

        scheduler_access_tpl = deepcopy(SCHEDULER_ACCESS_LOGGER_TPL)
        scheduler_access_tpl['job_id'] = event.job_id
        scheduler_access_tpl['message'] = message
        scheduler_access_tpl['status'] = 'FAILED'
        scheduler_access_tpl['exception'] = event.exception
        scheduler_access_tpl['error_trace'] = event.traceback

        SimpleCentralLogPublisher().publish(payload=scheduler_access_tpl)

    def __call__(self):
        self.start()

    def process_job(self, payload=None):

        def _add_job():
            """."""
            try:
                job = trigger.add_job(
                    self.scheduler,
                    job_id,
                    job_trigger_callback,
                    run_date=date_time_object,
                    recurrence=payload['recurrence'],
                    day_of_week=payload['day_of_week'],
                    emit_event='InitiateProcess',
                    user_id=payload['user_idn'],
                )
            except ConflictingIdError as error:
                scheduler_access_tpl = deepcopy(SCHEDULER_ACCESS_LOGGER_TPL)
                scheduler_access_tpl['job_id'] = job_id
                scheduler_access_tpl['status'] = 'FAILED'
                scheduler_access_tpl['error'] = str(error)
                scheduler_access_tpl['message'] = filled_code_message('CM0028', job_id=job_id)

                SimpleCentralLogPublisher().publish(payload=scheduler_access_tpl)

                return False, scheduler_access_tpl['message']

            else:

                scheduler_access_tpl = deepcopy(SCHEDULER_ACCESS_LOGGER_TPL)
                scheduler_access_tpl['job_id'] = job.id
                scheduler_access_tpl['next_run_time'] = job.next_run_time.isoformat()
                scheduler_access_tpl['message'] = filled_code_message('CM0029', schedule_type=schedule_type)

                SimpleCentralLogPublisher().publish(payload=scheduler_access_tpl)

                return True, scheduler_access_tpl

        response = {
            'result': True,
            'message': '',
            'error_message': '',
            'error_trace': ''
        }

        job_action = payload.get('job_action')

        _allowed_job_actions = ('add', 'update', 'remove', )

        if job_action not in _allowed_job_actions:

            _err_msg = filled_code_message('CM0030', allowed_job_actions=_allowed_job_actions)

            response.update({'result': False,
                             'message': '',
                             'error_message': _err_msg,
                             'error_trace': ''
                             })

            return False, response

        job_id = payload['job_id']

        if job_action == 'remove':

            if self.scheduler.get_job(job_id=job_id):
                self.scheduler.remove_job(job_id=job_id)

                if self.scheduler.get_job(job_id=job_id):
                    _message = filled_code_message('CM0031', job_id=job_id)

                    response.update({'result': False,
                                     'message': '',
                                     'error_message': _message,
                                     'error_trace': ''
                                     })

                else:
                    _message = filled_code_message('CM0032', job_id=job_id)

                    response.update({'result': True,
                                     'message': _message,
                                     'error_message': '',
                                     'error_trace': ''
                                     })

            else:
                _message = filled_code_message('CM0033', job_id=job_id)

                response.update({'result': False,
                                 'message': '',
                                 'error_message': _message,
                                 'error_trace': ''
                                 })

            return response['result'], response

        schedule_type = payload['schedule_type'].lower().replace(' ', '')

        start_date = payload['start_date']

        date_time_object = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")

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

        if job_action == 'add':
            _result, _response = _add_job()

        elif job_action == 'update':

            job = self.scheduler.get_job(job_id=job_id)

            if job:
                self.scheduler.remove_job(job_id=job_id)

            _result, _response = _add_job()

        return _result, _response
