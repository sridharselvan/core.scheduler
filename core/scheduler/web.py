# -*- coding: utf-8 -*-

"""

    Module :mod:``

    This Module is created to...

    LICENSE: The End User license agreement is located at the entry level.

"""

# ----------- START: Native Imports ---------- #
import json
from datetime import datetime
# ----------- END: Native Imports ---------- #

# ----------- START: Third Party Imports ---------- #
# ----------- END: Third Party Imports ---------- #

# ----------- START: In-App Imports ---------- #
from core.backend.utils.core_utils import (
    get_unique_id, AutoSession, get_loggedin_user_id
)

from core.db.model import (
    CodeScheduleTypeModel, JobDetailsModel, UserModel
)
from core.backend.utils.core_utils import decode
from core.backend.config import view_client_config
from core.mq import RPCSchedulerPublisher
# ----------- END: In-App Imports ---------- #


__all__ = [
    # All public symbols go here.
]


def save_scheduler_config(session, form_data):

    # TODO: move to constants
    _response_dict = {'result': False, 'data': None, 'alert_type': None, 'alert_what': None, 'msg': None}

    schedule_data = dict()
    start_date = form_data['start_date']
    schedule_type = form_data['type']

    string_date = "{0}-{1}-{2} {3}:{4}:00"\
        .format(start_date['year'],start_date['month'],start_date['day'],start_date['hour'],start_date['mins'])

    code_schedule_type = CodeScheduleTypeModel.fetch_one(
        session, schedule_type=schedule_type
    )

    schedule_data['schedule_type_idn'] = code_schedule_type.schedule_type_idn

    # TODO: move to constants
    schedule_data['start_date'] = datetime.strptime(string_date, "%Y-%m-%d %H:%M:%S")
    schedule_data['job_id'] = get_unique_id()
    schedule_data['user_idn'] = get_loggedin_user_id()

    valve_id = [valve['id'] for valve in form_data['ValveDetails'] if valve['selected']]

    schedule_data['params'] = ','.join(valve_id)
    #schedule_data['recurrence'] = int(form_data['recurs'])
    recurrence = [int(value['id']) for value in form_data['recurs'] if value['selected']]
    schedule_data['recurrence']  = recurrence

    week_id = [weekday['id'] for weekday in form_data['weekDays'] if weekday['selected']]

    schedule_data['day_of_week'] = ','.join(week_id)

    rpc_response = RPCSchedulerPublisher().publish(
        job_id=schedule_data['job_id'],
        schedule_type=schedule_type.lower(),
        job_action='add',
        start_date=string_date, #schedule_data['start_date'],
        day_of_week=schedule_data['day_of_week'],
        recurrence=schedule_data['recurrence'],
    )

    schedule_data['recurrence']  = ','.join(str(recur) for recur in recurrence)

    # Inserting schedule config into Job details
    import pdb;pdb.set_trace()
    job_details_idn = JobDetailsModel.insert(
        session, **schedule_data
    ).job_details_idn

    return _response_dict


def search_scheduled_job(session, form_data):

    _response_dict = {'result': True, 'data': None, 'alert_type': None, 'alert_what': None, 'msg': None}

    search_data = dict()
    schedule_type = form_data['searchScheduleType']

    scheduled_jobs = JobDetailsModel.scheduled_jobs(
        session, data_as_dict=True, schedule_type=schedule_type
    )

    client_config_data = view_client_config()

    for jobs in scheduled_jobs:
        if 'user_name' in jobs:
            jobs['user_name'] = decode(jobs['user_name'])

        if 'params' in jobs:

            jobs['params'] = ', '.join(
                [client_config_data[idn]['name']
                 for idn in jobs['params'].split(',')
                 ]
            )

    _response_dict.update({'data': scheduled_jobs})

    return _response_dict


def deactivate_scheduled_job(session, form_data):

    _response_dict = {'result': True, 'data': None, 'alert_type': None, 'alert_what': None, 'msg': None}

    deactivated_jobs = JobDetailsModel.deactivate_jobs(
        session, job_details_idn = form_data['job_details_idn']
    )

    _response_dict.update({'data': deactivated_jobs})

    return _response_dict

def update_scheduled_job(session, form_data):

    _response_dict = {'result': True, 'data': None, 'alert_type': None, 'alert_what': None, 'msg': None}

    schedule_data = dict()
    start_date = form_data['start_date']
    string_date = "{0}-{1}-{2} {3}:{4}:00"\
        .format(start_date['year'],start_date['month'],start_date['day'],start_date['hour'],start_date['mins'])

    code_schedule_type = CodeScheduleTypeModel.fetch_one(
        session, schedule_type=form_data['type']
    )

    schedule_data['schedule_type_idn'] = code_schedule_type.schedule_type_idn

    # TODO: move to constants
    schedule_data['start_date'] = datetime.strptime(string_date, "%Y-%m-%d %H:%M:%S")
    schedule_data['job_id'] = get_unique_id()
    schedule_data['user_idn'] = get_loggedin_user_id()

    valve_id = [valve['id'] for valve in form_data['ValveDetails'] if valve['selected']]

    schedule_data['params'] = ','.join(valve_id)
    schedule_data['recurrence'] = form_data['recurs']

    week_id = [weekday['id'] for weekday in form_data['weekDays'] if weekday['selected']]

    schedule_data['day_of_week'] = ','.join(week_id)

    updated_jobs = JobDetailsModel.update_jobs(
        session, job_details_idn = form_data['job_details_idn'],
        **schedule_data
    )

    _response_dict.update({'data': updated_jobs})

    return _response_dict
