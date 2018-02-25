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
    CodeScheduleTypeModel, JobDetailsModel
)
# ----------- END: In-App Imports ---------- #


__all__ = [
    # All public symbols go here.
]


def save_scheduler_config(form_data):

    # TODO: move to constants
    _response_dict = {'result': False, 'data': None, 'alert_type': None, 'alert_what': None, 'msg': None}

    schedule_data = dict()
    start_date = form_data['start_date']
    string_date = "{0}-{1}-{2} {3}:{4}:00"\
        .format(start_date['year'],start_date['month'],start_date['day'],start_date['hour'],start_date['mins'])    

    with AutoSession() as auto_session:
        code_schedule_type = CodeScheduleTypeModel.fetch_one(
            auto_session, schedule_type=form_data['type']
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

        # Inserting schedule config into Job details
        job_details_idn = JobDetailsModel.insert(
            auto_session, **schedule_data
        ).job_details_idn

    return _response_dict