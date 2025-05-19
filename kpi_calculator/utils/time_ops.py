# Copyright 2025, Battelle Energy Alliance, LLC, ALL RIGHTS RESERVED

from datetime import datetime, timedelta

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
TIME_ONLY_FORMAT = '%H:%M:%S'


def truncate_date(datetime: str) -> str:
    if '.' in datetime: 
        return datetime[:-5]
    elif len(datetime)==11:
        return datetime[:-3]
    else: 
        return datetime[:-1]
    
def events_time_diff_seconds(start_timestamp: str, end_timestamp: str) -> timedelta: 
    datetime_1 = truncate_date(start_timestamp)
    datetime_2 = truncate_date(end_timestamp)
    if(len(datetime_1)>8):
        time_1 = datetime.strptime(datetime_1, DATETIME_FORMAT)
        time_2 = datetime.strptime(datetime_2, DATETIME_FORMAT)
    else:
        time_1 = datetime.strptime(datetime_1, TIME_ONLY_FORMAT)
        time_2 = datetime.strptime(datetime_2, TIME_ONLY_FORMAT)

    time_diff_seconds = (time_2 - time_1).total_seconds()
    if time_diff_seconds < 0: 
        time_diff_seconds *= -1
    return time_diff_seconds