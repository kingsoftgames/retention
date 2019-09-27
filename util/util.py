#!/usr/bin/env python3
import logging
import os
import json
import time
from datetime import datetime, date, timedelta
import encodings

ARG_DATE_FORMAT = "%Y-%m-%d"
INVALID_VALUE = -1
YEAR = "year"
MONTH = "month"
DAY = "day"
FILE_PATH_DATES = {
    YEAR: ["<yyyy>"],
    MONTH: ["<MM>", "<M>"],
    DAY: ["<dd>", "<d>"]
}
FILE_PATH_DOUBLE_DIGITS_DATE = {"<MM>", "<dd>"}


logger = logging.getLogger()


def valid_date(time_str):
    try:
        datetime.strptime(time_str, ARG_DATE_FORMAT)
        if compare_date(time_str):
            raise RuntimeError()
        return time_str
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(time_str)
        raise argparse.ArgumentTypeError(msg)
    except RuntimeError:
        msg = "Date less than today"
        raise argparse.ArgumentTypeError(msg)


def compare_date(time_str):
    nowTime_str = datetime.now().strftime(ARG_DATE_FORMAT)
    e_time = time.mktime(time.strptime(nowTime_str, ARG_DATE_FORMAT))
    s_time = time.mktime(time.strptime(time_str, ARG_DATE_FORMAT))
    diff = int(s_time)-int(e_time)
    if diff >= 0:
        return True
    else:
        return False


def is_empty(s):
    return not bool(s and s.strip())


def get_yesterday():
    yesterday = (date.today() + timedelta(-1)).strftime(ARG_DATE_FORMAT)
    return yesterday


def get_prefix(s3_key_prefix, days):
    has_dates = {}
    for key, values in FILE_PATH_DATES.items():
        for value in values:
            if value in s3_key_prefix:
                has_dates[key] = value
    if len(has_dates) != len(FILE_PATH_DATES):
        logger.error(f"s3 prefix error. prefix: {s3_key_prefix}")
        raise RuntimeError()
    d = (date.today() + timedelta(days=days))
    year = d.strftime("%Y")
    month = get_date_month(has_dates, d)
    day = get_date_day(has_dates, d)
    s3_key_prefix = s3_key_prefix.replace(has_dates[YEAR], year)
    s3_key_prefix = s3_key_prefix.replace(has_dates[MONTH], month)
    s3_key_prefix = s3_key_prefix.replace(has_dates[DAY], day)
    return s3_key_prefix


def get_date_month(has_dates, d):
    if has_dates[MONTH] in FILE_PATH_DOUBLE_DIGITS_DATE:
        return d.strftime("%m")
    return str(d.month)


def get_date_day(has_dates, d):
    if has_dates[DAY] in FILE_PATH_DOUBLE_DIGITS_DATE:
        return d.strftime("%d")
    return str(d.day)


def get_player_ids(bucket, event, s3_key_prefix, days):
    player_set = set()
    logs, exist = get_logs(bucket, event, s3_key_prefix, days)
    if not exist:
        return player_set
    for log in logs:
        player_set.add(log["player_id"])
    logger.info(
        f"Get player ids event:{event} ."
        f"file prefix:{filter_prefix} ."
        f"player id size: {len(player_set)}")
    return player_set


def get_logs(bucket, event, s3_key_prefix, days):
    logs = []
    filter_prefix = get_prefix(s3_key_prefix, days)
    if not file_exist(bucket, filter_prefix):
        return logs, False
    add_logs(bucket, logs, event, filter_prefix)
    logger.info(
        f"Get logs event:{event} ."
        f"file prefix:{filter_prefix} ."
        f"log size: {len(logs)}")
    return logs, True


def add_logs(bucket, logs, event, filter_prefix):
    for obj in bucket.objects.filter(Prefix=filter_prefix):
        stream = encodings.utf_8.StreamReader(obj.get()["Body"])
        stream.readline
        for line in stream:
            obj = get_log(line, event)
            if obj:
                logs.append(obj)


# log format:time event json obj
def get_log(line, event):
    sub_lines = line.split(" ")
    if len(sub_lines) < 3:
        raise RuntimeError()
    try:
        obj = json.loads(sub_lines[2])
        if sub_lines[1] == event:
            obj["time"] = sub_lines[0]
            return obj
    except json.JSONDecodeError:
        logger.error(f"Json parse error. json string is: {line}")
    return None


def get_timestamp(time_str):
    return datetime.strptime(
        time_str, ARG_DATE_FORMAT).replace(microsecond=0).isoformat()


def get_days_with_today(any_day):
    today = date.today().strftime(ARG_DATE_FORMAT)
    date1 = datetime.strptime(today, ARG_DATE_FORMAT)
    date2 = datetime.strptime(any_day, ARG_DATE_FORMAT)
    return (date2-date1).days


def file_exist(bucket, filter_prefix):
    files = bucket.objects.filter(Prefix=filter_prefix)
    size = 0
    for b in files:
        size = size + 1
    if size == 0:
        logger.warn(f"File not exist. file name: {filter_prefix}",)
        return False
    for obj in files:
        if obj.key.find(filter_prefix) < 0:
            logger.warn(f"File not exist. file name: {filter_prefix}")
            return False
    return True


def get_paying_users_index_id(player_id, platform, channel):
    return player_id + "_" + platform.lower() + "_" + channel.lower()
