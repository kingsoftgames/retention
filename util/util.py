#!/usr/bin/env python3
import logging
import os
import json
import time
from datetime import datetime, date, timedelta
import encodings
from multipledispatch import dispatch
from collections import Counter

from model import PlayerIdMap

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
    return get_some_day(-1)


def get_some_day(days):
    return (date.today() + timedelta(days)).strftime(ARG_DATE_FORMAT)


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


def get_date_paths(s3_key_prefix, day):
    paths = []
    days = get_days_for_timezone(day)
    for d in days:
        paths.append(get_prefix(s3_key_prefix, d))
    return paths


def get_date_paths_for_multiple_days(s3_key_prefix, days):
    today = date.today().strftime(ARG_DATE_FORMAT)
    filter_prefixs_set = set()
    for day in days:
        d = days_compute(today, day)
        filter_prefixs = get_date_paths(s3_key_prefix, d)
        if len(filter_prefixs) > 0:
            filter_prefixs_set.update(filter_prefixs)
    return filter_prefixs_set


def get_players_multiple_days(bucket, event, s3_key_prefix, days, players):
    filter_prefixs = get_date_paths_for_multiple_days(s3_key_prefix, days)
    filter_prefixs, exist = files_exist(
        bucket, filter_prefixs, event)
    if not exist:
        return False
    start_time = get_start_timestamp_time_str(days[0])
    end_time = get_end_timestamp_time_str(days[-1])
    for filter_prefix in filter_prefixs:
        add_player(
            bucket, players, event, filter_prefix, start_time, end_time)
    logger.info(
        f"Get players event:{event} ."
        f"file prefixs:{filter_prefixs} ."
        f"Date start: {days[0]} ."
        f"end: {days[-1]}."
        f"player size: {len(players)}")
    return True


def get_players(bucket, event, s3_key_prefix, day):
    player_map = PlayerIdMap()
    filter_prefixs = get_date_paths(s3_key_prefix, day)
    filter_prefixs, exist = files_exist(bucket, filter_prefixs, event)
    if not exist:
        return player_map, False
    start_time = get_start_timestamp(day)
    end_time = get_end_timestamp(day)
    for filter_prefix in filter_prefixs:
        add_player(
            bucket, player_map, event, filter_prefix, start_time, end_time)
    logger.info(
        f"Get players event:{event} ."
        f"file prefixs:{filter_prefixs} ."
        f"player size: {player_map.size()}")
    return player_map, True


def add_player(bucket, players, event, filter_prefix, start_time, end_time):
    ret = set()
    for obj in bucket.objects.filter(Prefix=filter_prefix):
        stream = encodings.utf_8.StreamReader(obj.get()["Body"])
        stream.readline
        for line in stream:
            log = get_log(line, event, start_time, end_time)
            if log:
                add_player_id(players, log)


@dispatch(set, dict)
def add_player_id(players, log):
    players.add(log["player_id"])


@dispatch(dict, dict)
def add_player_id(players, log):
    time_str = get_local_time_str(log["time"])
    player_ids = players.get(time_str, set())
    player_ids.add(log["player_id"])
    players[time_str] = player_ids


@dispatch(PlayerIdMap, dict)
def add_player_id(players, log):
    time = get_local_time_str(log["time"])
    player_id = log["player_id"]
    players.put(time, get_platform(log), get_channel(log), player_id)


@dispatch(set, dict)
def add_player_id(players, log):
    players.add(log["player_id"])


def get_platform(log):
    if "platform" in log:
        return log["platform"]
    return "UNKNOWN"


def get_channel(log):
    if "channel" in log:
        return log["channel"]
    return "UNKNOWN"


# log format:time event json obj
def get_player_id(event, line, start_time, end_time):
    sub_lines = line.split(" ")
    if len(sub_lines) < 3:
        raise RuntimeError()
    try:
        obj = json.loads(sub_lines[2])
        if sub_lines[1] == event:
            log_time = int(sub_lines[0])
            if log_time >= start_time and log_time < end_time:
                return (obj["player_id"], log_time)
    except json.JSONDecodeError:
        logger.error(f"Json parse error. json string is: {line}")
    return (INVALID_VALUE, INVALID_VALUE)


def get_logs(bucket, event, s3_key_prefix, days):
    logs = []
    filter_prefixs = get_date_paths(s3_key_prefix, days)
    filter_prefixs, exist = files_exist(
        bucket, filter_prefixs, event)
    if not exist:
        return logs, False
    start_time = get_start_timestamp(days)
    end_time = get_end_timestamp(days)
    for filter_prefix in filter_prefixs:
        add_logs(bucket, logs, event, filter_prefix, start_time, end_time)
    logger.info(
        f"Get logs event:{event} ."
        f"file prefix:{filter_prefixs} ."
        f"log size: {len(logs)}")
    return logs, True


def add_logs(bucket, logs, event, filter_prefix, start_time, end_time):
    for obj in bucket.objects.filter(Prefix=filter_prefix):
        stream = encodings.utf_8.StreamReader(obj.get()["Body"])
        stream.readline
        for line in stream:
            obj = get_log(line, event, start_time, end_time)
            if obj:
                logs.append(obj)


# log format:time event json obj
def get_log(line, event, start_time, end_time):
    sub_lines = line.split(" ")
    if len(sub_lines) < 3:
        logger.error(f"line format error. line: {line}")
        return None
    try:
        obj = json.loads(sub_lines[2])
        if sub_lines[1] == event:
            log_time = int(sub_lines[0])
            if log_time >= start_time and log_time < end_time:
                obj["time"] = log_time
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


def files_exist(bucket, filter_prefixs, event):
    ret = set()
    for filter_prefix in filter_prefixs:
        if file_exist(bucket, filter_prefix, event):
            ret.add(filter_prefix)
    if len(ret) == 0:
        return ret, False
    return ret, True


def file_exist(bucket, filter_prefix, event):
    files = bucket.objects.filter(Prefix=filter_prefix)
    size = 0
    for b in files:
        size = size + 1
    if size == 0:
        logger.warn(
            f"File not exist. file name: {filter_prefix} ."
            f"event: {event}")
        return False
    for obj in files:
        if obj.key.find(filter_prefix) < 0:
            logger.warn(f"File not exist. file name: {filter_prefix}")
            return False
    return True


def get_paying_users_index_id(player_id, platform, channel):
    return player_id + "_" + platform.lower() + "_" + channel.lower()


def get_days_for_timezone(day):
    date_format = "%Y-%m-%d %H:%M:%S"
    days = []
    now = datetime.now()
    local_time_str = now.strftime(date_format)
    local_time = time.mktime(time.strptime(local_time_str, date_format))
    utc_time_str = datetime.utcfromtimestamp(local_time).strftime(date_format)
    utc_time = time.mktime(time.strptime(utc_time_str, date_format))
    diff = int(local_time)-int(utc_time)
    if diff > 0:
        days.append(day-1)
    if diff < 0:
        days.append(day+1)
    days.append(day)
    return days


def get_start_timestamp_time_str(time_str):
    d = datetime.strptime(time_str, ARG_DATE_FORMAT).date()
    return int(time.mktime(time.strptime(str(d), ARG_DATE_FORMAT)))


def get_end_timestamp_time_str(time_str):
    dt = datetime.strptime(time_str, ARG_DATE_FORMAT)
    d = (dt.date() + timedelta(days=1))
    return int(time.mktime(time.strptime(str(d), ARG_DATE_FORMAT))) - 1


def get_start_timestamp(day):
    d = (date.today() + timedelta(days=day))
    return int(time.mktime(time.strptime(str(d), ARG_DATE_FORMAT)))


def get_end_timestamp(day):
    day = day + 1
    d = (date.today() + timedelta(days=day))
    return int(time.mktime(time.strptime(str(d), ARG_DATE_FORMAT))) - 1


def days_compute(today, any_day):
    date1 = datetime.strptime(today, ARG_DATE_FORMAT)
    date2 = datetime.strptime(any_day, ARG_DATE_FORMAT)
    return (date2-date1).days


# w 是周一为第一天
def is_first_day_of_week(time_str):
    day = datetime.strptime(time_str, ARG_DATE_FORMAT).strftime("%w")
    return day == "1"


def is_first_day_of_month(time_str):
    day = datetime.strptime(time_str, ARG_DATE_FORMAT)
    firstDay = date(day.year, day.month, 1)
    return firstDay.day == day.day


def get_previous_one_week_days(time_str):
    start, end = get_previous_one_week(time_str)
    return get_date_list(start, end)


def get_previous_one_month_days(time_str):
    start, end = get_previous_one_month(time_str)
    return get_date_list(start, end)


def get_date_list(start, end):
    start = datetime.strptime(start, ARG_DATE_FORMAT)
    end = datetime.strptime(end, ARG_DATE_FORMAT)
    data = []
    days = (end-start).days + 1
    day = timedelta(days=1)
    for d in range(days):
        data.append((start + day*d).strftime(ARG_DATE_FORMAT))
    return data


def get_previous_one_week(time_str):
    d = datetime.strptime(time_str, ARG_DATE_FORMAT)
    dayscount = timedelta(days=d.isoweekday())
    dayto = d - dayscount
    sixdays = timedelta(days=6)
    dayfrom = dayto - sixdays
    date_from = datetime(
        dayfrom.year, dayfrom.month, dayfrom.day).strftime(
            ARG_DATE_FORMAT)
    date_to = datetime(
        dayto.year, dayto.month, dayto.day).strftime(
            ARG_DATE_FORMAT)
    logger.info(
        f"Get week start:{date_from} ."
        f"week end:{date_to} ."
        f"compute date: {time_str}")
    return date_from, date_to


def get_previous_one_month(time_str):
    d = datetime.strptime(time_str, ARG_DATE_FORMAT)
    dayscount = timedelta(days=d.day)
    dayto = d - dayscount
    date_from = datetime(
        dayto.year, dayto.month, 1).strftime(
            ARG_DATE_FORMAT)
    date_to = datetime(
        dayto.year, dayto.month, dayto.day).strftime(
            ARG_DATE_FORMAT)
    logger.info(
        f"Get month start:{date_from} ."
        f"week month:{date_to} ."
        f"compute date: {time_str}")
    return date_from, date_to


def get_some_day_of_one_day(time_str, days):
    one_day = datetime.strptime(time_str, ARG_DATE_FORMAT)
    return (one_day + timedelta(days)).strftime(ARG_DATE_FORMAT)


def get_local_time_str(timestamp):
    timeArray = time.localtime(timestamp)
    return time.strftime(ARG_DATE_FORMAT, timeArray)
