#!/usr/bin/env python3
import os
import boto3
import json
import decimal
import time
import argparse
import sys
import re
from datetime import datetime, date, timedelta
import requests
from requests.auth import HTTPBasicAuth
import encodings
from eslog import eslog
from util import util
from s3 import s3
from es import es
from model import PlayerIdMap

S3_KEY_PREFIX_CREATE_PLAYER = os.getenv("S3_KEY_PREFIX_CREATE_PLAYER")
S3_KEY_PREFIX_PLAYER_LOGIN = os.getenv("S3_KEY_PREFIX_PLAYER_LOGIN")
CREATE_PLAYER_EVENT = os.getenv("CREATE_PLAYER_EVENT")
PLAYER_LOGIN_EVENT = os.getenv("PLAYER_LOGIN_EVENT")
RETENTION_DAYS = os.getenv("RETENTION_DAYS")
ES_INDEX = os.getenv("ES_INDEX", "retention")
RETENTION_TRACK_DAYS = os.getenv("RETENTION_TRACK_DAYS", 30)

RETENTION_DAY_PREFIX = "day"
COMMA = ","

logger = eslog.get_logger(ES_INDEX)
bucket = None


def valid_params():
    params_errors = []

    if util.is_empty(S3_KEY_PREFIX_CREATE_PLAYER):
        params_errors.append("S3_KEY_PREFIX_CREATE_PLAYER")

    if util.is_empty(S3_KEY_PREFIX_PLAYER_LOGIN):
        params_errors.append("S3_KEY_PREFIX_PLAYER_LOGIN")

    if util.is_empty(CREATE_PLAYER_EVENT):
        params_errors.append("CREATE_PLAYER_EVENT")

    if util.is_empty(PLAYER_LOGIN_EVENT):
        params_errors.append("PLAYER_LOGIN_EVENT")

    if util.is_empty(RETENTION_DAYS):
        params_errors.append("RETENTION_DAYS")

    if len(params_errors) != 0:
        logger.error(f'Params error. {params_errors} is empty')
        raise RuntimeError()


def arg_parse(*args, **kwargs):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--day",
        nargs="?",
        const=util.get_yesterday(),
        type=util.valid_date,
        default=util.get_yesterday(),
        help="Date. The default date is yesterday. The format is YYYY-MM-DD"
    )
    args = parser.parse_args()
    process(args.day)


# 计算留存，次留，周留等
# 添加新用户留存追踪
def process(time_str):
    valid_params()
    global bucket
    bucket = s3.init_bucket_from_env()
    retentions = compute(time_str)
    output_to_es(time_str, retentions)
    logger.info("Process end.")


# ==========================for compute retention=============================

def compute(time_str):
    today = date.today().strftime(util.ARG_DATE_FORMAT)
    days = util.days_compute(today, time_str)
    login_map, _ = util.get_players(
        bucket, PLAYER_LOGIN_EVENT, S3_KEY_PREFIX_PLAYER_LOGIN, days)
    retention = compute_retention(time_str, login_map, days)
    retention_track = compute_retention_track(time_str, login_map)
    return {"retention": retention, "retention_track": retention_track}


def compute_retention_track(time_str, login_map):
    logger.info(
        f"Compute retention track date:{time_str}. "
        f"retention track days: {RETENTION_TRACK_DAYS}")
    start_date = util.get_some_day_of_one_day(
        time_str, -(RETENTION_TRACK_DAYS+1))
    end_date = util.get_some_day_of_one_day(
        time_str, -1)
    create_days = util.get_date_list(start_date, end_date)
    create_map = PlayerIdMap()
    ret = {}
    file_exist = util.get_players_multiple_days(
        bucket, CREATE_PLAYER_EVENT, S3_KEY_PREFIX_CREATE_PLAYER,
        create_days, create_map)
    if not file_exist:
        logger.info(f"Create player file not exist."
                    f" date start:{start_date}. "
                    f" date end:{end_date}. ")
        return ret
    for platform, channels in create_map.player_id_map.items():
        for channel, days in channels.items():
            for day, create_set in days.items():
                if len(create_set) == 0:
                    continue
                login_set, _ = login_map.get_all_day_player_ids(
                    platform, channel)
                count = get_retention_track(day, login_set, create_set)
                ret[(day, platform, channel)] = count
    logger.info(f"Compute retention track result:{ret}. ")
    return ret


def get_retention_track(time_str, login_set, create_set):
    ceate_size = len(create_set)
    if ceate_size == 0:
        return ()
    intersection_set = create_set.intersection(login_set)
    login_size = len(intersection_set)
    return (login_size, ceate_size)


def compute_retention(time_str, login_map, days):
    logger.info(
        f"Compute retention date:{time_str}. "
        f"retention days: {RETENTION_DAYS}")
    ret = {}
    retention_days = get_retention_days()
    if len(retention_days) == 0:
        return ret
    for key, values in retention_days.items():
        retention, valid = get_retention(time_str, login_map, days + values)
        if valid:
            ret[key] = retention
    logger.info(f"Compute retention result:{ret}. ")
    return ret


def get_retention_days():
    ret = {}
    if len(RETENTION_DAYS) == 0:
        logger.error("Params error. RETENTION_DAYS is empty")
        return ret
    days = RETENTION_DAYS.split(COMMA)
    for day in days:
        key = RETENTION_DAY_PREFIX + day
        value = - (int(day) - 1)
        ret[key] = value
    return ret


def get_retention(time_str, login_map, days):
    create_map, _ = util.get_players(
        bucket, CREATE_PLAYER_EVENT, S3_KEY_PREFIX_CREATE_PLAYER, days)
    create_player_ids = create_map.get_total_player_ids()
    ceates_size = len(create_map)
    ret = {}
    event_time = util.get_some_day(days)
    if ceates_size == 0:
        return ret, False
    for platform, channels in create_player_ids.items():
        for channel, create_set in channels.items():
            if len(create_set) == 0:
                continue
            login_set, _ = login_map.get_all_day_player_ids(platform, channel)
            intersection_set = create_set.intersection(login_set)
            login_count = len(intersection_set)
            create_count = len(create_set)
            ret[(event_time, platform, channel)] = (login_count, create_count)
    return ret, True


def get_date_path(event, day):
    has_dates = {}
    path = ""
    if event == CREATE_PLAYER_EVENT:
        path = S3_KEY_PREFIX_CREATE_PLAYER
    else:
        path = S3_KEY_PREFIX_PLAYER_LOGIN
    for key, values in FILE_PATH_DATES.items():
        for value in values:
            if value in path:
                has_dates[key] = value
    if len(has_dates) != len(FILE_PATH_DATES):
        logger.error(f"{event} path error. path: {path}")
        raise RuntimeError()
    d = (date.today() + timedelta(days=day))
    year = d.strftime("%Y")
    month = get_date_month(has_dates, d)
    day = get_date_day(has_dates, d)
    path = path.replace(has_dates[YEAR], year)
    path = path.replace(has_dates[MONTH], month)
    path = path.replace(has_dates[DAY], day)
    return path


# ==========================for output to es=============================
def output_to_es(time_str, retentions):
    if len(retentions) == 0:
        return
    for key, values in retentions.items():
        if key == "retention":
            for day, rates in values.items():
                for rate_key, rate_value in rates.items():
                    if rate_value:
                        es_add_rate_doc(
                            rate_key[0], key + "_" + day, rate_key, rate_value)
        else:
            for ret_key, ret_value in values.items():
                if ret_value:
                    es_add_track_doc(time_str, key, ret_key, ret_value)


def es_add_rate_doc(time_str, compute_type, ret_key, ret_value):
    path = ES_INDEX + \
        "/_doc/" + es_get_doc_id(
            time_str, ret_key[1], ret_key[2], compute_type)
    data = es_get_rate_doc(time_str, compute_type, ret_key, ret_value)
    es.add_doc(path, data)


def es_get_rate_doc(time_str, compute_type, ret_key, ret_value):
    timestamp = util.get_timestamp(time_str)
    data = {
        "@timestamp": timestamp,
        "type": compute_type,
        "platform": ret_key[1],
        "channel": ret_key[2],
        "login_count": ret_value[0],
        "create_count": ret_value[1]
    }
    return json.dumps(data)


def es_add_track_doc(time_str, compute_type, ret_key, ret_value):
    path = ES_INDEX + \
        "/_doc/" + es_get_doc_id(
            time_str, ret_key[1], ret_key[2], compute_type + "_" + ret_key[0])
    data = {
        "@timestamp": util.get_timestamp(time_str),
        "type": compute_type,
        "sub_type": ret_key[0],
        "platform": ret_key[1],
        "channel": ret_key[2],
        "login_count": ret_value[0],
        "create_count": ret_value[1]
    }
    es.add_doc(path, json.dumps(data))


def es_get_doc_id(time_str, platform, channel, compute_type):
    str_time = datetime.strptime(
        time_str, util.ARG_DATE_FORMAT).strftime(util.ARG_DATE_FORMAT)
    return str_time + "_" + platform + "_" + channel + "_" + compute_type


def test_output_to_es():
    time_str = "2019-01-07"
    retentions = {ONE_DAY_KEY: 0.11, ONE_WEEK_KEY: 0.13}
    output_to_es(time_str, retentions)


def test_compute_retention():
    time_str = "2019-06-30"
    compute_retention(time_str)


if __name__ == '__main__':
    try:
        sys.exit(arg_parse(*sys.argv))
    except KeyboardInterrupt:
        logger.exception("CTL-C Pressed.")
        exit("CTL-C Pressed.")
    except Exception as e:
        logger.exception(e)
        exit("Exception")
