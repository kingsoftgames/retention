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
from collections import Counter

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


# 新增，留存的手机品牌分布,临时脚本
def process(time_str):
    valid_params()
    global bucket
    bucket = s3.init_bucket_from_env()
    retention_devices = compute(time_str)
    output_to_es(time_str, retention_devices)
    logger.info("Process end.")


# ==========================for compute retention=============================

def compute(time_str):
    today = date.today().strftime(util.ARG_DATE_FORMAT)
    days = util.days_compute(today, time_str)
    login_map, _ = util.get_players(
        bucket, PLAYER_LOGIN_EVENT, S3_KEY_PREFIX_PLAYER_LOGIN, days)
    retention_devices = compute_retention(time_str, login_map, days)
    return retention_devices


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
    devices = get_devices(event_time)
    unknown = {("UNKNOWN", "UNKNOWN")}
    for platform, channels in create_player_ids.items():
        for channel, create_set in channels.items():
            if len(create_set) == 0:
                continue
            login_set, _ = login_map.get_all_day_player_ids(platform, channel)
            create_counter = Counter()
            login_counter = Counter()
            for create_id in create_set:
                if create_id in devices:
                    create_counter.update({devices[create_id]})
                else:
                    create_counter.update(unknown)
            intersection_set = create_set.intersection(login_set)
            for login_id in intersection_set:
                if login_id in devices:
                    login_counter.update({devices[login_id]})
                else:
                    login_counter.update(unknown)
            ret[(event_time, platform, channel)] = (
                login_counter, create_counter)
    return ret, True


def get_devices(time_str):
    str_time = time.strftime(
        "%Y.%m.%d", time.strptime(time_str, util.ARG_DATE_FORMAT))
    index = "gperf-index-" + str_time
    ret = {}
    logger.info("Get devices from es")
    logs = es.query_match_all(index, es.get_match_all_dsl())
    logger.info(f"Devices size is {len(logs)}")
    print(logs)
    for log in logs:
        ret[log["_source"]["tags"]["player_id"]] = (
            log["_source"]["device"]["vendor"],
            log["_source"]["device"]["model"])
    return ret


# ==========================for output to es=============================
def output_to_es(time_str, retention_devices):
    if len(retention_devices) == 0:
        return
    for day, values in retention_devices.items():
        for ret_key, counters in values.items():
            retention_type = "retention_device" + "_" + day
            retention_counter = counters[0]
            create_counter = counters[1]
            create_type = "create_device" + "_" + day
            for device, count in retention_counter.items():
                es_add_doc(
                            ret_key[0], retention_type, ret_key, device, count)
            for device1, count1 in create_counter.items():
                es_add_doc(
                            ret_key[0], create_type, ret_key, device1, count1)


def es_add_doc(time_str, compute_type, ret_key, device, count):
    path = ES_INDEX + \
        "/_doc/" + es_get_doc_id(
            time_str, ret_key[1], ret_key[2],
            compute_type, device[0], device[1])
    data = es_get_doc(time_str, compute_type, ret_key, device, count)
    es.add_doc(path, data)


def es_get_doc(time_str, compute_type, ret_key,  device, count):
    timestamp = util.get_timestamp(time_str)
    data = {
        "@timestamp": timestamp,
        "type": compute_type,
        "platform": ret_key[1],
        "channel": ret_key[2],
        "vendor": device[0],
        "model": device[1],
        "count": count
    }
    return json.dumps(data)


def es_get_doc_id(time_str, platform, channel, compute_type, vendor, model):
    str_time = datetime.strptime(
        time_str, util.ARG_DATE_FORMAT).strftime(util.ARG_DATE_FORMAT)
    id = str_time + "_" + platform + "_" + channel + "_" + compute_type + "_" \
        + vendor + "_" + model
    return id


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
