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
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import encodings
from eslog import eslog
from util import util
from s3 import s3
from es import es

S3_KEY_PREFIX_CREATE_PLAYER = os.getenv("S3_KEY_PREFIX_CREATE_PLAYER")
S3_KEY_PREFIX_PLAYER_LOGIN = os.getenv("S3_KEY_PREFIX_PLAYER_LOGIN")
CREATE_PLAYER_EVENT = os.getenv("CREATE_PLAYER_EVENT")
PLAYER_LOGIN_EVENT = os.getenv("PLAYER_LOGIN_EVENT")
RETENTION_DAYS = os.getenv("RETENTION_DAYS")
ES_INDEX = os.getenv("ES_INDEX", "retention")

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


def process(time_str):
    valid_params()
    global bucket
    bucket = s3.init_bucket_from_env()
    retentions = compute_retention(time_str)
    output_to_es(time_str, retentions)
    logger.info("Process end.")


# ==========================for compute retention=============================
def compute_retention(time_str):
    logger.info(
        f"Compute retention date:{time_str}. "
        f"retention days: {RETENTION_DAYS}")
    ret = {}
    retention_days = get_retention_days()
    if len(retention_days) == 0:
        return ret
    today = date.today().strftime(util.ARG_DATE_FORMAT)
    days = util.days_compute(today, time_str)
    login_set, file_exist = util.get_players(
        bucket, PLAYER_LOGIN_EVENT, S3_KEY_PREFIX_PLAYER_LOGIN, days)
    if not file_exist:
        logger.error(f"Login log file not exist. Date: {time_str}")
        return ret
    for key, values in retention_days.items():
        retention = get_retention(login_set, days + values)
        if retention != util.INVALID_VALUE:
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


def get_retention(login_set, days):
    create_set, _ = util.get_players(
        bucket, CREATE_PLAYER_EVENT, S3_KEY_PREFIX_CREATE_PLAYER, days)
    ceate_size = len(create_set)
    if ceate_size == 0:
        return util.INVALID_VALUE
    intersection_set = create_set.intersection(login_set)
    login_size = len(intersection_set)
    return round(login_size/ceate_size, 2)


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
        es_add_doc(time_str, key, values)


def es_add_doc(time_str, retention_day, retention):
    path = ES_INDEX + \
        "/_doc/" + es_get_doc_id(time_str, retention_day)
    data = es_get_doc(time_str, retention_day, retention)
    es.add_doc(path, data)


def es_get_doc(time_str, retention_day, retention):
    timestamp = util.get_timestamp(time_str)
    data = {
        "@timestamp": timestamp,
        "type": retention_day,
        "retention": retention
    }
    return json.dumps(data)


def http_error_log(url, response):
    logger.error(
        f"Http error, Url;{url}. Http code ：{response.status_code}. "
        f"Http content:{response.content}")


def es_get_doc_id(time_str, retention_day):
    str_time = datetime.strptime(
        time_str, util.ARG_DATE_FORMAT).strftime(util.ARG_DATE_FORMAT)
    return str_time + "_" + retention_day


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
