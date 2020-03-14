#!/usr/bin/env python3
import argparse
import decimal
import encodings
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta

from eslog import eslog
from util import util
from s3 import s3
from es import es
from collections import Counter

AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_KEY_PREFIX_CREATE_PLAYER = os.getenv("S3_KEY_PREFIX_CREATE_PLAYER")
S3_KEY_PREFIX_PLAYER_LOGIN = os.getenv("S3_KEY_PREFIX_PLAYER_LOGIN")
CREATE_PLAYER_EVENT = os.getenv("CREATE_PLAYER_EVENT")
PLAYER_LOGIN_EVENT = os.getenv("PLAYER_LOGIN_EVENT")
ES_INDEX = os.getenv("ES_INDEX", "retention")

EFFECTIVE_INTERVAL = os.getenv("EFFECTIVE_INTERVAL", 7)
CREATE_PLAYER_EFFECTIVE_DAYS = os.getenv("CREATE_PLAYER_EFFECTIVE_DAYS", 2)
PLAYER_LOGIN_EFFECTIVE_DAYS = os.getenv("PLAYER_LOGIN_EFFECTIVE_DAYS", 3)

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


# 每天计算有效的新增和留存的用户数
# 假如有效间隔为7天，今天为3月13号，也就是说3月6注册的 在3月6号到12号进行登录的
def process(time_str):
    valid_params()
    global bucket
    bucket = s3.init_bucket_from_env()
    create_date, effective_counts = compute_retention_effective_count(time_str)
    output_to_es(create_date, effective_counts)
    logger.info("Process end.")


# ==========================for compute retention count========================
def compute_retention_effective_count(time_str):
    start_date = util.get_some_day_of_one_day(
        time_str, (-EFFECTIVE_INTERVAL) + 1)
    end_date = time_str
    create_set = get_create_players(start_date)
    login_counter = get_login_players(start_date, end_date)
    create_effective, login_effective = compute_count(
        create_set, login_counter)
    ret = {}
    ret["effective_create_count"] = create_effective
    ret["effective_login_count"] = login_effective
    logger.info(ret)
    return start_date, ret


def get_create_players(time_str):
    today = date.today().strftime(util.ARG_DATE_FORMAT)
    create_day = util.days_compute(today, time_str)
    create_set, file_exist = util.get_players(
        bucket, CREATE_PLAYER_EVENT, S3_KEY_PREFIX_CREATE_PLAYER, create_day)
    if not file_exist:
        logger.error(
            f"Create log file not exist. Date: {create_day}")
    print("======================================")
    return create_set


def get_login_players(start_date, end_date):
    login_days = util.get_date_list(start_date, end_date)
    login_counter = Counter()
    file_exist = util.get_players_multiple_days(
        bucket, PLAYER_LOGIN_EVENT, S3_KEY_PREFIX_PLAYER_LOGIN,
        login_days, login_counter)
    if not file_exist:
        logger.error(
            f"Login log file not exist. Date satrt: {start_date}"
            f"end:{end_date} .")
    return login_counter


def compute_count(create_set, login_counter):
    print(create_set)
    print(login_counter)
    if len(create_set) == 0:
        return util.INVALID_VALUE, util.INVALID_VALUE
    create_effective = 0
    login_effective = 0
    for create in create_set:
        login_days = login_counter[create]
        if login_days > CREATE_PLAYER_EFFECTIVE_DAYS:
            create_effective = create_effective + 1
        if login_days > PLAYER_LOGIN_EFFECTIVE_DAYS:
            login_effective = login_effective + 1
    return create_effective, login_effective


# ==========================for output to es=============================
def output_to_es(time_str, effective_counts):
    if len(effective_counts) == 0:
        return
    for key, value in effective_counts.items():
        if value != util.INVALID_VALUE:
            es_add_doc(time_str, key, value)


def es_add_doc(time_str, retention_type, retention_count):
    path = ES_INDEX + \
        "/_doc/" + es_get_doc_id(time_str, retention_type)
    data = es_get_doc(time_str, retention_type, retention_count)
    es.add_doc(path, data)


def es_get_doc(time_str, retention_type, retention_count):
    timestamp = util.get_timestamp(time_str)
    data = {
        "@timestamp": timestamp,
        "type": retention_type,
        "count": retention_count
    }
    return json.dumps(data)


def es_get_doc_id(time_str, retention_type):
    str_time = datetime.strptime(
        time_str, util.ARG_DATE_FORMAT).strftime(util.ARG_DATE_FORMAT)
    return str_time + "_" + retention_type


def test_output_to_es():
    time_str = "2019-01-07"
    retentions = {ONE_DAY_KEY: 0.11, ONE_WEEK_KEY: 0.13}
    output_to_es(time_str, retentions)


def test_compute_retention():
    time_str = "2019-06-30"
    compute_retention(time_str)


def get_start_timestamp(day):

    d = (date.today() + timedelta(days=day))
    return int(time.mktime())


if __name__ == '__main__':
    try:
        sys.exit(arg_parse(*sys.argv))
    except KeyboardInterrupt:
        logger.exception("CTL-C Pressed.")
        exit("CTL-C Pressed.")
    except Exception as e:
        logger.exception(e)
        exit("Exception")
