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

AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_KEY_PREFIX_CREATE_PLAYER = os.getenv("S3_KEY_PREFIX_CREATE_PLAYER")
S3_KEY_PREFIX_PLAYER_LOGIN = os.getenv("S3_KEY_PREFIX_PLAYER_LOGIN")
CREATE_PLAYER_EVENT = os.getenv("CREATE_PLAYER_EVENT")
PLAYER_LOGIN_EVENT = os.getenv("PLAYER_LOGIN_EVENT")
ES_INDEX = os.getenv("ES_INDEX", "retention")

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


# 每天计算日留存数量，每个星期一计算周留存数量，每个月1号计算月留存数量
def process(time_str):
    valid_params()
    global bucket
    bucket = s3.init_bucket_from_env()
    retentions = compute_retention_count(time_str)
    output_to_es(retentions)
    logger.info("Process end.")


# ==========================for compute retention count========================
def compute_retention_count(time_str):
    ret = {}
    ret["day"] = compute_retention_day_count(time_str)
    if util.is_first_day_of_week(time_str):
        ret["week"] = compute_retention_week_count(time_str)
    if util.is_first_day_of_month(time_str):
        ret["month"] = compute_retention_month_count(time_str)
    return ret


def compute_retention_day_count(time_str):
    logger.info(
        f"Compute retention_day_count date:{time_str}. ")
    ret = {}
    today = date.today().strftime(util.ARG_DATE_FORMAT)
    days = util.days_compute(today, time_str)
    login_set, file_exist = util.get_players(
        bucket, PLAYER_LOGIN_EVENT, S3_KEY_PREFIX_PLAYER_LOGIN, days)
    if not file_exist:
        logger.error(f"Login log file not exist. Date: {time_str}")
        return (ret, time_str)
    days = days - 1
    create_set, file_exist = util.get_players(
        bucket, CREATE_PLAYER_EVENT, S3_KEY_PREFIX_CREATE_PLAYER, days)
    if not file_exist:
        logger.error(f"Create log file not exist. Date: {time_str}")
        return (ret, time_str)
    ret = compute_count(login_set, create_set)
    logger.info(f"Compute retention result:{ret}. ")
    return (ret, time_str)


def compute_retention_week_count(time_str):
    print(type(time_str))
    print(time_str)
    logger.info(
        f"Compute retention_week_count date:{time_str}. ")
    login_days = util.get_previous_one_week_days(time_str)
    create_days = util.get_previous_one_week_days(login_days[0])
    return get_retention_count(create_days, login_days)


def compute_retention_month_count(time_str):
    logger.info(
        f"Compute retention_month_count date:{time_str}. ")
    login_days = util.get_previous_one_month_days(time_str)
    create_days = util.get_previous_one_month_days(login_days[0])
    return get_retention_count(create_days, login_days)


def get_retention_count(create_days, login_days):
    ret_date = login_days[-1]
    create_set, file_exist = util.get_players_multiple_days(
        bucket, CREATE_PLAYER_EVENT, S3_KEY_PREFIX_CREATE_PLAYER, create_days)
    if not file_exist:
        logger.error(
            f"Create log file not exist. Date satrt: {create_days[0]}"
            f"end:{create_days[-1]} .")
        return (util.INVALID_VALUE, ret_date)
    login_set, file_exist = util.get_players_multiple_days(
        bucket, PLAYER_LOGIN_EVENT, S3_KEY_PREFIX_PLAYER_LOGIN, login_days)
    if not file_exist:
        logger.error(
            f"Login log file not exist. Date satrt: {login_days[0]}"
            f"end:{login_days[-1]} .")
    ret = compute_count(login_set, create_set)
    logger.info(f"Compute retention result:{ret}. ")
    return (ret, ret_date)


def compute_count(login_set, create_set):
    ceate_size = len(create_set)
    if ceate_size == 0:
        return util.INVALID_VALUE
    intersection_set = create_set.intersection(login_set)
    login_size = len(intersection_set)
    return login_size

# ==========================for output to es=============================


def output_to_es(retentions):
    if len(retentions) == 0:
        return
    for key, values in retentions.items():
        if values[0] != util.INVALID_VALUE:
            es_add_doc(values[1], key, values[0])


def es_add_doc(time_str, retention_type, retention_count):
    path = ES_INDEX + \
        "/_doc/" + es_get_doc_id(time_str, retention_type)
    data = es_get_doc(time_str, retention_type, retention_count)
    es.add_doc(path, data)


def es_get_doc(time_str, retention_type, retention_count):
    timestamp = util.get_timestamp(time_str)
    data = {
        "@timestamp": timestamp,
        "type": "count_" + retention_type,
        "retention_count": retention_count
    }
    return json.dumps(data)


def es_get_doc_id(time_str, retention_type):
    str_time = datetime.strptime(
        time_str, util.ARG_DATE_FORMAT).strftime(util.ARG_DATE_FORMAT)
    return str_time + "_count_" + retention_type


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
