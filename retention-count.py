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
from model import PlayerIdMap

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
# 添加计算流失数量，月流失和周流失,对应的是月留存和周留存
# 添加计算周回流用户数量，周回流和月回流
def process(time_str):
    valid_params()
    global bucket
    bucket = s3.init_bucket_from_env()
    retentions = compute(time_str)
    output_to_es(retentions)
    logger.info("Process end.")


# ==========================for compute retention count========================
def compute(time_str):
    ret = {}
    ret.update(compute_retention_day_count(time_str))
    if util.is_first_day_of_week(time_str):
        ret.update(compute_week_count(time_str))
    if util.is_first_day_of_month(time_str):
        ret.update(compute_month_count(time_str))
    logger.info(f"Compute result:{ret}. ")
    return ret


def compute_retention_day_count(time_str):
    logger.info(
        f"Compute retention_day_count. date:{time_str}. ")
    today = date.today().strftime(util.ARG_DATE_FORMAT)
    days = util.days_compute(today, time_str)
    create_days = days - 1
    create_map, file_exist = util.get_players(
        bucket, CREATE_PLAYER_EVENT, S3_KEY_PREFIX_CREATE_PLAYER, create_days)
    event_day = util.get_some_day(create_days)
    if not file_exist:
        logger.info(f"Create player file not exist. date:{time_str}. ")
        return {"retention_day": {time_str, {}}}
    login_map, _ = util.get_players(
        bucket, PLAYER_LOGIN_EVENT, S3_KEY_PREFIX_PLAYER_LOGIN, days)
    count = compute_retention_count(login_map, create_map)
    return {"retention_day": {event_day: count}}


def compute_week_count(time_str):
    logger.info(
        f"Compute week_count date:{time_str}. ")
    last_week_days = util.get_previous_one_week_days(time_str)
    one_week_ago_days = util.get_previous_one_week_days(last_week_days[0])
    counts_date, counts, last_login_set = get_retention_and_churn_counts(
        one_week_ago_days, last_week_days)
    ret = {}
    ret["retention_week"] = {counts_date: counts["retention_count"]}
    ret["churn_week"] = {counts_date: counts["churn_count"]}
    two_week_ago_days = util.get_previous_one_week_days(
        one_week_ago_days[0])
    count = get_returning_count(
        two_week_ago_days, one_week_ago_days, last_login_set)
    ret["returning_week"] = {two_week_ago_days[0]: count}
    return ret


def compute_month_count(time_str):
    logger.info(
        f"Compute month_count date:{time_str}. ")
    ret = {}
    last_month_days = util.get_previous_one_month_days(time_str)
    one_month_ago_days = util.get_previous_one_month_days(last_month_days[0])
    counts_date, counts, last_login_map = get_retention_and_churn_counts(
        one_month_ago_days, last_month_days)
    ret["retention_month"] = {counts_date: counts["retention_count"]}
    ret["churn_month"] = {counts_date: counts["churn_count"]}
    two_month_ago_days = util.get_previous_one_month_days(
        one_month_ago_days[0])
    count = get_returning_count(
        two_month_ago_days, one_month_ago_days, last_login_map)
    ret["returning_month"] = {two_month_ago_days[0]: count}
    return ret


def get_returning_count(create_days, first_login_days, second_login_map):
    create_map = PlayerIdMap()
    ret = {}
    file_exist = util.get_players_multiple_days(
        bucket, CREATE_PLAYER_EVENT, S3_KEY_PREFIX_CREATE_PLAYER,
        create_days, create_map)
    if not file_exist:
        logger.info(f"Create player file not exist."
                    f" date start:{create_days[0]}. "
                    f" date end:{create_days[-1]}. ")
        return ret
    first_login_map = PlayerIdMap()
    util.get_players_multiple_days(
        bucket, PLAYER_LOGIN_EVENT, S3_KEY_PREFIX_PLAYER_LOGIN,
        first_login_days, first_login_map)
    ret = compute_returning_count(
        create_map, first_login_map, second_login_map)
    return ret


def compute_returning_count(create_map, first_login_map, second_login_map):
    ret = {}
    ceate_size = len(create_map)
    if ceate_size == 0:
        return ret
    create_player_ids = create_map.get_total_player_ids()
    for platform, channels in create_player_ids.items():
        for channel, create_set in channels.items():
            if len(create_set) == 0:
                continue
            first_login_set, _ = first_login_map.get_all_day_player_ids(
                platform, channel)
            second_login_set, _ = second_login_map.get_all_day_player_ids(
                platform, channel)
            churn_set = create_set.difference(first_login_set)
            returning_set = churn_set.intersection(second_login_set)
            ret[(platform, channel)] = (len(returning_set), len(create_set))
    return ret


def get_retention_and_churn_counts(create_days, login_days):
    ret_date = create_days[0]
    create_map = PlayerIdMap()
    login_map = PlayerIdMap()
    ret = {
        "retention_count": {},
        "churn_count": {}}
    file_exist = util.get_players_multiple_days(
        bucket, CREATE_PLAYER_EVENT, S3_KEY_PREFIX_CREATE_PLAYER,
        create_days, create_map)
    if not file_exist:
        logger.info(f"Create player file not exist."
                    f" date start:{create_days[0]}. "
                    f" date end:{create_days[-1]}. ")
        return ret_date, ret, login_map
    util.get_players_multiple_days(
        bucket, PLAYER_LOGIN_EVENT, S3_KEY_PREFIX_PLAYER_LOGIN,
        login_days, login_map)
    retention_count = compute_retention_count(login_map, create_map)
    churn_count = compute_churn_count(retention_count)
    ret = {"retention_count": retention_count, "churn_count": churn_count}
    logger.info(f"Compute retention count result:{retention_count}. "
                f"Compute churn count result:{churn_count}. ")
    return ret_date, ret, login_map


def compute_retention_count(login_map, create_map):
    ret = {}
    ceate_size = len(create_map)
    if ceate_size == 0:
        return ret
    create_player_ids = create_map.get_total_player_ids()
    for platform, channels in create_player_ids.items():
        for channel, create_set in channels.items():
            if len(create_set) == 0:
                continue
            login_set, _ = login_map.get_all_day_player_ids(platform, channel)
            intersection_set = create_set.intersection(login_set)
            login_count = len(intersection_set)
            create_count = len(create_set)
            ret[(platform, channel)] = (login_count, create_count)
    return ret


def compute_churn_count(retention_count):
    ret = {}
    for key, value in retention_count.items():
        ret[key] = (value[1] - value[0], value[1])
    return ret


# ==========================for output to es=============================
def output_to_es(retentions):
    if len(retentions) == 0:
        return
    for key, values in retentions.items():
        for time_str, ret in values.items():
            for ret_key, ret_value in ret.items():
                es_add_doc(time_str, key + "_count", ret_key, ret_value)


def es_add_doc(time_str, compute_type, ret_key, ret_value):
    path = ES_INDEX + \
        "/_doc/" + es_get_doc_id(time_str, compute_type, ret_key)
    data = es_get_doc(time_str, compute_type, ret_key, ret_value)
    es.add_doc(path, data)


def es_get_doc(time_str, compute_type, ret_key, ret_value):
    timestamp = util.get_timestamp(time_str)
    data = {
        "@timestamp": timestamp,
        "platform": ret_key[0],
        "channel": ret_key[1],
        "count": ret_value[0],
        "type": compute_type
    }
    return json.dumps(data)


def es_get_doc_id(time_str, compute_type, ret_key):
    str_time = datetime.strptime(
        time_str, util.ARG_DATE_FORMAT).strftime(util.ARG_DATE_FORMAT)
    return str_time + "_" + ret_key[0] + "_" + ret_key[1] + "_" +\
        compute_type


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
