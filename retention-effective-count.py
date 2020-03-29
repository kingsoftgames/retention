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
from collections import Counter

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

EFFECTIVE_INTERVAL = os.getenv("EFFECTIVE_INTERVAL", 7)
CREATE_PLAYER_EFFECTIVE_DAYS = os.getenv("CREATE_PLAYER_EFFECTIVE_DAYS", 2)
PLAYER_LOGIN_EFFECTIVE_DAYS = os.getenv("PLAYER_LOGIN_EFFECTIVE_DAYS", 3)

CHURN_DAYS = os.getenv("CHURN_DAYS", "1,3")

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
# 计算前1天和前3天的流失率,放在一起是因为计算周期是一样的
def process(time_str):
    valid_params()
    global bucket
    bucket = s3.init_bucket_from_env()
    create_date, effective_counts, churn_rates = compute(time_str)
    output_to_es(create_date, effective_counts, churn_rates)
    logger.info("Process end.")


# ==========================for compute retention count========================
def compute(time_str):
    start_date = util.get_some_day_of_one_day(
        time_str, (-EFFECTIVE_INTERVAL) + 1)
    end_date = time_str
    creates_without_day = get_create_players(start_date)
    if len(creates_without_day) == 0:
        return start_date, {}, {}
    login_start_date = util.get_some_day_of_one_day(
        start_date, 1)
    login_map, login_days = get_login_players(login_start_date, end_date)
    effective_ret = compute_effective_count(
        creates_without_day, login_map)
    churn_ret = compute_churn_rate(
        creates_without_day, login_map, login_days)
    logger.info(
        f"compute effective count and churn rate. Date:{start_date} ."
        f"end date:{end_date} ."
        f"effective count:{effective_ret} ."
        f"churn ret:{churn_ret} .")
    return start_date, effective_ret, churn_ret


def compute_churn_rate(creates_without_day, login_map, login_days):
    days = CHURN_DAYS.split(",")
    keys = sorted(login_days)
    ret = {}
    for day in days:
        surplus_day = int(day) - 1
        compute_days = keys[surplus_day:]
        for platform, channels in creates_without_day.items():
            for channel, create_set in channels.items():
                if len(create_set) == 0:
                    continue
                compute_ret = set()
                for compute_day in compute_days:
                    ids, _ = login_map.get_some_day_player_ids(
                            platform, channel, compute_day)
                    compute_ret.update(ids)
                compute_ret = create_set.difference(compute_ret)
                key = "churn_rate_" + day + "_day"
                ret[(key, platform, channel)] = (
                    len(compute_ret), len(create_set))
    return ret


def get_churn_rate_ret(creates, login_map, ret):
    compute_ret = set()
    for compute_day in compute_days:
        compute_ret.update(
            login_map.get_some_day_player_ids(
                platform, channel, compute_day))
    compute_ret = create_set.difference(compute_ret)
    key = "churn_rate_" + day + "_day"
    ret[(key, platform, channel)] = (
        len(compute_ret), len(create_set))


def get_create_players(time_str):
    today = date.today().strftime(util.ARG_DATE_FORMAT)
    create_day = util.days_compute(today, time_str)
    create_map, file_exist = util.get_players(
        bucket, CREATE_PLAYER_EVENT, S3_KEY_PREFIX_CREATE_PLAYER, create_day)
    if not file_exist:
        logger.error(
            f"Create log file not exist. Date: {create_day}")
    return create_map.get_total_player_ids()


def get_login_players(start_date, end_date):
    login_days = util.get_date_list(start_date, end_date)
    login_map = PlayerIdMap()
    file_exist = util.get_players_multiple_days(
        bucket, PLAYER_LOGIN_EVENT, S3_KEY_PREFIX_PLAYER_LOGIN,
        login_days, login_map)
    if not file_exist:
        logger.error(
            f"Login log file not exist. Date satrt: {start_date}"
            f"end:{end_date} .")
    return login_map, login_days


def compute_effective_count(creates_without_day, login_map):
    ret = {}
    for platform, channels in creates_without_day.items():
        for channel, create_set in channels.items():
            create_effective = 0
            login_effective = 0
            if len(create_set) == 0:
                continue
            counter, has_id = login_map.get_all_day_player_ids_counter(
                platform, channel)
            if has_id:
                for create in create_set:
                    # +1 是加上创建的那天
                    login_days = counter[create] + 1
                    if login_days >= CREATE_PLAYER_EFFECTIVE_DAYS:
                        create_effective = create_effective + 1
                    if login_days >= PLAYER_LOGIN_EFFECTIVE_DAYS:
                        login_effective = login_effective + 1
            create_key = ("effective_create_count", platform, channel)
            login_key = ("effective_login_count", platform, channel)
            ret[create_key] = create_effective
            ret[login_key] = login_effective
    return ret


# ==========================for output to es=============================
def output_to_es(time_str, effective_counts, churn_rates):
    if len(effective_counts) > 0:
        for key, value in effective_counts.items():
            es_add_doc(time_str, key, value)
    if len(churn_rates) > 0:
        for key, value in churn_rates.items():
            es_add_doc(time_str, key, value)


def es_add_doc(time_str, ret_key, ret_value):
    path = ES_INDEX + \
        "/_doc/" + es_get_doc_id(time_str, ret_key)
    data = es_get_doc(time_str, ret_key, ret_value)
    es.add_doc(path, data)


def es_get_doc(time_str, ret_key, ret_value):
    timestamp = util.get_timestamp(time_str)
    data = {
        "@timestamp": timestamp,
        "type": ret_key[0],
        "platform": ret_key[1],
        "channel": ret_key[2]
    }
    if "churn_rate" in ret_key[0]:
        data["login_count"] = ret_value[0]
        data["create_count"] = ret_value[1]
    else:
        data["count"] = ret_value
    return json.dumps(data)


def es_get_doc_id(time_str, ret_key):
    str_time = datetime.strptime(
        time_str, util.ARG_DATE_FORMAT).strftime(util.ARG_DATE_FORMAT)
    return str_time + "_" + ret_key[1] + "_" + \
        ret_key[2] + "_" + ret_key[0]


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
