#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
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

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s: %(levelname)s: %(message)s")
logging.root.setLevel(level=logging.INFO)

AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_KEY_PREFIX_CREATE_PLAYER = os.getenv("S3_KEY_PREFIX_CREATE_PLAYER")
S3_KEY_PREFIX_PLAYER_LOGIN = os.getenv("S3_KEY_PREFIX_PLAYER_LOGIN")
CREATE_PLAYER_EVENT = os.getenv("CREATE_PLAYER_EVENT")
PLAYER_LOGIN_EVENT = os.getenv("PLAYER_LOGIN_EVENT")
RETENTION_DAYS = os.getenv("RETENTION_DAYS")
ES_USER = os.getenv("ES_USER")
ES_PWD = os.getenv("ES_PWD")
ES_URL = os.getenv("ES_URL")
ES_INDEX = os.getenv("ES_INDEX", "retention")

ARG_DATE_FORMAT = "%Y-%m-%d"
INVALID_VALUE = -1
RETENTION_DAY_PREFIX = "day"
COMMA = ","

YEAR = "year"
MONTH = "month"
DAY = "day"
FILE_PATH_DATES = {
    YEAR: ["<yyyy>"],
    MONTH: ["<MM>", "<M>"],
    DAY: ["<dd>", "<d>"]
}

FILE_PATH_DOUBLE_DIGITS_DATE = {"<MM>", "<dd>"}
es_headers = {"Content-Type": "application/json"}
es_auth = HTTPBasicAuth(ES_USER, ES_PWD)

bucket = None
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


def valid_params():
    params_errors = []
    if is_empty(S3_BUCKET):
        params_errors.append("S3_BUCKET")

    if is_empty(S3_KEY_PREFIX_CREATE_PLAYER):
        params_errors.append("S3_KEY_PREFIX_CREATE_PLAYER")

    if is_empty(S3_KEY_PREFIX_PLAYER_LOGIN):
        params_errors.append("S3_KEY_PREFIX_PLAYER_LOGIN")

    if is_empty(CREATE_PLAYER_EVENT):
        params_errors.append("CREATE_PLAYER_EVENT")

    if is_empty(PLAYER_LOGIN_EVENT):
        params_errors.append("PLAYER_LOGIN_EVENT")

    if is_empty(RETENTION_DAYS):
        params_errors.append("RETENTION_DAYS")

    if is_empty(AWS_REGION):
        params_errors.append("AWS_REGION")

    if is_empty(ES_USER):
        params_errors.append("ES_USER")

    if is_empty(ES_PWD):
        params_errors.append("ES_PWD")

    if is_empty(ES_URL):
        params_errors.append("ES_URL")

    if len(params_errors) != 0:
        logger.error(f'Params error. {params_errors} is empty')
        raise RuntimeError()


def is_empty(s):
    return not bool(s and s.strip())


def arg_parse(*args, **kwargs):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--day",
        nargs="?",
        const=1,
        type=valid_date,
        default=get_yesterday(),
        help="Date. The default date is yesterday. The format is YYYY-MM-DD"
    )
    args = parser.parse_args()
    process(args.day)


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


def process(time_str):
    valid_params()
    init_bucket()
    retentions = compute_retention(time_str)
    output_to_es(time_str, retentions)
    logger.info("Process sucess.")


def init_bucket():
    aws_access_key_id_name = "AWS_ACCESS_KEY_ID"
    aws_secret_access_key_name = "AWS_SECRET_ACCESS_KEY"
    aws_access_key_id = os.getenv(aws_access_key_id_name)
    aws_secret_access_key = os.getenv(aws_secret_access_key_name)
    if os.environ.get(aws_access_key_id_name) is not None:
        if is_empty(aws_access_key_id):
            os.environ.pop("AWS_ACCESS_KEY_ID")
    if os.environ.get(aws_secret_access_key_name) is not None:
        if is_empty(aws_secret_access_key):
            os.environ.pop("AWS_SECRET_ACCESS_KEY")
    global bucket
    bucket = boto3.resource("s3", AWS_REGION).Bucket(S3_BUCKET)


def get_yesterday():
    yesterday = (date.today() + timedelta(-1)).strftime(ARG_DATE_FORMAT)
    return yesterday

# ==========================for compute retention==============================


def compute_retention(time_str):
    logger.info(
        f"Compute retention date:{time_str}. "
        f"retention days: {RETENTION_DAYS}")
    ret = {}
    retention_days = get_retention_days()
    if len(retention_days) == 0:
        return ret
    today = date.today().strftime(ARG_DATE_FORMAT)
    days = days_compute(today, time_str)
    login_set, file_exist = get_players(PLAYER_LOGIN_EVENT, days)
    if not file_exist:
        logger.error(f"Login log file not exist. Date: {time_str}")
        return ret
    for key, values in retention_days.items():
        retention = get_retention(login_set, days + values)
        if retention != INVALID_VALUE:
            ret[key] = retention
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


def days_compute(today, any_day):
    date1 = datetime.strptime(today, ARG_DATE_FORMAT)
    date2 = datetime.strptime(any_day, ARG_DATE_FORMAT)
    return (date2-date1).days


def get_retention(login_set, days):
    create_set, _ = get_players(CREATE_PLAYER_EVENT, days)
    ceate_size = len(create_set)
    if ceate_size == 0:
        return INVALID_VALUE
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


def get_date_month(has_dates, d):
    if has_dates[MONTH] in FILE_PATH_DOUBLE_DIGITS_DATE:
        return d.strftime("%m")
    return str(d.month)


def get_date_day(has_dates, d):
    if has_dates[DAY] in FILE_PATH_DOUBLE_DIGITS_DATE:
        return d.strftime("%d")
    return str(d.day)


def get_players(event, day):
    player_set = set()
    filter_prefix = get_date_path(event, day)
    if not file_exist(filter_prefix):
        return player_set, False
    add_player(player_set, event, filter_prefix)
    logger.info(
        f"Get players event:{event} ."
        f"file prefix:{filter_prefix} ."
        f"player size: {len(player_set)}")
    return player_set, True


def add_player(player_set, event, filter_prefix):
    for obj in bucket.objects.filter(Prefix=filter_prefix):
        stream = encodings.utf_8.StreamReader(obj.get()["Body"])
        stream.readline
        for line in stream:
            player_id = get_player_id(event, line)
            if player_id != INVALID_VALUE:
                player_set.add(player_id)


def file_exist(filter_prefix):
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


# log format:time event json obj
def get_player_id(event, line):
    sub_lines = line.split(" ")
    if len(sub_lines) < 3:
        raise RuntimeError()
    obj = json.loads(sub_lines[2])
    if sub_lines[1] == event:
        return obj["player_id"]
    return INVALID_VALUE

# ==========================for output to es=============================


def output_to_es(time_str, retentions):
    if len(retentions) == 0:
        return
    global ES_URL
    if ES_URL != "/" and ES_URL.endswith("/"):
        ES_URL = ES_URL[:-1]
    logger.info(f"Output to es. adress: {ES_URL}")
    if not es_index_exist():
        es_create_index()
    for key, values in retentions.items():
        es_add_doc(time_str, key, values)


def es_index_exist():
    url = ES_URL + "/" + ES_INDEX
    response = requests.head(url, verify=False, auth=es_auth)
    if response.status_code == requests.codes.ok:
        return True
    elif response.status_code == requests.codes.not_found:
        return False
    else:
        http_error_log(url, response)
        raise requests.HTTPError(response)


def es_create_index():
    url = ES_URL + "/" + ES_INDEX
    index_template = get_index_template()
    response = requests.put(url, headers=es_headers,
                            data=index_template, verify=False, auth=es_auth)
    if response.status_code != requests.codes.ok:
        http_error_log(url, response)
        raise requests.HTTPError(response)
    logger.info(f"Create Index success. url: {url}")


def es_add_doc(time_str, retention_day, retention):
    url = ES_URL + "/" + ES_INDEX + \
        "/_doc/" + es_get_doc_id(time_str, retention_day)
    index_doc = es_get_doc(time_str, retention_day, retention)
    response = requests.put(url, headers=es_headers,
                            data=index_doc, verify=False, auth=es_auth)
    if (response.status_code != requests.codes.ok and
            response.status_code != requests.codes.created):
        http_error_log(url, response)
        raise requests.HTTPError(response)
    logger.info(f"Add doc success. url: {url}")


def es_get_doc(time_str, retention_day, retention):
    timestamp = get_timestamp(time_str)
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
        time_str, ARG_DATE_FORMAT).strftime(ARG_DATE_FORMAT)
    return str_time + "_" + retention_day


def get_timestamp(time_str):
    dt = time.mktime(time.strptime(time_str, ARG_DATE_FORMAT))
    return (int(round(dt * 1000)))


def get_index_template():
    template = {
        "settings": {
            "index.refresh_interval": "5s"
        },
        "mappings": {
            "_default_": {
                "dynamic_templates": [{
                    "message_field": {
                        "path_match": "message",
                        "match_mapping_type": "string",
                        "mapping": {
                            "type": "text",
                                    "norms": False
                        }
                    }
                }, {
                    "string_fields": {
                        "match": "*",
                        "match_mapping_type": "string",
                        "mapping": {
                            "type": "text", "norms": False,
                            "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                            }
                        }
                    }
                }],
                "properties": {
                    "@timestamp": {
                        "format": "epoch_millis",
                        "type": "date"
                    },
                    "@version": {"type": "keyword"}
                }
            }
        }
    }
    return json.dumps(template)


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
        exit("CTL-C Pressed.")
    except Exception as e:
        logging.exception(e)
        exit("Exception")
