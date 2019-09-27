#!/usr/bin/env python3
import os
import json
import argparse
import sys
import array

from util import util
from s3 import s3
from es import es
from eslog import eslog

S3_KEY_PREFIX = os.getenv("S3_KEY_PREFIX")
IAP_EVENT = os.getenv("IAP_EVENT")
ES_PAYING_USERS_INDEX = os.getenv(
    "ES_PAYING_USERS_INDEX", "paying-users")

logger = eslog.get_logger(ES_PAYING_USERS_INDEX)
bucket = None


def valid_params():
    params_errors = []

    if util.is_empty(S3_KEY_PREFIX):
        params_errors.append("S3_KEY_PREFIX")

    if util.is_empty(IAP_EVENT):
        params_errors.append("IAP_EVENT")

    if len(params_errors) != 0:
        logger.error(f'Params error. {params_errors} is empty')
        raise RuntimeError()


def arg_parse(*args, **kwargs):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--day",
        nargs="?",
        const=1,
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
    players = get_paying_users(time_str)
    logger.info(f"Pay player size is {len(players)}")
    output_to_es(time_str, players)
    logger.info("Process end.")
# ==========================for get logs =============================


def get_paying_users(time_str):
    player_map = {}
    days = util.get_days_with_today(time_str)
    logs, exist = util.get_logs(bucket, IAP_EVENT, S3_KEY_PREFIX, days)
    if not exist:
        return player_map
    for log in logs:
        id = util.get_paying_users_index_id(
            log["player_id"], log["platform"], log["channel"])
        player_map[id] = log
    return player_map


# ==========================for output to es=============================


def output_to_es(time_str, players):
    if len(players) == 0:
        return
    data = []

    for key, value in players.items():
        data.append(es_get_doc(time_str, key, value))
    size = len(data)
    start = 0
    end = 0
    while (end < size):

        start = end
        end = end + es.ES_BULK_SIZE
        if end >= size:
            end = size
        es.batch_add_doc(ES_PAYING_USERS_INDEX, "".join(data[start: end]))
        logger.info(f"Add docs success. between {start} and {end}")


def es_get_doc(time_str, id, log):
    timestamp = util.get_timestamp(time_str)
    action = {
        "index": {"_id": id}
    }
    ret = json.dumps(action) + es.ES_NEWLINE
    source = {
        "player_id": log["player_id"],
        "platform": log["platform"],
        "channel":  log["channel"],
        "@timestamp": timestamp
    }
    return ret + json.dumps(source) + es.ES_NEWLINE


def test_output_to_es():
    time_str = "2019-01-07"
    retentions = {ONE_DAY_KEY: 0.11, ONE_WEEK_KEY: 0.13}
    output_to_es(time_str, retentions)


if __name__ == '__main__':
    try:
        sys.exit(arg_parse(*sys.argv))
    except KeyboardInterrupt:
        logger.info("CTL-C Pressed.")
        exit("CTL-C Pressed.")
    except Exception as e:
        logger.exception(e)
        exit("Exception")
