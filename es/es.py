#!/usr/bin/env python3
import logging
import requests
import os
from requests.auth import HTTPBasicAuth
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from util import util
import json

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

ES_USER = os.getenv("ES_USER")
ES_PWD = os.getenv("ES_PWD")
ES_URL = os.getenv("ES_URL")

ES_BULK_SIZE = 1000
ES_NEWLINE = "\n"

es_json_headers = {"Content-Type": "application/json"}
es_x_ndjson_headers = {"Content-Type": "application/x-ndjson"}
es_auth = HTTPBasicAuth(ES_USER, ES_PWD)

logger = logging.getLogger()


def add_doc(path, data):
    url = get_base_url()
    if ES_URL != "/" and ES_URL.endswith("/"):
        url = ES_URL[:-1]
    url = url + "/" + path
    response = requests.put(url, headers=es_json_headers, timeout=10,
                            data=data, verify=False, auth=es_auth)
    if (response.status_code != requests.codes.ok and
            response.status_code != requests.codes.created):
        http_error_log(url, response)
        raise requests.HTTPError(response)
    logger.info(f"Add doc success. url: {url}")


def batch_add_doc(index, data):
    url = get_base_url()
    url = url + "/" + index + "/_doc/_bulk"
    response = requests.post(url, headers=es_x_ndjson_headers, timeout=60,
                             data=data, verify=False, auth=es_auth)
    if (response.status_code != requests.codes.ok and
            response.status_code != requests.codes.created):
        http_error_log(url, response)
        raise requests.HTTPError(response)
    logger.info(f"Add docs success. url: {url}")


def query_match_all(index, data):
    ret = []
    url = get_base_url()
    url = url + "/" + index + "/_search?scroll=2m"
    response = requests.post(url, headers=es_json_headers, timeout=60,
                             data=data, verify=False, auth=es_auth)
    if response.status_code != requests.codes.ok:
        http_error_log(url, response)
        if response.status_code == requests.codes.not_found:
            return ret
        else:
            raise requests.HTTPError(response)
    logger.info(f"Query success. url: {url}, request param is {data}")
    hits, scroll_id = get_hits(response)
    has_more = True
    if hits:
        ret.extend(hits)
    else:
        has_more = False

    while(has_more):
        has_more, scroll_id = get_next(ret, scroll_id)
    del_scroll(scroll_id)
    return ret


def get_next(ret, scroll_id):
    url = get_base_url()
    url = url + "/_search/scroll?scroll=2m"
    data = get_scroll_data(scroll_id)
    response = requests.post(url, headers=es_json_headers, timeout=60,
                             data=data, verify=False, auth=es_auth)
    if (response.status_code != requests.codes.ok and
            response.status_code != requests.codes.created):
        http_error_log(url, response)
        raise requests.HTTPError(response)
    logger.info(f"Get next success. url: {url}, scroll_id is {scroll_id}")
    hits, scroll_id = get_hits(response)
    if hits:
        ret.extend(hits)
        return True, scroll_id
    else:
        return False, scroll_id


def del_scroll(scroll_id):
    url = get_base_url()
    url = url + "/_search/scroll"
    data = get_scroll_data(scroll_id)
    response = requests.delete(
        url, headers=es_json_headers, timeout=60, data=data, verify=False,
        auth=es_auth)
    if (response.status_code != requests.codes.ok and
            response.status_code != requests.codes.created):
        http_error_log(url, response)
        raise requests.HTTPError(response)
    logger.info(f"Detele scroll success. url: {url}, scroll_id is {scroll_id}")


def get_scroll_data(scroll_id):
    data = {
        "scroll_id": scroll_id
    }
    return json.dumps(data)


def get_hits(response):
    data = response.json()
    scroll_id = data["_scroll_id"]
    hits = data["hits"]["hits"]
    size = len(hits)
    logger.info(f"Get hits .hits size is {size}")
    if size > 0:
        return hits, scroll_id
    return None, scroll_id


def valid_params():
    if util.is_empty(ES_USER) or util.is_empty(
            ES_PWD) or util.is_empty(ES_URL):
        logger.error(f'Params error. ES_USER or ES_PWD or ES_URL is empty')
        raise RuntimeError()


def get_base_url():
    valid_params()
    url = ES_URL
    if ES_URL != "/" and ES_URL.endswith("/"):
        url = ES_URL[:-1]
    return url


def http_error_log(url, response):
    logger.error(
        f"Http error, Url;{url}. Http code ：{response.status_code}. "
        f"Http content:{response.content}")


def get_match_all_dsl(size=1000):
    data = {
        "query": {
            "match_all": {}
        },
        "sort": [
            "_doc"
        ],
        "size": size
    }
    return json.dumps(data)
