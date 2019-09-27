import logging
from datetime import datetime
import logging
from es import es
import json
import uuid

format = "%(asctime)s: %(levelname)s:%(module)s: %(funcName)s: %(message)s"


def get_logger(index_name):
    index_name = index_name + "-logs"
    logger = logging.getLogger()
    logging.basicConfig(format=format)
    handler = ESLogHandler(index_name)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


class ESLogHandler(logging.Handler):
    def __init__(self, index_name, level=logging.NOTSET, enable_to_es=True):
        logging.Handler.__init__(self, level=level)
        self.to_es_logs = []
        self.index_name = index_name
        self.batches = str(uuid.uuid4())

    @staticmethod
    def __get_es_datetime_str(timestamp):
        current_date = datetime.utcfromtimestamp(timestamp)
        return "{0!s}.{1:03d}Z".format(current_date.strftime(
            '%Y-%m-%dT%H:%M:%S'), int(current_date.microsecond / 1000))

    def emit(self, record):
        action = {
            "index": {}
        }
        ret = json.dumps(action) + es.ES_NEWLINE
        source = dict()
        source["@timestamp"] = self.__get_es_datetime_str(record.created)
        source["level"] = record.levelname
        source["name"] = record.name
        source["lineno"] = record.lineno
        source["message"] = self.format(record)
        source["module"] = record.module
        source["batches"] = self.batches
        source["funcName"] = record.funcName
        ret = ret + json.dumps(source) + es.ES_NEWLINE
        self.to_es_logs.append(ret)

    def output_to_es(self):
        size = len(self.to_es_logs)
        if size == 0:
            return
        start = 0
        end = 0
        while (end < size):
            start = end
            end = end + es.ES_BULK_SIZE
            if end >= size:
                end = size
            es.batch_add_doc(
                self.index_name, "".join(self.to_es_logs[start: end]))

    def close(self):
        self.output_to_es()
