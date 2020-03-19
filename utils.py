import logging

import redis
import requests
import yaml
import shutil


def download_image(url, dst, logger):
    success = False
    try:
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(dst, 'wb') as out_file:
                shutil.copyfileobj(response.raw, out_file)
        else:
            logger.error('download %s failed, status_code: %s' %(url, response.status_code))
        success = True
    except Exception as e:
        logger.error('download %s failed, error: %s' %(url, e))
    return success


def load_config(config_file):
    config = dict()
    try:
        with open(config_file) as f:
            config = yaml.full_load(f)
    except Exception as e:
        print('failed to load config from %s, error: %s' %(config_file, e))
    return config


def init_logger(log_file, log_level, log_formatter):
    log_level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    log_level = log_level_map.get(log_level)

    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)

    formatter = logging.Formatter(log_formatter)

    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(formatter)

    fh = logging.FileHandler(log_file)
    fh.setLevel(log_level)
    fh.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)

    return logger


def init_redis_conn(redis_config, logger):
    redis_host = redis_config.get('redis_host')
    redis_port = redis_config.get('redis_port')
    redis_db = redis_config.get('redis_db')
    redis_conn = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
    return redis_conn
