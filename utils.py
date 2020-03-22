import shutil
import logging
import asyncio

import redis
import requests
import yaml
from bs4 import BeautifulSoup


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


def get_item_page(key, item_name, redis_conn, logger):
    item_url = redis_conn.hget(key, "url").strip().strip('/')
    logger.info('parse %s info: %s' %(item_name, item_url))
    item_id = item_url.split('/')[-1]
    item_url_ch = 'https://www.javbus.com/' + item_name + '/' + item_id
    item_url_ja = 'https://www.javbus.com/ja/' +item_name + '/' + item_id
    try:
        r_ch = requests.get(item_url_ch)
        r_ja = requests.get(item_url_ja)
        if r_ch.status_code == 200 and r_ja.status_code == 200:
            r_ch_soup = BeautifulSoup(r_ch.content, 'xml')
            r_ja_soup = BeautifulSoup(r_ja.content, 'xml')
            item_ch_name = r_ch_soup.find('div', class_="alert alert-success alert-common").p.b.text.split('-')[0].strip()
            item_ja_name = r_ja_soup.find('div', class_="alert alert-success alert-common").p.b.text.split('-')[0].strip()

            item_info = {}
            if item_name == "star":
                try:
                    r = requests.get(item_url)
                    if r.status_code == 200:
                        r_soup = BeautifulSoup(r.content, 'xml')
                        avatar_url = r_soup.find('div', class_="photo-frame").get('src')
                        for i in r_soup.find('div', class_='photo-info').find_all('p'):
                            item = i.text.strip().split(':')[0]
                            value = i.text.strip().split(':')[1].strip()
                            item_info[item] = value
                    else:
                        logger.error('failed to get en for %s page: %s, error code: en_page: %s' %(item_name, item_url, r.status_code))
                except Exception as e:
                    logger.error('failed to parse %s page: %s, error: %s' % (item_name, item_url, e))

            item_info['name_ch'] = item_ch_name
            item_info['name_ja'] = item_ja_name
            item_info['url_ja'] = item_url_ja
            item_info['url_ch'] = item_url_ch
            redis_conn.hmset(key, item_info)
            logger.info('parsed info for %s page: %s' % (item_name, item_url))
        else:
            logger.error('failed to get ch/ja for %s page: %s, error code: ch_page: %s, ja_page: %s' %(item_name, item_url, r_ch.status_code, r_ja.status_code))
    except Exception as e:
        logger.error('failed to parse %s page: %s, error: %s' % (item_name, item_url, e))


async def get_item_info(key_list, item_name, redis_conn, logger):
    loop = asyncio.get_event_loop()
    await_list = []
    for key in key_list:
        a = loop.run_in_executor(None, get_item_page, key, item_name, redis_conn, logger)
        await_list.append(a)
    await asyncio.wait(await_list)
