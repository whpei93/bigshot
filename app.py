from pprint import pprint
import logging

import redis
import yaml

from parse_list_page import parse_list_page


def main(url, logger, redis_conn):
    parse_success, movies, next_url = parse_list_page(url, logger)
    if parse_success:
        pprint(movies)
        for movie_id, movie_url in movies.items():
            need_to_parse_movie_key = 'need_to_parse_' + movie_id
            done_parse_movie_key = 'done_parse_' + movie_id
            # insert movie into redis only it not been seen and not been parsed
            if not redis_conn.get(need_to_parse_movie_key) and not redis_conn.get(done_parse_movie_key):
                redis_conn.set(need_to_parse_movie_key, movie_url)
            else:
                # movies sorted by release date on list page, if some movie on the page has been got
                # then movies behind it are older, no need to parse next page  
                next_url = ''
    else:
        failed_url_key = 'failed_list_page' +  url
        redis_conn.set(failed_url_key, url)
    return next_url


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


if __name__ == "__main__":
    root_url = 'https://www.javbus.com/genre/g/1460'

    config = load_config('config.yml')

    log_config = config.get('log')
    log_file = log_config.get('log_file')
    log_level = log_config.get('log_level')
    log_formatter = log_config.get('log_formatter')
    logger = init_logger(log_file, log_level, log_formatter)

    redis_config = config.get('redis')
    redis_host = redis_config.get('redis_host')
    redis_port = redis_config.get('redis_port')
    redis_db = redis_config.get('redis_db')
    redis_conn = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

    # start from every genres' root page
    genres_info_key_list = redis_conn.keys('genre_info_*')
    if genres_info_key_list:
        genres_root_page_list = []
        for genre_info_key in genres_info_key_list:
            genres_root_page_list.append(redis_conn.hget(genre_info_key, 'url'))
        for url in genres_root_page_url:
            next_url = main(url, logger, redis_conn)
            while next_url:
                next_url = main(next_url, logger, redis_conn)
    else:
        next_url = main(root_url, logger, redis_conn)
        while next_url:
            next_url = main(next_url, logger, redis_conn)

