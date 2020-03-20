from pprint import pprint
import logging
import json

import redis
import yaml

from parse_movie_page import parse_movie_page
from utils import init_logger, load_config, init_redis_conn


def get_movie(movie_key_list, redis_conn, logger):
    for movie_key in movie_key_list:
        # movie already been parsed
        if redis_conn.hget(movie_key, "parsed") == 1:
            continue
        movie_url = redis_conn.hget(movie_key, "url")
        movie_id = movie_url.split('/')[-1]
        domain_name = movie_url.strip('/en/'+movie_id)
        movie_url_ch = domain_name + '/' +  movie_id
        movie_url_ja = domain_name + '/ja/' + movie_id

        parse_success, movie_info = parse_movie_page(movie_url, logger, redis_conn)
        if parse_success:
            update_info = {}
            update_info['parsed'] = 1
            update_info['id'] = movie_id
            update_info['movie_url_ch'] = movie_url_ch
            update_info['movie_url_ja'] = movie_url_ja
            update_info['movie_info'] = json.dumps(movie_info)
            redis_conn.hmset(movie_key, update_info)
            logger.info('done parse %s' %movie_url)


def main():
    config = load_config('config.yml')

    log_config = config.get('log')
    log_file = log_config.get('get_movie_log_file')
    log_level = log_config.get('log_level')
    log_formatter = log_config.get('log_formatter')
    logger = init_logger(log_file, log_level, log_formatter)

    redis_config = config.get('redis')
    redis_conn = init_redis_conn(redis_config, logger)

    cursor = 0
    batch_count = 100
    cursor, movie_key_list = redis_conn.scan(cursor, "movie_*", batch_count)
    while cursor != 0:
        get_movie(movie_key_list, redis_conn, logger)
        cursor, movie_key_list = redis_conn.scan(cursor, "movie_*", batch_count)
    get_movie(movie_key_list, redis_conn, logger)


if __name__ == "__main__":
    main()