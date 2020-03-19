from pprint import pprint
import logging
import json

import redis
import yaml

from parse_movie_page import parse_movie_page
from utils import init_logger, load_config, init_redis_conn


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


if __name__ == "__main__":
    config = load_config('config.yml')

    log_config = config.get('log')
    log_file = log_config.get('get_movie_log_file')
    log_level = log_config.get('log_level')
    log_formatter = log_config.get('log_formatter')
    logger = init_logger(log_file, log_level, log_formatter)

    redis_config = config.get('redis')
    redis_conn = init_redis_conn(redis_config, logger)

    movie_key_list = redis_conn.keys("movie_*")
    for movie_key in movie_key_list:
        # movie already been parsed
        if redis_conn.hget(movie_key, "parsed") == 1:
            continue
        movie_url = redis_conn.hget(movie_key, "url")
        movie_id = movie_url.split('/')[-1]
        domain_name = movie_url.strip('en/'+movie_id)
        movie_url_ch = domain_name + movie_id
        movie_url_ja = domain_name + 'ja/' + movie_id

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