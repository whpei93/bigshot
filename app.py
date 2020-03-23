import asyncio

from parse_list_page import parse_list_page
from utils import init_logger, load_config, init_redis_conn


def parse_genre_page(url, logger, redis_conn):
    parse_success, movies, next_url = parse_list_page(url, logger)
    if parse_success:
        #logger.info(movies)
        for movie_id, movie_url in movies.items():
            need_to_parse_movie_key = 'need_to_parse_' + movie_id
            done_parse_movie_key = 'done_parse_' + movie_id
            redis_conn.set(need_to_parse_movie_key, movie_url)
    else:
        failed_url_key = 'failed_list_page' +  url
        redis_conn.set(failed_url_key, url)
    return next_url


def parse_genre(url, logger, redis_conn):
    next_url = parse_genre_page(url, logger, redis_conn)
    while next_url:
        next_url = parse_genre_page(next_url, logger, redis_conn)


async def get_movie_from_genre(genres_info_key_list, redis_conn, logger):
    loop = asyncio.get_event_loop()
    if genres_info_key_list:
        genres_root_page_list = []
        await_list = []
        for genre_info_key in genres_info_key_list:
            genres_root_page_list.append(redis_conn.hget(genre_info_key, 'url'))
    else:
        genres_root_page_list = ['https://www.javbus.com/en/genre/g',]

    for url in genres_root_page_list:
        a = loop.run_in_executor(None, parse_genre, url, logger, redis_conn)
        await_list.append(a)
    await asyncio.wait(await_list)


def main():
    config = load_config('config.yml')

    log_config = config.get('log')
    log_file = log_config.get('log_file')
    log_level = log_config.get('log_level')
    log_formatter = log_config.get('log_formatter')
    logger = init_logger(log_file, log_level, log_formatter)

    redis_config = config.get('redis')
    redis_conn = init_redis_conn(redis_config, logger)

    # start from every genres' root page
    genres_info_key_list = redis_conn.keys('genre_*')

    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_movie_from_genre(genres_info_key_list, redis_conn, logger))
    loop.close()


if __name__ == "__main__":
    main()


