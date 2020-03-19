import json

from utils import init_logger, load_config, init_redis_conn


def main():
    config = load_config('config.yml')

    log_config = config.get('log')
    log_file = log_config.get('log_file')
    log_level = log_config.get('log_level')
    log_formatter = log_config.get('log_formatter')
    logger = init_logger(log_file, log_level, log_formatter)

    redis_conn = init_redis_conn(config.get('redis'), logger)

    movie_key_list = redis_conn.keys('movie_info_*')
    for movie_key in movie_key_list:
        movie_info = json.loads(redis_conn.get(movie_key))
        for star_id, star in movie_info['stars'].items():
            star_key = "start_info_" + star_id
            redis_conn.hmset(star_key, star)
            print(star_key)

if __name__ == "__main__":
    main()
