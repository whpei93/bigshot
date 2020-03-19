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
        for item, value in movie_info.items():
            if item in ['stars', 'url', 'title', 'big_image_url', 'sample_image_urls', 'sample_small_image_urls', 'magnets']:
                pass
            else:
                redis_conn.hmset(item+'_info', value)
                print(item+'_info')

if __name__ == "__main__":
    main()
