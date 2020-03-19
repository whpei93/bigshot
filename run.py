import json
import copy

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
    item_key_map = {
            "識別碼": "id",
            "發行日期": "release_date",
            "長度": "length",
            "製作商": "studio",
            "發行商": "label",
            "系列": "series",
            "導演": "director"
            }
    for movie_key in movie_key_list:
        movie_info = json.loads(redis_conn.get(movie_key))
        movie_info_tmp = copy.deepcopy(movie_info)
        for item, value in movie_info.items():
            changed = True
            if item in ['genre', 'stars', 'url', 'title', 'big_image_url', 'sample_image_urls', 'sample_small_image_urls', 'magnets']:
                pass
            else:
                item_name = item_key_map.get(item)
                if not item_name:
                    changed = False
                    item_name = item
                movie_info_tmp[item_name] = value
                if changed:
                    movie_info_tmp[item_name]['name'] = item
                if value['url']:
                    redis_conn.hmset(item_name+'_info', value)
                    redis_conn.delete(item+'_info')
                else:
                    redis_conn.delete(item+'_info')
        redis_conn.set(movie_key, json.dumps(movie_info_tmp))

if __name__ == "__main__":
    main()
