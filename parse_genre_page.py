import requests
from bs4 import BeautifulSoup

from utils import init_logger, load_config, init_redis_conn


def get_genre(genre_info_key_list, redis_conn, logger):
    for genre_info_key in genre_info_key_list:
        genre_url_en = redis_conn.hget(genre_info_key, "url").strip().strip('/')
        genre_id = genre_url_en.split('/')[-1]
        genre_url_ch = 'https://www.javbus.com/genre/' + genre_id
        genre_url_ja = 'https://www.javbus.com/ja/genre/' + genre_id
        try:
            r_ch = requests.get(genre_url_ch)
            r_ja = requests.get(genre_url_ja)
            if r_ch.status_code == 200 and r_ja.status_code == 200:
                r_ch_soup = BeautifulSoup(r_ch.content, 'xml')
                r_ja_soup = BeautifulSoup(r_ja.content, 'xml')
                genre_ch_name = r_ch_soup.find('div', class_="alert alert-success alert-common").p.b.text.split('-')[0].strip()
                genre_ja_name = r_ja_soup.find('div', class_="alert alert-success alert-common").p.b.text.split('-')[0].strip()
                redis_conn.hmset(genre_info_key, {"name_ch": genre_ch_name, "name_ja": genre_ja_name, "url_ch": genre_url_ch, "url_ja": genre_url_ja})
                logger.info('parsed info for genre page: %s' % genre_url_en)
            else:
                logger.error('failed to get en/ja for genre page: %s, error code: en_page: %s, ja_page: %s' %(genre_url_en, r_en.status_code, r_ja.status_code))
        except Exception as e:
            logger.error('failed to parse genre page: %s, error: %s' % (genre_url_en, e))


def main():
    config = load_config('config.yml')

    log_config = config.get('log')
    log_file = log_config.get('parse_genre_log_file')
    log_level = log_config.get('log_level')
    log_formatter = log_config.get('log_formatter')
    logger = init_logger(log_file, log_level, log_formatter)

    redis_conn = init_redis_conn(config.get('redis'), logger)

    cursor = 0
    batch_count = 100
    cursor, genre_info_key_list = redis_conn.scan(cursor, "genre_*", batch_count)
    while cursor != 0:
        get_genre(genre_info_key_list, redis_conn, logger)
        cursor, genre_info_key_list = redis_conn.scan(cursor, "genre_*", batch_count)
    get_genre(genre_info_key_list, redis_conn, logger)



if __name__ == "__main__":
    main()
