import requests
from bs4 import BeautifulSoup

from utils import init_logger, load_config, init_redis_conn


def main():
    config = load_config('config.yml')

    log_config = config.get('log')
    log_file = log_config.get('parse_genre_log_file')
    log_level = log_config.get('log_level')
    log_formatter = log_config.get('log_formatter')
    logger = init_logger(log_file, log_level, log_formatter)

    redis_conn = init_redis_conn(config.get('redis'), logger)

    genre_info_key_list = redis_conn.keys("genre_info_*")
    for genre_info_key in genre_info_key_list:
        genre_url = redis_conn.hget(genre_info_key, "url").strip()
        host = genre_url.split('genre')[0]
        genre_id = genre_url.split('genre')[1]
        genre_url_en = host + 'en/' + 'genre' + genre_id
        genre_url_ja = host + 'ja/' + 'genre' + genre_id
        try:
            r_en = requests.get(genre_url_en)
            r_ja = requests.get(genre_url_ja)
            if r_en.status_code == 200 and r_ja.status_code == 200:
                r_en_soup = BeautifulSoup(r_en.content, 'xml')
                r_ja_soup = BeautifulSoup(r_ja.content, 'xml')
                genre_en_name = r_en_soup.find('div', class_="alert alert-success alert-common").p.b.text.split('-')[0].strip()
                genre_ja_name = r_ja_soup.find('div', class_="alert alert-success alert-common").p.b.text.split('-')[0].strip()
                redis_conn.hmset(genre_info_key, {"name_en": genre_en_name, "name_ja": genre_ja_name})
                logger.info('parsed info for genre page: %s' % genre_url)
            else:
                logger.error('failed to get en/ja for genre page: %s, error code: en_page: %s, ja_page: %s' %(genre_url, r_en.status_code, r_ja.status_code))
        except Exception as e:
            logger.error('failed to parse genre page: %s, error: %s' % (genre_url, e))




if __name__ == "__main__":
    main()
