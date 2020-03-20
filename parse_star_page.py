import requests
from bs4 import BeautifulSoup

from utils import init_logger, load_config, init_redis_conn


def get_star(star_info_key_list, redis_conn, logger):
    for star_info_key in star_info_key_list:
        star_url = redis_conn.hget(star_info_key, "url").strip().strip('/')
        logger.info('parse star info %s' %star_url)
        star_id = star_url.split('/')[-1]
        star_url_ch = 'https://www.javbus.com/star/' + star_id
        star_url_ja = 'https://www.javbus.com/ja/star/' + star_id
        try:
            r_ch = requests.get(star_url_ch)
            r_ja = requests.get(star_url_ja)
            if r_ch.status_code == 200 and r_ja.status_code == 200:
                r_ch_soup = BeautifulSoup(r_ch.content, 'xml')
                r_ja_soup = BeautifulSoup(r_ja.content, 'xml')
                star_ch_name = r_ch_soup.find('div', class_="alert alert-success alert-common").p.b.text.split('-')[0].strip()
                star_ja_name = r_ja_soup.find('div', class_="alert alert-success alert-common").p.b.text.split('-')[0].strip()

                avatar_url = r_ch_soup.find('div', class_="photo-frame").get('src')

                star_info = {}
                for i in r_ch_soup.find('div', class_='photo-info').find_all('p'):
                    item = i.text.strip().split(':')[0]
                    value = i.text.strip().split(':')[1].strip()
                    star_info[item] = value
                update_info = star_info
                update_info['name_ch'] = star_ch_name
                update_info['name_ja'] = star_ja_name
                update_info['url_ja'] = star_url_ja
                update_info['url_ch'] = star_url_ch
                redis_conn.hmset(star_info_key, update_info)
                logger.info('parsed info for star page: %s' % star_url)
            else:
                logger.error('failed to get ch/ja for star page: %s, error code: ch_page: %s, ja_page: %s' %(star_url, r_ch.status_code, r_ja.status_code))
        except Exception as e:
            logger.error('failed to parse star page: %s, error: %s' % (star_url, e))


def main():
    config = load_config('config.yml')

    log_config = config.get('log')
    log_file = log_config.get('parse_star_log_file')
    log_level = log_config.get('log_level')
    log_formatter = log_config.get('log_formatter')
    logger = init_logger(log_file, log_level, log_formatter)

    redis_conn = init_redis_conn(config.get('redis'), logger)

    cursor = 0
    batch_count = 100
    cursor, star_info_key_list = redis_conn.scan(cursor, "star_*", batch_count)
    while cursor != 0:
        get_star(star_info_key_list, redis_conn, logger)
        cursor, star_info_key_list = redis_conn.scan(cursor, "star_*", batch_count)
    get_star(star_info_key_list, redis_conn, logger)


if __name__ == "__main__":
    main()
