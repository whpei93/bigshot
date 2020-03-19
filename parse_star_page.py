import requests
from bs4 import BeautifulSoup

from utils import init_logger, load_config, init_redis_conn


def main():
    config = load_config('config.yml')

    log_config = config.get('log')
    log_file = log_config.get('parse_star_log_file')
    log_level = log_config.get('log_level')
    log_formatter = log_config.get('log_formatter')
    logger = init_logger(log_file, log_level, log_formatter)

    redis_conn = init_redis_conn(config.get('redis'), logger)

    star_info_key_list = redis_conn.keys("star_info_*")
    for star_info_key in star_info_key_list:
        star_url = redis_conn.hget(star_info_key, "url").strip()
        host = star_url.split('star')[0]
        star_id = genre_url.split('star')[1]
        star_url_en = host + 'en/' + 'star' + star_id
        star_url_ja = host + 'ja/' + 'star' + star_id
        try:
            r_en = requests.get(star_url_en)
            r_ja = requests.get(star_url_ja)
            if r_en.status_code == 200 and r_ja.status_code == 200:
                r_en_soup = BeautifulSoup(r_en.content, 'xml')
                r_ja_soup = BeautifulSoup(r_ja.content, 'xml')
                star_en_name = r_en_soup.find('div', class_="alert alert-success alert-common").p.b.text.split('-')[0].strip()
                star_ja_name = r_ja_soup.find('div', class_="alert alert-success alert-common").p.b.text.split('-')[0].strip()

                avatar_url = r_en_soup.find('div', class_="photo-frame").get('src')

                star_info = {}
                for i in r_en_soup.find('div', class_='photo-info').find_all('p'):
                    item = i.text.strip().split(':')[0]
                    value = i.text.strip().split(':')[1].strip()
                    star_info[item] = value
                update_info = {}
                update_info['name_en'] = star_en_name
                update_info['name_ja'] = star_ja_name
                update_info['star_info'] = star_info

                redis_conn.hmset(star_info_key, update_info)
                logger.info('parsed info for star page: %s' % star_url)
            else:
                logger.error('failed to get en/ja for star page: %s, error code: en_page: %s, ja_page: %s' %(star_url, r_en.status_code, r_ja.status_code))
        except Exception as e:
            logger.error('failed to parse star page: %s, error: %s' % (star_url, e))




if __name__ == "__main__":
    main()
