from utils import init_logger, load_config, init_redis_conn
import requests
from bs4 import BeautifulSoup
import asyncio


def get_item_page(key, item_name, redis_conn, logger):
    item_url = redis_conn.hget(key, "url").strip().strip('/')
    logger.info('parse %s info: %s' %(item_name, item_url))
    item_id = item_url.split('/')[-1]
    item_url_ch = 'https://www.javbus.com/' + item_name + '/' + item_id
    item_url_ja = 'https://www.javbus.com/ja/' +item_name + '/' + item_id
    try:
        r_ch = requests.get(item_url_ch)
        r_ja = requests.get(item_url_ja)
        if r_ch.status_code == 200 and r_ja.status_code == 200:
            r_ch_soup = BeautifulSoup(r_ch.content, 'xml')
            r_ja_soup = BeautifulSoup(r_ja.content, 'xml')
            item_ch_name = r_ch_soup.find('div', class_="alert alert-success alert-common").p.b.text.split('-')[0].strip()
            item_ja_name = r_ja_soup.find('div', class_="alert alert-success alert-common").p.b.text.split('-')[0].strip()

            item_info = {}
            if item_name == "star":
                try:
                    r = requests.get(item_url)
                    if r.status_code == 200:
                        r_soup = BeautifulSoup(r.content, 'xml')
                        avatar_url = r_soup.find('div', class_="photo-frame").get('src')
                        for i in r_soup.find('div', class_='photo-info').find_all('p'):
                            item = i.text.strip().split(':')[0]
                            value = i.text.strip().split(':')[1].strip()
                            item_info[item] = value
                    else:
                        logger.error('failed to get en for %s page: %s, error code: en_page: %s' %(item_name, item_url, r.status_code))
                except Exception as e:
                    logger.error('failed to parse %s page: %s, error: %s' % (item_name, item_url, e))

            item_info['name_ch'] = item_ch_name
            item_info['name_ja'] = item_ja_name
            item_info['url_ja'] = item_url_ja
            item_info['url_ch'] = item_url_ch
            redis_conn.hmset(key, item_info)
            logger.info('parsed info for %s page: %s' % (item_name, item_url))
        else:
            logger.error('failed to get ch/ja for %s page: %s, error code: ch_page: %s, ja_page: %s' %(item_name, item_url, r_ch.status_code, r_ja.status_code))
    except Exception as e:
        logger.error('failed to parse %s page: %s, error: %s' % (item_name, item_url, e))


async def get_item_info(key_list, item_name, redis_conn, logger):
    loop = asyncio.get_event_loop()
    await_list = []
    for key in key_list:
        a = loop.run_in_executor(None, get_item_page, key, item_name, redis_conn, logger)
        await_list.append(a)
    await asyncio.wait(await_list)


def main():
    config = load_config('config.yml')
    log_config = config.get('log')
    log_file = log_config.get('parse_item_info_log_file')
    log_level = log_config.get('log_level')
    log_formatter = log_config.get('log_formatter')
    logger = init_logger(log_file, log_level, log_formatter)

    redis_conn = init_redis_conn(config.get('redis'), logger)

    # cursor = 0
    # batch_count = 10000
    item_name = "director"
    # cursor, key_list = redis_conn.scan(cursor, "%s_*" % item_name, batch_count)
    key_list = redis_conn.keys("%s_*" % item_name)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_item_info(key_list, item_name, redis_conn, logger))
    loop.close()


if __name__ == "__main__":
    main()
