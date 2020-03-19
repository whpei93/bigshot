from pprint import pprint

import requests
from bs4 import BeautifulSoup


def parse_movie_page(url, logger, redis_conn):
    movie_info = {}
    parse_success = False
    logger.info('start parsing %s' %url)
    try:
        r = requests.get(url)
        if r.status_code == 200:
            c = r.content
            soup = BeautifulSoup(c, 'xml')
            movie_info['url'] = url

            # parse movie title
            movie_info['title'] = soup.h3.text.strip()

            # parse big image url
            bigImage = soup.find("a", class_="bigImage")
            bigImage_src = bigImage.img['src'].strip()
            movie_info['big_image_url'] = bigImage_src

            # parse sample image urls
            movie_info['sample_image_urls'] = {}
            movie_info['sample_small_image_urls'] = {}
            sample_image_list = soup.find_all("a", class_="sample-box")
            tmp_count = 1
            for sample_image in sample_image_list:
                movie_info['sample_image_urls'][tmp_count] = sample_image['href'].strip().strip('/')
                movie_info['sample_small_image_urls'][tmp_count] = sample_image.img['src'].strip().strip('/')

            # parse movie info
            info_table = soup.find("div", class_="col-md-3 info")

            genre_index = info_table.find("p", class_="header")
            for item in genre_index.find_all_previous("p"):
                item_name = item.text.split(':')[0].strip().lower()
                item_name = '_'.join(item_name.split())
                item_value = item.text.split(':')[1].strip().lower()
                item_value = '_'.join(item_value.split())
                if item.find("a"):
                    item_url = item.find("a")['href'].strip().strip('/')
                    item_id = item_url.split('/')[-1]
                else:
                    item_url = ''
                    item_id = ''
                movie_info[item_name] = {}
                movie_info[item_name]['value'] = item_value
                movie_info[item_name]['id'] = item_id
                movie_info[item_name]['url'] = item_url
                if item_url and item_id:
                    redis_conn.hmset(item_name+'_'+item_id, movie_info[item_name])

            movie_info['genre'] = {}
            if genre_index.find_next("p"):
                for genre in genre_index.find_next("p").find_all("a"):
                    genre_url = genre["href"]
                    genre_id = genre_url.split("/")[-1]
                    genre_name = genre.text
                    movie_info['genre'][genre_id] = {}
                    movie_info['genre'][genre_id]['name'] = genre_name
                    movie_info['genre'][genre_id]['url'] = genre_url
                    redis_conn.hmset('genre_'+genre_id, movie_info['genre'][genre_id])
            else:
                logger.info('no genre info for %s' % url)

            movie_info['stars'] = {}
            star_index = info_table.find("p", class_="star-show")
            if star_index.find_next("p"):
                for star in star_index.find_next("p").find_all("a"):
                    star_url = star["href"].strip().strip('/')
                    star_id = star["href"].strip().strip('/').split("/")[-1]
                    star_name = star.text
                    movie_info['stars'][star_id] = {}
                    movie_info['stars'][star_id]['name'] = star_name
                    movie_info['stars'][star_id]['url'] = star_url
                    redis_conn.hmset('star_'+star_id, movie_info['stars'][star_id])
            else:
                logger.info('no star info for %s' % url)


            # parse magnet urls
            movie_info['magnets'] = {}
            js_url = "https://www.javbus.com/ajax/uncledatoolsbyajax.php?"
            js_vars = soup.find('script', src="https://www.javbus.com/js/focus.js?v=8.7").find_next('script').text.strip().split('\n')
            for i in js_vars:
                var_name = i.split()[1].strip()
                var_value = i.split()[-1].strip(';')
                js_url += '%s=%s&' %(var_name, var_value)
            magnet_response = requests.get(js_url, headers={"referer": url})
            if magnet_response.status_code == 200:
                magnet_content = magnet_response.content
                magnet_soup = BeautifulSoup(magnet_content, 'xml')
                if magnet_soup.find_all('tr'):
                    for magnet_item in magnet_soup.find_all('tr'):
                        name_td = magnet_item.find('td')
                        if name_td.find('a'):
                            name = name_td.text.strip()
                            url = name_td.find('a')['href'].strip().strip('/')
                            size_td = name_td.find_next('td')
                            size = size_td.text.strip()
                            share_date = size_td.find_next('td').text.strip()
                            magnet_id = '%s_%s_%s' %(name, size, share_date)
                            movie_info['magnets'][magnet_id] = {}
                            movie_info['magnets'][magnet_id]['name'] = name
                            movie_info['magnets'][magnet_id]['size'] = size
                            movie_info['magnets'][magnet_id]['share_date'] = share_date
                            movie_info['magnets'][magnet_id]['url'] = url
                        else:
                            logger.info('no magnet url for %s' %url)
                else:
                    logger.info('no magnet url for %s' %url)
            else:
                pass
            parse_success = True
        else:
            logger.warning('failed to get %s, status_code %s' %(url, r.status_code))
    except Exception as e:
        logger.warning('failed to parse %s, error %s' %(url, e))
    return parse_success, movie_info


if __name__ == "__main__":
    url = "https://www.javbus.com/en/FSDSS-012"
    pprint(parse_detail_page(url))
