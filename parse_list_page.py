from pprint import pprint

import requests
from bs4 import BeautifulSoup
import redis


def parse_list_page(url, logger):
    logger.info('parsing movies from %s' % url)
    all_movies = {}
    parse_success = False
    headers = {"cookie": "existmag=all"}
    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            c = r.content
            soup = BeautifulSoup(c, 'xml')
            movies = soup.find_all('a', class_='movie-box')
            for movie in movies:
                movie_url = movie['href']
                movie_id = movie_url.split('/')[-1].upper()
                all_movies[movie_id] = movie_url
            next_url_item = soup.find('a', id='next')
            if next_url_item:
                next_url = next_url_item['href']
                protocal = url.split('//')[0]
                host = url.split('//')[1].split('/')[0]
                next_url = protocal + '//' + host + next_url
            else:
                logger.info('no next page for %s' %url)
                next_url = ''
            logger.info('done parsing movies from %s, %s movies' % (url, len(all_movies)))
            parse_success = True
            print(next_url)
        else:
            logger.warning('parsing movies from %s failed, status_code: %s' % (url, status_code))
    except Exception as e:
        logger.warning('parsing movies from %s failed, exception: %s' % (url, e))
    return parse_success, all_movies, next_url

if __name__ == "__main__":
    url = 'https://www.javbus.com/label/6z6'
    parse_list_page(url)
