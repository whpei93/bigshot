from pprint import pprint

import requests
from bs4 import BeautifulSoup



def handle_detail_page(url):
    movie_info = {}
    r = requests.get(url)
    print(r.status_code)
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
        movie_info['sample_image_urls'] = []
        sample_image_list = soup.find_all("a", class_="sample-box")
        for sample_image in sample_image_list:
            movie_info['sample_image_urls'].append(sample_image['href'].strip())

        # parse movie info
        info_table = soup.find("div", class_="col-md-3 info")

        genre_index = info_table.find("p", class_="header")
        for item in genre_index.find_all_previous("p"):
            item_name = item.text.split(':')[0].strip().lower()
            item_name = '_'.join(item_name.split())
            item_value = item.text.split(':')[1].strip().lower()
            item_value = '_'.join(item_value.split())
            if item.find("a"):
                item_url = item.find("a")['href'].strip()
                item_id = item_url.split('/')[-1]
            else:
                item_url = ''
                item_id = ''
            movie_info[item_name] = {}
            movie_info[item_name]['value'] = item_value
            movie_info[item_name]['id'] = item_id
            movie_info[item_name]['url'] = item_url
            print(item_name, item_value, item_url, item_id)

        movie_info['genre'] = {}
        for genre in genre_index.find_next("p").find_all("a"):
            genre_url = genre["href"]
            genre_id = genre_url.split("/")[-1]
            genre_name = genre.text
            movie_info['genre'][genre_id] = {}
            movie_info['genre'][genre_id]['name'] = genre_name
            movie_info['genre'][genre_id]['url'] = genre_url

        movie_info['stars'] = {}
        star_index = info_table.find("p", class_="star-show")
        for star in star_index.find_next("p").find_all("a"):
            star_url = star["href"]
            star_id = star["href"].split("/")[-1]
            star_name = star.text
            movie_info['stars'][star_id] = {}
            movie_info['stars'][star_id]['name'] = star_name
            movie_info['stars'][star_id]['url'] = star_url
            print(star_url, star_id, star_name)

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
            magnet_soup = BeautifulSoup(magnet_content)
            for magnet_item in magnet_soup.find_all('tr'):
                name_td = magnet_item.find('td')
                name = name_td.text.strip()
                url = name_td.find('a')['href']
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
            pass
    
    else:
        print(r.status_code)

    return movie_info


if __name__ == "__main__":
    url = "https://www.javbus.com/en/FSDSS-012"
    pprint(handle_detail_page(url))
