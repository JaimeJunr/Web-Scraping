import requests
import time
import random
import csv
from bs4 import BeautifulSoup
import concurrent.futures
import logging


USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'
OUTPUT_FILE = 'movies.csv'

MAX_THREADS = 10


def setup_logging():
    logging.basicConfig(filename='imdb_scraper.log', level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')


def extract_movie_details(movie_link):
    logging.info(f"Fetching details for: {movie_link}")
    time.sleep(random.uniform(0, 0.2))
    try:
        response = requests.get(movie_link, headers={'User-Agent': USER_AGENT})
        response.raise_for_status() 
        soup = BeautifulSoup(response.content, 'html.parser')

        movie_data = soup.find('div', attrs={'class': 'sc-92625f35-3 frxYSZ'})
        if not movie_data:
            logging.warning(f"No movie data found for: {movie_link}")
            return

        title = movie_data.find('h1').get_text() if movie_data.find('h1') else None
        date = movie_data.find(
            'a', attrs={'class': 'ipc-link ipc-link--baseAlt ipc-link--inherit-color'}
            ).get_text().strip() if movie_data.find('a', class_='ipc-link') else None

        rating = soup.find(
            'span', attrs={'sc-bde20123-1 cMEQkK'}
            ).get_text() if soup.find('span', attrs={'sc-bde20123-1 cMEQkK'}) else None
        
        plot_text = soup.find(
            'span', attrs={'data-testid': 'plot-xs_to_m'}
            ).get_text().strip() if soup.find('span', attrs={'data-testid': 'plot-xs_to_m'}) else None

        if all([title, date, rating, plot_text]):
            with open(OUTPUT_FILE, 'a', newline='') as file:
                movie_writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                movie_writer.writerow([title, date, rating, plot_text])
                logging.info(f"Movie details saved for: {title}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching details for {movie_link}: {e}")


def extract_movies(soup):
    movies_table = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'}).find('ul')
    movies_table_rows = movies_table.find_all('li')
    movie_links = ['https://imdb.com' + movie.find('a')['href'] for movie in movies_table_rows]

    threads = min(MAX_THREADS, len(movie_links))
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        
        executor.map(extract_movie_details, movie_links)


def main():
    setup_logging()
    start_time = time.time()

    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'
    response = requests.get(popular_movies_url, headers={'User-Agent': USER_AGENT})
    soup = BeautifulSoup(response.content, 'html.parser')

    extract_movies(soup)

    end_time = time.time()
    logging.info(f'Total time taken: {end_time - start_time:.2f} seconds')


if __name__ == '__main__':
    main()
