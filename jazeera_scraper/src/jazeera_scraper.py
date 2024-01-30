import argparse
import logging
import requests
import os

from joblib import Parallel, delayed
from tqdm import tqdm
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from undetected_chromedriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

import polars as pl

from time import sleep

class JazeeraScraper:

    def __init__(self, num_workers=-1, wait=0) -> None:
        self.link_retrieval_error_log = []
        self.links = []
        self.num_workers=num_workers
        self.searches = []
        self.articles = None
        self.anti_detection_wait = wait

    def save(self, path):
        if path[-1] == "/":
            path = path[:-1]
        if not os.path.exists(path):
            os.makedirs(path)
        search_df = pl.LazyFrame(data=self.searches, schema=["search_terms", "start_date", "end_date"])
        search_df.sink_csv(path + "/searches.csv")
        self.articles.write_csv(path + "/articles.csv")
        pl.LazyFrame(data=self.links, schema=["url"]).sink_csv(path + "/links.csv")
        pl.LazyFrame(data=self.link_retrieval_error_log, schema=["start_date", "end_date", "search_terms", "error_type", "url"]).sink_csv(path + "/error_log.csv")

    @staticmethod
    def load(path, *args, **kwargs):
        self = JazeeraScraper(*args, **kwargs)
        self.searches = pl.read_csv(path + "/searches.csv").to_numpy().astype(str).tolist()
        self.articles = pl.read_csv(path + "/articles.csv")
        self.articles = self.articles.with_columns(date = pl.col("date").cast(pl.Date))
        self.links = pl.read_csv(path + "/links.csv").to_numpy().T.astype(str).tolist()[0]
        try:
            self.link_retrieval_error_log = pl.read_csv(path + "/error_log.csv").to_numpy().astype(str).tolist()
        except pl.exceptions.NoDataError:
            self.link_retrieval_error_log = []
        return self
    
# Time range generators

    def generate_daily_dates(self, start_date_str, end_date_str):
        """
        Generate a list of daily ISO formatted dates between the given start and end dates.

        Parameters:
        - start_date_str (str): Start date in the format 'YYYY-MM-DD'.
        - end_date_str (str): End date in the format 'YYYY-MM-DD'.

        Returns:
        - List of ISO formatted dates.
        """
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        # Generate a list of daily ISO formatted dates
        iso_dates = [(start_date + timedelta(days=x)).strftime("%Y-%m-%d") for x in range((end_date - start_date).days + 1)]

        return iso_dates
    
    def get_week_ranges(self, start_date_str, end_date_str):
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d") - timedelta(days=datetime.strptime(start_date_str, "%Y-%m-%d").weekday())
        
        while start_date <= datetime.strptime(end_date_str, "%Y-%m-%d"):
            end_date = start_date + timedelta(days=6)
            yield start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
            start_date += timedelta(days=7)

    def get_month_ranges(self, start_date_str, end_date_str):
        
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d").replace(day=1)

            while start_date <= datetime.strptime(end_date_str, "%Y-%m-%d"):
                end_date = (start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                yield start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
                start_date = (end_date + timedelta(days=1)).replace(day=1)

    def are_dom_updates_complete(self, driver):
        return driver.execute_script("return (document.readyState === 'complete');")
    

    def _get_morebutton(self, driver):
        try:
            more_button = driver.find_element(By.CLASS_NAME, ("show-more-button"))
        except NoSuchElementException:
            more_button = None
        return more_button

    def _wait_until_results_loaded(self, driver, timeout=3):

        # Waiting for show_more_button to disappear and loading to appear
        WebDriverWait(driver, timeout).until(
                EC.all_of(
                    EC.invisibility_of_element_located( 
                        (
                            By.CLASS_NAME,
                            "show-more-button"
                        )
                    ),
                    EC.presence_of_element_located(
                        (
                            By.CLASS_NAME,
                            "loading-animation"
                        )
                    ),
                )
            )
        
        # waiting for loading to disappear again, as well as jquery deactivation
        WebDriverWait(driver, timeout).until(
            EC.all_of(
                EC.invisibility_of_element_located( 
                    (
                        By.CLASS_NAME,
                        "loading-animation"
                    )
                ),
                self.are_dom_updates_complete
            )
        )


    def _render_complete_results(self, driver, url, override_overflow=False):
        driver.get(url)
        WebDriverWait(driver, 3).until(
                    EC.any_of(
                        EC.presence_of_element_located(
                            (By.CLASS_NAME, "u-clickable-card__link")
                            ), # Results are here
                        EC.presence_of_element_located(
                            (By.CLASS_NAME, "search-results__no-results")
                            ) # No results found
                    )
                )
        intermediate_soup = BeautifulSoup(driver.page_source,features="lxml")
        amount = intermediate_soup.find("span", class_="search-summary__query")

        if "Top 100" in amount.text:
            if override_overflow:
                f"OVERRIDING: More than 100 links found. Consider refining this search, our current method can only fetch the first page. Search Page can't offer more."
            else:
                raise OverflowError(f"More than 100 links found. Consider refining this search, our current method can only fetch the first page.")
        
        # Shape of input: "About {number} results"
        found_link_amount = int(amount.text.split(" ")[1])
        more_button = self._get_morebutton(driver)
        link_elements = intermediate_soup.find_all("a", class_="u-clickable-card__link")
        prev_scroll_link_amount = len(link_elements)
        overscroll_counter = 0

        while more_button and prev_scroll_link_amount < found_link_amount and overscroll_counter < 20: # Edge case: If exactly 10 links are found, searchbutton re-appears but does not load new elements.
            driver.execute_script("arguments[0].click();", more_button)
            # Wait until show more button is gone (to avoid acc. Click spamming)
            self._wait_until_results_loaded(driver, timeout=3)
            more_button = self._get_morebutton(driver)
            intermediate_soup = BeautifulSoup(driver.page_source,features="lxml")
            link_elements = intermediate_soup.find_all("a", class_="u-clickable-card__link")
            
            if len(link_elements) <= prev_scroll_link_amount:
                logging.debug(f"Scrolling {url}, link elements did not increase on scroll.")
                overscroll_counter += 1

            prev_scroll_link_amount = len(link_elements)

    def _get_articles_in_daterange_scrolling(self, driver, search_terms, start_date, end_date, retry_count = 5, override_overflow=False):
        """
        Al Jazeera demands distinct dates, but search range is inclusive. Thus, the minimum amount of days that can be searched is 2.

        Args:
            driver: Selenium Driver to use. Try to use a separate one for each parallel task.
            search_terms: String with space separated search terms. Both will occur in each result.
            start_date: Date in form YYYY_MM_DD. Articles from this day are included in the results.
            end_date: Date in form YYYY_MM_DD. Articles from this day are included in results.
        """
        base_url = "https://www.aljazeera.com"
        full_search_query = f"{search_terms} after:{start_date} before:{end_date}" # Unique
        url = f"https://www.aljazeera.com/search/{full_search_query.replace(' ', '%20')}"

        links = [] # Define as Fallback if necessary

        # Wait for the page to load (adjust the timeout as needed)
        failure_counter = 0
        success = False
        while (not success) and (failure_counter < retry_count):
            try:
                self._render_complete_results(driver, url, override_overflow=override_overflow) 
                soup = BeautifulSoup(driver.page_source, features="lxml")
                link_elements = soup.find_all("a", class_="u-clickable-card__link")
                links = set()

                amountspan = soup.find("span", class_="search-summary__query")
                if not amountspan:
                    raise TimeoutException(f"{start_date} - {end_date} : {search_terms} : Page didn't load correctly.")   
                amount = int(amountspan.text.split(" ")[1])
                if len(link_elements) < amount:
                    logging.warning(f"{start_date} - {end_date} Retrieved only {len(link_elements)} of {amount} search results. Incomplete.")
                    self.link_retrieval_error_log.append((start_date, end_date, search_terms,"INCOMPLETE", url))
                for a_tag in link_elements:
                    if base_url not in a_tag["href"]:
                        links.add(f'{base_url}/{a_tag["href"]}')
                    else:
                        links.add(a_tag["href"])
                links = list(links)
                logging.debug(f"{start_date} - {end_date}, {len(link_elements)} article links found. Expected: {amount}. Unique: {len(links)}")
                success = True
            except OverflowError:
                # Pass on with better info
                raise OverflowError(f"{start_date} : {end_date}: Overflow.")
            except TimeoutException: 
                # Timing out means that none of the above conditions was met. 
                # This could indicate a server rejection due to spam. 
                # We can not check as we are forced to use selenium.
                failure_counter += 1
                if "403" in str(driver.page_source): 
                    logging.debug("Unauthorized detected. Going to sleep...")
                # Escalating wait time to try and trick spam rage
                    sleep(2 ** (failure_counter + 1))
                logging.debug(f"{start_date} - {end_date}: Timed out for the {failure_counter} th time.")
            
                if failure_counter >= 5:
                    self.link_retrieval_error_log.append((start_date, end_date, search_terms, "FAILED", url))
                    raise TimeoutException(f"{start_date} - {end_date} failed {failure_counter} times. giving up on this time range.")
        if override_overflow:
            self.link_retrieval_error_log.append((start_date, end_date, search_terms,"OVERFLOW", url))

        
        return links


    def get_articles_in_month(self, search_terms, start_date):
        opitons = Options()
        opitons.add_argument("--headless")
        driver = Chrome(opitons)
        driver.implicitly_wait(self.anti_detection_wait)

        month_start_date = datetime.strptime(start_date, "%Y-%m-%d")
        month_end_date = (month_start_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)


        try:
            month_links = self._get_articles_in_daterange_scrolling(driver, search_terms, month_start_date.strftime("%Y-%m-%d"), month_end_date.strftime("%Y-%m-%d"))
        except OverflowError:
            month_links = []
            logging.debug(f"{month_start_date.strftime('%Y-%m-%d')} more than max links found in MONTH : {month_end_date.strftime('%Y-%m-%d')}. Looking with finer granularity...")
            # If more than max displayed links are found, sample down into weeks
            for week_start_date, week_end_date in self.get_week_ranges(month_start_date.strftime("%Y-%m-%d"), month_end_date.strftime("%Y-%m-%d")):
                month_links.extend(self.get_articles_in_week(search_terms, week_start_date, driver))

        except TimeoutException as e:
            month_links = [] # Surrogate
            logging.warning(e.msg)

        driver.close()
        driver.quit()
        del driver
        return month_links

    def get_articles_in_week(self, search_terms, start_date, driver):
        week_start_date = datetime.strptime(start_date, "%Y-%m-%d")
        week_end_date = week_start_date + timedelta(days=6)

        try:
            week_links = self._get_articles_in_daterange_scrolling(driver, search_terms, week_start_date.strftime("%Y-%m-%d"), week_end_date.strftime("%Y-%m-%d"))
        except TimeoutException as e:
            week_links = []
            logging.warning(e.msg)
        except OverflowError:
            week_links = []
            logging.debug(f"more than max links found in WEEK {week_start_date.strftime('%Y-%m-%d')} : {week_end_date.strftime('%Y-%m-%d')}. Looking with finer granularity...")
            # If more than one link is found, recursively get articles for each day
            for day_date in self.generate_daily_dates(week_start_date.strftime("%Y-%m-%d"), week_end_date.strftime("%Y-%m-%d")):
                try:
                    week_links.extend(
                        self._get_articles_in_daterange_scrolling(
                            driver, 
                            search_terms, 
                            day_date, 
                            (datetime.strptime(day_date, '%Y-%m-%d') + timedelta(days=1))
                                .strftime('%Y-%m-%d'),
                            override_overflow=True # override overflow, we have reached minimal resolution
                                )
                    )
                except TimeoutException as e:
                    logging.warning(e.msg)
        return week_links

    def collect_article_links(self, search_terms, start_date_str, end_date_str):
        self.searches.append((search_terms, start_date_str, end_date_str))
        month_ranges = self.get_month_ranges(start_date_str, end_date_str)
        month_link_lists = [(lambda date: self.get_articles_in_month(search_terms, date))(start_date) for start_date, end_date in tqdm(list(month_ranges))]
        links = self.links
        for linklist in month_link_lists:
            links.extend(linklist)
        self.links = list(set(links)) # Unique
        return self.links
    
    def get_and_prepare_article(self, url):
        logging.debug(f"Requesting {url}")
        sleep(self.anti_detection_wait)
        resp = requests.get(url)
        if resp.status_code == 200:
            html = resp.content
            soup = BeautifulSoup(html, features="lxml")
            title = soup.title.text

            date_div = soup.find("div", class_="date-simple")
            if date_div:
                date_text = date_div.find("span", attrs={"aria-hidden": "true"}).text
                date = datetime.strptime(date_text, "%d %b %Y")
                date_formatted = date.strftime("%Y-%m-%d") # This will allow polars translation through cast.
            else: date_formatted = None
            paragraphs = soup.find_all("p")
            text = ""
            for i in paragraphs:
                text += i.text

            return {
                "title": [title],
                "date": [date_formatted],
                "url": [url],
                "paragraphs_text": [text],
                "html": [str(html)],
            }
        else:
            logging.warning(f"Request to {url} failed with status code {resp.status_code}")
            return {
                "title": [None],
                "date": [None],
                "url": [url],
                "paragraphs_text": [None],
                "html": [None],
            }


    def get_article_texts(self):

        parallel = Parallel(n_jobs=self.num_workers, return_as="generator")

        links = self.links

        if self.articles is not None: # Avoid reloading existing articles
            loaded_links = self.articles.select(["url", "paragraphs_text"]).filter(pl.col("url").is_in(links) & pl.col("paragraphs_text").is_not_null())
            loaded_links = loaded_links.select("url").to_numpy().astype(str).T[0].tolist()
            links = list(set(links) - set(loaded_links))

        res_generator = parallel(
            delayed(self.get_and_prepare_article)(url=url) for url in links
        )

        treated_counter = 0
        for i in tqdm(res_generator, total=len(links)):
            sub_df = pl.DataFrame(i).with_columns(date = pl.col("date").cast(pl.Date))
            if self.articles is None:
                self.articles = sub_df
                logging.debug("Initialized articles Dataframe")
            else:
                self.articles = self.articles.extend(sub_df)
                logging.debug(f"Extended articles Dataframe by request {sub_df['title']}")
            
            if treated_counter % 100 == 0:
                self.articles = self.articles.rechunk()
        
        return self.articles
    
    def correct_unretrieved_links(self):
        error_log = self.link_retrieval_error_log
        # TODO: Make this safe. We are perfectly able to lose our log here.
        self.link_retrieval_error_log = []
        logging.info("Correcting unretrieved links...")
        for start_date, end_date, search_terms, err_type, url in tqdm(error_log):
            if err_type in ["INCOMPLETE", "FAILED", "OVERFLOW"]:
                try:
                    opitons = Options()
                    opitons.add_argument("--headless")
                    driver = Chrome(opitons)
                    driver.implicitly_wait(self.anti_detection_wait)
                    logging.debug(f"Correcting {start_date} - {end_date} : {search_terms}")
                    if err_type=="OVERFLOW":
                        try:
                            self.links.extend(self._get_articles_in_daterange_scrolling(driver, search_terms, start_date, end_date))
                        except OverflowError:
                            self.link_retrieval_error_log.append((start_date, end_date, search_terms, "OVERFLOW", url))
                    else:
                        self.links.extend(self._get_articles_in_daterange_scrolling(driver, search_terms, start_date, end_date, override_overflow=True))
                    driver.close()
                    driver.quit()
                except TimeoutException as e:
                    logging.warning(str(e))
        self.links = list(set(self.links))

    def do_scrape(self, search_terms, start_date, end_date, output_path):
        if output_path[-1] == "/":
            output_path = output_path[:-1]
        if not os.path.exists(output_path):
            os.makedirs(output_path) # Attempt folder creation early to avoid losing data
        logging.info("Collecting links...")
        self.collect_article_links(search_terms, start_date, end_date)
        logging.info("Getting article texts...")
        self.get_article_texts()
        # Try two corrections
        logging.info("First correction...")
        self.correct_unretrieved_links()
        logging.info("Second correction...")
        self.correct_unretrieved_links()
        # Get any newbies
        logging.info("Getting new articles...")
        self.get_article_texts()
        self.save(output_path)
        return self.articles



if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("--search_terms", "-q", dest="search_terms", default="fifa qatar", type=str, help="Search terms for scraping")
    argparser.add_argument("--start_date", "-s", dest="start_date", default="01-01-2010", type=str, help="Start date of the scrape time area. Format: DD-MM-YYYY")
    argparser.add_argument("--end_date", "-e", dest="end_date", default="31-12-2023", type=str, help="End date of the scrape time area. Format: DD-MM-YYYY")
    argparser.add_argument("--path", "-p", type=str, dest="path", help="Path to save crawl to / load it from", default="test_crawl")
    argparser.add_argument("--num_workers", "-w", dest="num_workers", type=int, default=-1, help="Number of parallel workers to use. Default: -1 (all available)")
    args = argparser.parse_args()
    
    if args.path[-1] == "/":
        args.path = args.path[:-1]
    if not os.path.exists(args.path):
        crawler = JazeeraScraper(num_workers=args.num_workers, wait=0)
    else:
        crawler = JazeeraScraper.load(args.path, num_workers=args.num_workers, wait=0)
    articles = crawler.do_scrape(search_terms=args.search_terms, start_date=args.start_date, end_date=args.end_date, output_path=args.path)