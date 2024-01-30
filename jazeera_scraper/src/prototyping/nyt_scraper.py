import logging
import requests

from joblib import Parallel, delayed
from tqdm import tqdm
from datetime import datetime, timedelta, date
import numpy as np
from bs4 import BeautifulSoup
from undetected_chromedriver import Chrome
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

import polars as pl

from time import sleep

class NYTScraper:

    def __init__(self, num_workers=-1, wait=0) -> None:
        self.link_retrieval_error_log = []
        self.links = []
        self.num_workers=num_workers
        self.searches = []
        self.articles = None
        self.anti_detection_wait = wait

    def save(self, path):
        search_df = pl.LazyFrame(data=self.searches, schema=pl.Schema(["search_terms", "start_date", "end_date"]))
        search_df.sink_csv(path + "/searches.csv")
        self.articles.sink_csv(path + "/articles.csv")
        pl.LazyFrame(data=self.links, schema=pl.Schema(["url"])).sink_csv(path + "/links.csv")
        pl.LazyFrame(data=self.link_retrieval_error_log, schema=pl.Schema(["start_date", "end_date", "search_terms", "error_type", "url"])).sink_csv(path + "/error_log.csv")

    def load(self, path):
        self.searches = pl.read_csv(path + "/searches.csv").to_numpy().astype(str).T.tolist()
        self.articles = pl.read_csv(path + "/articles.csv")
        self.links = pl.read_csv(path + "/links.csv").to_numpy().astype(str).T[0].tolist()
        self.link_retrieval_error_log = pl.read_csv(path + "/error_log.csv").to_numpy().astype(str).T.tolist()
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
            more_button = driver.driver.find_element_by_xpath('//input[@data-testid="search-show-more-button"]')
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

    def _extract_links_from_searpage(self, soup):
        result_elements = soup.find_all("div", class_="css-e1lvw9")
        link_elements = [result_elements.find("a") for result_elements in result_elements]
        links = [link_elements["href"] for link_elements in link_elements]
        return links
    def _extract_result_amount_from_searchpage(self, soup):
        amount = soup.find("span", class_="css-nayoou")
        # Shape of input: "Showing {number} results for:"
        found_link_amount = int(amount.text.split(" ")[1].replace(",", ""))
        return found_link_amount

    def _render_complete_results(self, driver, url, override_overflow=False):
        driver.get(url)
        WebDriverWait(driver, 3).until(
                    EC.any_of(
                        EC.presence_of_element_located(
                            (By.CLASS_NAME, "css-e1lvw9") # single result div
                            ), # Results are here
                        EC.presence_of_element_located(
                            (By.CLASS_NAME, "css-nayoou") # Search summary div
                            )
                    )
                )
        intermediate_soup = BeautifulSoup(driver.page_source,features="lxml")
        found_link_amount = self._extract_result_amount_from_searchpage(intermediate_soup)
        more_button = self._get_morebutton(driver)
        links = self._extract_links_from_searpage(intermediate_soup)
        prev_scroll_link_amount = len(links)
        overscroll_counter = 0

        while more_button and prev_scroll_link_amount < found_link_amount and overscroll_counter < 20: # Edge case: If exactly 10 links are found, searchbutton re-appears but does not load new elements.
            driver.execute_script("arguments[0].click();", more_button)
            # Wait until show more button is gone (to avoid acc. Click spamming)
            self._wait_until_results_loaded(driver, timeout=3)
            more_button = self._get_morebutton(driver)
            intermediate_soup = BeautifulSoup(driver.page_source,features="lxml")
            links = self._extract_links_from_searpage(intermediate_soup)
            
            if len(links) <= prev_scroll_link_amount:
                logging.debug(f"Scrolling {url}, link elements did not increase on scroll.")
                overscroll_counter += 1

            prev_scroll_link_amount = len(links)

    def _get_articles_scrolling(self, driver, search_terms, retry_count = 5, override_overflow=False):
        """
        Al Jazeera demands distinct dates, but search range is inclusive. Thus, the minimum amount of days that can be searched is 2.

        Args:
            driver: Selenium Driver to use. Try to use a separate one for each parallel task.
            search_terms: String with space separated search terms. Both will occur in each result.
            start_date: Date in form YYYY_MM_DD. Articles from this day are included in the results.
            end_date: Date in form YYYY_MM_DD. Articles from this day are included in results.
        """
        base_url = "https://www.nytimes.com/search?dropmab=true&sort=oldest&types=article"
        full_search_query = f"{search_terms}" # Unique
        url = f"{base_url}&{full_search_query.replace(' ', '%20')}"

        links = [] # Define as Fallback if necessary

        # Wait for the page to load (adjust the timeout as needed)
        failure_counter = 0
        success = False
        while (not success) and (failure_counter < retry_count):
            try:
                self._render_complete_results(driver, url, override_overflow=override_overflow) 
                soup = BeautifulSoup(driver.page_source, features="lxml")
                links = self._extract_links_from_searpage(soup)
                links = links

                amountspan = soup.find("span", class_="search-summary__query")
                if not amountspan:
                    raise TimeoutException(f"{search_terms} : Page didn't load correctly.")   
                amount = int(amountspan.text.split(" ")[1])
                if len(links) < amount:
                    logging.warning(f"Retrieved only {len(links)} of {amount} search results. Incomplete.")
                    self.link_retrieval_error_log.append((search_terms,"INCOMPLETE", url))
                links = list(links)
                logging.debug(f"{len(links)} article links found. Expected: {amount}. Unique: {len(links)}")
                success = True
            except OverflowError:
                # Pass on with better info
                raise OverflowError(f"Overflow.")
            except TimeoutException: 
                # Timing out means that none of the above conditions was met. 
                # This could indicate a server rejection due to spam. 
                # We can not check as we are forced to use selenium.
                failure_counter += 1
                if "403" in str(driver.page_source): 
                    logging.debug("Unauthorized detected. Going to sleep...")
                # Escalating wait time to try and trick spam rage
                    sleep(2 ** (failure_counter + 1))
                logging.debug(f"Timed out for the {failure_counter} th time.")
            
                if failure_counter >= 5:
                    self.link_retrieval_error_log.append((search_terms, "FAILED", url))
                    raise TimeoutException(f"failed {failure_counter} times. giving up on this time range.")
        if override_overflow:
            self.link_retrieval_error_log.append((search_terms,"OVERFLOW", url))

        
        return links

    def collect_article_links(self, search_terms):
        opitons = Options()
        opitons.add_argument("--headless")
        driver = Chrome(opitons)
        driver.implicitly_wait(self.anti_detection_wait)
        links = self._get_articles_scrolling(driver, search_terms)
        self.links = set(links)
        return self.links
    
    def get_and_prepare_article(self, url):
        logging.debug(f"Requesting {url}")
        sleep(self.anti_detection_wait)
        resp = requests.get(url)
        if resp.status_code == 200:
            html = resp.content
            soup = BeautifulSoup(html, features="lxml")
            title = soup.title.text

            date_div = soup.find("time", class_="css-1z1nqv e16638kd0")
            if date_div:
                date_text = date_div.text
                date = datetime.strptime(date_text, "%b. %d, %Y")
                date_formatted = date.strftime("%Y-%m-%d") # This will allow polars translation through cast.
            else: date_formatted = None
            paragraphs = soup.find_all("p", class_="css-at9mc1 evys1bk0")
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
            loaded_links = self.articles.select(["url", "paragraphs_text"]).filter(pl.col("url").is_in(links) & pl.col("paragraphs_text").is_not_null()).select("url").to_numpy().astype(str).T[0].tolist()
            links = list(set(links) - set(loaded_links))

        res_generator = parallel(
            delayed(self.get_and_prepare_article)(url=url) for url in links
        )

        treated_counter = 0
        for i in tqdm(res_generator, total=len(self.links)):
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
        self.link_retrieval_error_log = []
        logging.info("Correcting unretrieved links...")
        for start_date, end_date, search_terms, err_type, url in tqdm(error_log):
            if err_type in ["INCOMPLETE", "FAILED"]:
                try:
                    opitons = Options()
                    opitons.add_argument("--headless")
                    driver = Chrome(opitons)
                    driver.implicitly_wait(self.anti_detection_wait)
                    logging.debug(f"Correcting {start_date} - {end_date} : {search_terms}")
                    self.links.extend(self._get_articles_scrolling(driver, search_terms, start_date, end_date, override_overflow=True))
                    driver.close()
                except TimeoutException as e:
                    logging.warning(e.msg)
            if err_type == "OVERFLOW": # Can't treat overflows that have not been treated.
                self.link_retrieval_error_log.append((start_date, end_date, search_terms, err_type, url))
        self.links = list(set(self.links))

    def do_scrape(self, search_terms, start_date, end_date, output_path):
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
    logging.basicConfig(level=logging.DEBUG)
    crawler = NYTScraper(num_workers=4, wait=0)
    crawler.collect_article_links("qatar fifa")
    articles = crawler.do_scrape("qatar fifa", "2010-01-01", "2024-01-31", "data/jazeera_long")