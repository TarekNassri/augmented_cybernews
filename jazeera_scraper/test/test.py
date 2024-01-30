from jazeera_scraper import JazeeraScraper

def test_error_correction():
    scraper = JazeeraScraper()
    JazeeraScraper.error_log = [
        "2020-01-01","2020-01-31","qatar world cup","INCOMPLETE","https://www.aljazeera.com/search/qatar%20world%20cup%20after:2020-01-01%20before:2020-01-31"
    ]
    scraper.correct_unretrieved_links()
    assert len(scraper.error_log) == 0