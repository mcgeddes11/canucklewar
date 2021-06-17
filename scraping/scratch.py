from utils.build_utils import save_data
import hockey_scraper as hs
data = hs.scrape_games([2015020001], True, data_format='pandas')
print("scraped")
save_data(data["pbp"], "pbp.csv")
