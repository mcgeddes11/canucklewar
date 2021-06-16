import hockey_scraper as hs
data = hs.scrape_games([2014020001, 2015020001], True, data_format='pandas')
print("scraped")
data["pbp"].to_csv("pbp.csv")
