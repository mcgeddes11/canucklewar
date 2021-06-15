import hockey_scraper
from scraping.scraping_functions import get_franchises, get_teams, get_schedule, get_players

if __name__ == '__main__':

    data = get_players(season_id="20172018")
    print(data)

    # scraped_data = hockey_scraper.scrape_games([2014020001], False, data_format='Pandas')
    # print('data scraped')
    # scraped_data.to_csv("foo.csv")
    #
    # pbp_df = scraped_data




