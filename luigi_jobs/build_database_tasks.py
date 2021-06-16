from luigi_jobs.luigi_extensions import ConfigurableTask
import luigi
import pandas
import os
from scraping.scraping_functions import get_schedule, get_players, get_teams, get_franchises
from utils.build_utils import save_data, load_data, generate_season_id_list
from time import sleep
from random import randint
import hockey_scraper

class GetScheduleData(ConfigurableTask):

    def run(self):
        season_id_list = generate_season_id_list(self.job["first_season"], self.job["last_season"])
        schedule_data_all = []
        for season in season_id_list:
            schedule_data = get_schedule(season)
            schedule_data_all.append(schedule_data)
            sleep(randint(1,8))

        schedule_dataframe = self.json_to_dataframe_schedule(schedule_data_all)
        save_data(schedule_dataframe, self.output()["ScheduleData"].path)

    def output(self):
        return {"ScheduleData": luigi.LocalTarget(os.path.join(self.job["data_repository"], "schedule.csv"))}

    def json_to_dataframe_schedule(self,json_data_list):
        df_list = []
        for season_schedule in json_data_list:
            for season_dates in season_schedule:
                for game in season_dates["games"]:
                    game ={"game_id": game["gamePk"],
                           "season": game["season"],
                           "game_date": game["gameDate"],
                           "game_type": game["gameType"],
                           "away_team_name": game["teams"]["away"]["team"]["name"],
                           "away_team_id": game["teams"]["away"]["team"]["id"],
                           "home_team_name": game["teams"]["home"]["team"]["name"],
                           "home_team_id": game["teams"]["home"]["team"]["id"],
                           "venue_name": game["venue"]["name"],
                           "venue_id": game["venue"]["id"] if "id" in game["venue"].keys() else None}
                    df_list.append(game)
        df = pandas.DataFrame.from_records(df_list)
        return df

class GetTeamsData(ConfigurableTask):

    def run(self):
        pass

    def output(self):
        pass


class GetPlayerData(ConfigurableTask):

    def run(self):
        season_id_list = generate_season_id_list(self.job["first_season"], self.job["last_season"])
        player_data = {}
        for season in season_id_list:
            new_data = get_players(season)
            player_data.update(new_data)
            sleep(randint(1, 8))
        df_players = pandas.DataFrame.from_records(list(player_data.values()))
        save_data(df_players, self.output()["PlayerData"].path)


    def output(self):
        return {"PlayerData": luigi.LocalTarget(os.path.join(self.job["data_repository"], "players.csv"))}

class GetPlayByPlayData(ConfigurableTask):

    def run(self):

        pass

    def output(self):
        pass