import requests
from datetime import datetime

BASE_API_URL = "https://statsapi.web.nhl.com/api/v1/"

def get_schedule(season_id: str) -> [dict]:
    # Get schedule for a seaons. season_id should be of form "20182019" for 18/19 season
    url = BASE_API_URL + "schedule?season=" + season_id
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    # TODO: unpack all games to a flat structure
    return data["dates"]

def get_players(season_id: str) -> [dict]:
    # Get players for a season. Similar season_id as get_schedule.
    # Returns a dict, because we don't so much care who a player plays for, but that they exist
    teams = get_teams(include_rosters=True, season_id=season_id)
    # Get a list of players
    players = {}
    for team in teams:
        roster = team["roster"]["roster"]
        for player in roster:
            person = player["person"]
            player_id = person["id"]
            players[player_id] = {"id": player_id,
                                  "full_name": person["fullName"],
                                  "position_code": player["position"]["code"],
                                  "position_type": player["position"]["type"]
                                  }
            if "jerseyNumber" in player.keys():
                players[player_id]["jersey_number"] = player["jerseyNumber"]
            else:
                players[player_id]["jersey_number"] = None
    return players

def get_teams(include_rosters=False, season_id=None) -> [dict]:
    # Returns a list of team objects
    if include_rosters and season_id is not None:
        url = BASE_API_URL + "teams?expand=team.roster&season=" + season_id
    else:
        url = BASE_API_URL + "teams"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data["teams"]

def get_franchises() -> [dict]:
    # Returns a list of franchise objects
    url = BASE_API_URL + "franchises"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data["franchises"]

