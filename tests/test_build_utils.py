import pytest
from utils.build_utils import generate_season_id_list

def test_generate_season_id_list():
    first_season_id = "20122013"
    last_season_id = "20172018"
    season_list = generate_season_id_list(first_season_id, last_season_id)
    assert(season_list == ["20122013","20132014","20142015","20152016","20162017","20172018"])

