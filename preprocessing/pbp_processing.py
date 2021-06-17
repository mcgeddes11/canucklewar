import pandas
import numpy
from utils.build_utils import save_data, load_data

def count_shot_attempts_function(pdf_subgroup):
    return numpy.sum(pdf_subgroup["Event"].isin(["SHOT","MISS","BLOCK"]))

def absolute_seconds_elapsed(pbp_data):
    pbp_data["absolute_seconds_elapsed"] = ((pbp_data["Period"] - 1) * 20 * 60) + pbp_data["Seconds_Elapsed"]
    return pbp_data

def assign_shifts(pbp_data: pandas.DataFrame) -> pandas.DataFrame:
    # Create sets of players
    cols = ["homePlayer1_id","homePlayer2_id","homePlayer3_id","homePlayer4_id","homePlayer5_id","homePlayer6_id",\
            "awayPlayer1_id","awayPlayer2_id","awayPlayer3_id","awayPlayer4_id","awayPlayer5_id","awayPlayer6_id"]
    player_sets = [set(x[1:-1]) for x in pbp_data[cols].itertuples()]
    # Define shifts as any time the set of 12 players on the ice changes
    shifts = []
    shift_num = 1
    # add first row
    shifts.append(shift_num)
    for ix, p_set in enumerate(player_sets):
        if ix > 0:
            if p_set == player_sets[ix-1]:
                shifts.append(shift_num)
            else:
                shift_num += 1
                shifts.append(shift_num)
    pbp_data["shift_id"] = shifts
    return pbp_data

def rapm_feature_engineering(pbp_data: pandas.DataFrame, player_data: pandas.DataFrame, schedule_data: pandas.DataFrame) -> pandas.DataFrame:
    player_data = player_data.sort_values(by="player_id", ascending=True).reset_index(drop=True)
    player_data.index = player_data["player_id"].values
    player_dict = player_data.to_dict("index")
    # Get the headers for the player-on-ice binaries
    col_headers = list(player_dict.keys())
    # Get shift lengths
    shift_length = pbp_data.groupby("shift_id").agg({"absolute_seconds_elapsed": ["min", "max"]}).reset_index()
    # Get shot counts
    shot_counts = pbp_data.groupby(["shift_id", "Home_Zone"]).apply(count_shot_attempts_function).reset_index()






