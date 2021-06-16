import pandas

def assign_shifts(pbp_data: pandas.DataFrame):
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

