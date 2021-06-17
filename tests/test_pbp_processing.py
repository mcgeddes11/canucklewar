from tests import TESTS_ROOT, DATA_PATH
import pytest
from utils.build_utils import save_data, load_data
import os
from preprocessing.pbp_processing import assign_shifts, rapm_feature_engineering, absolute_seconds_elapsed
import numpy


def test_pbp_shift_assignment():
    pbp_data = load_data(os.path.join(DATA_PATH, "pbp.csv"))
    output = assign_shifts(pbp_data)
    assert(numpy.all(numpy.equal(output["shift_id"].values[0:13], numpy.array([1,1,1,1,2,3,3,3,3,3,4,4,5]))))

def test_rapm_feature_engineering():
    pbp_data = load_data(os.path.join(DATA_PATH, "pbp.csv"))
    pbp_data = assign_shifts(pbp_data)
    pbp_data = absolute_seconds_elapsed(pbp_data)
    player_data = load_data(os.path.join(DATA_PATH, "players.csv"))
    schedule_data = load_data(os.path.join(DATA_PATH, "schedule.csv"))
    output = rapm_feature_engineering(pbp_data, player_data, schedule_data)


