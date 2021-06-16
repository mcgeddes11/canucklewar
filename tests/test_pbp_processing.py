from tests import TESTS_ROOT, DATA_PATH
import pytest
from utils.build_utils import save_data, load_data
import os
from preprocessing.pbp_processing import assign_shifts
import numpy


def test_pbp_shift_assignment():
    pbp_data = load_data(os.path.join(DATA_PATH, "pbp.csv"))
    output = assign_shifts(pbp_data)
    assert(numpy.all(numpy.equal(output["shift_id"].values[0:5], numpy.array([1,1,1,2,3]))))





