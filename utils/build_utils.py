
import os
import pickle
import json
import yaml
import numpy
import pandas
import logging
from datetime import datetime, timedelta


LOGGER = logging.getLogger('luigi-interface')
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))


def utc_now(days = 0, hours = 0, minutes = 0, seconds = 0):
    """Returns a timestamp of the current UTC time with optional offset (if specified)."""
    current_time = datetime.utcnow()
    delta = timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    offset_time = current_time + delta
    time_string = offset_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    return time_string


def env_vars_to_python_types(input_val):
    """Returns corresponding numerics based on contents of string variables"""
    if isinstance(input_val, str):
        if input_val in ['true', 'True']:
            return True
        elif input_val in ['false', 'False']:
            return False
        try:
            return int(input_val)
        except ValueError:
            try:
                return float(input_val)
            except ValueError:
                return input_val
    else:
        return input_val


def override_config_from_environment(config_dict):
    """Helper function to override config with environment variables of the same name"""

    for key in config_dict.keys():
        if key in os.environ:
            env_var = os.environ[key]
            config_dict[key] = env_vars_to_python_types(env_var)

    return config_dict


def absolute_path_from_project_root(rel_path_from_project_root):
    """Return the absolute path of an item with a relative path from the project root"""
    return os.path.join(PROJECT_ROOT, rel_path_from_project_root)


def create_folder(filename):
    """Utility function to create folder for output"""
    absolute_dirname = absolute_path_from_project_root(filename)

    last_element = os.path.basename(absolute_dirname)
    if '.' in last_element: # not a folder
        absolute_dirname = os.path.dirname(absolute_dirname)

    LOGGER.info('Creating ' + absolute_dirname)
    if not os.path.exists(absolute_dirname):
        os.makedirs(absolute_dirname)


def load_data(pathname, **kwargs):
    absolute_pathname = absolute_path_from_project_root(pathname)
    if pathname.endswith(".pickle"):
        with open(absolute_pathname,"rb") as f:
            d = pickle.load(f, **kwargs)
    elif pathname.endswith(".csv"):
        d = pandas.read_csv(absolute_pathname, **kwargs)
    elif pathname.endswith(".json"):
        with open(absolute_pathname, 'r') as f:
            d = json.load(f, **kwargs)
    elif pathname.endswith(".txt"):
        with open(absolute_pathname, 'r') as f:
            d = f.read()
    else:
        extension = os.path.basename(pathname).split(".")[-1]
        raise Exception('Unrecognized file extension: "{}"'.format(extension))
    return d


def save_data(data_object, pathname, protocol=2, index=False, date_format = "%d%b%Y %H:%M"):
    absolute_pathname = absolute_path_from_project_root(pathname)
    if pathname.endswith(".pickle"):
        with open(absolute_pathname,"wb") as f:
            pickle.dump(data_object,f,protocol=protocol)
    elif pathname.endswith(".csv") and isinstance(data_object, pandas.DataFrame):
        data_object.to_csv(absolute_pathname, index=index, date_format=date_format)
    elif pathname.endswith(".json") and (isinstance(data_object, dict) or (isinstance(data_object,list) and numpy.all([isinstance(x,dict) for x in data_object]))):
        with open(absolute_pathname, 'w') as f:
            f.write(json.dumps(data_object))
    elif pathname.endswith(".txt") and isinstance(data_object, str):
        with open(absolute_pathname, 'w') as f:
            f.write(data_object)
    elif pathname.endswith(".yaml") and isinstance(data_object,dict):
        with open(absolute_pathname, 'w') as file:
            yaml.dump(data_object,file)
    else:
        extension = os.path.basename(pathname).split(".")[-1]
        object_type = type(data_object)
        raise Exception('Unrecognized file extension "{}" and type "{}"'.format(extension, object_type))


def load_yaml_config(filename):
    full_path = absolute_path_from_project_root(filename)
    with open(full_path, 'r') as file:
        return yaml.load(file)


def parse_const(module_name):
    output = {}
    for const in dir(module_name):
        if const[:2] != '__':
            output[const] = getattr(module_name,const)
    return output


def reconstitute_config(filename):
    if filename.endswith('.yaml') or filename.endswith('.json'):
        return load_data(filename)
    else:
        raise Exception('config has to be read from either .yaml or .json, .{} format given'.format(filename.split('.')[-1]))


def get_cols_rename_mapping(input_yaml_path):
    raw_mapping = load_yaml_config(input_yaml_path)
    output = raw_mapping['mapping']
    return output


def prepare_dict_for_json(input_dict):
    output_dict = {}
    for key,value in input_dict.items():
        new_key = key
        new_value = value
        if not isinstance(key,str) and not isinstance(key,int) and not isinstance(key,float) and not isinstance(key,bool):
            new_key = str(key)
        if isinstance(value,dict):
            new_value = prepare_dict_for_json(value)
        output_dict[new_key] = new_value
    return output_dict


def generate_season_id_list(first_season_id: str, last_season_id: str) -> [str]:
    first_year = int(first_season_id[0:4])
    last_year = int(last_season_id[4:8])
    if first_year > last_year:
        raise Exception("First season must be before last season")
    year_range = range(first_year, last_year+1,1)
    season_id_list = []
    for ix, val in enumerate(year_range):
        if ix > 0:
            season_id_list.append(str(year_range[ix-1]) + str(val))

    return season_id_list
