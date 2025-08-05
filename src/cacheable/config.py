from cacheable.backend import CacheableBackend
from pathlib import Path
import os
import tomllib

dynamic_parameters = {}

def cacheable_config_parameter(parameter_name, location='base', backend=None):
    def decorator(function):
        if location not in dynamic_parameters:
            dynamic_parameters[location] = {}
        if backend and backend not in dynamic_parameters[location]:
            dynamic_parameters[location][backend] = {}

        if backend:
            dynamic_parameters[location][backend][parameter_name] = function
        else:
            dynamic_parameters[location][parameter_name] = function
    return decorator

def is_fs_root(p):
     return os.path.splitdrive(str(p))[1] == os.sep

def get_config(location='base', backend_class=CacheableBackend):

    #Find pyproject.toml or cacheable.ini
    current_directory = Path.cwd()

    config_file = None
    while not is_fs_root(current_directory):
        if current_directory.joinpath('pyproject.toml').exists():
            config_file = current_directory.joinpath('pyproject.toml')

        current_directory = current_directory.parent

    #TODO: enable ini and environment variable configuration

    with open(config_file, "rb") as f:
        config = tomllib.load(f)

    requested_parameters = backend_class.config_parameters
    backend_name = backend_class.backend_name

    location_params = config['tool']['cacheable'][location]

    if backend_name in location_params:
        backend_specific_params = config['tool']['cacheable'][location][backend_name]
    else:
        backend_specific_params = {}

    # Merge the two together
    merged_config = backend_specific_params | location_params

    filtered_config = {k: merged_config[k] for k in merged_config if k in requested_parameters}

    # Now call all the relevant config registrations and add them
    for p in requested_parameters:
        if location in dynamic_parameters:
            if backend_name in dynamic_parameters[location] and p in dynamic_parameters[location][backend_name]:
                filtered_config[p] = dynamic_parameters[location][backend_name][p]()
            elif p in dynamic_parameters[location]:
                filtered_config[p] = dynamic_parameters[location][p]()

    return filtered_config
