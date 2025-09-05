"""
The config module is used to get the config for a given location and backend.

For instance you can set parameters for the `root` location, a `local` local
for lower latency caching or a number of `mirror` locations, and you can
set specific parameters for specific backends (i.e. terracotta and zarr
could leverage different filesystems if specified)
"""
from pathlib import Path
import os
import tomllib

dynamic_parameters = {}

def config_parameter(parameter_name, location='root', backend=None, secret=False):
    """A decorator to register a function as a dynamic parameter.

    Args:
        parameter_name (str): The name of the parameter.
        location (str, optional): The location to register the parameter. One of 'local', 'root', or 'mirror'
        backend (str, optional): The backend to register the parameter.
        secret (bool): whether the parameter is secret
    """
    def decorator(function):
        if location not in dynamic_parameters:
            dynamic_parameters[location] = {}
        if backend and backend not in dynamic_parameters[location]:
            dynamic_parameters[location][backend] = {}

        if backend:
            dynamic_parameters[location][backend][parameter_name] = (function, secret)
        else:
            dynamic_parameters[location][parameter_name] = (function, secret)
    return decorator

def _is_fs_root(p):
    """Check if a path is the root of a filesystem."""
    return os.path.splitdrive(str(p))[1] == os.sep

def get_config(location='root', requested_parameters=[], backend_name=None, mask_secrets=False):
    """Get the config for a given location and backend.

    Args:
        location (str, optional): The location to get the config for. One of 'local', 'root', or 'mirror'
        requested_parameters (list, optional): The parameters to get the config for.
        backend_name (str, optional): The backend to get the config for.
        mask_secrets (bool): Whether to hide secret parameters. Useful for printing.
    """
    #Find pyproject.toml
    current_directory = Path.cwd()

    config_file = None
    while not _is_fs_root(current_directory):
        if current_directory.joinpath('pyproject.toml').exists():
            config_file = current_directory.joinpath('pyproject.toml')

        current_directory = current_directory.parent

    #TODO: enable ini and environment variable configuration

    with open(config_file, "rb") as f:
        config = tomllib.load(f)

    # Assume all parameters set without a location specified apply to root and all backends
    location_params = {}
    if location == 'root':
        location_params = config['tool']['nuthatch']
        if location in config['tool']['nuthatch']:
            location_params.update(config['tool']['nuthatch'][location])
    else:
        if location in config['tool']['nuthatch']:
            location_params = config['tool']['nuthatch'][location]

    if backend_name and backend_name in location_params:
        backend_specific_params = config['tool']['nuthatch'][location][backend_name]
    else:
        backend_specific_params = {}

    # Merge the two together
    merged_config = backend_specific_params | location_params

    filtered_config = {k: merged_config[k] for k in merged_config if k in requested_parameters}

    # Now call all the relevant config registrations and add them
    for p in requested_parameters:
        if location in dynamic_parameters:
            # If a dynamic parameter has been set call it and use it to override any static config
            if backend_name in dynamic_parameters[location] and p in dynamic_parameters[location][backend_name]:
                secret = dynamic_parameters[location][backend_name][p][1]
                param = dynamic_parameters[location][backend_name][p][0]()
                if secret and mask_secrets:
                    filtered_config[p] = '*'*len(param)
                else:
                    filtered_config[p] = param
            elif p in dynamic_parameters[location]:
                secret = dynamic_parameters[location][p][1]
                param = dynamic_parameters[location][p][0]()
                if secret and mask_secrets:
                    filtered_config[p] = '*'*len(param)
                else:
                    filtered_config[p] = param


    return filtered_config
