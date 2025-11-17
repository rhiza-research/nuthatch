"""
The config module is used to get the config for a given location and backend.

For instance you can set parameters for the `root` location, a `local` local
for lower latency caching or a number of `mirror` locations, and you can
set specific parameters for specific backends (i.e. terracotta and zarr
could leverage different filesystems if specified)
"""
from pathlib import Path
import os
import inspect
import tomllib

import logging
logger = logging.getLogger(__name__)


dynamic_parameters = {}

def _is_fs_root(p):
    """Check if a path is the root of a filesystem."""
    return os.path.splitdrive(str(p))[1] == os.sep

def find_pyproject(start_dir):

    if isinstance(start_dir, str):
        start_dir = Path(start_dir)

    current_directory = start_dir

    config_file = None
    while not _is_fs_root(current_directory):
        if current_directory.joinpath('pyproject.toml').exists():
            config_file = current_directory.joinpath('pyproject.toml')

        current_directory = current_directory.parent

    return config_file

def get_callers_pyproject():

    # Get two frames back
    frame = inspect.currentframe().f_back.f_back

    # Get the module object from the caller's globals
    module = inspect.getmodule(frame)

    path = os.path.dirname(module.__file__)
    return find_pyproject(path)

def get_global_config():
    expanded = os.path.expanduser('~/nuthatch.toml')
    if os.path.exists(expanded):
        return expanded
    else:
        return None


def get_current_pyproject():
    current_directory = Path.cwd()
    return find_pyproject(current_directory)

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

def extract_params(config, location, requested_parameters, backend_name):
    # Assume all parameters set without a location specified apply to root and all backends
    if 'tool' not in config or 'nuthatch' not in config['tool']:
        return None

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

    return filtered_config


def get_config(location='root', requested_parameters=[], backend_name=None, mask_secrets=False):
    """Get the config for a given location and backend.

    Args:
        location (str, optional): The location to get the config for. One of 'local', 'root', or 'mirror'
        requested_parameters (list, optional): The parameters to get the config for.
        backend_name (str, optional): The backend to get the config for.
        mask_secrets (bool): Whether to hide secret parameters. Useful for printing.
    """
    # If we are root or local we must try and find a config from either our current project or a global project
    if location == 'root' or location == 'local':
        # Lowest priority - global
        final_config = {}
        global_config_file = get_global_config()
        if global_config_file:
            logger.debug(f"Found global config file: {global_config_file}")
            with open(global_config_file, "rb") as f:
                global_config = tomllib.load(f)

            global_params = extract_params(global_config, location, requested_parameters, backend_name)

            if global_params:
                final_config.update(global_params)

        current_config_file = get_current_pyproject()
        logger.debug(f"Current pyrpoject path is: {current_config_file}")

        if current_config_file:
            with open(current_config_file, "rb") as f:
                current_config = tomllib.load(f)

            current_params = extract_params(current_config, location, requested_parameters, backend_name)
            if current_params:
                final_config.update(current_params)

        # Now call all the relevant config registrations and add them
        for p in requested_parameters:
            if location in dynamic_parameters:
                # If a dynamic parameter has been set call it and use it to override any static config
                if backend_name in dynamic_parameters[location] and p in dynamic_parameters[location][backend_name]:
                    secret = dynamic_parameters[location][backend_name][p][1]
                    param = dynamic_parameters[location][backend_name][p][0]()
                    if secret and mask_secrets:
                        final_config[p] = '*'*len(param)
                    else:
                        final_config[p] = param
                elif p in dynamic_parameters[location]:
                    secret = dynamic_parameters[location][p][1]
                    param = dynamic_parameters[location][p][0]()
                    if secret and mask_secrets:
                        final_config[p] = '*'*len(param)
                    else:
                        final_config[p] = param

        return final_config

    elif location == "mirror":
        caller_config_file = get_callers_pyproject()
        logger.debug(f"Callers pyrpoject path is: {caller_config_file}")

        config_file = get_current_pyproject()
        logger.debug(f"Current pyrpoject path is: {config_file}")
    else:
        raise ValueError("Location must be one of 'root', 'local', or 'mirror'.")

