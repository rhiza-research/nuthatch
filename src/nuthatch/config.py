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
        if current_directory.joinpath('pyproject.toml').exists() :
            config_file = current_directory.joinpath('pyproject.toml')

        if current_directory.joinpath('nuthatch.toml').exists() :
            config_file = current_directory.joinpath('nuthatch.toml')

        current_directory = current_directory.parent

    return config_file

def get_callers_pyproject(wrapped_path):
    logger.debug(f"Looking for caller's config starting at: {wrapped_path}")
    return find_pyproject(wrapped_path)

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

def extract_dynamic_params(existing_params, location, requested_parameters, backend_name, mask_secrets=False):
    # Now call all the relevant config registrations and add them
    for p in requested_parameters:
        if location in dynamic_parameters:
            # If a dynamic parameter has been set call it and use it to override any static config
            if backend_name in dynamic_parameters[location] and p in dynamic_parameters[location][backend_name]:
                secret = dynamic_parameters[location][backend_name][p][1]
                param = dynamic_parameters[location][backend_name][p][0]()
                if secret and mask_secrets:
                    existing_params[p] = '*'*len(param)
                else:
                    existing_params[p] = param
            elif p in dynamic_parameters[location]:
                secret = dynamic_parameters[location][p][1]
                param = dynamic_parameters[location][p][0]()
                if secret and mask_secrets:
                    existing_params[p] = '*'*len(param)
                else:
                    existing_params[p] = param

    return existing_params


def get_config(location='root', requested_parameters=[], backend_name=None, mask_secrets=False, config_from=None, wrapped_path=None):
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

        final_config = extract_dynamic_params(final_config, location, requested_parameters, backend_name, mask_secrets)

        return final_config

    elif location == "mirror":
        # Mirrors require a different approach, because we are trying to build a set of them
        mirror_configs = {}

        # First get an global mirrors
        global_config_file = get_global_config()
        if global_config_file:
            logger.debug(f"Found global config file: {global_config_file}")
            with open(global_config_file, "rb") as f:
                global_config = tomllib.load(f)

            global_params = extract_params(global_config, location, requested_parameters, backend_name)

            if global_params:
                mirror_configs['global'] = global_params

        # Get the caller's config to check if it's the same
        caller_config_file = get_callers_pyproject(wrapped_path)
        logger.debug(f"Callers pyrpoject path is: {caller_config_file}")

        # Now get any mirrors from the current project
        current_config_file = get_current_pyproject()
        logger.debug(f"Current pyrpoject path is: {current_config_file}")

        if current_config_file:
            with open(current_config_file, "rb") as f:
                current_config = tomllib.load(f)

            current_params = extract_params(current_config, location, requested_parameters, backend_name)

            # If the current and caller are the same update the current with dynamics (since it is the one that is imported)
            if current_config_file == caller_config_file:
                current_params = extract_dynamic_params(current_params, location, requested_parameters, backend_name, mask_secrets)

            if current_params:
                mirror_configs['current'] = current_params

        if caller_config_file and caller_config_file != current_config_file:
            # For the caller extract both mirror and root locations as mirros and update with dynamic parameters
            with open(caller_config_file, "rb") as f:
                caller_config = tomllib.load(f)

            caller_root_params = extract_params(caller_config, 'root', requested_parameters, backend_name)
            caller_root_params = extract_dynamic_params(caller_root_params, 'root', requested_parameters, backend_name, mask_secrets)

            if caller_root_params:
                mirror_configs['caller_root'] = caller_root_params

            caller_mirror_params = extract_params(caller_config, 'mirror', requested_parameters, backend_name)
            caller_mirror_params = extract_dynamic_params(caller_mirror_params, 'mirror', requested_parameters, backend_name, mask_secrets)

            if caller_mirror_params:
                mirror_configs['caller_mirror'] = caller_mirror_params

        if len(mirror_configs) > 0:
            if config_from:
                return mirror_configs[config_from]
            else:
                return mirror_configs
        else:
            return None
    else:
        raise ValueError("Location must be one of 'root', 'local', or 'mirror'.")

