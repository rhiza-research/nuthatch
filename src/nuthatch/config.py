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
import inspect

import logging
logger = logging.getLogger(__name__)


dynamic_parameters = {}
global_parameters = {}

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
            break

        if current_directory.joinpath('nuthatch.toml').exists() :
            config_file = current_directory.joinpath('nuthatch.toml')
            break

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


def set_parameter(parameter_value, parameter_name=None, location='root', backend=None):
    """A decorator to register a function as a dynamic parameter.

    Args:
        parameter_name (str): The name of the parameter.
        location (str, optional): The location to register the parameter. One of 'local', 'root', or 'mirror'
        backend (str, optional): The backend to register the parameter.
        secret (bool): whether the parameter is secret
    """
    # Get calling module
    caller_frame = inspect.stack()[1]
    module = inspect.getmodule(caller_frame.frame)
    if hasattr(module, '__name__'):
        module = module.__name__.partition('.')[0]

    logger.debug(f"Caller module {module}")
    if module not in global_parameters:
        global_parameters[module] = {}

    module_parameters = global_parameters[module]

    if not parameter_name:
        if not isinstance(parameter_value, dict):
            raise ValueError("If a parameter name is not provided, parameters must be passed as key/value pairs.")

    if parameter_name and location and backend:
        if location not in module_parameters:
            module_parameters[location] = {}
        if backend not in module_parameters[location]:
            module_parameters[location][backend] = {}
        module_parameters[location][backend][parameter_name] = parameter_value
    elif parameter_name and location:
        if location not in module_parameters:
            module_parameters[location] = {}
        module_parameters[location][parameter_name] = parameter_value
    elif location and backend:
        if location not in module_parameters:
            module_parameters[location] = {}
        if backend not in module_parameters[location]:
            module_parameters[location][backend] = {}
        module_parameters[location][backend].update(parameter_value)
    elif location:
        if location not in module_parameters:
            module_parameters[location] = {}
        module_parameters[location].update(parameter_value)
    else:
        for key in parameter_value:
            if key != 'root' and key != 'local' and key != 'mirror':
                raise ValueError("Parameter value dictionaries must have top level keys of root, local, or mirror if location is not passed")
        module_parameters.update(parameter_value)


def config_parameter(parameter_name, location='root', backend=None, secret=False):
    """A decorator to register a function as a dynamic parameter.

    Args:
        parameter_name (str): The name of the parameter.
        location (str, optional): The location to register the parameter. One of 'local', 'root', or 'mirror'
        backend (str, optional): The backend to register the parameter.
        secret (bool): whether the parameter is secret
    """
    def decorator(function):
        # Get the module object associated with the caller's frame
        caller_frame = inspect.stack()[1]
        module = inspect.getmodule(caller_frame.frame)
        if hasattr(module, '__name__'):
            module = module.__name__.partition('.')[0]
        logger.debug(f"Caller module {module}")
        if module not in dynamic_parameters:
            dynamic_parameters[module] = {}

        dynamic_module_parameters = dynamic_parameters[module]

        if location not in dynamic_module_parameters:
            dynamic_module_parameters[location] = {}
        if backend and backend not in dynamic_module_parameters[location]:
            dynamic_module_parameters[location][backend] = {}

        if backend:
            dynamic_module_parameters[location][backend][parameter_name] = (function, secret)
        else:
            dynamic_module_parameters[location][parameter_name] = (function, secret)
    return decorator

def extract_set_params(location, requested_parameters, backend_name, wrapped_module):
    location_params = {}
    logger.debug(f"Extracting parameters from global parameters {global_parameters} for module {wrapped_module}.")

    if wrapped_module in global_parameters:
        module_parameters = global_parameters[wrapped_module]
    else:
        return location_params

    logger.debug(module_parameters)
    logger.debug(location)
    if location in module_parameters:
        location_params.update(module_parameters[location])

    logger.debug(location_params)

    backend_specific_params = {}
    if location in module_parameters and backend_name and backend_name in module_parameters[location]:
        backend_specific_params.update(module_parameters[location][backend_name])

    merged_config = backend_specific_params | location_params
    filtered_config = {k: merged_config[k] for k in merged_config if k in requested_parameters}
    return filtered_config

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

def extract_dynamic_params(location, requested_parameters, backend_name, mask_secrets=False, wrapped_module=None):
    # Now call all the relevant config registrations and add them
    logger.debug(f"Extracting dynamic parameters from {dynamic_parameters} for module {wrapped_module}.")
    if wrapped_module in dynamic_parameters:
        module_dynamic_parameters = dynamic_parameters[wrapped_module]
    else:
        return {}

    location_params = {}
    for p in requested_parameters:
        if location in module_dynamic_parameters:
            # If a dynamic parameter has been set call it and use it to override any static config
            if backend_name in module_dynamic_parameters[location] and p in module_dynamic_parameters[location][backend_name]:
                secret = module_dynamic_parameters[location][backend_name][p][1]
                param = module_dynamic_parameters[location][backend_name][p][0]()
                if secret and mask_secrets:
                    location_params[p] = '*'*len(param)
                else:
                    location_params[p] = param
            elif p in module_dynamic_parameters[location]:
                secret = module_dynamic_parameters[location][p][1]
                param = module_dynamic_parameters[location][p][0]()
                if secret and mask_secrets:
                    location_params[p] = '*'*len(param)
                else:
                    location_params[p] = param

    return location_params


def get_config(location='root', requested_parameters=[], backend_name=None, mask_secrets=False, config_from=None, wrapped_module=None):
    """Get the config for a given location and backend.

    Args:
        location (str, optional): The location to get the config for. One of 'local', 'root', or 'mirror'
        requested_parameters (list, optional): The parameters to get the config for.
        backend_name (str, optional): The backend to get the config for.
        mask_secrets (bool): Whether to hide secret parameters. Useful for printing.
    """
    # If we are root or local we must try and find a config from either our current project or a global project
    if location == 'root' or location == 'local':
        # For Local and root caches our goals:
        # Find a config from the project we are currently running in!
        # Don't let imported projects overwrite our config with their parameters
        # Still enable some environments where python-only packaging requires a package install
        # for all parameter setting
        # To enable this ->
        #   -> Only allow setting dynamic parameters for functions in the same module as the dynamic param call
        #   ->    This prevents most mishaps, but still doesn't enable you to call external_module.func but save it in your cache
        #   -> Only allow overwriting root if (1) there is not current config (2) there is not caller nuthatch.toml

        # Scenarios
        # (1) We installed a package in a repl or a blank environment - no local config, in site-package, valid nuthatch.toml
        #       - never overwrite root.
        # (2) We installed a package in another project using nuthatch - local config, in site-package, valid nuthatch.toml
        #       - never overwrite root
        # (3) We install a package in a cluster - no local config, in site-package, no nuthatch.toml
        #       - overwrite root at low priority
        # (4) We are running from our own package - local config, not in site-package, maybe nuthatch.toml
        #       - overwrie all parameters with dynamics

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
        caller_config_file = get_callers_pyproject(wrapped_module.__file__)
        logger.debug(f"Caller pyrpoject path is: {caller_config_file}")

        if current_config_file:
            with open(current_config_file, "rb") as f:
                current_config = tomllib.load(f)

            current_params = extract_params(current_config, location, requested_parameters, backend_name)
            if current_params:
                final_config.update(current_params)

        set_params = extract_set_params(location, requested_parameters, backend_name, wrapped_module)
        dynamic_params = extract_dynamic_params(location, requested_parameters, backend_name, mask_secrets, wrapped_module)
        if hasattr(wrapped_module, '__file__') and ('site-packages' in wrapped_module.__file__ or 'dist-packages' in wrapped_module.__file__):
            if not current_config_file and not caller_config_file:
                # This is the situation to allow low priority overwriting
                set_params |= dynamic_params
                set_params |= final_config
                final_config = set_params
        else:
            # If we aren't in site package we do a high priority dynamic update to allow runtime config changes in editable packages
            final_config |= set_params
            final_config |= dynamic_params

        if hasattr(wrapped_module, '__name__'):
            wrapped_module = wrapped_module.__name__.partition('.')[0]


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
        caller_config_file = get_callers_pyproject(wrapped_module.__file__)
        logger.debug(f"Callers pyrpoject path is: {caller_config_file}")

        # Now get any mirrors from the current project
        current_config_file = get_current_pyproject()
        logger.debug(f"Current pyrpoject path is: {current_config_file}")

        if hasattr(wrapped_module, '__name__'):
            wrapped_module = wrapped_module.__name__.partition('.')[0]

        if current_config_file:
            with open(current_config_file, "rb") as f:
                current_config = tomllib.load(f)

            current_params = extract_params(current_config, location, requested_parameters, backend_name)


            # If the current and caller are the same update the current with dynamics (since it is the one that is imported)
            if current_config_file == caller_config_file:
                set_params = extract_set_params(location, requested_parameters, backend_name, wrapped_module)
                current_params.update(set_params)
                dynamic_params = extract_dynamic_params(location, requested_parameters, backend_name, mask_secrets, wrapped_module)
                current_params.update(dynamic_params)

            if current_params:
                mirror_configs['mirror'] = current_params

        if caller_config_file and caller_config_file != current_config_file:
            # For the caller extract both mirror and root locations as mirros and update with dynamic parameters
            with open(caller_config_file, "rb") as f:
                caller_config = tomllib.load(f)

            caller_root_params = extract_params(caller_config, 'root', requested_parameters, backend_name)
            set_params = extract_set_params('root', requested_parameters, backend_name, wrapped_module)
            caller_root_params.update(set_params)
            caller_dynamic_params = extract_dynamic_params('root', requested_parameters, backend_name, mask_secrets, wrapped_module)
            caller_root_params.update(caller_dynamic_params)

            if caller_root_params:
                mirror_configs[wrapped_module + ' root'] = caller_root_params

            caller_mirror_params = extract_params(caller_config, 'mirror', requested_parameters, backend_name)
            set_params = extract_set_params('mirror', requested_parameters, backend_name, wrapped_module)
            caller_mirror_params.update(set_params)
            caller_dynamic_params = extract_dynamic_params('mirror', requested_parameters, backend_name, mask_secrets, wrapped_module)
            caller_mirror_params.update(caller_dynamic_params)

            if caller_mirror_params:
                mirror_configs[wrapped_module + ' mirror'] = caller_mirror_params

        if len(mirror_configs) > 0:
            if config_from:
                return mirror_configs[config_from]
            else:
                return mirror_configs
        else:
            return None
    else:
        raise ValueError("Location must be one of 'root', 'local', or 'mirror'.")

