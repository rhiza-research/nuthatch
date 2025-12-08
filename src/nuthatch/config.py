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
import tomli_w
import inspect
import copy

import logging
import nuthatch
logger = logging.getLogger(__name__)


dynamic_parameters = {}

def set_global_skipped_filesystem(filesystem):
    config_file = os.path.expanduser('~/.nuthatch.toml')

    config_data = {}
    if os.path.exists(config_file):
        with open(config_file, 'rb') as f:
            config_data = tomllib.load(f)

    tool_data = config_data.setdefault('tool', {})
    nuthatch_data = tool_data.setdefault('nuthatch', {})
    skip_list = nuthatch_data.setdefault('skipped_filesystems', [])
    if filesystem in skip_list:
        return
    else:
        skip_list.append(filesystem)

    with open(config_file, "wb") as f:
        tomli_w.dump(config_data, f)


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

    if module not in dynamic_parameters:
        dynamic_parameters[module] = {}

    module_parameters = dynamic_parameters[module]

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
            if key != 'root' and key != 'local' and (not key.startswith('mirror')):
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


class NuthatchConfig:

    def _is_fs_root(self, p):
        """Check if a path is the root of a filesystem."""
        return os.path.splitdrive(str(p))[1] == os.sep

    def _find_nuthatch_config(self, start_dir):
        if isinstance(start_dir, str) and start_dir.endswith('nuthatch.toml') and os.path.exists(start_dir):
            return start_dir
        if isinstance(start_dir, str):
            start_dir = Path(start_dir)

        current_directory = start_dir

        config_file = None
        while not self._is_fs_root(current_directory):
            if current_directory.joinpath('pyproject.toml').exists() :
                config_file = current_directory.joinpath('pyproject.toml')
                break

            if current_directory.joinpath('nuthatch.toml').exists() :
                config_file = current_directory.joinpath('nuthatch.toml')
                break

            current_directory = current_directory.parent

        return config_file

    def _get_config_file(self, path):
        config_file = self._find_nuthatch_config(path)
        logger.debug(f"Config file path is: {config_file}")
        if config_file:
            with open(config_file, "rb") as f:
                config = tomllib.load(f)
                if 'nuthatch' in config:
                    return config['nuthatch']
                elif 'tool' in config and 'nuthatch' in config['tool']:
                    return config['tool']['nuthatch']

        return {}

    def _get_environ_config(self):
        config = {}
        for key, value in os.environ.items():
            if key.startswith('NUTHATCH'):
                parts = key.split('_')
                if len(parts) == 1:
                    pass
                if len(parts) == 2:
                    config[parts[1]] = value
                if len(parts) > 2:
                    if parts[1] in ['ROOT', 'LOCAL', 'MIRROR'] or parts[1].startswith('MIRROR'):
                        if self._is_backend(parts[2].lower()):
                            if len(parts) == 3:
                                logger.warn(f"Found nuthatch environment variable {key} with location and backend but not parameter name. Skipping.")
                            else:
                                location_config = config.setdefault(parts[1].lower(), {})
                                backend_config = location_config.setdefault(parts[2].lower(), {})
                                backend_config['_'.join(parts[3:]).lower()] = value
                        else:
                            location_config = config.setdefault(parts[1].lower(), {})
                            location_config['_'.join(parts[2:]).lower()] = value
                    elif self._is_backend(parts[1].lower()):
                        backend_config = config.setdefault(parts[1].lower(), {})
                        backend_config['_'.join(parts[2:]).lower()] = value
                    else:
                        config['_'.join(parts[1:]).lower()] = value

        return config

    def _get_dynamic_config(self, wrapped_module):
        root_module_name = None
        if hasattr(wrapped_module, '__name__'):
            root_module_name = wrapped_module.__name__.partition('.')[0]
        elif isinstance(wrapped_module, str):
            root_module_name = wrapped_module

        logger.debug(f"Getting dynamic config for module {wrapped_module} with name {root_module_name}")
        logger.debug(dynamic_parameters)

        if root_module_name and root_module_name in dynamic_parameters:
            return copy.deepcopy(dynamic_parameters[root_module_name])

        return {}

    def _is_location(self, key):
        if key in ['root', 'local', 'mirro'] or key.startswith('mirror'):
            return True
        else:
            return False

    def _is_backend(self, key):
        return key in nuthatch.backend.registered_backends.keys()

    def _expand_config(self, config):
        # Expand config so that all config parameters fall in [location][backend] format

        # Start by moving things that are not location-scoped into root
        root_config = {}
        keys_to_delete = []
        for key, value in config.items():
            if not self._is_location(key):
                root_config[key] = value
                keys_to_delete.append(key)


        root_ref = config.setdefault('root', {})
        for key, value in root_config.items():
            if key not in root_ref:
                root_ref[key] = value
        for key in keys_to_delete:
            if key in config:
                del config[key]

        # Now copy parameters to all registered backends softly so that if a backend has parameters
        # they aren't overwritten but filled out
        for location, location_values in config.items():
            generic_config = {}

            # Get everything that's not for a specific backend and put it in the generic section
            for backend_or_key, value in location_values.items():
                if not self._is_backend(backend_or_key):
                    generic_config[backend_or_key] = value

            # For all registered backends, update the values with the generics
            for backend in nuthatch.backend.registered_backends.keys():
                backend_config = location_values.setdefault(backend, {})
                for key, value in generic_config.items():
                    if key not in backend_config:
                        backend_config[key] = value

        return config

    def __init__(self, wrapped_module, mask_secrets=False, sub_config=None):
        self.mask_secrets = mask_secrets
        self.wrapped_module = wrapped_module
        if sub_config:
            self.config = sub_config
            return

        logger.debug("Constructing top level config")
        final_config = {}

        # These configurations come from out current environment - they are always safe
        global_config = self._get_config_file(os.path.expanduser('~/.nuthatch.toml'))
        current_config = self._get_config_file(Path.cwd())
        environ_config = self._get_environ_config()

        # These configurations are scoped to the environment of the wrapped function
        # If the wrapped function is in our package that's fine - they should be merged in
        # But if it's in another package we don't always want them to overwrite our root settings (very rarely)
        # And we often want to add them as additional mirros to our current environment
        wrapped_config = {}
        if hasattr(wrapped_module, '__file__'):
            wrapped_config = self._get_config_file(wrapped_module.__file__)

        dynamic_config = self._get_dynamic_config(wrapped_module)

        if ((hasattr(wrapped_module, '__file__') and ('site-packages' not in wrapped_module.__file__) and ('dist-packages' not in wrapped_module.__file__)) or
            isinstance(wrapped_module, str)):
            # The case where the wrapped function is in our current package:
            # Either wrapped_module is passed, but it's not in a site packages folder
            # OR wrapped module is passed as an explicit string to match (which is the case when using the CLI)
            final_config |= global_config
            final_config |= current_config
            final_config |= environ_config
            final_config |= wrapped_config

            logger.debug(dynamic_config)
            logger.debug(final_config)
            final_config |= dynamic_config
            logger.debug(final_config)
        elif hasattr(wrapped_module, '__file__') and ('site-packages' in wrapped_module.__file__ or 'dist-packages' in wrapped_module.__file__):
            if os.getenv("NUTHATCH_ALLOW_INSTALLED_PACKAGE_CONFIGURATION", 'False').lower() in ('true', '1', 't'):
                # We are in an installed package, but we have a specific overwride to enable the installed package to set root parameters
                final_config |= global_config
                final_config |= current_config
                final_config |= environ_config
                final_config |= wrapped_config
                final_config |= dynamic_config
            else:
                # We are in an installed package - We want the wrapped_config and dynamic config roots to be set as mirrors instead
                final_config |= global_config
                final_config |= current_config
                final_config |= environ_config

                external_config = {}
                external_config |= wrapped_config
                external_config |= dynamic_config
                # Take the external config and fill it out for backends and locations
                external_config = self._expand_config(external_config)

                external_name = 'external'
                if hasattr(wrapped_module, '__name__'):
                    external_name = wrapped_module.__name__.partition('.')[0]
                final_config['mirror-' + external_name + '-root'] = external_config['root']
                for key, value in external_config.items():
                    if key == 'mirror':
                        final_config['mirror-' + external_name] = external_config[key]
                    elif key.startswith('mirror'):
                        final_config['mirror-' + external_name + '-' + key.replace('mirror', '')] = external_config[key]
        else:
            # wrapped module is for some reason not set. Don't allow dynamic parameters (they shouldn't work anyway due to wrapped module not being set)
            final_config |= global_config
            final_config |= current_config
            final_config |= environ_config

        self.config = self._expand_config(final_config)
        logger.debug("Config finished.")
        logger.debug(self.config)

    def __setitem__(self, key, value):
        self.config[key] = value

    def __getitem__(self, key):
        if key not in self.config:
            # If we are being called from a backend class take the backend name and try it as a key, if so return that
            frame = inspect.currentframe().f_back
            if 'self' in frame.f_locals:
                caller_self = frame.f_locals['self']
                caller_class = type(caller_self) # Get the class of the caller's 'self'

                # Access the class variable from the identified caller class
                if hasattr(caller_class, 'backend_name') and isinstance(caller_class, nuthatch.backend.NuthatchBackend):
                    backend_name = caller_class.backend_name
                    logger.debug(f"Inferring backend call from {backend_name} for config")
                    if backend_name in self.config:
                        return NuthatchConfig(self.wrapped_module, self.mask_secrets, self.config[backend_name])

            raise KeyError(f"Key {key} not found in nuthatch config object")
        else:
            return self._return_existing_value(key)

    def _return_existing_value(self, key):
        if isinstance(self.config[key], dict):
                return NuthatchConfig(self.wrapped_module, self.mask_secrets, self.config[key])
        elif isinstance(self.config[key], tuple):
            fn = self.config[key][0]
            if hasattr(fn, '__call__'):
                value = fn()
                # memoize the value
                self.config[key] = (value, self.config[key][1])

            if self.config[key][1] and self.mask_secrets:
                return '*' * len(self.config[key][0])
            else:
                return self.config[key][0]
        else:
            return self.config[key]


    def __contains__(self, key):
        if key in self.config:
            return True

        if key not in self.config:
            # If we are being called from a backend class take the backend name and try it as a key, if so return that
            frame = inspect.currentframe().f_back
            if 'self' in frame.f_locals:
                caller_self = frame.f_locals['self']
                caller_class = type(caller_self) # Get the class of the caller's 'self'

                # Access the class variable from the identified caller class
                if hasattr(caller_class, 'backend_name') and isinstance(caller_class, nuthatch.backend.NuthatchBackend):
                    backend_name = caller_class.backend_name
                    if backend_name in self.config and key in self.config[backend_name]:
                        return True

        return False

    def items(self):
        ilist = []
        for key, value in self.config.items():
            ilist.append((key, self._return_existing_value(key)))

        return ilist

    def __len__(self):
        return len(self.config)

    def keys(self):
        return self.config.keys()

    def __str__(self):
        return str(self.config)
