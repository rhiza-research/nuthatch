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

from pydantic import BaseModel, ConfigDict, ValidationError

import logging
import nuthatch
logger = logging.getLogger(__name__)


dynamic_parameters = {}

# Environment variables for config file locations
NUTHATCH_GLOBAL_CONFIG_ENV = "NUTHATCH_GLOBAL_CONFIG"
NUTHATCH_PROJECT_CONFIG_ENV = "NUTHATCH_PROJECT_CONFIG"


class GlobalConfigSchema(BaseModel):
    """Schema for global nuthatch config (~/.nuthatch.toml).

    Global config is restricted to only filesystem_options and skipped_filesystems.
    All other configuration must go in project-specific nuthatch.toml files.
    """
    model_config = ConfigDict(extra='forbid')

    filesystem_options: dict = {}
    skipped_filesystems: list = []


class FileBackendConfigSchema(BaseModel):
    """Schema for file-based backend configuration (basic, zarr, delta, parquet).

    File backends store cached data on a filesystem (local, GCS, S3, etc.).
    Used by: basic.py, zarr.py, delta.py, parquet.py (all inherit from FileBackend)
    """
    model_config = ConfigDict(extra='forbid')

    # From backend.py:231 - self.base_path = os.path.expanduser(self.config['filesystem'])
    filesystem: str | None = None
    # From backend.py:243-256 - fs_options = self.config['filesystem_options']
    filesystem_options: dict = {}


class DatabaseBackendConfigSchema(BaseModel):
    """Schema for database backend configuration (sql).

    Database backends store cached data in a SQL database.
    Used by: sql.py (inherits from DatabaseBackend)
    """
    model_config = ConfigDict(extra='forbid')

    # From backend.py:280-288 - all required for DatabaseBackend
    driver: str | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    username: str | None = None
    password: str | None = None
    # From backend.py:293-303 - optional separate write credentials
    write_username: str | None = None
    write_password: str | None = None


class TerracottaBackendConfigSchema(BaseModel):
    """Schema for terracotta backend configuration.

    Terracotta inherits from BOTH DatabaseBackend AND FileBackend.
    Used by: terracotta.py
    """
    model_config = ConfigDict(extra='forbid')

    # From FileBackend (backend.py:231, 243-256)
    filesystem: str | None = None
    filesystem_options: dict = {}
    # From DatabaseBackend (backend.py:280-288)
    driver: str | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    username: str | None = None
    password: str | None = None
    # From terracotta.py:116 - tc.update_settings(SQL_USER=self.config['write_username'], ...)
    write_username: str | None = None
    write_password: str | None = None


class LocationConfigSchema(BaseModel):
    """Schema for a cache location (root, local, or mirrors.<name>).

    Location-level settings are inherited by all backends.
    Backend subsections can override these settings.
    """
    model_config = ConfigDict(extra='forbid')

    # File backend settings (from backend.py FileBackend)
    filesystem: str | None = None
    filesystem_options: dict = {}

    # Database backend settings (from backend.py DatabaseBackend)
    driver: str | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    username: str | None = None
    password: str | None = None
    write_username: str | None = None
    write_password: str | None = None

    # Backend-specific overrides
    sql: DatabaseBackendConfigSchema | None = None
    delta: FileBackendConfigSchema | None = None
    basic: FileBackendConfigSchema | None = None
    zarr: FileBackendConfigSchema | None = None
    terracotta: TerracottaBackendConfigSchema | None = None
    parquet: FileBackendConfigSchema | None = None


class ProjectConfigSchema(GlobalConfigSchema):
    """Schema for project nuthatch config (nuthatch.toml).

    Project config format:

    ```toml
    [root]
    filesystem = "gs://my-bucket/caches"

    [local]
    filesystem = "~/.nuthatch/caches"

    [mirrors.public]
    filesystem = "gs://public-bucket/caches"
    ```
    """
    model_config = ConfigDict(extra='forbid')

    dynamic_config_path: str | None = None

    # Location sections
    local: LocationConfigSchema | None = None
    root: LocationConfigSchema | None = None

    # Mirrors dict ([mirrors.name] sections)
    mirrors: dict | None = None



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
        if module is None:
            # Fallback to frame's globals when inspect.getmodule returns None
            # (can happen with mounted files in Docker, dynamically executed code, etc.)
            module_name = caller_frame.frame.f_globals.get('__name__', '')
            module = module_name.partition('.')[0] if module_name else None
        elif hasattr(module, '__name__'):
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

    # Defaults that can be overridden
    CONFIG_FILE_NAME = 'nuthatch.toml'
    DEFAULT_GLOBAL_CONFIG_PATH = '~/.nuthatch.toml'

    def _is_fs_root(self, p):
        """Check if a path is the root of a filesystem."""
        return os.path.splitdrive(str(p))[1] == os.sep

    def _get_global_config_path(self):
        """Get the path to the global config file.

        Checks NUTHATCH_GLOBAL_CONFIG env var first, falls back to default.
        """
        env_path = os.environ.get(NUTHATCH_GLOBAL_CONFIG_ENV)
        if env_path:
            return os.path.expanduser(env_path)
        return os.path.expanduser(self.DEFAULT_GLOBAL_CONFIG_PATH)

    def _find_project_config(self, start_dir=None):
        """Find config file starting from start_dir and searching upward.

        Args:
            start_dir: Directory to start searching from. Defaults to cwd.
                       Can also be a direct path to the config file.

        Returns:
            Path to config file or None if not found.
        """
        if start_dir is None:
            start_dir = Path.cwd()

        if isinstance(start_dir, str) and start_dir.endswith(self.CONFIG_FILE_NAME) and os.path.exists(start_dir):
            return start_dir
        if isinstance(start_dir, str):
            start_dir = Path(start_dir)

        current_directory = start_dir

        while not self._is_fs_root(current_directory):
            if current_directory.joinpath(self.CONFIG_FILE_NAME).exists():
                return current_directory.joinpath(self.CONFIG_FILE_NAME)

            current_directory = current_directory.parent

        return None

    def _get_project_config_path(self):
        """Get the path to the project config file.

        Checks NUTHATCH_PROJECT_CONFIG env var first, falls back to searching
        upward from cwd for the config file.

        Returns:
            Path to config file or None if not found
        """
        env_path = os.environ.get(NUTHATCH_PROJECT_CONFIG_ENV)
        if env_path:
            expanded = os.path.expanduser(env_path)
            if os.path.exists(expanded):
                return expanded
            raise FileNotFoundError(f"{NUTHATCH_PROJECT_CONFIG_ENV} set to '{env_path}' but file does not exist")
        return self._find_project_config(Path.cwd())

    def set_global_skipped_filesystem(self, filesystem):
        """Add a filesystem to the global skip list."""
        config_file = self._get_global_config_path()

        config_data = {}
        if os.path.exists(config_file):
            with open(config_file, 'rb') as f:
                config_data = tomllib.load(f)

        skip_list = config_data.setdefault('skipped_filesystems', [])
        if filesystem in skip_list:
            return
        else:
            skip_list.append(filesystem)

        with open(config_file, "wb") as f:
            tomli_w.dump(config_data, f)

    def _get_config_file(self, path):
        """Load config from a config file.

        Args:
            path: Either a direct path to config file or a directory to search from

        Returns:
            Configuration dictionary or empty dict if not found

        Supports two formats:
            - Top-level keys: filesystem = "...", [local], etc.
            - Under [nuthatch] section (for backwards compatibility)
        """
        config_file = self._find_project_config(path)
        logger.debug(f"Config file path is: {config_file}")
        if config_file:
            with open(config_file, "rb") as f:
                config = tomllib.load(f)
                # Support [nuthatch] section for backwards compatibility
                if 'nuthatch' in config:
                    return config['nuthatch']
                # Otherwise return top-level config
                return config

        return {}

    def _validate_global_config(self, config):
        """Validate that global config only contains allowed keys.

        Raises:
            ValueError: If config contains keys other than allowed keys
        """
        try:
            GlobalConfigSchema(**config)
        except ValidationError as e:
            config_path = self._get_global_config_path()
            allowed_keys = set(GlobalConfigSchema.model_fields.keys())
            raise ValueError(
                f"Global nuthatch config at '{config_path}' is invalid: {e}. "
                f"Global config can ONLY contain: {allowed_keys}. "
                f"Move other configuration to your project's nuthatch.toml file."
            ) from e
        return config

    def _load_global_config(self):
        """Load and validate the global configuration file.

        The global config can ONLY contain filesystem_options and skipped_filesystems.
        """
        config_path = self._get_global_config_path()

        if not os.path.exists(config_path):
            return {}

        with open(config_path, "rb") as f:
            config = tomllib.load(f)

        # Handle [nuthatch] section format
        if 'nuthatch' in config:
            config = config['nuthatch']

        return self._validate_global_config(config)

    def _load_project_config(self):
        """Load the project configuration file.

        Checks NUTHATCH_PROJECT_CONFIG env var first, then searches for config file.
        """
        config_path = self._get_project_config_path()

        if not config_path or not os.path.exists(config_path):
            return {}

        with open(config_path, "rb") as f:
            config = tomllib.load(f)

        # Handle [nuthatch] section format
        if 'nuthatch' in config:
            return config['nuthatch']
        return config

    def _get_environ_config(self):
        """Parse nuthatch configuration from environment variables.

        Supports formats:
        - NUTHATCH_ROOT_<PARAM> - root location parameter
        - NUTHATCH_ROOT_FILESYSTEM_OPTIONS_<KEY> - nested filesystem_options
        - NUTHATCH_LOCAL_<PARAM> - local location parameter
        - NUTHATCH_MIRRORS_<NAME>_<PARAM> - mirror parameter
        """
        config = {}

        for key, value in os.environ.items():
            if key.startswith('NUTHATCH'):
                parts = key.split('_')
                if len(parts) <= 2:
                    continue
                # NUTHATCH_MIRRORS_<name>_<param>
                if parts[1] == 'MIRRORS' and len(parts) >= 4:
                    mirror_name = parts[2].lower()
                    mirrors = config.setdefault('mirrors', {})
                    mirror_config = mirrors.setdefault(mirror_name, {})
                    if self._is_backend(parts[3].lower()):
                        if len(parts) == 4:
                            logger.warning(f"Found nuthatch environment variable {key} with mirror, backend but no parameter. Skipping.")
                        else:
                            backend_config = mirror_config.setdefault(parts[3].lower(), {})
                            self._set_nested_param(backend_config, parts[4:], value)
                    else:
                        self._set_nested_param(mirror_config, parts[3:], value)
                # Standard locations: ROOT, LOCAL
                elif parts[1] in ['ROOT', 'LOCAL']:
                    if self._is_backend(parts[2].lower()):
                        if len(parts) == 3:
                            logger.warning(f"Found nuthatch environment variable {key} with location and backend but not parameter name. Skipping.")
                        else:
                            location_config = config.setdefault(parts[1].lower(), {})
                            backend_config = location_config.setdefault(parts[2].lower(), {})
                            self._set_nested_param(backend_config, parts[3:], value)
                    else:
                        location_config = config.setdefault(parts[1].lower(), {})
                        self._set_nested_param(location_config, parts[2:], value)

        return config

    def _set_nested_param(self, config_dict, parts, value):
        """Set a parameter value, handling nested dicts like filesystem_options.

        Supports:
        - ['FILESYSTEM'] -> config_dict['filesystem'] = value
        - ['FILESYSTEM_OPTIONS', 'KEY'] -> config_dict['filesystem_options']['key'] = value
        - ['FILESYSTEM_OPTIONS_KEY'] -> config_dict['filesystem_options']['key'] = value
        """
        # Known dict fields that can have nested keys
        dict_fields = {'filesystem_options'}

        # Join parts to form potential param name
        param_parts = [p.lower() for p in parts]

        # Check if this is a nested dict field (e.g., FILESYSTEM_OPTIONS_KEY)
        for i in range(len(param_parts)):
            potential_dict_field = '_'.join(param_parts[:i+1])
            if potential_dict_field in dict_fields and i + 1 < len(param_parts):
                # This is a nested dict field with a key after it
                nested_dict = config_dict.setdefault(potential_dict_field, {})
                nested_key = '_'.join(param_parts[i+1:])
                nested_dict[nested_key] = value
                return

        # Not a nested field, just set the value directly
        param_name = '_'.join(param_parts)
        config_dict[param_name] = value

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
        """Check if a config key represents a cache location.

        Recognizes: 'root', 'local', 'mirrors'
        """
        return key in ['root', 'local', 'mirrors']

    def _is_backend(self, key):
        return key in nuthatch.backend.registered_backends.keys()

    def _expand_config(self, config):
        """Expand config so that all config parameters fall in [location][backend] format.

        Validates that config uses the correct format:
        - Location config must be under [root], [local], or [mirrors.<name>]
        - No top-level keys except global settings (filesystem_options, skipped_filesystems, dynamic_config_path)
        """
        # These keys are valid at top level (global settings), not per-location
        global_top_level_keys = {'filesystem_options', 'skipped_filesystems', 'dynamic_config_path'}

        # Validate no invalid top-level keys
        for key in config.keys():
            if not self._is_location(key) and key not in global_top_level_keys:
                raise ValueError(
                    f"Invalid top-level config key '{key}'. "
                    "Configuration must be under [root], [local], or [mirrors.<name>] sections."
                )

        # Expand backend config for all locations
        # Copy generic location parameters to all registered backends
        for location, location_values in config.items():
            if not isinstance(location_values, dict):
                continue

            # Handle mirrors dict specially
            if location == 'mirrors':
                for mirror_config in location_values.values():
                    if isinstance(mirror_config, dict):
                        self._expand_location_backends(mirror_config)
            else:
                self._expand_location_backends(location_values)

        return config

    def _expand_location_backends(self, location_values):
        """Expand backend config for a single location."""
        generic_config = {}
        for backend_or_key, value in location_values.items():
            if not self._is_backend(backend_or_key):
                generic_config[backend_or_key] = value

        for backend in nuthatch.backend.registered_backends.keys():
            backend_config = location_values.setdefault(backend, {})
            for key, value in generic_config.items():
                if key not in backend_config:
                    backend_config[key] = value

    def __init__(self, wrapped_module, mask_secrets=False, sub_config=None):
        self.mask_secrets = mask_secrets
        self.default_local = False
        self.wrapped_module = wrapped_module
        if sub_config is not None:
            self.config = sub_config
            return


        logger.debug("Constructing top level config")
        final_config = {}

        # These configurations come from our current environment - they are always safe
        # Global config: ~/.nuthatch.toml or NUTHATCH_GLOBAL_CONFIG env var
        # Can ONLY contain filesystem_options and skipped_filesystems
        global_config = self._load_global_config()
        # Project config: nuthatch.toml in project or NUTHATCH_PROJECT_CONFIG env var
        # Can contain all configuration options
        current_config = self._load_project_config()
        environ_config = self._get_environ_config()

        # These configurations are scoped to the environment of the wrapped function
        # If the wrapped function is in our package that's fine - they should be merged in
        # But if it's in another package we don't always want them to overwrite our root settings (very rarely)
        # And we often want to add them as additional mirros to our current environment
        # Skip filesystem search if project config is explicitly set via env var
        wrapped_config = {}
        if not os.environ.get(NUTHATCH_PROJECT_CONFIG_ENV) and hasattr(wrapped_module, '__file__'):
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

                # Use new mirrors dict structure (will be flattened by _expand_config)
                mirrors = final_config.setdefault('mirrors', {})
                mirrors[f'{external_name}-root'] = external_config['root']

                # Handle mirrors from external config (already in dict form after _expand_config)
                if 'mirrors' in external_config:
                    for mirror_name, mirror_config in external_config['mirrors'].items():
                        mirrors[f'{external_name}-{mirror_name}'] = mirror_config
        else:
            # wrapped module is for some reason not set. Don't allow dynamic parameters (they shouldn't work anyway due to wrapped module not being set)
            final_config |= global_config
            final_config |= current_config
            final_config |= environ_config

        if 'local' not in final_config or 'fileysstem' not in final_config['local']:
            local = final_config.setdefault('local', {})
            local['filesystem'] = "~/.nuthatch/caches"
            self.default_local = True

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

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def copy(self):
        return self.config.copy()

    def __str__(self):
        return str(self.config)
