"""The backend module is used to register and get backends.

It also contains the abstract base class for all backends.
"""
from abc import ABC, abstractmethod
import os
import fsspec
import sqlalchemy

import logging
logger = logging.getLogger(__name__)

registered_backends = {}
default_backends = {}

def register_backend(backendClass):
    """Register a backend class with the nuthatch system.

    This function registers a backend class so it can be used by the nuthatch
    caching system. The backend class must have a `backend_name` attribute.
    Optionally, if the backend class has a `default_for_type` attribute, it
    will be set as the default backend for that data type.

    Args:
        backendClass: The backend class to register. Must have a `backend_name`
                     attribute and optionally a `default_for_type` attribute.

    Returns:
        The registered backend class (allows use as a decorator).

    Example:
        @register_backend
        class MyBackend:
            backend_name = "my_backend"
            default_for_type = "my_data_type"
    """

    registered_backends[backendClass.backend_name] = backendClass

    if hasattr(backendClass, 'default_for_type'):
        if not isinstance(backendClass.default_for_type, list):
            backendClass.default_for_type = [backendClass.default_for_type]

        for default in backendClass.default_for_type:
            default_backends[default] = backendClass.backend_name

    return backendClass

def get_backend_by_name(backend_name):
    """Retrieve a registered backend class by its name.

    Args:
        backend_name (str): The name of the backend to retrieve.

    Returns:
        The backend class associated with the given name.

    Raises:
        KeyError: If no backend is registered with the given name.
    """
    return registered_backends[backend_name]

def get_default_backend(data_type):
    """Get the default backend name for a specific data type.

    Args:
        data_type (str): The data type to get the default backend for.

    Returns:
        str: The name of the default backend for the data type, or 'basic'
             if no default is set.
    """
    if data_type in default_backends:
        return default_backends[data_type]
    else:
        return 'basic'

class NuthatchBackend(ABC):

    def __init__(self, cacheable_config, cache_key, namespace, args, backend_kwargs):
        """The abstract base class for all cacheable backends.

        This is the base class for all cacheable backends in the nuthatch system.
        Each backend instance manages a specific cache entry with its own
        configuration and storage mechanism.

        Args:
            cacheable_config (dict): Configuration dictionary containing static or
                dynamic parameters. Parameters are used to configure a backend's
                access to its storage. Required parameters should be listed as strings
                in the backend's `config_parameters` class attribute.
            cache_key (str): Unique identifier for the cache entry. This key is
                used to distinguish between different cached items.
            namespace (str, optional): Optional namespace to organize cache entries.
                If provided, cache entries will be stored under this namespace.
            args (dict): Key-value pairs of arguments and values that were passed
                to the function being cached. These are used to generate cache
                keys and may influence backend behavior.
            backend_kwargs (dict): User-configurable keyword arguments specific to
                the backend implementation. For example, the zarr backend uses
                these for per-argument-value chunking configuration.

        Note:
            This is an abstract base class. Subclasses must implement the abstract
            methods: `write()`, `upsert()`, `read()`, `delete()`, `exists()`, `get_uri()`,
            and `sync()`. They must also handle namespacing their reads and writes appropriately.
        """

        # Store base
        self.config = cacheable_config
        self.cache_key = cache_key
        self.namespace = namespace
        self.backend_kwargs = backend_kwargs
        self.args = args


    @abstractmethod
    def write(self, data):
        """Write data to the backend. Overwrite if exists.

        This method is responsible for writing data to the backend. It should
        be implemented by subclasses to handle the specific storage mechanism
        of the backend.

        Args:
            data (any): The data to write to the backend.

        Returns:
            A version or copy of the written data.
        """
        pass

    @abstractmethod
    def upsert(self, data, upsert_keys=None):
        """Upsert/append data into the backend.

        This method is responsible for upserting data to the backend based
        on the upsert keys. It should
        be implemented by subclasses to handle the specific storage mechanism
        of the backend.

        Args:
            data (any): The data to write to the backend.
            upsert_keys (list, optional): List of primary key columns to use
                for upsert operations.

        Returns:
            A version/copy of the written data.
        """
        pass


    @abstractmethod
    def read(self, engine=None):
        """Read data from the backend.

        This method is responsible for reading data from the backend. It should
        be implemented by subclasses to handle the specific storage mechanism
        of the backend.

        Args:
            engine (str or type): The data processing engine to use for
                reading data from the backend (i.e. 'pandas' or pd.DataFrame).
                By default will just use the default for each backend.

        Returns:
            Any: The data read from the backend.
        """
        pass

    @abstractmethod
    def delete(self):
        """Delete data from the backend.

        This method is responsible for deleting data from the backend. It should
        be implemented by subclasses to handle the specific storage mechanism
        of the backend.
        """
        pass

    @abstractmethod
    def exists(self):
        """Check if the data exists in the backend.

        This method is responsible for checking if the data exists in the backend.
        It should be implemented by subclasses to handle the specific storage
        mechanism of the backend.

        Returns:
            bool: True if the data exists in the backend, False otherwise.
        """
        pass

    @abstractmethod
    def get_uri(self):
        """Get the file path of the data in the backend.

        This method is responsible for returning the file path of the data in the
        backend. It should be implemented by subclasses to handle the specific
        storage mechanism of the backend.

        Returns:
            str: The file path of the data in the backend.
        """
        pass

    @abstractmethod
    def sync(self, from_backend):
        """Sync data from one backend to another.

        This method is responsible for syncing data from one backend to another.
        It should be implemented by subclasses to handle the specific storage
        mechanism of the backend.

        The most basic version of this could implement self.write(from_backend.read()),
        but there is likely a more efficient way of transferring backend-specific data.

        Args:
            from_backend (NuthatchBackend): The backend to sync data from.

        """
        pass


class FileBackend(NuthatchBackend):
    """Base class for all backends that rely on a filesystem."""

    def __init__(self, cacheable_config, cache_key, namespace, args, backend_kwargs, extension=None):
        super().__init__(cacheable_config, cache_key, namespace, args, backend_kwargs)

        self.base_path = os.path.expanduser(self.config['filesystem'])

        if namespace:
            self.raw_cache_path = os.path.join(self.base_path, namespace, cache_key)
        else:
            self.raw_cache_path = os.path.join(self.base_path, cache_key)

        self.temp_cache_path = os.path.join(self.base_path, 'temp', cache_key)
        self.extension = extension
        if extension:
            self.path = self.raw_cache_path + '.' + extension

        if 'filesystem_options' not in self.config:
            self.config['filesystem_options'] = {}

        # This instantiates an fsspec filesystem
        if fsspec.utils.get_protocol(self.path) == 'file':
            # If the protocol is a local filesystem, we need to create the directory if it doesn't exist
            self.fs = fsspec.core.url_to_fs(self.path, auto_mkdir=True, **self.config['filesystem_options'])[0]
        else:
            self.fs = fsspec.core.url_to_fs(self.path, **self.config['filesystem_options'])[0]

    def exists(self):
        return (self.fs.exists(self.path))

    def delete(self):
        self.fs.rm(self.path, recursive=True)

    def get_uri(self):
        return self.path


    def sync(self, from_backend):
        # Proper way to copy data from a remote to a local filesystem
        from_backend.fs.get(from_backend.path, self.path)


class DatabaseBackend(NuthatchBackend):
    """Base class for all backends that rely on a database."""

    def __init__(self, cacheable_config, cache_key, namespace, args, backend_kwargs):
        super().__init__(cacheable_config, cache_key, namespace, args, backend_kwargs)

        logger.debug(self.config)
        if not all(prop in self.config for prop in ['driver', 'username', 'password', 'host', 'port', 'database']):
            raise RuntimeError("Missing configuration value required for database backend.")

        database_url = sqlalchemy.URL.create(self.config['driver'],
                                username = self.config['username'],
                                password = self.config['password'],
                                host = self.config['host'],
                                port = self.config['port'],
                                database = self.config['database'])

        logger.debug(f"Connecting to database {database_url}")
        self.connection = sqlalchemy.create_engine(database_url)

        if 'write_username' in self.config and 'write_password' in self.config:
            write_database_url = sqlalchemy.URL.create(self.config['driver'],
                                username = self.config['write_username'],
                                password = self.config['write_password'],
                                host = self.config['host'],
                                port = self.config['port'],
                                database = self.config['database'])
            logger.debug(f"Connecting to database {write_database_url}")
            self.write_connection = sqlalchemy.create_engine(write_database_url)
        else:
            self.write_connection = self.connection

    def sync(self, local_backend):
        raise NotImplementedError("Backend syncing not implemented for database-like backends.")
