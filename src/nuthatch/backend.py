from abc import ABC, abstractmethod
from pathlib import Path
import fsspec

registered_backends = {}
default_backends = {}

def register_backend(backendClass):
    registered_backends[backendClass.backend_name] = backendClass

    if 'default_for_type' in backendClass.__dict__:
        default_backends[backendClass.default_for_type] = backendClass.backend_name

    return backendClass

def get_backend_by_name(backend_name):
    return registered_backends[backend_name]

def get_default_backend(data_type):
    if data_type in default_backends:
        return default_backends[data_type]
    else:
        return 'basic'

class NuthatchBackend(ABC):
    config_parameters = []

    def __init__(self, cacheable_config, cache_key, namespace, args, backend_kwargs):
        """The base class for all cacheable backends.

        At it's core a backend gets several key parameters:
            cacheable_config (dict): A dictionary of static or dynamic
                parameters. requested parameters should be set as a string
                in the config_parameters list of the backend.
            cache_key (string): the unique key of the cache
            args (dict): key/value pairs of arguments and values
                that were passed the function being called
            backend_kwargs (dict): A set of user-settable, per function
                kwargs that are backend specific. (i.e. the zarr
                backend implements per-argument-value chunking)
        """

        # Store base
        self.config = cacheable_config
        self.cache_key = cache_key
        self.namespace = namespace
        self.backend_kwargs = backend_kwargs
        self.args = args


    @abstractmethod
    def write(self, data):
        pass

    @abstractmethod
    def read(self, engine):
        pass

    @abstractmethod
    def delete(self):
        pass

    @abstractmethod
    def exists(self):
        pass

    @abstractmethod
    def get_file_path(self):
        pass

    @abstractmethod
    def sync(self, NuthatchBackend):
        pass


class FileBackend(NuthatchBackend):
    """Base class for all backends that rely on a filesystem."""
    config_parameters = ["filesystem", "filesystem_options"]

    def __init__(self, cacheable_config, cache_key, namespace, args, backend_kwargs, extension):
        super().__init__(cacheable_config, cache_key, args, backend_kwargs)

        base_path = Path(self.config['filesystem'])

        if namespace:
            self.raw_cache_path = base_path.joinpath(namespace, cache_key)
        else:
            self.raw_cache_path = base_path.joinpath(cache_key)

        self.temp_cache_path = basepath.joinpath('temp', cache_key)
        self.path = self.raw_cache_path + '.' + extension
        self.fs = fsspec.core.url_to_fs(self.path, **self.config['filesystem_options'])[0]


    def exists(self):
        return (self.fs.exists(self.path))

    def delete(self):
        self.fs.rm(self.path)

    def get_file_path(self):
        return self.path


class DatabaseBackend(NuthatchBackend):
    """Base class for all backends that rely on a filesystem."""
    config_parameters = ["driver", "host", "port", "database", "username", "password", "write_username", "write_password"]

    def __init__(self, cacheable_config, cache_key, namespace, args, backend_kwargs):
        super().__init__(cacheable_config, cache_key, namespace, args, backend_kwargs)

        database_url = URL.create(self.config['driver'],
                                username = self.config['username'],
                                password = self.config['password'],
                                host = self.config['host'],
                                port = self.config['port'],
                                database = self.config['database'])
        self.engine = sqlalchemy.create_engine(database_url)
        self.uri = database_url.render_as_string()

        if self.config['write_username'] and self.config['write_password']:
            write_database_url = URL.create(self.config['driver'],
                                username = self.config['write_username'],
                                password = self.config['write_password'],
                                host = self.config['host'],
                                port = self.config['port'],
                                database = self.config['database'])
            self.write_engine = sqlalchemy.create_engine(write_database_url)
            self.write_uri = write_database_url.render_as_string()
        else:
            self.write_engine = self.engine
            self.write_uri = self.uri


    def get_file_path(self):
        raise NotImplementedError("File path returns not supported for database-like backends.")

    def sync(self, local_backend):
        raise NotImplementedError("Backend syncing not implemented for database-like backends.")
