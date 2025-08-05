from abc import ABC, abstractmethod
from pathlib import Path
import fsspec

class CacheableBackend(ABC):
    config_parameters = []

    def __init__(self, cacheable_config, cache_key, args, backend_kwargs):
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
    def delete_null(self):
        pass

    @abstractmethod
    def exists(self):
        pass

    @abstractmethod
    def write_null(self):
        pass

    @abstractmethod
    def get_file_path():
        pass


class FileBackend(CacheableBackend):
    """Base class for all backends that rely on a filesystem."""
    config_parameters = ["filesystem", "filesystem_options"]

    def __init__(self, cacheable_config, cache_key, args, backend_kwargs, extension):
        super().__init__(cacheable_config, cache_key, args, backend_kwargs)

        base_path = Path(self.config['filesystem'])
        self.raw_cache_path = base_path.joinpath(cache_key)
        self.path = self.raw_cache_path + '.' + extension
        self.null_path = self.raw_cache_path + '.null'
        self.fs = fsspec.core.url_to_fs(self.path, **self.config['filesystem_options'])[0]


    def exists(self):
        return (self.fs.exists(self.path))

    def delete(self):
        self.fs.rm(self.path)

    def write_null(self):
        self.fs.touch(self.null_path)

    def delete_null(self):
        if self.fs.exists(self.null_path):
            self.fs.rm(self.null_path)

    def get_file_path(self):
        return self.path


class VerifyableFileBackend(FilesystemBackend):
    """Base class for all filesystem backends with self-implemented psuedo-consistency."""
    def __init__(self, cacheable_config, cache_key, args, backend_kwargs, extension):
        super().__init__(cacheable_config, cache_key, args, backend_kwargs)
        self.verify_path = self.raw_cache_path + '.verify'

    def exists(self):
        return (self.fs.exists(self.path) and self.fs.exists(self.verify_path))

    def delete(self):
        self.fs.rm(self.verify_path)
        self.fs.rm(self.path)


class DatabaseBackend(CacheableBackend):
    """Base class for all backends that rely on a filesystem."""
    config_parameters = ["driver", "host", "port", "database", "username", "password", "write_username", "write_password"]

    def __init__(self, cacheable_config, cache_key, args, backend_kwargs, extension):
        super().__init__(cacheable_config, cache_key, args, backend_kwargs)

    def get_file_path(self):
        raise NotImplementedError("File path returns not supported for database-like backends.")
