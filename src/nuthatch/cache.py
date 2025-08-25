from deltalake import DeltaTable, write_deltalake
import fsspec
import datetime
from .backend import get_backend_by_name
from .config import get_config

class Cache():
    config_parameters=['filesystem', 'filesystem_options']
    backend_name = ['metadata']

    def __init__(self, config, cache_key, namespace, args, location, requested_backend, backend_kwargs):
        self.cache_key = cache_key
        self.config = config
        self.namespace = namespace
        self.fs = fsspec.core.url_to_fs(self.config['filesystem'], **self.config['filesystem_options'])[0]

        # Instantiate the metadata store here so that _get_backend_from_metadata() works


        self.backend = None
        backend_class = None
        if requested_backend:
            backend_class = get_backend_by_name(requested_backend)
        elif self._get_backend_from_metadata():
            backend_class = get_backend_by_name(self._get_backend_from_metadata())

        if backend_class:
            backend_config = get_config(location=location, requested_parameters=backend_class.config_parameters,
                                        backend_name=backend_class.backend_name)
            self.backend = backend_class(backend_config, cache_key, namespace, args, backend_kwargs)

    def is_null(self):
        pass

    def set_null(self):
        pass

    def delete_null(self):
        pass

    def _delete_metadata(self):
        pass

    def _metadata_exists(self):
        pass

    def _get_backend_from_metadata(self):
        pass

    def _set_metadata_pending(self):
        pass

    def _commit_metadata(self):
        pass

    def get_backend(self):
        return self.backend.backend_name

    def exists(self):
        if self._metadata_exists() and not self.backend:
            raise RuntimeError("If metadata exists then there should be a backend. Inconsistent state error.")
        elif not self.backend:
            # We just aren't initialized yet
            return False
        elif self._metadata_exists() and self.backend.exists():
            # Both exists!
            return True
        elif self._metadata_exists() and not self.backend.exists():
            # Inconsistent state - should probably throw a warning
            self._delete_matadata()
            return False

    def write(self, ds, upsert, primary_keys):
        if self.backend:
            return self.backend.write(ds, upsert, primary_keys)
        else:
            raise RuntimeError("Cannot not write to an uninitialized backend")

    def read(self, engine=None):
        if self.backend:
            return self.backend.read(engine)
        else:
            raise RuntimeError("Cannot not read from an uninitialized backend")

    def delete(self):
        self._delete_metadata()
        if self.backend:
            return self.backend.delete()

    def get_file_path(self):
        if self.backend:
            return self.backend.get_file_path()
        else:
            raise RuntimeError("Cannot not get file path for an uninitialized backend")
