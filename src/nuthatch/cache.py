import os
import copy
import datetime
import getpass
from abc import ABC, abstractmethod

import git
import fsspec
import polars as ps

from nuthatch.backend import get_backend_by_name

import logging
logger = logging.getLogger(__name__)

class Metastore(ABC):
    """The base class for a cache metastore. Must basically implement schema and table
    create, check conditional row existence, row insert, row, select, row update, and row delete.
    """
    def __init__(self, config):
        """Must create a metastore of the specified schema.

        Arguments:
        """
        pass

    @abstractmethod
    def cache_exists(self, cache_key, namespace):
        """Check if metastore row exists based on where condition.

        Arguments:
            where (dict): dict of key/value pairs to check

        Returns:
            True if exists, false otherwise
        """
        pass


    @abstractmethod
    def get_cache(self, cache_key, namespace):
        """Return columns for rows matching the where clause.

        Arguments:
            columns (str or list(str)): columns to select
            where (dict): key/value pairse to match

        Returns:
            list(dict) of matching row values
        """
        pass

    @abstractmethod
    def write_cache(self, cache_key, namespace, values):
        """Updates or creates a row of values that matches the were clause.

        Arguments:
            values (dict): key values to upsert
            where (dict): conditions on which to upsert
        """
        pass


    @abstractmethod
    def delete_cache(self, cache_key, namespace):
        """Updates or creates a row of values that matches the were clause.

        Arguments:
            values (dict): key values to upsert
            where (dict): conditions on which to upsert
        """
        pass

    @abstractmethod
    def list_caches(self, cache_key, namespace, backend):
        pass


class NuthatchMetastore(Metastore):
    """Nuthatch custom metastore."""
    def __init__(self, config):
        base_path = config['filesystem']
        table_path = os.path.expanduser(os.path.join(base_path, 'nuthatch_metadata.nut'))
        self.table_path = table_path
        self.extension = 'parquet'
        self.config = config
        self.exists = os.path.join(table_path, 'exists.nut')

        if 'filesystem_options' not in config:
            self.config['filesystem_options'] = {}

        # This instantiates an fsspec filesystem
        if fsspec.utils.get_protocol(self.table_path) == 'file':
            # If the protocol is a local filesystem, we need to create the directory if it doesn't exist
            self.fs = fsspec.core.url_to_fs(self.table_path, auto_mkdir=True, **self.config['filesystem_options'])[0]
        else:
            self.fs = fsspec.core.url_to_fs(self.table_path, **self.config['filesystem_options'])[0]

        # This often fails, and that's fine so briefly suppress user errors
        module = getattr(getattr(self.fs, '__class__', None), '__module__', None)
        if module:
            module = module.partition('.')[0]
            pre_level = logging.getLogger(module).getEffectiveLevel()
            logging.getLogger(module).setLevel(logging.CRITICAL + 1)

        if not self.fs.exists(self.exists):
            self.fs.touch(self.exists)

        # Reset to the original level
        if module:
            logging.getLogger(module).setLevel(pre_level)

    def is_null(self, cache_key, namespace):
        if namespace:
            null_path = os.path.join(self.table_path, cache_key, namespace) + '.null'
        else:
            null_path = os.path.join(self.table_path, cache_key) + '.null'
        return self.fs.exists(null_path)

    def set_null(self, cache_key, namespace):
        if namespace:
            null_path = os.path.join(self.table_path, cache_key, namespace) + '.null'
        else:
            null_path = os.path.join(self.table_path, cache_key) + '.null'
        return self.fs.touch(null_path)

    def delete_null(self, cache_key, namespace):
        if namespace:
            null_path = os.path.join(self.table_path, cache_key, namespace) + '.null'
        else:
            null_path = os.path.join(self.table_path, cache_key) + '.null'
        return self.fs.rm(null_path)

    def cache_exists(self, cache_key, namespace, backend):
        if namespace:
            full_path = os.path.join(self.table_path, cache_key, namespace, backend) + '.' + self.extension
        else:
            full_path = os.path.join(self.table_path, cache_key, backend) + '.' + self.extension
        return self.fs.exists(full_path)

    def get_cache(self, cache_key, namespace, backend):
        if namespace:
            full_path = os.path.join(self.table_path, cache_key, namespace, backend) + '.' + self.extension
        else:
            full_path = os.path.join(self.table_path, cache_key, backend) + '.' + self.extension
        df = ps.read_parquet(self.fs.open(full_path, 'rb'))
        return df.to_dicts()[0]

    def get_backends(self, cache_key, namespace):
        if namespace:
            full_path = os.path.join(self.table_path, cache_key, namespace, '*') + '.' + self.extension
        else:
            full_path = os.path.join(self.table_path, cache_key, '*') + '.' + self.extension
        blist = self.fs.glob(full_path)
        return [b.split('/')[-1].split('.')[0] for b in blist]

    def write_cache(self, cache_key, namespace, backend, values):
        if namespace:
            full_path = os.path.join(self.table_path, cache_key, namespace, backend) + '.' + self.extension
        else:
            full_path = os.path.join(self.table_path, cache_key, backend) + '.' + self.extension
        df = ps.DataFrame(values)
        df.write_parquet(full_path, mkdir=True)

    def delete_cache(self, cache_key, namespace, backend):
        if namespace:
            full_path = os.path.join(self.table_path, cache_key, namespace, backend) + '.' + self.extension
        else:
            full_path = os.path.join(self.table_path, cache_key, backend) + '.' + self.extension
        self.fs.rm(full_path)

    def list_nulls(self, cache_key, namespace):
        join_list = [self.table_path]
        if cache_key and '/' not in cache_key and cache_key.endswith('*'):
            cache_key = cache_key + '/*'
        elif not cache_key:
            cache_key = '*/*'


        join_list.append(cache_key)

        if namespace:
            join_list.append(namespace)


        full_path = os.path.join(*join_list) + '.null'

        return self.fs.glob(full_path)

    def list_caches(self, cache_key, namespace, backend, verbose=False):
        join_list = [self.table_path]
        if cache_key and '/' not in cache_key and cache_key.endswith('*'):
            cache_key = cache_key + '/*'
        elif not cache_key:
            cache_key = '*/*'

        join_list.append(cache_key)

        if namespace:
            join_list.append(namespace)

        if not backend:
            backend = '*'

        join_list.append(backend)

        full_path = os.path.join(*join_list) + '.' + self.extension

        if verbose:
            if '*' in full_path:
                raise ValueError("Verbose list a full path with a specified backend")
            df = ps.read_parquet(full_path)
            return df
        else:
            return self.fs.glob(full_path)



class Cache():
    """The cache class interacts with the metastore and the backends to store the
    data itself and information about cache state. Currently delta and sql
    metastores are implemented.

    It is responsible for:
    - Instantiating the correct backend
    - Managing the metadata in the metadata database or delta table
    - Writing and reading data to the backend
    """
    def __init__(self, config, cache_key, namespace, version, args, requested_backend, backend_kwargs):
        self.cache_key = cache_key
        self.config = config
        self.namespace = namespace
        self.version = version
        self.args = args
        self.backend_kwargs = backend_kwargs

        # Either instantiate a delta table or a postgres table
        logger.debug(f"Using Nuthatch Metastore at {config['filesystem']}")
        self.metastore = NuthatchMetastore(config)

        self.backend = None
        self.backend_name = None

        backend_class = None
        if requested_backend:
            backend_class = get_backend_by_name(requested_backend)
            self.backend_name = requested_backend
        elif self.cache_key:
            stored_backend = self._get_backend_from_metadata()
            if stored_backend:
                backend_class = get_backend_by_name(stored_backend)
                self.backend_name = stored_backend

        if backend_class and self.cache_key:
            if backend_class.backend_name in config:
                try:
                    self.backend  = backend_class(self.config[backend_class.backend_name], cache_key, namespace, args, copy.deepcopy(backend_kwargs))
                except RuntimeError:
                    pass

    def is_null(self):
        """Check if the metadata is null.

        Returns:
            bool: True if the metadata is null, False otherwise.
        """
        return self.metastore.is_null(self.cache_key, self.namespace)

    def set_null(self):
        """Set the metadata to null."""
        self.metastore.set_null(self.cache_key, self.namespace)

    def delete_null(self):
        """Delete the metadata that is null."""
        # Deleting a null is really just deleting a metadata
        if self.is_null():
            self.metastore.delete_null(self.cache_key, self.namespace)
        else:
            raise RuntimeError("Trying to delete a null cache that is not null")

    def _metadata_confirmed(self):
        """Check if the metadata is confirmed.

        Returns:
            bool: True if the metadata is confirmed, False otherwise.
        """
        if not self.backend:
            return False

        if self.metastore.cache_exists(self.cache_key, self.namespace, self.backend_name):
            c = self.metastore.get_cache(self.cache_key, self.namespace, self.backend_name)
            if (c['state'] == 'confirmed') and (c['version'] == self.version):
                return True

        return False

    def _delete_metadata(self):
        self.metastore.delete_cache(self.cache_key, self.namespace, self.backend_name)

    def _get_backend_from_metadata(self):
        """Get the backend from the metadata.

        Returns:
            The backend from the metadata.
        """
        rows = self.metastore.get_backends(self.cache_key, self.namespace)
        if len(rows) == 0:
            return None
        else:
            return rows[0]

    def last_modified(self):
        """Get the last modified time of the metadata.

        Returns:
            The last modified time of the metadata.
        """
        rows = self.metastore.get_cache(self.cache_key, self.namespace, self.backend_name)
        if len(rows) == 0:
            return None
        else:
            return rows['last_modified']

    def _commit_metadata(self):
        """Update the state of the metadata.

        If metadata doesn't exist, it will be created.

        Args:
            state (str, optional): The state to update the metadata to.
        """
        try:
            repo = git.Repo(search_parent_directories=True)
            sha = repo.head.object.hexsha
        except: # noqa: E722
            sha = 'no_git_repo'

        path = 'None'
        if self.backend:
            path = self.backend.get_uri()

        values = {
            'cache_key': self.cache_key,
            'namespace': self.namespace,
            'version': self.version,
            'backend': self.backend_name,
            'state': 'confirmed',
            'last_modified': datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000000,
            'commit_hash': sha,
            'user': getpass.getuser(),
            'path': path
        }

        self.metastore.write_cache(self.cache_key, self.namespace, self.backend_name, values)


    def exists(self):
        """Check if the metadata exists, is confirmed, and the data exists in the backend.

        Returns:
            bool: True if the metadata exists, False otherwise.
        """
        confirmed = self._metadata_confirmed()
        if confirmed and not self.backend:
            raise RuntimeError("If metadata exists then there should be a backend. Inconsistent state error.")
        elif not self.backend:
            # We just aren't initialized yet
            return False
        elif confirmed and self.backend.exists():
            # Both exists!
            return True
        elif confirmed and not self.backend.exists():
            # The data doesn't exist in the backend, so delete the metadata
            logger.warning("Found valid metadata, but no valid data. Deleting metadata.")
            self._delete_metadata()
            return False
        elif not confirmed:
            return False
        else:
            raise ValueError("Inconsistent and unknown cache state.")


    def write(self, ds):
        """Write data to the backend.

        First we set the metadata to pending, then we write the data to the backend,
        and then we commit the metadata.

        Args:
            ds (any): The data to write to the backend.
        """
        if self.backend:
            ds = self.backend.write(ds)
            self._commit_metadata()
            return ds
        else:
            raise RuntimeError("Cannot not write to an uninitialized backend")


    def upsert(self, ds, upsert_keys=None):
        if self.backend:
            ds = self.backend.upsert(ds, upsert_keys)
            self._commit_metadata()
            return ds
        else:
            raise RuntimeError("Cannot not upsert to an uninitialized backend")


    def read(self, engine=None):
        """Read data from the backend.

        Args:
            engine (str or type, optional): The data processing engine to use for
                reading data from the backend.

        Returns:
            The data from the backend.
        """
        if self.backend:
            return self.backend.read(engine)
        else:
            raise RuntimeError("Cannot not read from an uninitialized backend")

    def delete(self):
        """Delete the metadata and the data from the backend."""
        if self.backend and self.backend.exists():
            self.backend.delete()

        self._delete_metadata()

    def get_backend(self):
        """Return the backend."""
        return self.backend.backend_name

    def get_uri(self):
        """Get the file path of the data in the backend.

        Returns:
            The file path of the data in the backend.
        """
        if self.backend:
            return self.backend.get_uri()
        else:
            raise RuntimeError("Cannot not get file path for an uninitialized backend")

    def sync(self, from_cache):
        """Sync the data from one cache to another.

        Args:
            from_cache (Cache): The cache to sync data from.
        """
        if self.exists():
            if from_cache.exists():
                # If we both exists, copy if last modified is before other cache
                if self.last_modified() < from_cache.last_modified():
                    self.backend.sync(from_cache.backend)
                    self._commit_metadata()
                else:
                    return
            else:
                # If it's not in the form cache either don't sync
                return
        else:
            if from_cache.exists():
                # We don't exist at all. Setup backend and write
                backend_class = get_backend_by_name(from_cache.backend_name)
                backend_name = backend_class.backend_name
                if backend_name in self.config:
                    self.backend = backend_class(self.config[backend_name], self.cache_key, self.namespace, self.args, copy.deepcopy(self.backend_kwargs))
                    self.backend_name = backend_class.backend_name
                else:
                    raise RuntimeError("Error finding backend config for syncing.")

                self.backend.sync(from_cache.backend)
                self._commit_metadata()
            else:
                return
