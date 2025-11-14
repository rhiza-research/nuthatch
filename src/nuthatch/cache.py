import os
import copy
import datetime
import getpass
from abc import ABC, abstractmethod

import git
import pyarrow as pa
import sqlalchemy
import polars as ps
from deltalake import DeltaTable
import deltalake

from .backend import get_backend_by_name
from .config import get_config

import logging
logger = logging.getLogger(__name__)

class Metastore(ABC):
    """The base class for a cache metastore. Must basically implement schema and table
    create, check conditional row existence, row insert, row, select, row update, and row delete.
    """
    def __init__(self, config, schema):
        """Must create a metastore of the specified schema.

        Arguments:
            schema (dict): column_name: python type
        """
        pass

    @abstractmethod
    def check_row_exists(self, where):
        """Check if metastore row exists based on where condition.

        Arguments:
            where (dict): dict of key/value pairs to check

        Returns:
            True if exists, false otherwise
        """
        pass

    @abstractmethod
    def select_row(self, columns, where):
        """Return columns for rows matching the where clause.

        Arguments:
            columns (str or list(str)): columns to select
            where (dict): key/value pairse to match

        Returns:
            list(dict) of matching row values
        """
        pass

    @abstractmethod
    def upsert(self, values, where):
        """Updates or creates a row of values that matches the were clause.

        Arguments:
            values (dict): key values to upsert
            where (dict): conditions on which to upsert
        """
        pass

class DeltaMetastore(Metastore):
    """Deltalake metastore."""

    # Class variables for keeping the delta table in memory
    # slightly improves performance
    delta_tables = {}
    delta_table_configs = {}

    def __init__(self, config, backend_location, schema):
        base_path = config['filesystem']
        table_path = os.path.expanduser(os.path.join(base_path, 'nuthatch_metadata.delta'))
        self.table_path = table_path
        self.backend_location = backend_location

        options = None
        if 'filesystem_options' in config:
            options = config['filesystem_options']
            for key, value in options.items():
                options[key] = str(value)

        # Create pyarrow schema to match schema
        schema_list = []
        for key, value in schema.items():
            if value is str:
                schema_list.append(pa.field(key, pa.string()))
            elif value is int:
                schema_list.append(pa.field(key, pa.int64()))

        if (backend_location in self.__class__.delta_tables and
            self.__class__.delta_table_configs[backend_location] == config):
            logger.debug("Loading delta table from cache.")
            self.pscan = self.__class__.delta_tables[backend_location]
        else:
            try:
                self.pscan = ps.scan_delta(table_path).collect()
                logging.debug("Opened delta table.")
            except deltalake.exceptions.TableNotFoundError:
                logger.info("Instantiating empty delta table.")
                DeltaTable.create(table_path,
                                  schema=pa.schema(schema_list),
                                  storage_options=options)

                self.pscan = ps.scan_delta(table_path).collect()
            except FileNotFoundError:
                logger.info("Instantiating empty delta table.")
                DeltaTable.create(table_path,
                                  schema=pa.schema(schema_list),
                                  storage_options=options)

                self.pscan = ps.scan_delta(table_path).collect()

            # Read the table for easy caching
            self.__class__.delta_tables[backend_location] = self.pscan
            self.__class__.delta_table_configs[backend_location] = copy.deepcopy(config)
            logger.debug(f"Cached delta metadata table at {table_path} for backend_location {backend_location}")


    def check_row_exists(self, where):
        """Check if metastore row exists based on where condition.

        Arguments:
            where (dict): dict of key/value pairs to check

        Returns:
            True if exists, false otherwise
        """
        # Try this in polars instead?

        base = """select cache_key from metadata"""

        first = True
        for key, value in where.items():
            if first:
                base += f" where {key} = '{value}'"
                first = False
            else:
                base += f" AND {key} = '{value}'"

        logging.info("start query")
        rows = self.pscan.sql(base, table_name = "metadata")
        logging.info("end query")

        #rows = QueryBuilder().register('metadata', self.dt).execute(base).read_all()

        if len(rows) > 0:
            return True
        else:
            return False


    def select_row(self, columns, where={}, like={}):
        """Return columns for rows matching the where clause.

        Arguments:
            columns (str or list(str)): columns to select
            where (dict): key/value pairs to match
            like (dict): key/value pairs to like match

        Returns:
            list(dict) of matching row values
        """
        if isinstance(columns, str):
            columns = [columns]

        base = f"""select {', '.join(f'"{s}"' for s in columns)} from metadata"""

        first = True
        for key, value in where.items():
            if first:
                base += f" where {key} = '{value}'"
                first = False
            else:
                base += f" AND {key} = '{value}'"

        for key, value in like.items():
            if first:
                base += f" where {key} LIKE '{value}'"
                first = False
            else:
                base += f" AND {key} LIKE '{value}'"

        rows = self.pscan.sql(base, table_name = "metadata")
        return rows.to_dicts()


    def upsert(self, values, where):
        """Updates or creates a row of values that matches the were clause.

        Arguments:
            values (dict): key values to upsert
            where (dict): conditions on which to upsert
        """
        for key, value in values.items():
            if value is None:
                values[key] = str(value)


        base = ""
        first = True
        for key, value in where.items():
            if first:
                base += f"s.{key} = '{value}'"
                first = False
            else:
                base += f" AND s.{key} = '{value}'"

        df = ps.DataFrame(values)
        df.write_delta(
            self.table_path,
            mode="merge",
            delta_merge_options={
                "predicate": base,
                "source_alias": "t",
                "target_alias": "s",
            },
        ).when_matched_update_all().when_not_matched_insert_all().execute()
        self.pscan = ps.scan_delta(self.table_path).collect()
        self.__class__.delta_tables[self.backend_location] = self.pscan

    def delete(self, where):
        base = ""
        first = True
        for key, value in where.items():
            if first:
                base += f"s.{key} = '{value}'"
                first = False
            else:
                base += f" AND s.{key} = '{value}'"

        self.pscan.write_delta(
            self.table_path,
            mode="merge",
            delta_merge_options={
                "predicate": base,
                "source_alias": "t",
                "target_alias": "s",
            },
        ).when_matched_delete().execute()

        self.pscan = ps.scan_delta(self.table_path).collect()
        self.__class__.delta_tables[self.backend_location] = self.pscan


class SQLMetastore(Metastore):
    """SQL metastore."""

    def __init__(self, config, schema):
        # Create sqlalchemy schema to match schema
        schema_list = []
        for key, value in schema.items():
            if value is str:
                schema_list.append(sqlalchemy.Column(key, sqlalchemy.String))
            elif value is int:
                schema_list.append(sqlalchemy.Column(key, sqlalchemy.BigInteger))

        # This is a database type
        database_url = sqlalchemy.URL.create(config['driver'],
                                             username = config['username'],
                                             password = config['password'],
                                             host = config['host'],
                                             port = config['port'],
                                             database = config['database'])
        self.engine = sqlalchemy.create_engine(database_url)

        metadata = sqlalchemy.MetaData()
        self.db_table = sqlalchemy.Table('nuthatch_metadata', metadata, *schema_list)

        metadata.create_all(self.engine, checkfirst=True)


    def check_row_exists(self, where):
        """Check if metastore row exists based on where condition.

        Arguments:
            where (dict): dict of key/value pairs to check

        Returns:
            True if exists, false otherwise
        """
        statement = sqlalchemy.select(sqlalchemy.func.count())

        for key, value in where.items():
            statement = statement.where(self.db_table.c[key] == value)

        with self.engine.connect() as conn:
            num = conn.execute(statement)
            if num.fetchone()[0] > 0:
                return True
            else:
                return False


    def select_row(self, columns, where={}, like={}):
        """Return columns for rows matching the where clause.

        Arguments:
            columns (str or list(str)): columns to select
            where (dict): key/value pairs to match
            like (dict): key/value pairs to like match

        Returns:
            list(dict) of matching row values
        """
        if isinstance(columns, str):
            columns = [columns]

        statement = sqlalchemy.select(self.db_table.c[*columns])

        for key, value in where.items():
            statement = statement.where(self.db_table.c[key] == value)

        for key, value in like.items():
            statement = statement.where(self.db_table.c[key].like(value))


        with self.engine.connect() as conn:
            rows = conn.execute(statement)
            return rows.mappings().all()


    def upsert(self, values, where):
        """Updates or creates a row of values that matches the were clause.

        Arguments:
            values (dict): key values to upsert
            where (dict): conditions on which to upsert
        """
        if self.check_row_exists(where):
            statement = sqlalchemy.update(self.db_table)

            for key, value in where.items():
                statement = statement.where(self.db_table.c[key] == value)

            statement = statement.values(values)
        else:
            statement = sqlalchemy.insert(self.db_table).values(values)

        with self.engine.connect() as conn:
            conn.execute(statement)
            conn.commit()


    def delete(self, where):
        """Delete items from the cache metastore matching the where clause."""
        statement = sqlalchemy.delete(self.db_table)

        for key, value in where.items():
            statement = statement.where(self.db_table.c[key] == value)

        with self.engine.connect() as conn:
            conn.execute(statement)
            conn.commit()


class Cache():
    """The cache class interacts with the metastore and the backends to store the
    data itself and information about cache state. Currently delta and sql
    metastores are implemented.

    It is responsible for:
    - Instantiating the correct backend
    - Managing the metadata in the metadata database or delta table
    - Writing and reading data to the backend
    """
    database_parameters = ["driver", "host", "port", "database", "username", "password"]
    config_parameters = ['filesystem', 'filesystem_options', 'metadata_location'] + database_parameters
    backend_name = "cache_metadata"

    def __init__(self, config, cache_key, namespace, version, args, backend_location, requested_backend, backend_kwargs):
        self.cache_key = cache_key
        self.config = config
        self.namespace = namespace
        self.version = version
        self.location = backend_location
        self.args = args
        self.backend_kwargs = backend_kwargs

        schema = {
            'cache_key': str,
            'backend': str,
            'namespace': str,
            'version': str,
            'state': str,
            'last_modified': int,
            'commit_hash': str,
            'user': str,
            'path': str
        }

        # Either instantiate a delta table or a postgres table
        if (('metadata_location' in config and config['metadata_location'] == 'filesystem') or
            ('metadata_location' not in config and 'filesystem' in config) or
            (any(param not in config for param in self.__class__.database_parameters))):
            logger.debug(f"Using Delta Metastore for {backend_location}")
            self.metastore = DeltaMetastore(config, backend_location, schema)
        else:
            logger.debug(f"Using SQL Metastore for {backend_location}")
            self.metastore = SQLMetastore(config, schema)


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
            backend_config = get_config(location=backend_location, requested_parameters=backend_class.config_parameters,
                                        backend_name=backend_class.backend_name)
            if backend_config:
                self.backend = backend_class(backend_config, cache_key, namespace, args, copy.deepcopy(backend_kwargs))

    def _check_row_exists(self, state=None, include_backend=False):
        """Check if the metadata exists in the database or delta table.

        Args:
            state (str, optional): The state of the metadata to check for.
            include_backend (bool, optional): Whether to include the backend in the check.

        Returns:
            bool: True if the metadata exists in the database or delta table, False otherwise.
        """
        where = {
            'cache_key': self.cache_key,
            'namespace': self.namespace,
            'version': self.version
        }

        if state:
            where['state'] = state

        if include_backend:
            where['backend']: self.backend

        return self.metastore.check_row_exists(where)

    def _get_row(self, select, include_backend=False):
        """Get a row from the database or delta table.

        Args:
            select (str): The column to select.
            include_backend (bool, optional): Whether to include the backend in the check.

        Returns:
            The row from the database or delta table.
        """
        where = {
            'namespace': self.namespace,
            'version': self.version
        }

        like = {
            'cache_key': self.cache_key,
        }

        if include_backend:
            where['backend'] = self.backend_name

        return self.metastore.select_row(select, where, like)

    def is_null(self):
        """Check if the metadata is null.

        Returns:
            bool: True if the metadata is null, False otherwise.
        """
        return self._check_row_exists(state='null', include_backend=False)

    def set_null(self):
        """Set the metadata to null."""
        self._update_metadata_state(state='null')

    def delete_null(self):
        """Delete the metadata that is null."""
        # Deleting a null is really just deleting a metadata
        self._delete_metadata(null=True)

    def _delete_metadata(self, null=False):
        """Delete the metadata from the database or delta table.

        Args:
            null (bool, optional): Whether to delete the metadata that is null.
        """

        where = {
            'cache_key': self.cache_key,
            'namespace': self.namespace
        }

        if null:
            where['state'] = 'null'
        else:
            if self.backend_name:
                where['backend'] = self.backend_name
            else:
                raise RuntimeError("Can only delete non-null metadata with a valid backend")

        self.metastore.delete(where)


    def _metadata_confirmed(self):
        """Check if the metadata is confirmed.

        Returns:
            bool: True if the metadata is confirmed, False otherwise.
        """
        if not self.backend:
            return False

        return self._check_row_exists(state='confirmed', include_backend=True)

    def _metadata_exists(self):
        """Check if the metadata exists.

        Returns:
            bool: True if the metadata exists, False otherwise.
        """
        return self._check_row_exists(state=None, include_backend=True)

    def _get_backend_from_metadata(self):
        """Get the backend from the metadata.

        Returns:
            The backend from the metadata.
        """
        rows = self._get_row('backend', include_backend=False)
        if len(rows) == 0:
            return None
        else:
            return rows[0]['backend']

    def last_modified(self):
        """Get the last modified time of the metadata.

        Returns:
            The last modified time of the metadata.
        """
        rows = self._get_row('last_modified', include_backend=True)
        if len(rows) == 0:
            return None
        else:
            return rows[0]['last_modified']

    def _set_metadata_pending(self):
        """Set the metadata to pending."""
        self._update_metadata_state(state='pending')

    def _commit_metadata(self):
        """Commit the metadata."""
        self._update_metadata_state(state='confirmed')

    def _update_metadata_state(self, state=None):
        """Update the state of the metadata.

        If metadata doesn't exist, it will be created.

        Args:
            state (str, optional): The state to update the metadata to.
        """
        repo = git.Repo(search_parent_directories=True)
        if repo:
            sha = repo.head.object.hexsha
        else:
            sha = 'no_git_repo'

        path = 'None'
        if self.backend:
            path = self.backend.get_uri()

        values = {
            'cache_key': self.cache_key,
            'namespace': self.namespace,
            'version': self.version,
            'backend': self.backend_name,
            'state': state,
            'last_modified': datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000000,
            'commit_hash': sha,
            'user': getpass.getuser(),
            'path': path
        }

        where = {
            'cache_key': self.cache_key,
            'namespace': self.namespace
        }

        if state != 'null':
            where['backend'] = self.backend_name

        self.metastore.upsert(values, where)


    def get_backend(self):
        """Get the backend name.

        Returns:
            The backend name.
        """
        return self.backend.backend_name


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
            self._set_metadata_pending()
            ds = self.backend.write(ds)
            self._commit_metadata()
            return ds
        else:
            raise RuntimeError("Cannot not write to an uninitialized backend")


    def upsert(self, ds, upsert_keys=None):
        if self.backend:
            self._set_metadata_pending()
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
                    self._set_metadata_pending()
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
                backend_config = get_config(location=self.location, requested_parameters=backend_class.config_parameters,
                                            backend_name=backend_class.backend_name)
                if backend_config:
                    self.backend = backend_class(backend_config, self.cache_key, self.namespace, self.args, copy.deepcopy(self.backend_kwargs))
                    self.backend_name = backend_class.backend_name
                else:
                    raise RuntimeError("Error finding backend config for syncing.")

                self._set_metadata_pending()
                self.backend.sync(from_cache.backend)
                self._commit_metadata()
            else:
                return
