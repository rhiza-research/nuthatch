from deltalake import DeltaTable, write_deltalake, QueryBuilder
from os.path import join
import copy
import git
import getpass
import datetime
from .backend import get_backend_by_name
from .config import get_config
import pyarrow as pa
import pandas as pd
import sqlalchemy

class Cache():
    """The cache class is the main class that manages the cache.

    It is responsible for:
    - Instantiating the correct backend
    - Managing the metadata in the metadata database or delta table
    - Writing and reading data to the backend
    """
    database_parameters = ["driver", "host", "port", "database", "username", "password"]
    config_parameters = ['filesystem', 'filesystem_options', 'metadata_location'] + database_parameters
    backend_name = "cache_metadata"
    delta_tables = {}
    delta_table_configs = {}

    def __init__(self, config, cache_key, namespace, args, backend_location, requested_backend, backend_kwargs):
        self.cache_key = cache_key
        self.config = config
        self.namespace = namespace
        self.location = backend_location
        self.args = args
        self. backend_kwargs = backend_kwargs

        # Either instantiate a delta table or a postgres table
        if (('metadata_location' in config and config['metadata_location'] == 'filesystem') or
            ('metadata_location' not in config and 'filesystem' in config) or
            (any(param not in config for param in self.__class__.database_parameters))):
            # This is a delta/filesystem type
            base_path = self.config['filesystem']
            table_path = join(base_path, 'nuthatch_metadata.delta')

            options = None
            if 'filesystem_options' in self.config:
                options = self.config['filesystem_options']
                for key, value in options.items():
                    options[key] = str(value)

            self.store = 'delta'
            if (backend_location in self.__class__.delta_tables and
                self.__class__.delta_table_configs[backend_location] == config):
                self.dt = self.__class__.delta_tables[backend_location]
            else:
                # Instantiate the metadata store here so that _get_backend_from_metadata() works
                if not DeltaTable.is_deltatable(table_path, storage_options=options):
                    print("Instantiating empty delta table.")
                    DeltaTable.create(table_path,
                                      schema=pa.schema(
                                        [pa.field("cache_key", pa.string()), pa.field("backend", pa.string()),
                                         pa.field("namespace", pa.string()), pa.field("state", pa.string()),
                                         pa.field("last_modified", pa.int64()), pa.field("commit_hash", pa.string()),
                                         pa.field("user", pa.string()), pa.field("path", pa.string())]
                                      ),
                                      storage_options=options,
                                      partition_by="cache_key")

                self.dt = DeltaTable(table_path, storage_options=options)
                self.__class__.delta_tables[backend_location] = self.dt
                self.__class__.delta_table_configs[backend_location] = config
        else:
            # This is a database type
            database_url = sqlalchemy.URL.create(self.config['driver'],
                                                 username = self.config['username'],
                                                 password = self.config['password'],
                                                 host = self.config['host'],
                                                 port = self.config['port'],
                                                 database = self.config['database'])
            self.engine = sqlalchemy.create_engine(database_url)
            metadata = sqlalchemy.MetaData()

            self.db_table = sqlalchemy.Table(
                'nuthatch_metadata', metadata,
                sqlalchemy.Column('cache_key', sqlalchemy.String),
                sqlalchemy.Column('backend', sqlalchemy.String),
                sqlalchemy.Column('namespace', sqlalchemy.String),
                sqlalchemy.Column('state', sqlalchemy.String),
                sqlalchemy.Column('last_modified', sqlalchemy.BigInteger),
                sqlalchemy.Column('commit_hash', sqlalchemy.String),
                sqlalchemy.Column('user', sqlalchemy.String),
                sqlalchemy.Column('path', sqlalchemy.String)
            )

            metadata.create_all(self.engine, checkfirst=True)
            self.store = 'database'

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

    def _delta_check_exists(self, state=None, include_backend=False):
        """Check if the metadata exists in the delta table.

        Args:
            state (str, optional): The state of the metadata to check for.
            include_backend (bool, optional): Whether to include the backend in the check.

        Returns:
            bool: True if the metadata exists in the delta table, False otherwise.
        """
        base = f"""select * from metadata where cache_key = '{self.cache_key}' AND namespace = '{self.namespace}'"""
        if include_backend:
            base += f" AND backend = '{self.backend_name}'"
        if state:
            base += f" AND state = '{state}'"

        rows = QueryBuilder().register('metadata', self.dt).execute(base).read_all()

        if len(rows) > 0:
            return True
        else:
            return False

    def _sql_check_exists(self, state=None, include_backend=False):
        """Check if the metadata exists in the database.

        Args:
            state (str, optional): The state of the metadata to check for.
            include_backend (bool, optional): Whether to include the backend in the check.

        Returns:
            bool: True if the metadata exists in the database, False otherwise.
        """
        statement = sqlalchemy.select(sqlalchemy.func.count(self.db_table.c.cache_key)).where(self.db_table.c.cache_key == self.cache_key)\
                                                                    .where(self.db_table.c.namespace == self.namespace)
        if state:
            statement = statement.where(self.db_table.c.state == state)
        if include_backend:
            statement = statement.where(self.db_table.c.backend == self.backend_name)

        with self.engine.connect() as conn:
            num = conn.execute(statement)
            if num.fetchone()[0] > 0:
                return True
            else:
                return False

    def _check_row_exists(self, state=None, include_backend=False):
        """Check if the metadata exists in the database or delta table.

        Args:
            state (str, optional): The state of the metadata to check for.
            include_backend (bool, optional): Whether to include the backend in the check.

        Returns:
            bool: True if the metadata exists in the database or delta table, False otherwise.
        """
        if self.store == 'delta':
            return self._delta_check_exists(state, include_backend)
        else:
            return self._sql_check_exists(state, include_backend)

    def _sql_get_row(self, select, include_backend=False):
        """Get a row from the database.

        Args:
            select (str): The column to select.
            include_backend (bool, optional): Whether to include the backend in the check.

        Returns:
            The row from the database.
        """
        if isinstance(select, str):
            select = [select]

        statement = sqlalchemy.select(self.db_table.c[*select]).where(self.db_table.c.namespace == self.namespace)\
                                                               .where(self.db_table.c.cache_key.like(self.cache_key))

        if include_backend:
            statement = statement.where(self.db_table.c.backend == self.backend_name)

        with self.engine.connect() as conn:
            rows = conn.execute(statement)
            return rows.mappings().all()

    def _delta_get_row(self, select, include_backend=False):
        """Get a row from the delta table.

        Args:
            select (str): The column to select.
            include_backend (bool, optional): Whether to include the backend in the check.

        Returns:
            The row from the delta table.
        """
        if isinstance(select, str):
            select = [select]

        base = f"""select {', '.join(f'"{s}"' for s in select)} from metadata where namespace = '{self.namespace}' AND
                                                                  cache_key LIKE '{self.cache_key}'"""
        if include_backend:
            base += f" AND backend = '{self.backend_name}'"

        rows = QueryBuilder().register('metadata', self.dt).execute(base).read_all()

        return rows.to_struct_array().to_pylist()

    def _get_row(self, select, include_backend=False):
        """Get a row from the database or delta table.

        Args:
            select (str): The column to select.
            include_backend (bool, optional): Whether to include the backend in the check.

        Returns:
            The row from the database or delta table.
        """
        if self.store == 'delta':
            return self._delta_get_row(select, include_backend)
        else:
            return self._sql_get_row(select, include_backend)

    def list(self, cache_key):
        #convert cache_key glob to valid sql pattern matching
        if not self.cache_key:
            cache_key = cache_key.replace('*', '%')
            cache_key = cache_key.replace('?', '_')
            self.cache_key = cache_key

        if self.backend_name:
            include_backend = True
        else:
            include_backend = False

        return self._get_row(['cache_key',
                              'namespace',
                              'backend',
                              'state',
                              'last_modified',
                              'user',
                              'commit_hash',
                              'path'], include_backend=include_backend)

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
        if self.store == 'delta':
            if null:
                self.dt.delete(predicate=f"cache_key = '{self.cache_key}' AND namespace = '{self.namespace}' AND state = 'null'")
            else:
                if self.backend_name:
                    self.dt.delete(predicate=f"cache_key = '{self.cache_key}' AND namespace = '{self.namespace}' AND backend = '{self.backend_name}'")
                else:
                    raise RuntimeError("Can only delete non-null metadata with a valid backend")
        else:
            statement = sqlalchemy.delete(self.db_table).where(self.db_table.c.cache_key == self.cache_key)\
                                                        .where(self.db_table.c.namespace == self.namespace)
            if null:
                statement = statement.where(self.db_table.c.state == 'null')
            else:
                statement = statement.where(self.db_table.c.backend == self.backend_name)
            with self.engine.connect() as conn:
                conn.execute(statement)
                conn.commit()


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
            path = self.backend.get_file_path()

        if self.store == 'delta':
            if self._metadata_exists():
                values = {'state': state, 'last_modified': datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000000,
                          'commit_hash': sha, 'user': getpass.getuser(), 'path': path}
                if state == 'null':
                    self.dt.update(predicate=f"cache_key = '{self.cache_key}' AND namespace = '{self.namespace}'",
                                   new_values = values)
                else:
                    self.dt.update(predicate=f"cache_key = '{self.cache_key}' AND namespace = '{self.namespace}' AND backend = '{self.backend_name}'",
                                   new_values= values)
            else:
                df = pd.DataFrame({'cache_key': [self.cache_key],
                                   'namespace': [str(self.namespace)],
                                   'backend': [self.backend_name],
                                   'commit_hash': [sha],
                                   'user': [getpass.getuser()],
                                   'path' : [path],
                                   'state': [state],
                                   'last_modified': [datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000000]})

                write_deltalake(self.dt, df, mode='append')
        else:
            if self._metadata_exists():
                statement = sqlalchemy.update(self.db_table).where(self.db_table.c.cache_key == self.cache_key)\
                                                            .where(self.db_table.c.namespace == self.namespace)

                if state != 'null':
                    statement = statement.where(self.db_table.c.backend == self.backend_name)

                statement = statement.values(state=state, last_modified=datetime.datetime.now(datetime.timezone.utc).timestamp()*1000000,
                                             commit_hash=sha, user=getpass.getuser(), path=path)
            else:
                statement = sqlalchemy.insert(self.db_table).values(state=state,
                                                                    last_modified=datetime.datetime.now(datetime.timezone.utc).timestamp()*1000000,
                                                                    commit_hash=sha, user=getpass.getuser(), path=path,
                                                                    backend=self.backend_name,
                                                                    cache_key=self.cache_key,
                                                                    namespace=self.namespace)

            with self.engine.connect() as conn:
                conn.execute(statement)
                conn.commit()

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
        if self._metadata_confirmed() and not self.backend:
            raise RuntimeError("If metadata exists then there should be a backend. Inconsistent state error.")
        elif not self.backend:
            # We just aren't initialized yet
            return False
        elif self._metadata_confirmed() and self.backend.exists():
            # Both exists!
            return True
        elif self._metadata_confirmed() and not self.backend.exists():
            # The data doesn't exist in the backend, so delete the metadata
            self._delete_metadata()
            return False
        elif not self._metadata_confirmed():
            return False
        else:
            raise ValueError("Inconsistent and unknown cache state.")

    def write(self, ds, upsert=False, primary_keys=None):
        """Write data to the backend.

        First we set the metadata to pending, then we write the data to the backend,
        and then we commit the metadata.

        Args:
            ds (any): The data to write to the backend.
            upsert (bool, optional): Whether to upsert the data.
            primary_keys (list, optional): The primary keys to use for upsert.
        """
        if self.backend:
            self._set_metadata_pending()
            self.backend.write(ds, upsert, primary_keys)
            self._commit_metadata()
        else:
            raise RuntimeError("Cannot not write to an uninitialized backend")

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

    def get_file_path(self):
        """Get the file path of the data in the backend.

        Returns:
            The file path of the data in the backend.
        """
        if self.backend:
            return self.backend.get_file_path()
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
