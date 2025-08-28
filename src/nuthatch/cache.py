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
    database_parameters = ["driver", "host", "port", "database", "username", "password"]
    config_parameters = ['filesystem', 'filesystem_options', 'metadata_location'] + database_parameters
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
        elif self._get_backend_from_metadata():
            backend_class = get_backend_by_name(self._get_backend_from_metadata())

        if backend_class:
            backend_config = get_config(location=backend_location, requested_parameters=backend_class.config_parameters,
                                        backend_name=backend_class.backend_name)
            if backend_config:
                self.backend = backend_class(backend_config, cache_key, namespace, args, copy.deepcopy(backend_kwargs))
                self.backend_name = backend_class.backend_name

    def _delta_check_exists(self, state=None, include_backend=False):
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
        if self.store == 'delta':
            return self._delta_check_exists(state, include_backend)
        else:
            return self._sql_check_exists(state, include_backend)

    def _sql_get_row(self, select, include_backend=False):
        statement = sqlalchemy.select(select).where(self.db_table.c.cache_key == self.cache_key)\
                                             .where(self.db_table.c.namespace == self.namespace)
        if include_backend:
            statement = statement.where(self.db_table.c.backend == self.backend_name)

        with self.engine.connect() as conn:
            rows = conn.execute(statement)
            if rows.rowcount == 0:
                return None
            else:
                return rows.fetchone()[0]

    def _delta_get_row(self, select, include_backend=False):
        base = f"""select {select} from metadata where cache_key = '{self.cache_key}' AND namespace = '{self.namespace}'"""
        if include_backend:
            base += f" AND backend = '{self.backend_name}'"

        rows = QueryBuilder().register('metadata', self.dt).execute(base).read_all()

        if len(rows) == 0:
            return None
        else:
            return rows.to_batches()[0][0][0].as_py()

    def _get_row(self, select, include_backend=False):
        if self.store == 'delta':
            return self._delta_get_row(select, include_backend)
        else:
            obj = None
            if select == 'backend':
                obj = self.db_table.c.backend
            elif select == 'last_modified':
                obj = self.db_table.c.last_modified

            return self._sql_get_row(obj, include_backend)

    def is_null(self):
        return self._check_row_exists(state='null', include_backend=False)

    def set_null(self):
        self._update_metadata_state(state='null')

    def delete_null(self):
        # Deleting a null is really just deleting a metadata
        self._delete_metadata(null=True)

    def _delete_metadata(self, null=False):
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
        if not self.backend:
            return False

        return self._check_row_exists(state='confirmed', include_backend=True)

    def _metadata_exists(self):
        return self._check_row_exists(state=None, include_backend=True)

    def _get_backend_from_metadata(self):
        return self._get_row('backend', include_backend=False)

    def last_modified(self):
        return self._get_row('last_modified', include_backend=False)

    def _set_metadata_pending(self):
        self._update_metadata_state(state='pending')

    def _commit_metadata(self):
        self._update_metadata_state(state='confirmed')

    def _update_metadata_state(self, state=None):
        repo = git.Repo(search_parent_directories=True)
        if repo:
            sha = repo.head.object.hexsha
        else:
            sha = 'no_git_repo'
        if self.store == 'delta':
            if self._metadata_exists():
                values = {'state': state, 'last_modified': datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000000,
                          'commit_hash': sha, 'user': getpass.getuser(), 'path': self.backend.get_file_path()}
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
                                   'path' : [self.backend.get_file_path()],
                                   'state': [state],
                                   'last_modified': [datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000000]})

                write_deltalake(self.dt, df, mode='append')
        else:
            if self._metadata_exists():
                statement = sqlalchemy.update(self.db_table).where(self.db_table.c.cache_key == self.cache_key)\
                                                            .where(self.db_table.c.namespace == self.namespace)

                if state == 'null':
                    statement = statement.where(self.db_table.c.backend == self.backend_name)

                statement = statement.values(state=state, last_modified=datetime.datetime.now(datetime.timezone.utc).timestamp()*1000000,
                                             commit_hash=sha, user=getpass.getuser(), path=self.backend.get_file_path())
            else:
                statement = sqlalchemy.insert(self.db_table).values(state=state,
                                                                    last_modified=datetime.datetime.now(datetime.timezone.utc).timestamp()*1000000,
                                                                    commit_hash=sha, user=getpass.getuser(), path=self.backend.get_file_path(),
                                                                    backend=self.backend_name,
                                                                    cache_key=self.cache_key,
                                                                    namespace=self.namespace)

            with self.engine.connect() as conn:
                conn.execute(statement)
                conn.commit()

    def get_backend(self):
        return self.backend.backend_name

    def exists(self):
        if self._metadata_confirmed() and not self.backend:
            raise RuntimeError("If metadata exists then there should be a backend. Inconsistent state error.")
        elif not self.backend:
            # We just aren't initialized yet
            return False
        elif self._metadata_confirmed() and self.backend.exists():
            # Both exists!
            return True
        elif self._metadata_confirmed() and not self.backend.exists():
            # Inconsistent state - should probably throw a warning
            self._delete_metadata()
            return False
        elif not self._metadata_confirmed():
            return False
        else:
            raise ValueError("Inconsistent and unknown cache state.")

    def write(self, ds, upsert=False, primary_keys=None):
        if self.backend:
            self._set_metadata_pending()
            self.backend.write(ds, upsert, primary_keys)
            self._commit_metadata()
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

    def sync(self, from_cache):
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
