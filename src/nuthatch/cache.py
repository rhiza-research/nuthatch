from deltalake import DeltaTable, write_deltalake, QueryBuilder
from os.path import join
import datetime
from .backend import get_backend_by_name
from .config import get_config
import pyarrow as pa
import pandas as pd

class Cache():
    config_parameters=['filesystem', 'filesystem_options']

    def __init__(self, config, cache_key, namespace, args, backend_location, requested_backend, backend_kwargs):
        self.cache_key = cache_key
        self.config = config
        self.namespace = namespace

        base_path = self.config['filesystem']
        self.table_path = join(base_path, 'nuthatch_metadata.delta')

        # Instantiate the metadata store here so that _get_backend_from_metadata() works
        if not DeltaTable.is_deltatable(self.table_path, storage_options=self.config['filesystem_options']):
            print("Instantiating empty delta table.")
            DeltaTable.create(self.table_path,
                              schema=pa.schema(
                                [pa.field("cache_key", pa.string()), pa.field("backend", pa.string()),
                                 pa.field("namespace", pa.string()), pa.field("state", pa.string()),
                                 pa.field("last_modified", pa.timestamp('us')), pa.field("commit_hash", pa.string()),
                                 pa.field("user", pa.string()), pa.field("path", pa.string())]
                              ),
                              storage_options=self.config['filesystem_options'],
                              partition_by="cache_key")

        self.dt = DeltaTable(self.table_path, storage_options=self.config['filesystem_options'])

        self.backend = None
        backend_class = None
        if requested_backend:
            backend_class = get_backend_by_name(requested_backend)
        elif self._get_backend_from_metadata():
            backend_class = get_backend_by_name(self._get_backend_from_metadata())

        if backend_class:
            backend_config = get_config(location=backend_location, requested_parameters=backend_class.config_parameters,
                                        backend_name=backend_class.backend_name)
            self.backend = backend_class(backend_config, cache_key, namespace, args, backend_kwargs)
            self.backend_name = backend_class.backend_name

    def is_null(self):
        null_rows = QueryBuilder().register('metadata', self.dt).execute(f"""select * from metadata where cache_key = '{self.cache_key}'
                                                                         AND namespace = '{self.namespace}'
                                                                         AND state = 'null'""").read_all()
        if len(null_rows) > 0:
            return True

    def set_null(self):
        self._update_metadata_state(state='null')

    def delete_null(self):
        # Deleting a null is really just deleting a metadata
        self._delete_metadata(null=True)

    def _delete_metadata(self, null=False):
        if null:
            self.dt.delete(predicate="cache_key = '{self.cache_key}' AND namespace = {self.namespace} AND state = 'null'")
        else:
            if self.backend_name:
                self.dt.delete("cache_key = '{self.cache_key}' AND namespace = '{self.namespace}' AND backend = '{self.backend_name}'")
            else:
                raise RuntimeError("Can only delete non-null metadata with a valid backend")

    def _metadata_exists(self):
        rows = QueryBuilder().register('metadata', self.dt).execute(f"""select backend from metadata where cache_key = '{self.cache_key}'
                                                                                AND namespace = '{self.namespace}'
                                                                                AND backend = '{self.backend_name}'
                                                                                AND state = 'confirmed'""").read_all()

        if len(rows) > 0:
            return True
        else:
            return False

    def _get_backend_from_metadata(self):
        backend_rows = QueryBuilder().register('metadata', self.dt).execute(f"""select backend from metadata where metadata.cache_key = '{self.cache_key}'
                                                                                AND metadata.namespace = '{self.namespace}'""").read_all()
        if len(backend_rows) == 0:
            return None
        else:
            return backend_rows[0]

    def _set_metadata_pending(self):
        self._update_metadata_state(state='pending')

    def _commit_metadata(self):
        self._update_metadata_state(state='confirmed')

    def _update_metadata_state(self, state=None):
        if self._metadata_exists():
            if state == 'null':
                self.dt.update(predicate="cache_key = {self.cache_key} AND namespace = {self.namespace}",
                               update={'state': 'null', 'last_modified': datetime.datetime.now()})
            else:
                self.dt.update(predicate="cache_key = {self.cache_key} AND namespace = {self.namespace} AND backend = {self.backend_name}",
                               update={'state': state, 'last_modified': datetime.datetime.now()})
        else:
            df = pd.DataFrame({'cache_key': self.cache_key,
                               'namespace': self.namespace,
                               'backend': self.backend_name,
                               'state': state,
                               'last_modified': datetime.datetime.now()})
            write_deltalake(self.dt, df, mode='append')

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
            self._set_metadata_pending()
            data = self.backend.write(ds, upsert, primary_keys)
            self._commit_metadata()
            return data
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
