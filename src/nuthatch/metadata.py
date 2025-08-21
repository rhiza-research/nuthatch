from deltalake import DeltaTable, write_deltalake
import fsspec
from pathlib import Path
import json
import datetime

class CacheMetdata():
    config_parameters=['filesystem', 'filesystem_options']
    backend_name = ['metadata']

    def __init__(self, config, cache_key)
        self.cache_key = cache_key
        self.config = config
        self.fs = fsspec.core.url_to_fs(self.config['filesystem'], **self.config['filesystem_options'])[0]
        self.metadata_file = Path(self.config['filesystem']).joinpath(cache_key, 'metadata.json')

        if self.fs.exists(self.metadata_file)
            with self.fs.open(self.metadata_file, 'r') as f:
                self.current_metadata = json.load(f)
        else:
            self.current_metadata = {}


    def is_null(self):
        if 'null' in self.current_metadata:
            return bool(self.current_metadata['null'])


    def write_null(self):
        self.current_metdata['null'] = True
        self.current_metdata['cache_key'] = self.cache_key
        with self.fs.open(self.metadata_file) as f:
            f.write(json.dumps(self.current_metadata))


    def delete(self):
        self.fs.rm(self.metadata_file)
        self.current_metadata = {}


    def delete_null(self):
        self.fs.rm(self.metadata_file)
        self.current_metadata = {}


    def get_backend(self):
        if 'backend' in self.current_metadata:
            return self.current_metadata['backend']
        else:
            return None


    def exists(self):
        return 'cache_key' in self.current_metadata


    def commit(self, backend, path):
        self.current_metadata['backend'] = backend
        self.current_metadata['cache_key'] = self.cache_key
        self.current_metadata['null'] = False
        self.current_metadata['last_updated'] = datetime.datetime.now().timestamp()
        with self.fs.open(self.metadata_file) as f:
            f.write(json.dumps(self.current_metadata))

