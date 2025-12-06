#!/usr/bin/env python3
"""
Command-line interface for nuthatch.

This module provides a CLI for interacting with nuthatch caching functionality,
including cache management, backend operations, and configuration.
"""

import importlib
import click
import fsspec
import shutil
from nuthatch.config import NuthatchConfig
from .backend import get_backend_by_name, registered_backends
from nuthatch.cache import Cache
import pandas as pd
from pathlib import Path
import tomllib
import os

def strip_base(path, base_path):
    if path.startswith(base_path):
        path = path[len(base_path):]

    stripped = fsspec.core.strip_protocol(base_path)
    if path.startswith(stripped):
        path = path[len(stripped):]

    if path.startswith('/'):
        path = path[1:]

    return path


def get_metadata_backend(path):
    return path.split('/')[-1].split('.')[0]

def get_metadata_namespace(path):
    return path.split('/')[-2]

def get_null_metadata_namespace(path):
    return path.split('/')[-1].split('.')[0]

def get_metadata_cache_key(path, base_path, namespace):
    path = strip_base(path, base_path)
    if namespace:
        return '/'.join(path.split('/')[:-2])
    else:
        return '/'.join(path.split('/')[:-1])

def get_null_metadata_cache_key(path, base_path, namespace):
    path = strip_base(path, base_path)
    if namespace:
        return '/'.join(path.split('/')[:-1])
    else:
        return path.split('.')[0]


def get_cache_key(path, base_path, namespace, extension):
        if path.endswith('.' + extension):
            path = path[:-len('.' + extension)]

        if path.startswith(base_path):
            path = path[len(base_path):]

        stripped = fsspec.core.strip_protocol(base_path)
        if path.startswith(stripped):
            path = path[len(stripped):]

        if path.startswith('/'):
            path = path[1:]

        if namespace:
            if path.startswith(namespace):
                path = path[len(namespace):]

        if path.startswith('/'):
            path = path[1:]

        return path


root_module = None

@click.group()
@click.version_option(version="0.1.0", prog_name="nuthatch")
def cli():
    """Nuthatch - Caching and recalling big data pipelines.

    This CLI provides tools for managing cache entries, inspecting backends,
    and configuring the nuthatch system.
    """

    config = NuthatchConfig(wrapped_module=None)
    global root_module
    if 'dynamic_config_path' in config['root']:
        try:
            importlib.import_module(config['root']['dynamic_config_path'])
            root_module = config['root']['dynamic_config_path'].partition('.')[0]
        except Exception as e:
            click.echo(f"WARNGIN: Failed to import {config['root']['dynamic_config_path']} with '{e}'. You may be missing dynamic secret resolution.")

    if not root_module:
        config_file = config._find_nuthatch_config(Path.cwd())
        if config_file:
            with open(config_file, "rb") as f:
                config = tomllib.load(f)
                if 'project' in config and 'name' in config['project']:
                    root_module = config['project']['name']




@cli.command('import')
@click.argument('cache_key')
@click.option('--namespace', help='Namespace for the cache')
@click.option('--version', help='Version of the imported cache')
@click.option('--backend', help='Backend to use', required=True)
@click.option('--location', help='Location to search', default='root')
def import_data(cache_key, namespace, version, backend, location):
    """Import data from a glob pattern."""

    # First instantiate the backend based on the passed backend
    global root_module
    if backend != 'null':
        backend_name = backend
        backend_class = get_backend_by_name(backend)
        config = NuthatchConfig(wrapped_module=root_module)
        backend = backend_class(config[location][backend], cache_key, namespace, None, {})
        base_path = backend.base_path
        extension = backend.extension
        fs = backend.fs
        path = backend.path
    else:
        config = NuthatchConfig(wrapped_module=root_module)
        backend_name = 'null'
        base_path = config[location]['filesystem']
        extension = 'null'

        if 'filesystem_options' not in config:
            config['filesystem_options'] = {}

        if namespace:
            path = os.path.join(base_path, cache_key, namespace) + '.null'
        else:
            path = os.path.join(base_path, cache_key)  + '.null'
        fs = fsspec.core.url_to_fs(base_path, **config['filesystem_options'])[0]

    cache_keys = []
    if  fs is not None:
        paths = fs.glob(path)
        for path in paths:
            cache_keys.append(get_cache_key(path, base_path, namespace, extension))

    print(cache_keys)
    if len(cache_keys) > 0:
        click.confirm(f"Are you sure you want to import {len(paths)} cache entries?", abort=True)
    else:
        print("No caches found for import.")

    for key in cache_keys:
        print(f"Importing {key}.")

        if backend_name == 'null':
            config = NuthatchConfig(wrapped_module=root_module)
            cache = Cache(config[location], key, namespace, None, version, None, {})
            if cache.is_null():
                print(f"{key} already in cache as null!")
            elif cache.exists():
                print(f"Cache {key} already exists and is valid. Skipping entry. Delete this cache key if you would like to reimport it as null.")
            else:
                cache.set_null()
                print(f"Set {key} successfully to null.")
        else:
            config = NuthatchConfig(wrapped_module=root_module)
            cache = Cache(config[location], key, namespace, None, version, backend_name, {})
            if not cache.exists():
                cache._commit_metadata()
                print(f"Imported {key} successfully.")
            else:
                print(f"{key} already in cache!")

def list_helper(cache_key, namespace, backend, location, verbose=False):
    """List all cache entries."""
    global root_module
    config = NuthatchConfig(wrapped_module=root_module)
    cache = Cache(config[location], None, namespace, None, None, backend, {})

    nulls = pd.DataFrame(cache.metastore.list_nulls(cache_key, namespace), columns=['cache_key'])
    if not namespace:
        nulls['cache_key'] = nulls['cache_key'].map(lambda x: get_null_metadata_cache_key(x, cache.metastore.table_path, namespace))
    else:
        nulls['namespace'] = nulls['cache_key'].map(lambda x: get_null_metadata_namespace(x))
        nulls['cache_key'] = nulls['cache_key'].map(lambda x: get_null_metadata_cache_key(x, cache.metastore.table_path, namespace))

    nulls['backend'] = 'null'

    if verbose:
        caches = cache.metastore.list_caches(cache_key, namespace, cache.backend_name, verbose).to_pandas()
    else:
        caches = pd.DataFrame(cache.metastore.list_caches(cache_key, namespace, cache.backend_name), columns=['cache_key'])
        if not namespace:
            caches['backend'] = caches['cache_key'].map(lambda x: get_metadata_backend(x))
            caches['cache_key'] = caches['cache_key'].map(lambda x: get_metadata_cache_key(x, cache.metastore.table_path, namespace))
        else:
            caches['backend'] = caches['cache_key'].map(lambda x: get_metadata_backend(x))
            caches['namespace'] = caches['cache_key'].map(lambda x: get_metadata_namespace(x))
            caches['cache_key'] = caches['cache_key'].map(lambda x: get_metadata_cache_key(x, cache.metastore.table_path, namespace))

    return pd.concat([nulls, caches])

@cli.command('list')
@click.argument('cache_key', required=False)
@click.option('--namespace', help='Namespace for the cache')
@click.option('--backend', help='Backend filter')
@click.option('--location', help='Location to search', default='root')
@click.option('--verbose', '-v', is_flag=True, help='List all information about the cache')
def list_caches(cache_key, namespace, backend, location, verbose):

    caches = list_helper(cache_key, namespace, backend, location, verbose)

    if len(caches) == 0:
        click.echo("No caches found")
        return

    pager = len(caches) > shutil.get_terminal_size()[0]

    pd.set_option('display.max_rows', None)
    if pager:
        click.echo_via_pager(str(caches))
    else:
        click.echo(caches)


@cli.command('delete')
@click.argument('cache_key')
@click.option('--namespace', help='Namespace for the cache')
@click.option('--backend', help='Backend to use')
@click.option('--location', help='Location to search', default='root')
@click.option('--force', '-f', is_flag=True, help='Force deletion without confirmation')
@click.option('--metadata-only', '-m', is_flag=True, help='Only delete the metadata for the cache, not the underlying data.')
def delete_cache(cache_key, namespace, backend, location, force, metadata_only):
    """Clear cache entries."""
    caches = list_helper(cache_key, namespace, backend, location)
    global root_module
    config = NuthatchConfig(wrapped_module=root_module)
    cache = Cache(config[location], None, namespace, None, None, backend, {})

    if len(caches) == 0:
        print("No caches found to delete.")
        return

    click.confirm(f"Are you sure you want to delete {len(caches)} cache entries?\n{str(caches[['cache_key', 'backend']])}\n", abort=True)

    for cache in caches.to_dict(orient='records'):
        if cache['backend'] == 'null':
            cache = Cache(config, cache['cache_key'], namespace, None, None, None, {})
        else:
            cache = Cache(config, cache['cache_key'], namespace, None, None, cache['backend'], {})
        click.echo(f"Deleting {cache.cache_key} from {cache.location} with backend {cache.backend_name}.")
        if metadata_only:
            cache._delete_metadata()
        else:
            cache.delete()


@cli.command('print-config')
@click.option('--location', help='Location to search', default='root')
@click.option('--backend', help='Backend to use')
@click.option('--show-secrets', '-s', is_flag=True, help='Only delete the metadata for the cache, not the underlying data.')
def get_config_value(location, backend, show_secrets):
    """Get configuration value for a specific key."""
    # Get the root module we are executing from from the dynamic secrets path or the project name
    global root_module
    mask = (not show_secrets)
    config = NuthatchConfig(wrapped_module=root_module, mask_secrets=mask)

    if location not in config:
        click.echo(f"No configuration found for location {location}")
        return

    if backend:
        if backend not in config[location]:
            click.echo(f"No configuration found for location {location} and backend {backend}")
            return

        click.echo(backend + ':')
        for key, value in config[location][backend].items():
            click.echo(f"\t{key}: {value}")
        click.echo()
    else:
        backend_classes = list(registered_backends.values())

        for backend in backend_classes:
            click.echo(backend.backend_name.title())
            for key, value in config[location][backend.backend_name].items():
                click.echo(f"\t{key}: {value}")
            click.echo()


def main():
    return cli()


if __name__ == '__main__':
    main()
