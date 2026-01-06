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
    """Import existing data files into the nuthatch cache.

    Scans for data files matching CACHE_KEY and registers them in the
    nuthatch metastore without re-writing the data.

    \b
    Examples:
      nuthatch import "mydata/*" --backend zarr    # Import zarr stores
      nuthatch import "results/*" --backend basic  # Import pickle files
      nuthatch import "empty/*" --backend null     # Mark keys as null
    """

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

        fs_options = config[location].get('filesystem_options', {})

        if namespace:
            path = os.path.join(base_path, cache_key, namespace) + '.null'
        else:
            path = os.path.join(base_path, cache_key)  + '.null'
        fs = fsspec.core.url_to_fs(base_path, **fs_options)[0]

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
    """List cache entries matching a pattern.

    CACHE_KEY is an optional glob pattern to filter results.

    \b
    Pattern examples:
      nuthatch list              # List all single-level cache keys
      nuthatch list "mydata*"    # Keys starting with 'mydata' (single-level)
      nuthatch list "proj/*"     # All keys under 'proj/' (multi-level)
      nuthatch list "a/b/*"      # All keys under 'a/b/' (3+ levels)

    Note: Patterns without '/' only match single-level cache keys.
    For hierarchical keys, include the path structure explicitly.
    """

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
    """Delete cache entries matching a pattern.

    CACHE_KEY is a glob pattern to match entries for deletion.

    \b
    Pattern examples:
      nuthatch delete "mydata"      # Delete exact key 'mydata'
      nuthatch delete "mydata*"     # Delete keys starting with 'mydata' (single-level)
      nuthatch delete "proj/*"      # Delete all keys under 'proj/' (multi-level)

    Note: Patterns without '/' only match single-level cache keys.
    """
    caches = list_helper(cache_key, namespace, backend, location)
    global root_module
    config = NuthatchConfig(wrapped_module=root_module)

    if len(caches) == 0:
        print("No caches found to delete.")
        return

    click.confirm(f"Are you sure you want to delete {len(caches)} cache entries?\n{str(caches[['cache_key', 'backend']])}\n", abort=True)

    for cache_entry in caches.to_dict(orient='records'):
        if cache_entry['backend'] == 'null':
            cache = Cache(config[location], cache_entry['cache_key'], namespace, None, None, None, {})
        else:
            cache = Cache(config[location], cache_entry['cache_key'], namespace, None, None, cache_entry['backend'], {})
        click.echo(f"Deleting {cache.cache_key} from {location} with backend {cache.backend_name}.")
        if metadata_only:
            cache._delete_metadata()
        else:
            cache.delete()

@cli.command('cp')
@click.argument('cache_key')
@click.option('--namespace', help='Namespace for the cache')
@click.option('--from-location', help='Location to copy data to', default='root')
@click.option('--to-location', help='Location to copy data to', default='mirror')
def copy_cache(cache_key, namespace, from_location, to_location):
    """Copy cache entries between locations.

    Copies data matching CACHE_KEY from one location to another
    (e.g., from root to a mirror or local cache).

    \b
    Examples:
      nuthatch cp "mydata*" --from-location root --to-location local
      nuthatch cp "proj/*" --to-location mirror
    """
    caches = list_helper(cache_key, namespace, None, from_location)
    from_config = get_config(location=from_location, requested_parameters=Cache.config_parameters)
    to_config = get_config(location=to_location, requested_parameters=Cache.config_parameters)[to_location]
    print(to_config)

    if len(caches) == 0:
        print("No caches found to copy.")
        return

    click.confirm(f"Are you sure you want to copy {len(caches)} cache entries?\n{str(caches[['cache_key', 'backend']])}\n", abort=True)

    for cache in caches.to_dict(orient='records'):
        if cache['backend'] == 'null':
            from_cache = Cache(from_config, cache['cache_key'], namespace, None, None, from_location, None, {})
            to_cache = Cache(to_config, cache['cache_key'], namespace, None, None, to_location, None, {})
        else:
            from_cache = Cache(from_config, cache['cache_key'], namespace, None, None, from_location, cache['backend'], {})
            to_cache = Cache(to_config, cache['cache_key'], namespace, None, None, to_location, cache['backend'], {})

        click.echo(f"Copy {cache.cache_key} from {from_location} to {to_location} with backend {from_cache.backend}.")
        to_cache.sync(from_cache)



@cli.command('print-config')
@click.option('--location', help='Location to search', default='root')
@click.option('--backend', help='Backend to use')
@click.option('--show-secrets', '-s', is_flag=True, help='Show secret values instead of masking them.')
def get_config_value(location, backend, show_secrets):
    """Print nuthatch configuration.

    Displays the current configuration for a location and optionally
    a specific backend. Secrets are masked by default.

    \b
    Examples:
      nuthatch print-config                    # Show root config
      nuthatch print-config --backend zarr     # Show zarr backend config
      nuthatch print-config -s                 # Show secrets unmasked
    """
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
