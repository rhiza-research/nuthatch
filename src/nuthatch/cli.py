#!/usr/bin/env python3
"""
Command-line interface for nuthatch.

This module provides a CLI for interacting with nuthatch caching functionality,
including cache management, backend operations, and configuration.
"""

import importlib
import click
import shutil
from .config import get_config
from .backend import get_backend_by_name, registered_backends
from .cache import Cache
import pandas as pd


@click.group()
@click.version_option(version="0.1.0", prog_name="nuthatch")
def cli():
    """Nuthatch - Intelligent caching system for data science workflows.

    This CLI provides tools for managing cache entries, inspecting backends,
    and configuring the nuthatch system.
    """

    config = get_config(location='root', requested_parameters=['dynamic_config_path'], backend_name=None)
    if 'dynamic_config_path' in config:
        try:
            importlib.import_module(config['dynamic_config_path'])
        except Exception as e:
            click.echo(f"WARNGIN: Failed to import {config['dynamic_config_path']} with '{e}'. You may be missing dynamic secret resolution.")



@cli.command('import')
@click.argument('cache_key')
@click.option('--namespace', help='Namespace for the cache')
@click.option('--backend', help='Backend to use', required=True)
@click.option('--location', help='Location to search', default='root')
def import_data(cache_key, namespace, backend, location):
    """Import data from a glob pattern."""

    # First instantiate the backend based on the passed backend
    backend_name = backend
    backend_class = get_backend_by_name(backend)
    config = get_config(location=location, requested_parameters=backend_class.config_parameters, backend_name=backend_class.backend_name)
    backend = backend_class(config, cache_key, namespace, None, {})

    cache_keys = []
    if hasattr(backend, 'fs') and backend.fs is not None:
        paths = backend.fs.glob(backend.path)
        for path in paths:
            cache_keys.append(backend.get_cache_key(path))

    if len(cache_keys) > 0:
        click.confirm(f"Are you sure you want to import {len(paths)} cache entries?", abort=True)
    else:
        print("No caches found for import.")

    for key in cache_keys:
        print(f"Importing {key}.")

        if backend_name == 'null':
            config = get_config(location=location, requested_parameters=Cache.config_parameters)
            cache = Cache(config, key, namespace, None, location, None, {})
            if cache.is_null():
                print(f"{key} already in cache as null!")
            elif cache.exists():
                print(f"Cache {key} already exists and is valid. Skipping entry. Delete this cache key if you would like to reimport it as null.")
            else:
                cache.set_null()
                print(f"Set {key} successfully to null.")
        else:
            config = get_config(location=location, requested_parameters=Cache.config_parameters)
            cache = Cache(config, key, namespace, None, location, backend_name, {})
            if not cache.exists():
                cache._commit_metadata()
                print(f"Imported {key} successfully.")
            else:
                print(f"{key} already in cache!")

def list_helper(cache_key, namespace, backend, location):
    """List all cache entries."""
    config = get_config(location=location, requested_parameters=Cache.config_parameters)
    cache = Cache(config, None, namespace, None, location, backend, {})

    if cache_key is None:
        cache_key = '*'

    caches = cache.list(cache_key)

    return caches

@cli.command('list')
@click.argument('cache_key', required=False)
@click.option('--namespace', help='Namespace for the cache')
@click.option('--backend', help='Backend filter')
@click.option('--location', help='Location to search', default='root')
@click.option('--long', '-l', is_flag=True, help='List all information about the cache')
def list_caches(cache_key, namespace, backend, location, long):

    caches = list_helper(cache_key, namespace, backend, location)
    pager = len(caches) > shutil.get_terminal_size()[0]

    if not long:
        caches = [cache['cache_key'] for cache in caches]
        caches = '\n'.join(caches)
    else:
        caches = pd.DataFrame(caches)
        caches['last_modified'] = pd.to_datetime(caches['last_modified'], unit='us').dt.floor('s')
        caches = caches[['cache_key', 'namespace', 'backend', 'state', 'last_modified', 'user', 'commit_hash', 'path']]
        caches = caches.to_string()


    if pager:
        click.echo_via_pager(caches)
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
    config = get_config(location=location, requested_parameters=Cache.config_parameters)

    click.confirm(f"Are you sure you want to delete {len(caches)} cache entries?", abort=True)

    for cache in caches:
        cache = Cache(config, cache.cache_key, cache.namespace, None, location, cache.backend, {})
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
    if backend:
        backend_classes = [get_backend_by_name(backend)]
    else:
        backend_classes = [Cache] + list(registered_backends.values())

    for backend_class in backend_classes:
        if show_secrets:
            config = get_config(location=location, requested_parameters=backend_class.config_parameters,
                                backend_name=backend_class.backend_name, mask_secrets=False)
        else:
            config = get_config(location=location, requested_parameters=backend_class.config_parameters,
                                backend_name=backend_class.backend_name, mask_secrets=True)

        click.echo(backend_class.backend_name.title())
        for key, value in config.items():
            click.echo(f"\t{key}: {value}")
        click.echo()

def main():
    return cli()


if __name__ == '__main__':
    main()
