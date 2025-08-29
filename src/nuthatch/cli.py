#!/usr/bin/env python3
"""
Command-line interface for nuthatch.

This module provides a CLI for interacting with nuthatch caching functionality,
including cache management, backend operations, and configuration.
"""

import click
import shutil
import pprint
from .config import get_config
from .backend import get_backend_by_name
from .cache import Cache
import pandas as pd


@click.group()
@click.version_option(version="0.1.0", prog_name="nuthatch")
def cli():
    """Nuthatch - Intelligent caching system for data science workflows.

    This CLI provides tools for managing cache entries, inspecting backends,
    and configuring the nuthatch system.
    """
    pass

@cli.command('import')
@click.argument('cache_key')
@click.option('--namespace', help='Namespace for the cache')
@click.option('--backend', help='Backend to use', default='infer')
def import_data(glob, namespace, backend):
    """Import data from a glob pattern."""
    if backend == 'infer':
        click.echo(f"Importing data from {glob} with namespace {namespace}.")
    else:
        click.echo(f"Importing data from {glob} with namespace {namespace} and backend {backend}")

    # Is the cache key file-like or database-like?

@cli.command('list')
@click.argument('cache_key', required=False)
@click.option('--namespace', help='Namespace for the cache')
@click.option('--backend', help='Backend filter')
@click.option('--location', help='Location to search', default='root')
@click.option('--long', '-l', is_flag=True, help='List all information about the cache')
def list_caches(cache_key, namespace, backend, location, long):
    """List all cache entries."""
    config = get_config(location=location, requested_parameters=Cache.config_parameters)
    cache = Cache(config, None, namespace, None, location, backend, {})

    if cache_key is None:
        cache_key = '*'

    caches = cache.list(cache_key)

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
def delete_cache(cache_key, namespace, backend, location, force):
    """Clear cache entries."""
    caches = list_caches(cache_key, namespace, backend, location)
    config = get_config(location=location, requested_parameters=Cache.config_parameters)

    click.confirm(f"Are you sure you want to delete {len(caches)} cache entries?", abort=True)

    for cache in caches:
        cache = Cache(config, cache.cache_key, cache.namespace, None, location, cache.backend_name, {})
        cache.delete()


@cli.command('print-config')
@click.option('--location', help='Location to search', default='root')
@click.option('--backend', help='Backend to use')
def get_config_value(location, backend):
    """Get configuration value for a specific key."""
    if backend:
        backend_class = get_backend_by_name(backend)
        backend_name = backend
    else:
        backend_class = Cache
        backend_name = None

    config = get_config(location=location, requested_parameters=backend_class.config_parameters,
                        backend_name=backend_name)

    click.echo(pprint.pformat(config))

def main():
    return cli()


if __name__ == '__main__':
    main()
