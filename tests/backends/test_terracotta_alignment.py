import os
from pathlib import Path

import fsspec
import numpy as np
import pytest
import rasterio
import xarray as xr
from pyproj import Transformer
from rasterio.transform import rowcol, xy
from shapely.geometry import box, mapping
from terracotta import get_driver
from rasterio.enums import Resampling

from nuthatch.backends.terracotta import TerracottaBackend

GRID_RESOLUTIONS = [1.5, 0.25, 0.1]
WEB_MERCATOR = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)


def _northup_source_dataset(resolution_degrees):
    # Regular lat/lon grid with global longitude coverage and Mercator-safe latitudes.
    half_res = resolution_degrees / 2.0
    lons = np.arange(-180.0 + half_res, 180.0, resolution_degrees)
    lats = np.arange(84.0 - half_res, -84.0, -resolution_degrees)
    values = np.arange(len(lats) * len(lons), dtype=float).reshape(len(lats), len(lons))
    return xr.Dataset(
        {"precip": (("lat", "lon"), values)},
        coords={"lat": lats, "lon": lons},
    )

AFRICA_BOUNDS = (-19.5, -40.5, 55.5, 40.5)
def _clip_source_to_pseudo_africa(ds):
    return (
        ds.rio.write_crs("EPSG:4326")
        .rio.set_spatial_dims("lon", "lat")
        .rio.clip(
            [mapping(box(*AFRICA_BOUNDS))],
            "EPSG:4326",
            drop=True,
        )
    )


def _make_test_backend(tmp_path: Path, cache_key: str):
    backend = TerracottaBackend.__new__(TerracottaBackend)
    backend.lat_dim = "lat"
    backend.lon_dim = "lon"
    backend.time_dim = "time"
    backend.resampling = Resampling.nearest
    backend.cache_key = cache_key
    backend.path = str(tmp_path / f"{cache_key}.terracotta")
    backend.override_path = backend.path
    backend.fs = fsspec.filesystem("file")
    Path(backend.path).mkdir(parents=True, exist_ok=True)
    backend.driver = get_driver(tmp_path / "terracotta_scope.sqlite", provider="sqlite")
    try:
        backend.driver.get_keys()
    except Exception:
        backend.driver.create(["key"])
    return backend


def _reference_points_from_dataset(ds):
    lon_indices = [ds.sizes["lon"] // 6, ds.sizes["lon"] // 2, (5 * ds.sizes["lon"]) // 6]
    lat_indices = [ds.sizes["lat"] // 6, ds.sizes["lat"] // 2, (5 * ds.sizes["lat"]) // 6]
    return [
        (float(ds.lon.values[lon_idx]), float(ds.lat.values[lat_idx]))
        for lon_idx, lat_idx in zip(lon_indices, lat_indices, strict=False)
    ]


@pytest.mark.parametrize("resolution_degrees", GRID_RESOLUTIONS)
def test_terracotta_write_preserves_latlon_resolution_for_crops(tmp_path, resolution_degrees):
    source = _northup_source_dataset(resolution_degrees)
    clipped = _clip_source_to_pseudo_africa(source)

    global_backend = _make_test_backend(tmp_path, "global")
    africa_backend = _make_test_backend(tmp_path, "africa")
    global_backend.write(source)
    africa_backend.write(clipped)

    global_path = tmp_path / "global.terracotta" / "_.tif"
    africa_path = tmp_path / "africa.terracotta" / "_.tif"

    with (
        rasterio.open(global_path) as global_tif,
        rasterio.open(africa_path) as africa_tif,
    ):
        assert global_tif.crs == africa_tif.crs
        assert global_tif.res == pytest.approx(africa_tif.res)

@pytest.mark.parametrize("resolution_degrees", GRID_RESOLUTIONS)
def test_terracotta_returns_same_tile_values_for_global_and_africa_scopes(tmp_path, resolution_degrees):
    source = _northup_source_dataset(resolution_degrees)
    clipped = _clip_source_to_pseudo_africa(source)

    global_backend = _make_test_backend(tmp_path, "global")
    africa_backend = _make_test_backend(tmp_path, "africa")
    global_backend.write(source)
    africa_backend.write(clipped)
    driver = global_backend.driver

    tile_bounds = driver.get_metadata(("africa",))["bounds"]
    global_tile = driver.get_raster_tile(
        {"key": "global"},
        tile_bounds=tile_bounds,
        tile_size=(256, 256),
        preserve_values=True,
    )
    africa_tile = driver.get_raster_tile(
        {"key": "africa"},
        tile_bounds=tile_bounds,
        tile_size=(256, 256),
        preserve_values=True,
    )

    assert np.array_equal(global_tile.filled(-9999), africa_tile.filled(-9999))


@pytest.mark.parametrize("resolution_degrees", GRID_RESOLUTIONS)
def test_terracotta_write_projects_reference_points_consistently(tmp_path, resolution_degrees):
    source = _northup_source_dataset(resolution_degrees)
    clipped = _clip_source_to_pseudo_africa(source)

    global_backend = _make_test_backend(tmp_path, "global")
    africa_backend = _make_test_backend(tmp_path, "africa")
    global_backend.write(source)
    africa_backend.write(clipped)

    reference_points = _reference_points_from_dataset(clipped)

    global_path = tmp_path / "global.terracotta" / "_.tif"
    africa_path = tmp_path / "africa.terracotta" / "_.tif"

    with (
        rasterio.open(global_path) as global_tif,
        rasterio.open(africa_path) as africa_tif,
    ):
        for lon, lat in reference_points:
            expected_x, expected_y = WEB_MERCATOR.transform(lon, lat)
            observed_centers = []

            for tif in (global_tif, africa_tif):
                row, col = rowcol(tif.transform, expected_x, expected_y)
                assert 0 <= row < tif.height
                assert 0 <= col < tif.width

                observed_x, observed_y = xy(tif.transform, row, col, offset="center")
                observed_centers.append((observed_x, observed_y))
                assert observed_x == pytest.approx(expected_x, abs=tif.res[0] / 2.0)
                assert observed_y == pytest.approx(expected_y, abs=tif.res[1] / 2.0)

            assert observed_centers[0][0] == pytest.approx(observed_centers[1][0])
            assert observed_centers[0][1] == pytest.approx(observed_centers[1][1])
