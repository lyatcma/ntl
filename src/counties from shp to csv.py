"""Join admin names from a shapefile to CSV lon/lat points."""

from __future__ import annotations

import warnings
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point


def load_polygons(shp_path: Path, admin_field: str) -> gpd.GeoDataFrame:
    polygons = gpd.read_file(shp_path)
    if admin_field not in polygons.columns:
        raise ValueError(f"Field '{admin_field}' not found in shapefile.")
    if polygons.crs is None:
        warnings.warn(
            "Shapefile has no CRS; assuming EPSG:4326 for polygons.",
            stacklevel=2,
        )
        polygons = polygons.set_crs("EPSG:4326")
    else:
        polygons = polygons.to_crs("EPSG:4326")
    return polygons[[admin_field, "geometry"]]


def load_points(csv_path: Path, lon_col: str, lat_col: str) -> gpd.GeoDataFrame:
    data = pd.read_csv(csv_path)
    for col in (lon_col, lat_col):
        if col not in data.columns:
            raise ValueError(f"Column '{col}' not found in CSV.")
    geometry = [Point(xy) for xy in zip(data[lon_col], data[lat_col])]
    return gpd.GeoDataFrame(data, geometry=geometry, crs="EPSG:4326")


def join_admin_name(
    csv_path: Path,
    shp_path: Path,
    output_csv: Path,
    lon_col: str,
    lat_col: str,
    admin_field: str,
    output_field: str,
) -> None:
    polygons = load_polygons(shp_path, admin_field)
    points = load_points(csv_path, lon_col, lat_col)
    joined = gpd.sjoin(points, polygons, how="left", predicate="within")
    joined = joined.rename(columns={admin_field: output_field})
    joined = joined.drop(columns=["geometry", "index_right"])
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    joined.to_csv(output_csv, index=False)


def main() -> None:
    csv_path = Path(r"D:/cmafiles/L/database/nighttime/Precess/Presults/ntl_adjusted3_A.csv")
    shp_path = Path(r"D:/cmafiles/L/database/gis/中国专题图/省级数据/海南省/海南省.shp")
    output_csv = Path(r"D:/cmafiles/L/database/nighttime/Precess/Presults/ntl_adjusted_shp.csv")
    lon_col = "lon"
    lat_col = "lat"
    admin_field = "分县连接成"
    output_field = "county"
    join_admin_name(
        csv_path=csv_path,
        shp_path=shp_path,
        output_csv=output_csv,
        lon_col=lon_col,
        lat_col=lat_col,
        admin_field=admin_field,
        output_field=output_field,
    )


if __name__ == "__main__":
    main()