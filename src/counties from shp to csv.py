"""Join admin names from a shapefile to CSV lon/lat points."""

from __future__ import annotations

import argparse
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
) -> None:
    polygons = load_polygons(shp_path, admin_field)
    points = load_points(csv_path, lon_col, lat_col)
    joined = gpd.sjoin(points, polygons, how="left", predicate="within")
    joined = joined.drop(columns=["geometry", "index_right"])
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    joined.to_csv(output_csv, index=False)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Join admin names from a shapefile to CSV points by longitude/latitude."
        )
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path(r"D:/cmafiles/L/database/nighttime/Precess/Presults/ntl_vza.csv"),
        help="Input CSV file containing longitude/latitude columns.",
    )
    parser.add_argument(
        "--shp",
        type=Path,
        default=Path(r"D:/cmafiles/L/database/gis/中国专题图/省级数据/海南省/海南省.shp"),
        help="Input shapefile containing polygon admin boundaries.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(r"D:/cmafiles/L/database/nighttime/Precess/Presults/ntl_vza_shp.csv"),
        help="Output CSV path with joined admin names.",
    )
    parser.add_argument(
        "--lon-col",
        type=str,
        default="lon",
        help="Longitude column name in the CSV.",
    )
    parser.add_argument(
        "--lat-col",
        type=str,
        default="lat",
        help="Latitude column name in the CSV.",
    )
    parser.add_argument(
        "--admin-field",
        type=str,
        default="分县连接成",
        help="Admin field name in the shapefile to export.",
    )
    args = parser.parse_args()
    join_admin_name(
        csv_path=args.csv,
        shp_path=args.shp,
        output_csv=args.output,
        lon_col=args.lon_col,
        lat_col=args.lat_col,
        admin_field=args.admin_field,
    )


if __name__ == "__main__":
    main()
