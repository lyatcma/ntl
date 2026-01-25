"""Export paired NTL (A1) and VZA (A2) GeoTIFFs to CSV."""

from __future__ import annotations

import argparse
import csv
import re
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import rasterio


DATE_PATTERN = re.compile(r"A(\d{4})(\d{3})")


def parse_date_from_name(name: str) -> str:
    match = DATE_PATTERN.search(name)
    if not match:
        raise ValueError(f"Unable to parse date from filename: {name}")
    year = int(match.group(1))
    day_of_year = int(match.group(2))
    date = datetime(year, 1, 1) + timedelta(days=day_of_year - 1)
    return date.strftime("%Y-%m-%d")


def build_file_map(files: list[Path]) -> dict[str, Path]:
    mapping: dict[str, Path] = {}
    for file_path in files:
        date_token = DATE_PATTERN.search(file_path.name)
        if not date_token:
            continue
        mapping[date_token.group(0)] = file_path
    return mapping


def find_pairs(
    ntl_dir: Path,
    vza_dir: Path,
    ntl_pattern: str,
    vza_pattern: str,
) -> list[tuple[Path, Path]]:
    ntl_files = sorted(ntl_dir.glob(ntl_pattern))
    if not ntl_files:
        raise FileNotFoundError(f"No NTL files found in {ntl_dir}")

    vza_files = sorted(vza_dir.glob(vza_pattern))
    if not vza_files:
        raise FileNotFoundError(f"No VZA files found in {vza_dir}")

    vza_map = build_file_map(vza_files)
    pairs: list[tuple[Path, Path]] = []
    for ntl_file in ntl_files:
        date_token = DATE_PATTERN.search(ntl_file.name)
        if not date_token:
            continue
        day_key = date_token.group(0)
        vza_file = vza_map.get(day_key)
        if not vza_file:
            raise FileNotFoundError(f"Missing VZA file for {ntl_file.name}")
        pairs.append((ntl_file, vza_file))
    if not pairs:
        raise FileNotFoundError("No matching NTL/VZA file pairs found.")
    return pairs


def export_csv(
    ntl_dir: Path,
    vza_dir: Path,
    output_csv: Path,
    ntl_pattern: str,
    vza_pattern: str,
) -> None:
    pairs = find_pairs(ntl_dir, vza_dir, ntl_pattern, vza_pattern)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["date", "lon", "lat", "ntl", "vza"])

        for ntl_path, vza_path in pairs:
            date = parse_date_from_name(ntl_path.name)
            with rasterio.open(ntl_path) as ntl_src, rasterio.open(vza_path) as vza_src:
                ntl = ntl_src.read(1)
                vza = vza_src.read(1)

                if ntl.shape != vza.shape:
                    raise ValueError(
                        f"Shape mismatch for {ntl_path.name} and {vza_path.name}"
                    )

                mask = np.isfinite(ntl)
                rows, cols = np.where(mask)
                if rows.size == 0:
                    continue

                xs, ys = rasterio.transform.xy(
                    ntl_src.transform, rows, cols, offset="center"
                )
                for lon, lat, ntl_val, vza_val in zip(xs, ys, ntl[rows, cols], vza[rows, cols]):
                    writer.writerow([date, lon, lat, float(ntl_val), float(vza_val)])


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Export paired NTL (VNP46A1) and VZA (VNP46A2) GeoTIFFs to CSV."
        )
    )
    parser.add_argument(
        "--ntl-dir",
        type=Path,
        default=Path(r"D:/cmafiles/L/database/nighttime/Precess/Presults/VNP46A2"),
        help="Directory containing VNP46A2 (NTL) *_presult.tif files.",
    )
    parser.add_argument(
        "--vza-dir",
        type=Path,
        default=Path(r"D:/cmafiles/L/database/nighttime/Precess/Presults/VNP46A1"),
        help="Directory containing VNP46A1 (VZA) *_presult.tif files.",
    )
    parser.add_argument(
        "--ntl-pattern",
        type=str,
        default="VNP46A2_A*_presult.tif",
        help="Glob pattern for NTL files (VNP46A2).",
    )
    parser.add_argument(
        "--vza-pattern",
        type=str,
        default="VNP46A1_A*_presult.tif",
        help="Glob pattern for VZA files (VNP46A1).",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=Path(r"D:/cmafiles/L/database/nighttime/Precess/Presults/ntl_vza.csv"),
        help="Output CSV path.",
    )
    args = parser.parse_args()
    export_csv(
        args.ntl_dir,
        args.vza_dir,
        args.output_csv,
        args.ntl_pattern,
        args.vza_pattern,
    )


if __name__ == "__main__":
    main()
