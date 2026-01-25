import numpy as np
import pandas as pd

PIXEL_SIZE = 1 / 240
INPUT_CSV = "D:/cmafiles/L/database/nighttime/Precess/Presults/ntl_adjusted1_extreme.csv"
OUTPUT_CSV = "D:/cmafiles/L/database/nighttime/Precess/Presults/ntl_adjusted2_wdav.csv"


def add_pixel_indices(data: pd.DataFrame) -> pd.DataFrame:
    data = data.copy()
    data["ix"] = np.floor(data["lon"] / PIXEL_SIZE).astype(int)
    data["iy"] = np.floor(data["lat"] / PIXEL_SIZE).astype(int)
    data["lon_center"] = (data["ix"] + 0.5) * PIXEL_SIZE
    data["lat_center"] = (data["iy"] + 0.5) * PIXEL_SIZE
    return data


def compute_window_mean(pixel_means: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    base_cols = group_cols + ["ix", "iy", "lon_center", "lat_center"]
    base = pixel_means[base_cols].copy()
    neighbor_frames = []
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            neighbor = pixel_means[group_cols + ["ix", "iy", "ntl_mis"]].copy()
            neighbor["ix"] = neighbor["ix"] - dx
            neighbor["iy"] = neighbor["iy"] - dy
            neighbor = neighbor.rename(columns={"ntl_mis": f"ntl_mis_{dx}_{dy}"})
            neighbor_frames.append(neighbor)

    merged = base
    join_cols = group_cols + ["ix", "iy"]
    for frame in neighbor_frames:
        merged = merged.merge(frame, on=join_cols, how="left")

    neighbor_cols = [col for col in merged.columns if col.startswith("ntl_mis_")]
    merged["ntl_mis_33"] = merged[neighbor_cols].mean(axis=1, skipna=True)
    return merged[group_cols + ["ix", "iy", "ntl_mis_33"]]


def main() -> None:
    df = pd.read_csv(INPUT_CSV)
    required = {"lon", "lat", "ntl_mis"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    group_cols = []
    if "date" in df.columns:
        group_cols.append("date")

    df = add_pixel_indices(df)
    pixel_means = df.groupby(group_cols + ["ix", "iy", "lon_center", "lat_center"], as_index=False)[
        "ntl_mis"
    ].mean()

    result = compute_window_mean(pixel_means, group_cols)
    df = df.merge(result, on=group_cols + ["ix", "iy"], how="left")
    df.to_csv(OUTPUT_CSV, index=False)


if __name__ == "__main__":
    main()