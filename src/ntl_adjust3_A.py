import pandas as pd


INPUT_CSV = "D:/cmafiles/L/database/nighttime/Precess/Presults/ntl_adjusted2_wdav.csv"
OUTPUT_CSV = "D:/cmafiles/L/database/nighttime/Precess/Presults/ntl_adjusted3_A.csv"


def add_pixel_id(df: pd.DataFrame) -> pd.DataFrame:
    coords = df[["lon", "lat"]].apply(tuple, axis=1)
    df = df.copy()
    df["pixel_id"] = pd.factorize(coords)[0] + 1
    return df


def add_date_group(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    min_dates = df.groupby("pixel_id")["date"].transform("min")
    day_offset = (df["date"] - min_dates).dt.days
    df["date_group"] = (day_offset.mod(16) + 1).astype(int)
    df["group"] = df["pixel_id"].astype(str) + "_" + df["date_group"].astype(str)
    return df


def compute_stats(df: pd.DataFrame) -> pd.DataFrame:
    df = add_pixel_id(df)
    df = add_date_group(df)

    filtered = df[df["is_extreme"] == "F"]

    ntl_yr = (
        filtered.groupby("pixel_id")["ntl_match"]
        .mean()
        .rename("ntl_yr")
        .reset_index()
    )

    group_stats = (
        filtered.groupby(["pixel_id", "date_group", "group"])["ntl_match"]
        .mean()
        .rename("ntl_match_mean")
        .reset_index()
    )

    result = df.merge(ntl_yr, on="pixel_id", how="left")
    result = result.merge(group_stats, on=["pixel_id", "date_group", "group"], how="left")
    result["A"] = result["ntl_match_mean"] / result["ntl_yr"]
    return result


data = pd.read_csv(INPUT_CSV)
stats = compute_stats(data)
stats.to_csv(OUTPUT_CSV, index=False)