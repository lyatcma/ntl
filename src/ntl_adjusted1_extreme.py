import pandas as pd

DEFAULT_INPUT_CSV = "D:/cmafiles/L/database/nighttime/Precess/Presults/ntl_vza.csv"
DEFAULT_OUTPUT_CSV = "D:/cmafiles/L/database/nighttime/Precess/Presults/ntl_adjusted1.csv"


def mark_extremes(df: pd.DataFrame) -> pd.DataFrame:
    required_columns = {"date", "lon", "lat", "vza", "ntl"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(sorted(missing))}")

    df = df.copy()
    group_cols = ["lon", "lat"]

    stats = df.groupby(group_cols)["ntl"].agg(["mean", "std"]).reset_index()
    stats = stats.rename(columns={"mean": "ntl_mean", "std": "ntl_std"})
    df = df.merge(stats, on=group_cols, how="left")

    df["is_extreme"] = (df["ntl"] - df["ntl_mean"]).abs() > 3 * df["ntl_std"]
    df["is_extreme"] = df["is_extreme"].map({True: "T", False: "F"})

    non_extreme = df[df["is_extreme"] == "F"].copy()

    def pixel_fix(pixel_series: pd.Series) -> float:
        if pixel_series.empty:
            return float("nan")
        threshold = pixel_series.quantile(0.05)
        low_values = pixel_series[pixel_series <= threshold]
        if low_values.empty:
            return float("nan")
        return low_values.mean()

    ntl_fix = (
        non_extreme.groupby(group_cols)["ntl"]
        .apply(pixel_fix)
        .rename("ntl_fix")
        .reset_index()
    )

    df = df.merge(ntl_fix, on=group_cols, how="left")
    df = df.drop(columns=["ntl_mean", "ntl_std"])
    return df


def main() -> None:
    data = pd.read_csv(DEFAULT_INPUT_CSV)
    result = mark_extremes(data)
    result.to_csv(DEFAULT_OUTPUT_CSV, index=False)


if __name__ == "__main__":
    main()