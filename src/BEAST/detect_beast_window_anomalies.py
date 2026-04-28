from pathlib import Path

import numpy as np
import pandas as pd


INPUT_FILE = Path(
    "C:/Users/liuy1/Desktop/0.文章/3.重点研发Chao/0.nightime/data/ntl/Presults/unified/ntl_2022_2024_unified.csv"
)
# BEAST 像元级结果目录（读取 time_series.csv 的输入源）
RESULT_DIR = Path(
    "C:/Users/liuy1/Desktop/0.文章/3.重点研发Chao/0.nightime/data/ntl/BEAST/pixelevel/anomaly/20260427_daily"
)
# 窗口异常与恢复检测结果的输出目录（本脚本生成的 CSV 写入此处）
OUTPUT_DIR = Path(
    "C:/Users/liuy1/Desktop/0.文章/3.重点研发Chao/0.nightime/data/ntl/BEAST/pixelevel/recovery/20260427"
)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

USE_BEAST_REGULAR_TIME = True
LANDFALL_DATE = pd.Timestamp("2024-09-05")   # 事件参考日（登陆日）
PRE_DAYS = 5                                  # 事件前纳入检测窗口的天数
POST_DAYS = 90                               # 事件后纳入检测窗口的天数
BASELINE_DAYS = 180                           # 基线窗口长度（天）
MIN_BASELINE_OBS = 3                          # 基线有效观测数下限（低于此数则不做异常判定）
EXCLUDE_DETECTION_WINDOW_FROM_BASELINE = True # 基线是否排除检测窗口本身

# ---- 恢复判定参数（主要调参入口） ----
# 恢复阈值：observed >= trend 记为"恢复中"
#   trend 为 BEAST 分解得到的去季节/异常后的本底趋势，用作恢复参考水平。
RECOVERY_PERSIST_DAYS = 2                             # 连续满足恢复条件的天数阈值（持续性）
RECOVERY_END_DATE = pd.Timestamp("2024-12-31")        # 恢复追踪终止日期（数据时序末端）
# 恢复起点：默认从每像元"首个异常日期"起向后追踪。
#   如需改为"最低点起跳"，可将 detect_anomalies 中 recovery_start 从 first_anomaly_date 改为 min_date。

PIXEL_RESULT_CSV = OUTPUT_DIR / "window_anomaly_pixel_level.csv"
ANOMALY_RECORD_CSV = OUTPUT_DIR / "window_anomaly_records.csv"
COUNTY_SUMMARY_CSV = OUTPUT_DIR / "window_anomaly_county_summary.csv"


def beast_time_to_date(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    if numeric.notna().mean() < 0.8:
        return pd.to_datetime(values, errors="coerce")

    out = pd.Series(pd.NaT, index=values.index, dtype="datetime64[ns]")
    ok = numeric.notna()
    years = np.floor(numeric[ok]).astype(int)
    year_start = pd.to_datetime(years.astype(str) + "-01-01")
    year_end = pd.to_datetime((years + 1).astype(str) + "-01-01")
    days_in_year = (year_end - year_start).dt.days
    offset_days = np.round((numeric[ok] - years) * days_in_year).astype(int)
    out.loc[ok] = year_start + pd.to_timedelta(offset_days, unit="D")
    return out


def read_time_series(result_dir: Path) -> pd.DataFrame:
    files = [
        p
        for p in result_dir.rglob("time_series.csv")
        if "plots" not in p.parts and p.parent != result_dir
    ]
    if not files:
        raise FileNotFoundError(f"No county time_series.csv files found under {result_dir}")

    data = []
    for p in files:
        d = pd.read_csv(p, encoding="utf-8-sig")
        d["source_file"] = str(p)
        data.append(d)

    out = pd.concat(data, ignore_index=True)
    out.columns = [c.strip() for c in out.columns]
    return out


def build_date_map(input_file: Path) -> pd.DataFrame:
    src = pd.read_csv(input_file, encoding="utf-8-sig")
    src.columns = [c.strip().lower() for c in src.columns]

    required = {"date", "pixel_id", "county"}
    missing = required.difference(src.columns)
    if missing:
        raise ValueError(f"Input file is missing required columns: {sorted(missing)}")

    src["date"] = pd.to_datetime(src["date"], errors="coerce")
    src["pixel_id"] = src["pixel_id"].astype(str)
    src["county"] = src["county"].astype(str).replace({"nan": "unknown_county", "": "unknown_county"})
    src = src.dropna(subset=["date", "pixel_id", "county"])

    # Same time axis as the R BEAST script: one row per observed date per county/pixel_id.
    date_map = (
        src[["county", "pixel_id", "date"]]
        .drop_duplicates()
        .sort_values(["county", "pixel_id", "date"])
        .copy()
    )
    date_map["t"] = date_map.groupby(["county", "pixel_id"]).cumcount() + 1
    return date_map


def prepare_data(df: pd.DataFrame, date_map: pd.DataFrame | None = None) -> pd.DataFrame:
    required = {"county", "pixel_id", "t", "observed"}
    if USE_BEAST_REGULAR_TIME:
        required.add("time")
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    if "season_comp" not in df.columns:
        df["season_comp"] = 0.0
    if "trend" not in df.columns:
        # 恢复判定依赖 trend；若缺失则置 NaN，相应像元的 recovery_date 将保持 NaT。
        df["trend"] = np.nan

    for col in ["observed", "season_comp", "trend", "lon", "lat"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["pixel_id"] = df["pixel_id"].astype(str)
    df["t"] = pd.to_numeric(df["t"], errors="coerce").astype("Int64")

    if USE_BEAST_REGULAR_TIME:
        df["date"] = beast_time_to_date(df["time"])
    else:
        if date_map is None:
            raise ValueError("date_map is required when USE_BEAST_REGULAR_TIME is False")

        date_map["pixel_id"] = date_map["pixel_id"].astype(str)
        date_map["t"] = pd.to_numeric(date_map["t"], errors="coerce").astype("Int64")

        df = df.merge(
            date_map,
            on=["county", "pixel_id", "t"],
            how="left",
            validate="m:1",
        )

        if df["date"].isna().any():
            fallback = (
                date_map.drop(columns=["county"])
                .drop_duplicates(["pixel_id", "t"], keep=False)
                .rename(columns={"date": "date_fallback"})
            )
            df = df.merge(
                fallback,
                on=["pixel_id", "t"],
                how="left",
                validate="m:1",
            )
            df["date"] = df["date"].fillna(df["date_fallback"])
            df = df.drop(columns=["date_fallback"])

    df["NTL_deseason"] = df["observed"] - df["season_comp"]
    df = df.dropna(subset=["county", "pixel_id", "date"])
    return df.sort_values(["county", "pixel_id", "date"]).reset_index(drop=True)


def find_recovery_date(
    pixel_df: pd.DataFrame,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp = RECOVERY_END_DATE,
    persist_days: int = RECOVERY_PERSIST_DAYS,
) -> pd.Timestamp:
    """识别受影响像元的恢复日期。

    判定逻辑：
      - 恢复条件：observed >= trend（观测值回到 BEAST 趋势分量之上）
      - 持续性：连续 persist_days 天满足恢复条件，方认定为"已恢复"
      - 追踪范围：start_date（首个异常日期）到 end_date（时序末端）
      - 返回：首个满足持续性条件的连续段的起始日期；若始终未恢复则返回 NaT

    可调参数：
      - persist_days：持续性天数阈值（默认 7）
      - end_date：追踪终止日期（默认 2024-12-31）
      - 恢复判据列：当前使用 trend；若需改为基线均值，可在此替换阈值列
    """
    if pd.isna(start_date):
        return pd.NaT

    tracking = pixel_df[
        (pixel_df["date"] >= start_date) & (pixel_df["date"] <= end_date)
    ].sort_values("date")
    if tracking.empty:
        return pd.NaT

    # above[i] 表示第 i 个观测是否满足恢复条件；NaN 视为未恢复
    above = (tracking["observed"] >= tracking["trend"]).fillna(False).to_numpy()
    dates = tracking["date"].to_numpy()

    # 扫描连续 True 游程：一旦某段长度达到 persist_days，返回该段起始日期
    run_start_idx = None
    for i, flag in enumerate(above):
        if flag:
            if run_start_idx is None:
                run_start_idx = i
            if (i - run_start_idx + 1) >= persist_days:
                return pd.Timestamp(dates[run_start_idx])
        else:
            run_start_idx = None
    return pd.NaT


def detect_anomalies(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    detect_start = LANDFALL_DATE - pd.Timedelta(days=PRE_DAYS)
    detect_end = LANDFALL_DATE + pd.Timedelta(days=POST_DAYS)

    if EXCLUDE_DETECTION_WINDOW_FROM_BASELINE:
        baseline_end = detect_start - pd.Timedelta(days=1)
    else:
        baseline_end = LANDFALL_DATE - pd.Timedelta(days=1)
    baseline_start = baseline_end - pd.Timedelta(days=BASELINE_DAYS - 1)

    pixel_rows = []
    anomaly_rows = []

    group_cols = ["county", "pixel_id"]
    for (county, pixel_id), g in df.groupby(group_cols, sort=False):
        baseline = g[(g["date"] >= baseline_start) & (g["date"] <= baseline_end)]
        window = g[(g["date"] >= detect_start) & (g["date"] <= detect_end)].copy()

        mu = baseline["NTL_deseason"].mean()
        sigma = baseline["NTL_deseason"].std(ddof=1)
        threshold = mu - 2 * sigma if pd.notna(mu) and pd.notna(sigma) else np.nan

        enough_baseline = baseline["NTL_deseason"].notna().sum() >= MIN_BASELINE_OBS
        window["baseline_mu"] = mu
        window["baseline_sigma"] = sigma
        window["threshold_mu_minus_2sigma"] = threshold
        window["is_anomaly"] = False

        if enough_baseline and pd.notna(threshold):
            window["is_anomaly"] = window["NTL_deseason"] < threshold

        anomalies = window[window["is_anomaly"]].copy()

        min_idx = window["NTL_deseason"].idxmin() if window["NTL_deseason"].notna().any() else None
        min_date = window.loc[min_idx, "date"] if min_idx is not None else pd.NaT
        min_value = window.loc[min_idx, "NTL_deseason"] if min_idx is not None else np.nan

        # ---- 恢复日期识别 ----
        # 仅对"受影响像元"（anomaly_flag=True）计算恢复日期；
        # 起点：首个异常日期（first_anomaly_date）；
        # 终点：RECOVERY_END_DATE（默认 2024-12-31）；
        # 若需从最低点起跳，可将 recovery_start 改为 min_date。
        first_anomaly_date = anomalies["date"].min() if not anomalies.empty else pd.NaT
        recovery_start = first_anomaly_date
        recovery_date = find_recovery_date(
            pixel_df=g,
            start_date=recovery_start,
            end_date=RECOVERY_END_DATE,
            persist_days=RECOVERY_PERSIST_DAYS,
        )
        recovery_days = (
            int((recovery_date - first_anomaly_date).days)
            if pd.notna(recovery_date) and pd.notna(first_anomaly_date)
            else np.nan
        )

        # 将像元级恢复信息广播到该像元的每条异常记录，便于在 records 表中直接读取
        if not anomalies.empty:
            anomalies["first_anomaly_date"] = first_anomaly_date
            anomalies["recovery_persist_days"] = RECOVERY_PERSIST_DAYS
            anomalies["recovery_end_date"] = RECOVERY_END_DATE
            anomalies["recovery_date"] = recovery_date
            anomalies["recovery_days"] = recovery_days
            anomalies["recovered_flag"] = bool(pd.notna(recovery_date))
            anomaly_rows.append(anomalies)

        pixel_rows.append(
            {
                "county": county,
                "pixel_id": pixel_id,
                "lon": g["lon"].dropna().iloc[0] if "lon" in g and g["lon"].notna().any() else np.nan,
                "lat": g["lat"].dropna().iloc[0] if "lat" in g and g["lat"].notna().any() else np.nan,
                "baseline_start": baseline_start.date(),
                "baseline_end": baseline_end.date(),
                "baseline_n": int(baseline["NTL_deseason"].notna().sum()),
                "baseline_mu": mu,
                "baseline_sigma": sigma,
                "threshold_mu_minus_2sigma": threshold,
                "detect_start": detect_start.date(),
                "detect_end": detect_end.date(),
                "window_n": int(window["NTL_deseason"].notna().sum()),
                "anomaly_flag": bool(not anomalies.empty),
                "anomaly_obs_count": int(anomalies.shape[0]),
                "first_anomaly_date": first_anomaly_date.date() if pd.notna(first_anomaly_date) else pd.NaT,
                "min_window_date": min_date.date() if pd.notna(min_date) else pd.NaT,
                "min_window_NTL_deseason": min_value,
                # 恢复相关输出：
                #   recovery_date —— 首个满足 "observed>=trend 连续 RECOVERY_PERSIST_DAYS 天" 的起始日期
                #   recovery_days —— 从首个异常日到恢复日的间隔天数（NaN 表示追踪终点前未恢复）
                "recovery_persist_days": RECOVERY_PERSIST_DAYS,
                "recovery_end_date": RECOVERY_END_DATE.date(),
                "recovery_date": recovery_date.date() if pd.notna(recovery_date) else pd.NaT,
                "recovery_days": recovery_days,
                "recovered_flag": bool(pd.notna(recovery_date)),
            }
        )

    pixel_result = pd.DataFrame(pixel_rows)
    anomaly_records = pd.concat(anomaly_rows, ignore_index=True) if anomaly_rows else pd.DataFrame()

    county_summary = (
        pixel_result.groupby("county", dropna=False)
        .agg(
            pixel_total=("pixel_id", "nunique"),
            valid_baseline_pixels=("baseline_n", lambda x: int((x >= MIN_BASELINE_OBS).sum())),
            anomaly_pixel_count=("anomaly_flag", "sum"),
            recovered_pixel_count=("recovered_flag", "sum"),
            # 异常像元的平均/中位恢复时长（单位：天），可用于县级恢复速度评估
            mean_recovery_days=("recovery_days", "mean"),
            median_recovery_days=("recovery_days", "median"),
        )
        .reset_index()
    )
    county_summary["anomaly_pixel_ratio"] = (
        county_summary["anomaly_pixel_count"] / county_summary["pixel_total"]
    )
    # 异常像元中已恢复的比例（分母为 anomaly_pixel_count，注意为 0 时结果为 NaN）
    county_summary["recovered_ratio_in_anomaly"] = (
        county_summary["recovered_pixel_count"] / county_summary["anomaly_pixel_count"]
    )

    return pixel_result, anomaly_records, county_summary


def main() -> None:
    date_map = None if USE_BEAST_REGULAR_TIME else build_date_map(INPUT_FILE)
    df = prepare_data(read_time_series(RESULT_DIR), date_map)
    pixel_result, anomaly_records, county_summary = detect_anomalies(df)

    pixel_result.to_csv(PIXEL_RESULT_CSV, index=False, encoding="utf-8-sig")
    anomaly_records.to_csv(ANOMALY_RECORD_CSV, index=False, encoding="utf-8-sig")
    county_summary.to_csv(COUNTY_SUMMARY_CSV, index=False, encoding="utf-8-sig")

    print(f"Saved: {PIXEL_RESULT_CSV}")
    print(f"Saved: {ANOMALY_RECORD_CSV}")
    print(f"Saved: {COUNTY_SUMMARY_CSV}")


if __name__ == "__main__":
    main()
