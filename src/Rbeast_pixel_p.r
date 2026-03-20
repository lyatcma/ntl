# 使用 BEAST 模型分离逐像元夜间灯光序列的趋势与季节项，
# 并在灾害窗口(2024-09-05~2024-09-08, ±10天)捕捉冲击强度。
#
# 输入 CSV 需包含列：date, lon, lat, county, NTL_SFAC, pixel_id
# 输出：
# 1) outputs/pixel_shock_summary.csv  每个像元的冲击指标
# 2) outputs/daily_shock_timeseries.csv 灾害窗口内逐日冲击
# 3) outputs/pixel_daily_decomposition.csv 每个像元2022-2024年逐日分解结果
# 4) outputs/beast_plots_<county>/beast_plot_<pixel_id>.png 某个 county 下全部像元示例图（默认关闭）

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
  library(lubridate)
  library(zoo)
  library(Rbeast)
  library(ggplot2)
  library(parallel)
})

# -------------------------
# 参数
# -------------------------
input_files <- c(
  "../data/ntl/Presults/2022/ntl_adjusted4_county.csv",
  "../data/ntl/Presults/2023/ntl_adjusted4_county.csv",
  "../data/ntl/Presults/2024/adjusted/ntl_adjusted_shp.csv"
)
output_dir <- "../data/ntl/BEAST/pixelevel_3/parallel"
example_county <- "海南省文昌市"                # 指定绘图县名；NULL 时自动选第一个 county
generate_plots <- T                         # 提速: 默认关闭绘图
n_cores <- max(1L, detectCores() - 1L)          # 并行核数
model_years <- 2022:2024
trend_order_minmax <- c(1L, 1L)                 # piecewise linear trend
coord_digits <- 6L

event_start <- as.Date("2024-09-05")
event_end <- as.Date("2024-09-08")
window_days <- 10L

# -------------------------
# 函数
# -------------------------
safe_beast <- function(x_ts) {
  # 对单个像元时间序列拟合 BEAST，失败则返回 NULL
  tryCatch({
    # period = 365 表示年周期（日尺度数据）
    beast(
      x_ts,
      season = "harmonic",
      period = 365,
      tcp.minmax = c(0, 10),
      sscp.minmax = c(0, 10),
      torder.minmax = trend_order_minmax,
      quiet = TRUE
    )
  }, error = function(e) NULL)
}

extract_decomp <- function(model, n) {
  # 提取 trend/season/residual，兼容不同版本输出字段
  trend <- model$trend$Y
  season <- model$season$Y
  
  # 部分版本 residual 在 outlier 或 remainder
  resid <- NULL
  if (!is.null(model$outlier) && !is.null(model$outlier$Y)) {
    resid <- model$outlier$Y
  } else if (!is.null(model$remainder) && !is.null(model$remainder$Y)) {
    resid <- model$remainder$Y
  } else {
    resid <- rep(NA_real_, n)
  }
  
  list(trend = as.numeric(trend), season = as.numeric(season), resid = as.numeric(resid))
}

process_one_pixel <- function(pid, dt_full, event_start, event_end, win_start, win_end, ts_start_year) {
  sub <- dt_full[pixel_id == pid]
  setorder(sub, date)
  
  x_ts <- ts(sub$NTL_filled, start = c(ts_start_year, 1), frequency = 365)
  
  fit <- safe_beast(x_ts)
  if (is.null(fit)) return(NULL)
  
  dcmp <- extract_decomp(fit, nrow(sub))
  sub[, trend := dcmp$trend]
  sub[, season := dcmp$season]
  sub[, shock := NTL_filled - (trend + season)]
  sub[, in_window := date >= win_start & date <= win_end]
  sub[, in_event := date >= event_start & date <= event_end]
  
  summary_row <- data.table(
    pixel_id = sub$pixel_id[1],
    lon = sub$lon[1],
    lat = sub$lat[1],
    county = sub$county[1],
    event_mean_shock = mean(sub$shock[sub$in_event], na.rm = TRUE),
    event_min_shock = min(sub$shock[sub$in_event], na.rm = TRUE),
    event_cum_shock = sum(sub$shock[sub$in_event], na.rm = TRUE),
    window_mean_shock = mean(sub$shock[sub$in_window], na.rm = TRUE)
  )
  
  list(decomp = sub, summary = summary_row)
}

# -------------------------
# main
# -------------------------
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

file_exists <- file.exists(input_files)
if (!all(file_exists)) {
  stop("以下输入文件不存在: ", paste(input_files[!file_exists], collapse = " | "))
}
dt <- rbindlist(lapply(input_files, fread), use.names = TRUE, fill = TRUE)
required_cols <- c("date", "lon", "lat", "county", "NTL_SFAC", "pixel_id", "county")
missing_cols <- setdiff(required_cols, names(dt))
if (length(missing_cols) > 0) {
  stop("缺少必要字段: ", paste(missing_cols, collapse = ", "))
}

dt <- dt[, ..required_cols]
dt[, date := as.Date(date)]
dt[, NTL_SFAC := as.numeric(NTL_SFAC)]
dt <- dt[!is.na(date) & year(date) %in% model_years]

# 统一 2022-2024 的 pixel_id：以经纬度坐标为准重建统一像元编号
# 说明：原始 pixel_id 在不同年份可能对应不同 lon/lat，这里按 lon+lat 对齐
# 保留原 pixel_id 供追溯，统一编号写回 pixel_id
dt[, pixel_id_raw := as.character(pixel_id)]
dt[, lon_round := round(as.numeric(lon), coord_digits)]
dt[, lat_round := round(as.numeric(lat), coord_digits)]
dt <- dt[!is.na(lon_round) & !is.na(lat_round)]
dt[, coord_key := sprintf(paste0("%.", coord_digits, "f_%.", coord_digits, "f"), lon_round, lat_round)]
keys <- unique(dt$coord_key)
dt[, pixel_id := match(coord_key, keys)]


if (nrow(dt) == 0) {
  stop("输入数据在 model_years 范围内为空，请检查日期或年份设置。")
}

start_year <- min(model_years)
end_year <- max(model_years)

# 生成完整日期并做笛卡尔积补齐
full_dates <- data.table(date = seq(as.Date(sprintf("%d-01-01", start_year)),
                                    as.Date(sprintf("%d-12-31", end_year)),
                                    by = "day"))

# 先把像元元信息压缩为每个 pixel_id 仅一行，避免连接时键重复导致 cartesian join
pixel_meta <- dt[
  , .(
    lon = lon_round[1],
    lat = lat_round[1],
    county = {
      tab <- sort(table(county), decreasing = TRUE)
      if (length(tab) == 0) NA_character_ else names(tab)[1]
    }
  ),
  by = pixel_id
]

# 把观测数据压缩为 pixel_id-date 唯一键，避免重复记录放大连接
obs_dt <- dt[
  , .(NTL_SFAC = {
    v <- NTL_SFAC[!is.na(NTL_SFAC)]
    if (length(v) == 0) NA_real_ else mean(v)
  }),
  by = .(pixel_id, date)
]

# 基础骨架：每个统一像元 x 每天
dt_full <- CJ(pixel_id = pixel_meta$pixel_id, date = full_dates$date, unique = TRUE)
setkey(dt_full, pixel_id)
setkey(pixel_meta, pixel_id)
dt_full <- pixel_meta[dt_full]
setkey(dt_full, pixel_id, date)
setkey(obs_dt, pixel_id, date)
dt_full[obs_dt, NTL_SFAC := i.NTL_SFAC]

setorder(dt_full, pixel_id, date)

# data.table 分组插值（更快）
dt_full[, NTL_filled := na.approx(NTL_SFAC, x = date, na.rm = FALSE), by = pixel_id]
dt_full[, NTL_filled := na.locf(NTL_filled, na.rm = FALSE), by = pixel_id]
dt_full[, NTL_filled := na.locf(NTL_filled, fromLast = TRUE, na.rm = FALSE), by = pixel_id]
dt_full[, pixel_median := median(NTL_filled, na.rm = TRUE), by = pixel_id]
dt_full[!is.finite(pixel_median), pixel_median := 0]
dt_full[is.na(NTL_filled), NTL_filled := pixel_median]
dt_full[, pixel_median := NULL]

# 灾害窗口（含灾前10天和灾后10天）
win_start <- event_start - window_days
win_end <- event_end + window_days

# 并行逐像元拟合
pixel_ids <- unique(dt_full$pixel_id)
use_cores <- min(n_cores, length(pixel_ids))

# Linux/macOS 优先 fork 并行，避免大对象重复序列化
if (.Platform$OS.type != "windows" && use_cores > 1L) {
  result_list <- mclapply(
    pixel_ids,
    process_one_pixel,
    dt_full = dt_full,
    event_start = event_start,
    event_end = event_end,
    win_start = win_start,
    win_end = win_end,
    ts_start_year = start_year,
    mc.cores = use_cores
  )
} else if (use_cores > 1L) {
  cl <- makeCluster(use_cores)
  on.exit(stopCluster(cl), add = TRUE)
  clusterEvalQ(cl, {
    suppressPackageStartupMessages(library(Rbeast))
    suppressPackageStartupMessages(library(data.table))
    NULL
  })
  clusterExport(
    cl,
    varlist = c("process_one_pixel", "safe_beast", "extract_decomp", "dt_full", "event_start", "event_end", "win_start", "win_end", "trend_order_minmax", "start_year"),
    envir = environment()
  )
  result_list <- parLapply(
    cl,
    pixel_ids,
    process_one_pixel,
    dt_full = dt_full,
    event_start = event_start,
    event_end = event_end,
    win_start = win_start,
    win_end = win_end,
    ts_start_year = start_year
  )
  stopCluster(cl)
} else {
  result_list <- lapply(
    pixel_ids,
    process_one_pixel,
    dt_full = dt_full,
    event_start = event_start,
    event_end = event_end,
    win_start = win_start,
    win_end = win_end,
    ts_start_year = start_year
  )
}

valid_results <- Filter(Negate(is.null), result_list)
if (length(valid_results) == 0) {
  stop("没有像元成功拟合 BEAST，请检查数据质量或模型参数。")
}

df_decomp <- rbindlist(lapply(valid_results, `[[`, "decomp"), use.names = TRUE, fill = TRUE)
df_shock <- rbindlist(lapply(valid_results, `[[`, "summary"), use.names = TRUE, fill = TRUE)
setorder(df_shock, event_mean_shock)

# 导出
daily_shock <- df_decomp[date >= win_start & date <= win_end,
                         .(pixel_id, lon, lat, county, date, NTL_filled, trend, season, shock, in_event)]
pixel_daily_all <- df_decomp[, .(pixel_id, lon, lat, county, date, NTL_filled, trend, season, shock, in_window, in_event)]

fwrite(df_shock, file.path(output_dir, "pixel_shock_summary.csv"))
fwrite(daily_shock, file.path(output_dir, "daily_shock_timeseries.csv"))
fwrite(pixel_daily_all, file.path(output_dir, "pixel_daily_decomposition.csv"))

# 导出某个 county 下全部像元分解图
if (generate_plots) {
  target_county <- example_county
  if (is.null(target_county)) {
    target_county <- sort(unique(df_decomp$county))[1]
  }
  plot_dir <- file.path(output_dir, paste0("beast_plots_", gsub("[[:space:]/]", "_", as.character(target_county))))
  dir.create(plot_dir, recursive = TRUE, showWarnings = FALSE)
  
  plot_ids <- unique(df_decomp[county == target_county, pixel_id])
  for (pid in plot_ids) {
    sub <- df_decomp[pixel_id == pid & county == target_county]
    setorder(sub, date)
    p <- ggplot(sub, aes(x = date)) +
      geom_point(aes(y = NTL_SFAC), color = "black", linewidth = 0.4) +
      geom_line(aes(y = trend), color = "#1f77b4", linewidth = 0.6) +
      geom_line(aes(y = season), color = "#2ca02c", linewidth = 0.6) +
      geom_rect(
        aes(xmin = event_start, xmax = event_end, ymin = -Inf, ymax = Inf),
        inherit.aes = FALSE,
        fill = "tomato", alpha = 0.12
      ) +
      labs(
        title = paste0("County ", target_county, " | Pixel ", pid, " : NTL + Trend + Season"),
        subtitle = "红色区域为灾害日期(2024-09-05~2024-09-08)",
        x = NULL, y = "NTL"
      ) +
      theme_minimal(base_size = 11)
    
    ggsave(file.path(plot_dir, paste0("beast_plot_", pid, ".png")), p, width = 10, height = 4, dpi = 150)
  }
}

cat("完成：\n")
cat("-", file.path(output_dir, "pixel_shock_summary.csv"), "\n")
cat("-", file.path(output_dir, "daily_shock_timeseries.csv"), "\n")
cat("-", file.path(output_dir, "pixel_daily_decomposition.csv"), "\n")
cat("-", paste0(plot_dir, "/beast_plot_*.png"), "\n")
cat("-", "绘图 county:", target_county, "\n")
cat("-", "并行核数:", use_cores, "\n")
cat("-", "趋势形式: piecewise linear (torder.minmax=1,1)", "\n")
cat("-", "建模年份:", paste(model_years, collapse = ","), "\n")
cat("-", "输入文件:", paste(input_files, collapse = " | "), "\n")
cat("-", "pixel_id统一方式: lon+lat (round digits=", coord_digits, ")", "\n", sep = "")
cat("-", "重复键处理: pixel_meta与pixel_id-date均先去重聚合", "\n")
cat("-", "绘图开关(generate_plots):", generate_plots, "\n")