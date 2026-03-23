# 使用 BEAST 模型分离逐像元夜间灯光序列的趋势与季节项，
# 并在灾害窗口(2024-09-05~2024-09-08, ±10天)捕捉冲击强度。
#
# 关键变更：
# 1) 不再对缺测值做线性插值或首尾填充；
# 2) 保留完整日历骨架，但让缺测保持为 NA；
# 3) 使用 beast123() 做批量拟合。beast123 是 beast()/beast.irreg() 的统一批处理接口；
# 4) 新增趋势变点日期、概率及逐日变点发生概率导出。
#
# 输入 CSV 需包含列：date, lon, lat, county, NTL_SFAC, pixel_id
# 输出：
# 1) outputs/pixel_shock_summary.csv              每个像元的冲击指标
# 2) outputs/daily_shock_timeseries.csv          灾害窗口内逐日冲击
# 3) outputs/pixel_daily_decomposition.csv       每个像元2022-2024年逐日分解结果
# 4) outputs/trend_changepoints.csv              趋势变点日期及概率
# 5) outputs/trend_cp_occurrence_probability.csv 趋势逐日变点发生概率
# 6) outputs/beast_plots_<county>/beast_plot_<pixel_id>.png 某个 county 下全部像元示例图（默认关闭）

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
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
output_dir <- "../data/ntl/BEAST/pixelevel/20260322"
example_county <- "海南省文昌市"               # 指定绘图县名；NULL 时自动选第一个 county
generate_plots <- FALSE                       # 提速: 默认关闭绘图
model_years <- 2022:2024
trend_order_minmax <- c(1L, 1L)               # piecewise linear trend
trend_cp_minmax <- c(0L, 10L)
season_cp_minmax <- c(0L, 0L)                 # 提速: 不搜索季节变点，只保留稳定季节项
coord_digits <- 6L
physical_cores <- suppressWarnings(detectCores(logical = FALSE))
if (is.na(physical_cores) || physical_cores < 1L) {
  physical_cores <- max(1L, detectCores() - 1L)
}
batch_threads <- max(1L, min(12L, physical_cores))  # Windows 上避免把逻辑线程全部打满
max_missing_rate_allowed <- 0.995             # 允许直接拟合高缺测序列

event_start <- as.Date("2024-09-05")
event_end <- as.Date("2024-09-08")
window_days <- 10L

# -------------------------
# 函数
# -------------------------
safe_mean <- function(x) {
  x <- x[is.finite(x)]
  if (!length(x)) return(NA_real_)
  mean(x)
}

safe_min <- function(x) {
  x <- x[is.finite(x)]
  if (!length(x)) return(NA_real_)
  min(x)
}

safe_sum <- function(x) {
  x <- x[is.finite(x)]
  if (!length(x)) return(NA_real_)
  sum(x)
}

extract_decomp <- function(model, n) {
  trend <- if (!is.null(model$trend) && !is.null(model$trend$Y)) {
    as.numeric(model$trend$Y)
  } else {
    rep(NA_real_, n)
  }

  season <- if (!is.null(model$season) && !is.null(model$season$Y)) {
    as.numeric(model$season$Y)
  } else {
    rep(NA_real_, n)
  }

  resid <- if (!is.null(model$outlier) && !is.null(model$outlier$Y)) {
    as.numeric(model$outlier$Y)
  } else if (!is.null(model$remainder) && !is.null(model$remainder$Y)) {
    as.numeric(model$remainder$Y)
  } else {
    rep(NA_real_, n)
  }

  list(trend = trend, season = season, resid = resid)
}

map_time_to_date <- function(locations, model_time, fallback_dates) {
  out <- rep(as.Date(NA), length(locations))
  if (!length(locations)) {
    return(out)
  }

  valid <- is.finite(locations)
  if (!any(valid)) {
    return(out)
  }

  if (length(model_time) == length(fallback_dates) && all(is.finite(model_time))) {
    idx <- vapply(
      locations[valid],
      function(x) which.min(abs(model_time - x)),
      integer(1)
    )
  } else {
    idx <- as.integer(round(locations[valid]))
    idx <- pmin(pmax(idx, 1L), length(fallback_dates))
  }

  out[valid] <- fallback_dates[idx]
  out
}

extract_trend_cps <- function(model, sub) {
  trend_part <- model$trend
  if (is.null(trend_part) || is.null(trend_part$cp) || is.null(trend_part$cpPr)) {
    return(data.table())
  }

  cp <- as.numeric(trend_part$cp)
  cp_pr <- as.numeric(trend_part$cpPr)
  cp_jump <- if (!is.null(trend_part$cpAbruptChange)) {
    as.numeric(trend_part$cpAbruptChange)
  } else {
    rep(NA_real_, length(cp))
  }

  cp_ci <- if (!is.null(trend_part$cpCI)) {
    as.matrix(trend_part$cpCI)
  } else {
    matrix(NA_real_, nrow = length(cp), ncol = 2)
  }

  model_time <- if (!is.null(model$time)) as.numeric(model$time) else numeric()
  valid <- is.finite(cp) & is.finite(cp_pr)
  if (!any(valid)) {
    return(data.table())
  }

  cp_dates <- map_time_to_date(cp[valid], model_time, sub$date)
  cp_ci_left <- map_time_to_date(cp_ci[valid, 1], model_time, sub$date)
  cp_ci_right <- map_time_to_date(cp_ci[valid, 2], model_time, sub$date)

  data.table(
    pixel_id = sub$pixel_id[1],
    lon = sub$lon[1],
    lat = sub$lat[1],
    county = sub$county[1],
    cp_rank = seq_len(sum(valid)),
    trend_ncp = if (!is.null(trend_part$ncp)) as.numeric(trend_part$ncp) else NA_real_,
    trend_ncp_mode = if (!is.null(trend_part$ncp_mode)) as.numeric(trend_part$ncp_mode) else NA_real_,
    trend_ncp_median = if (!is.null(trend_part$ncp_median)) as.numeric(trend_part$ncp_median) else NA_real_,
    trend_cp_location = cp[valid],
    trend_cp_date = as.IDate(cp_dates),
    trend_cp_prob = cp_pr[valid],
    trend_cp_abrupt_change = cp_jump[valid],
    trend_cp_ci_left = as.IDate(cp_ci_left),
    trend_cp_ci_right = as.IDate(cp_ci_right)
  )
}

process_one_pixel <- function(idx, pid, dt_full, batch_fit, event_start, event_end, win_start, win_end) {
  sub <- copy(dt_full[pixel_id == pid])
  setorder(sub, date)

  fit <- tryCatch(
    tsextract(batch_fit, index = idx),
    error = function(e) NULL
  )
  if (is.null(fit)) return(NULL)

  dcmp <- extract_decomp(fit, nrow(sub))
  if (length(dcmp$trend) != nrow(sub)) return(NULL)

  sub[, trend := dcmp$trend]
  sub[, season := dcmp$season]
  sub[, resid := dcmp$resid]
  sub[, shock := NTL_SFAC - (trend + season)]
  sub[, in_window := date >= win_start & date <= win_end]
  sub[, in_event := date >= event_start & date <= event_end]

  summary_row <- data.table(
    pixel_id = sub$pixel_id[1],
    lon = sub$lon[1],
    lat = sub$lat[1],
    county = sub$county[1],
    obs_days = sum(!is.na(sub$NTL_SFAC)),
    missing_rate = mean(is.na(sub$NTL_SFAC)),
    event_obs_days = sum(sub$in_event & !is.na(sub$NTL_SFAC)),
    event_mean_shock = safe_mean(sub$shock[sub$in_event]),
    event_min_shock = safe_min(sub$shock[sub$in_event]),
    event_cum_shock = safe_sum(sub$shock[sub$in_event]),
    window_mean_shock = safe_mean(sub$shock[sub$in_window]),
    trend_ncp = if (!is.null(fit$trend$ncp)) as.numeric(fit$trend$ncp) else NA_real_,
    trend_ncp_mode = if (!is.null(fit$trend$ncp_mode)) as.numeric(fit$trend$ncp_mode) else NA_real_,
    trend_ncp_median = if (!is.null(fit$trend$ncp_median)) as.numeric(fit$trend$ncp_median) else NA_real_
  )

  trend_cp <- extract_trend_cps(fit, sub)
  occ_prob <- if (!is.null(fit$trend$cpOccPr)) as.numeric(fit$trend$cpOccPr) else rep(NA_real_, nrow(sub))
  if (length(occ_prob) != nrow(sub)) {
    occ_prob <- rep(NA_real_, nrow(sub))
  }
  trend_cp_occ <- data.table(
    pixel_id = sub$pixel_id,
    lon = sub$lon,
    lat = sub$lat,
    county = sub$county,
    date = sub$date,
    trend_cp_occ_prob = occ_prob
  )

  list(
    decomp = sub,
    summary = summary_row,
    trend_cp = trend_cp,
    trend_cp_occ = trend_cp_occ
  )
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
required_cols <- c("date", "lon", "lat", "county", "NTL_SFAC", "pixel_id")
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
full_dates <- data.table(
  date = seq(
    as.Date(sprintf("%d-01-01", start_year)),
    as.Date(sprintf("%d-12-31", end_year)),
    by = "day"
  )
)

# 先把像元元信息压缩为每个 pixel_id 仅一行，避免连接时键重复导致 cartesian join
pixel_meta <- dt[
  ,
  .(
    lon = lon_round[1],
    lat = lat_round[1],
    county = {
      tab <- sort(table(county), decreasing = TRUE)
      if (length(tab) == 0) NA_character_ else names(tab)[1]
    }
  ),
  by = pixel_id
]
setorder(pixel_meta, pixel_id)

# 把观测数据压缩为 pixel_id-date 唯一键，避免重复记录放大连接
obs_dt <- dt[
  ,
  .(
    NTL_SFAC = {
      v <- NTL_SFAC[!is.na(NTL_SFAC)]
      if (length(v) == 0) NA_real_ else mean(v)
    }
  ),
  by = .(pixel_id, date)
]

pixel_obs_stats <- obs_dt[
  ,
  .(obs_days = sum(!is.na(NTL_SFAC))),
  by = pixel_id
]
valid_pixels <- pixel_obs_stats[obs_days > 0L, pixel_id]
if (!length(valid_pixels)) {
  stop("所有像元都没有有效观测值，无法运行 BEAST。")
}

pixel_meta <- pixel_meta[pixel_id %in% valid_pixels]
obs_dt <- obs_dt[pixel_id %in% valid_pixels]

# 基础骨架：每个统一像元 x 每天
dt_full <- CJ(pixel_id = pixel_meta$pixel_id, date = full_dates$date, unique = TRUE)
setkey(dt_full, pixel_id)
setkey(pixel_meta, pixel_id)
dt_full <- pixel_meta[dt_full]
setkey(dt_full, pixel_id, date)
setkey(obs_dt, pixel_id, date)
dt_full[obs_dt, NTL_SFAC := i.NTL_SFAC]
setorder(dt_full, pixel_id, date)

# 构建 time x pixel 矩阵，缺测保留为 NA，由 beast123 直接处理
pixel_ids <- pixel_meta$pixel_id
date_index <- full_dates$date
y_mat <- matrix(
  NA_real_,
  nrow = length(date_index),
  ncol = length(pixel_ids),
  dimnames = list(as.character(date_index), as.character(pixel_ids))
)

row_idx <- match(obs_dt$date, date_index)
col_idx <- match(obs_dt$pixel_id, pixel_ids)
y_mat[cbind(row_idx, col_idx)] <- obs_dt$NTL_SFAC

# 灾害窗口（含灾前10天和灾后10天）
win_start <- event_start - window_days
win_end <- event_end + window_days

# beast123 是 beast()/beast.irreg() 的统一批处理接口；这里保留 NA，不做插值
metadata <- list(
  whichDimIsTime = 1L,
  time = date_index,
  deltaTime = "1 day",
  period = "1 year",
  maxMissingRateAllowed = max_missing_rate_allowed
)
prior <- list(
  trendMinOrder = trend_order_minmax[1],
  trendMaxOrder = trend_order_minmax[2],
  trendMinKnotNum = trend_cp_minmax[1],
  trendMaxKnotNum = trend_cp_minmax[2],
  seasonMinKnotNum = season_cp_minmax[1],
  seasonMaxKnotNum = season_cp_minmax[2]
)
extra <- list(
  whichOutputDimIsTime = 1L,
  computeCredible = FALSE,
  fastCIComputation = TRUE,
  computeSeasonOrder = FALSE,
  computeTrendOrder = FALSE,
  computeSeasonChngpt = FALSE,
  computeTrendChngpt = TRUE,
  printProgress = FALSE,
  printParameter = FALSE,
  quiet = TRUE,
  dumpInputData = FALSE,
  numThreadsPerCPU = 1L,
  numParThreads = batch_threads
)

batch_fit <- tryCatch(
  beast123(
    y_mat,
    metadata = metadata,
    prior = prior,
    extra = extra,
    season = "harmonic"
  ),
  error = function(e) {
    stop("beast123 批处理拟合失败: ", conditionMessage(e))
  }
)

result_list <- lapply(
  seq_along(pixel_ids),
  function(i) {
    process_one_pixel(
      idx = i,
      pid = pixel_ids[i],
      dt_full = dt_full,
      batch_fit = batch_fit,
      event_start = event_start,
      event_end = event_end,
      win_start = win_start,
      win_end = win_end
    )
  }
)

valid_results <- Filter(Negate(is.null), result_list)
if (!length(valid_results)) {
  stop("没有像元成功拟合 BEAST，请检查数据质量、Rbeast 版本或模型参数。")
}

df_decomp <- rbindlist(lapply(valid_results, `[[`, "decomp"), use.names = TRUE, fill = TRUE)
df_shock <- rbindlist(lapply(valid_results, `[[`, "summary"), use.names = TRUE, fill = TRUE)
df_trend_cp <- rbindlist(lapply(valid_results, `[[`, "trend_cp"), use.names = TRUE, fill = TRUE)
df_trend_cp_occ <- rbindlist(lapply(valid_results, `[[`, "trend_cp_occ"), use.names = TRUE, fill = TRUE)
setorder(df_shock, event_mean_shock)

# 导出
daily_shock <- df_decomp[
  date >= win_start & date <= win_end,
  .(pixel_id, lon, lat, county, date, NTL_SFAC, trend, season, shock, in_event)
]
pixel_daily_all <- df_decomp[
  ,
  .(pixel_id, lon, lat, county, date, NTL_SFAC, trend, season, resid, shock, in_window, in_event)
]

fwrite(df_shock, file.path(output_dir, "pixel_shock_summary.csv"))
fwrite(daily_shock, file.path(output_dir, "daily_shock_timeseries.csv"))
fwrite(pixel_daily_all, file.path(output_dir, "pixel_daily_decomposition.csv"))
fwrite(df_trend_cp, file.path(output_dir, "trend_changepoints.csv"))
fwrite(df_trend_cp_occ, file.path(output_dir, "trend_cp_occurrence_probability.csv"))

# 导出某个 county 下全部像元分解图
plot_dir <- NA_character_
target_county <- NA_character_
if (isTRUE(generate_plots)) {
  target_county <- example_county
  if (is.null(target_county)) {
    target_county <- sort(unique(df_decomp$county))[1]
  }

  plot_dir <- file.path(
    output_dir,
    paste0("beast_plots_", gsub("[[:space:]/]", "_", as.character(target_county)))
  )
  dir.create(plot_dir, recursive = TRUE, showWarnings = FALSE)

  plot_ids <- unique(df_decomp[county == target_county, pixel_id])
  for (pid in plot_ids) {
    sub <- df_decomp[pixel_id == pid & county == target_county]
    setorder(sub, date)

    p <- ggplot(sub, aes(x = date)) +
      geom_point(aes(y = NTL_SFAC), color = "black", size = 0.5, na.rm = TRUE) +
      geom_line(aes(y = trend), color = "#1f77b4", linewidth = 0.6, na.rm = TRUE) +
      geom_line(aes(y = season), color = "#2ca02c", linewidth = 0.6, na.rm = TRUE) +
      geom_rect(
        aes(xmin = event_start, xmax = event_end, ymin = -Inf, ymax = Inf),
        inherit.aes = FALSE,
        fill = "tomato",
        alpha = 0.12
      ) +
      labs(
        title = paste0("County ", target_county, " | Pixel ", pid, " : NTL + Trend + Season"),
        subtitle = "红色区域为灾害日期(2024-09-05~2024-09-08)",
        x = NULL,
        y = "NTL"
      ) +
      theme_minimal(base_size = 11)

    ggsave(
      file.path(plot_dir, paste0("beast_plot_", pid, ".png")),
      p,
      width = 10,
      height = 4,
      dpi = 150
    )
  }
}

cat("完成：\n")
cat("-", file.path(output_dir, "pixel_shock_summary.csv"), "\n")
cat("-", file.path(output_dir, "daily_shock_timeseries.csv"), "\n")
cat("-", file.path(output_dir, "pixel_daily_decomposition.csv"), "\n")
cat("-", file.path(output_dir, "trend_changepoints.csv"), "\n")
cat("-", file.path(output_dir, "trend_cp_occurrence_probability.csv"), "\n")
if (!is.na(plot_dir)) {
  cat("-", paste0(plot_dir, "/beast_plot_*.png"), "\n")
  cat("-", "绘图 county:", target_county, "\n")
}
cat("-", "beast123内部线程数:", batch_threads, "\n")
cat("-", "趋势形式: piecewise linear (trendMinOrder=1, trendMaxOrder=1)", "\n")
cat("-", "建模年份:", paste(model_years, collapse = ","), "\n")
cat("-", "输入文件:", paste(input_files, collapse = " | "), "\n")
cat("-", "pixel_id统一方式: lon+lat (round digits=", coord_digits, ")", "\n", sep = "")
cat("-", "重复键处理: pixel_meta与pixel_id-date均先去重聚合", "\n")
cat("-", "缺测处理: 不插值，保留 NA，由 beast123 直接拟合", "\n")
cat("-", "绘图开关(generate_plots):", generate_plots, "\n")
