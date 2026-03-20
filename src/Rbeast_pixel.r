# 使用 BEAST 模型分离逐像元夜间灯光序列的趋势与季节项，
# 并在灾害窗口(2024-09-05~2024-09-08, ±10天)捕捉冲击强度。
#
# 输入 CSV 需包含列：date, lon, lat, NTL_SFAC, pixel_ID
# 输出：
# 1) outputs/pixel_shock_summary.csv  每个像元的冲击指标
# 2) outputs/daily_shock_timeseries.csv 灾害窗口内逐日冲击
# 3) outputs/pixel_daily_decomposition_2024.csv 每个像元全年逐日分解结果
# 4) outputs/beast_plot_<pixel_id>.png   示例像元分解图(前若干个)

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
  library(lubridate)
  library(zoo)
  library(Rbeast)
  library(ggplot2)
})

# -------------------------
# 参数区（按需修改）
# -------------------------
input_csv <- "../data/ntl/Presults/adjusted/ntl_adjusted_shp.csv"  # 你的输入文件路径
output_dir <- "../data/ntl/BEAST/pixelevel"
example_plot_n <- 6L                            # 导出前N个像元图

event_start <- as.Date("2024-09-05")
event_end   <- as.Date("2024-09-08")
window_days <- 10L

# -------------------------
# 函数定义
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
      quiet = TRUE
    )
  }, error = function(e) {
    message("BEAST failed: ", conditionMessage(e))
    NULL
  })
}

extract_decomp <- function(model, n) {
  # 提取 trend/season/residual，兼容不同版本输出字段
  trend <- model$trend$Y
  season <- model$season$Y
  
  # 部分版本 residual 在 outlier 或 remainder 中，做兜底
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

# -------------------------
# 主流程
# -------------------------
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

dt <- fread(input_csv)
required_cols <- c("date", "lon", "lat", "NTL_SFAC", "pixel_id")
missing_cols <- setdiff(required_cols, names(dt))
if (length(missing_cols) > 0) {
  stop("缺少必要字段: ", paste(missing_cols, collapse = ", "))
}

dt <- dt %>%
  mutate(
    date = as.Date(date),
    NTL_SFAC = as.numeric(NTL_SFAC)
  ) %>%
  filter(!is.na(date))

# 构建 2024 全年完整日历，逐像元补齐缺失日期
full_dates <- data.frame(date = seq(as.Date("2024-01-01"), as.Date("2024-12-31"), by = "day"))

pixels <- dt %>% distinct(pixel_id, lon, lat)

dt_full <- pixels %>%
  left_join(full_dates, by = character()) %>%
  left_join(dt %>% select(pixel_id, date, NTL_SFAC), by = c("pixel_id", "date")) %>%
  arrange(pixel_id, date)

# 对每个像元做缺失值插值：线性插值 + 首尾延拓
dt_full <- dt_full %>%
  group_by(pixel_id) %>%
  mutate(
    NTL_filled = na.approx(NTL_SFAC, x = date, na.rm = FALSE),
    NTL_filled = na.locf(NTL_filled, na.rm = FALSE),
    NTL_filled = na.locf(NTL_filled, fromLast = TRUE, na.rm = FALSE)
  ) %>%
  ungroup()

# 若全缺失，则以该像元全年中位数兜底（仍缺则0）
dt_full <- dt_full %>%
  group_by(pixel_id) %>%
  mutate(
    pixel_median = median(NTL_filled, na.rm = TRUE),
    pixel_median = ifelse(is.finite(pixel_median), pixel_median, 0),
    NTL_filled = ifelse(is.na(NTL_filled), pixel_median, NTL_filled)
  ) %>%
  ungroup() %>%
  select(-pixel_median)

# 灾害窗口（含灾前10天和灾后10天）
win_start <- event_start - window_days
win_end <- event_end + window_days

all_decomp <- list()
shock_summary <- list()
plot_counter <- 0L

pixel_ids <- unique(dt_full$pixel_id)
for (pid in pixel_ids) {
  sub <- dt_full %>% filter(pixel_id == pid) %>% arrange(date)
  
  y <- sub$NTL_filled
  x_ts <- ts(y, start = c(2024, 1), frequency = 365)
  
  fit <- safe_beast(x_ts)
  if (is.null(fit)) next
  
  dcmp <- extract_decomp(fit, n = nrow(sub))
  sub$trend <- dcmp$trend
  sub$season <- dcmp$season
  
  # 冲击定义：观测值 - (趋势 + 季节)
  sub$shock <- sub$NTL_filled - (sub$trend + sub$season)
  
  # 标记灾害窗口和灾中时段
  sub <- sub %>%
    mutate(
      in_window = date >= win_start & date <= win_end,
      in_event = date >= event_start & date <= event_end
    )
  
  all_decomp[[as.character(pid)]] <- sub
  
  # 汇总指标
  event_mean_shock <- mean(sub$shock[sub$in_event], na.rm = TRUE)
  window_mean_shock <- mean(sub$shock[sub$in_window], na.rm = TRUE)
  event_min_shock <- min(sub$shock[sub$in_event], na.rm = TRUE)
  event_cum_shock <- sum(sub$shock[sub$in_event], na.rm = TRUE)
  
  shock_summary[[as.character(pid)]] <- data.frame(
    pixel_id = pid,
    lon = sub$lon[1],
    lat = sub$lat[1],
    event_mean_shock = event_mean_shock,
    event_min_shock = event_min_shock,
    event_cum_shock = event_cum_shock,
    window_mean_shock = window_mean_shock
  )
  
  # 导出部分像元分解图
  if (plot_counter < example_plot_n) {
    plot_counter <- plot_counter + 1L
    p <- ggplot(sub, aes(x = date)) +
      geom_line(aes(y = NTL_filled), color = "black", linewidth = 0.4) +
      geom_line(aes(y = trend), color = "#1f77b4", linewidth = 0.6) +
      geom_line(aes(y = season), color = "#2ca02c", linewidth = 0.6) +
      geom_rect(
        aes(xmin = event_start, xmax = event_end, ymin = -Inf, ymax = Inf),
        inherit.aes = FALSE,
        fill = "tomato", alpha = 0.12
      ) +
      labs(
        title = paste0("Pixel ", pid, " : NTL + Trend + Season"),
        subtitle = "红色区域为灾害日期(2024-09-05~2024-09-08)",
        x = NULL, y = "NTL"
      ) +
      theme_minimal(base_size = 11)
    
    ggsave(
      filename = file.path(output_dir, paste0("beast_plot_", pid, ".png")),
      plot = p, width = 10, height = 4, dpi = 150
    )
  }
}

if (length(all_decomp) == 0) {
  stop("没有像元成功拟合 BEAST，请检查数据质量或模型参数。")
}

df_decomp <- bind_rows(all_decomp)
df_shock <- bind_rows(shock_summary) %>%
  arrange(event_mean_shock)

# 导出灾害窗口逐日冲击
daily_shock <- df_decomp %>%
  filter(date >= win_start, date <= win_end) %>%
  select(pixel_id, lon, lat, date, NTL_filled, trend, season, shock, in_event)

# 导出每个像元全年逐日结果
pixel_daily_all <- df_decomp %>%
  select(pixel_id, lon, lat, date, NTL_filled, trend, season, shock, in_window, in_event)

fwrite(df_shock, file.path(output_dir, "pixel_shock_summary.csv"))
fwrite(daily_shock, file.path(output_dir, "daily_shock_timeseries.csv"))
fwrite(pixel_daily_all, file.path(output_dir, "pixel_daily_decomposition_2024.csv"))

cat("完成：\n")
cat("-", file.path(output_dir, "pixel_shock_summary.csv"), "\n")
cat("-", file.path(output_dir, "daily_shock_timeseries.csv"), "\n")
cat("-", file.path(output_dir, "pixel_daily_decomposition_2024.csv"), "\n")
cat("-", "beast_plot_*.png (示例像元)\n")
