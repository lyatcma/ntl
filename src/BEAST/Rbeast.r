# BEAST-based trend/season decomposition and disaster impact extraction
# Input: long table with columns: date, county, ntl
# Output: per-county BEAST results and event-window impact metrics

library(data.table)
library(zoo)
library(Rbeast)

# ---- user parameters ----
input_csv <- "../data/ntl/Presults/adjusted/ntl_adjusted_shp_L.csv"
output_dir <- "../data/ntl/BEAST/citylevel"
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

disaster_start <- as.Date("2024-09-05")
disaster_end <- as.Date("2024-09-08")
window_days <- 15

# ---- load and prep ----
ntl <- fread(input_csv)
if (!all(c("date", "county", "ntl") %in% names(ntl))) {
  stop("Input must contain columns: date, county, ntl")
}

ntl[, date := as.Date(date)]
setorder(ntl, county, date)

# fill missing dates per county and interpolate missing NTL values
all_dates <- seq(min(ntl$date, na.rm = TRUE), max(ntl$date, na.rm = TRUE), by = "day")
ntl_full <- ntl[, .(date = all_dates), by = county][ntl, on = .(county, date)]
ntl_full[, ntl := zoo::na.approx(ntl, x = date, na.rm = FALSE), by = county]
ntl_full[, ntl := zoo::na.locf(ntl, na.rm = FALSE), by = county]
ntl_full[, ntl := zoo::na.locf(ntl, fromLast = TRUE, na.rm = FALSE), by = county]

# ---- BEAST decomposition per county ----
results <- ntl_full[, {
  y <- ntl
  tnum <- as.numeric(date)
  # daily data: use explicit time vector to preserve leap-year length
  b <- beast(y, time = tnum, season = "harmonic", period = 365)
  trend <- b$trend$Y
  season <- b$season$Y
  if (length(trend) != length(y)) {
    trend <- zoo::na.approx(trend, x = tnum[seq_along(trend)], xout = tnum, na.rm = FALSE)
  }
  if (length(season) != length(y)) {
    season <- zoo::na.approx(season, x = tnum[seq_along(season)], xout = tnum, na.rm = FALSE)
  }
  resid <- y - trend - season
  
  .(
    date = date,
    ntl = y,
    trend = trend,
    season = season,
    resid = resid
  )
}, by = county]

# ---- disaster window impact ----
window_start <- disaster_start - window_days
window_end <- disaster_end + window_days

impact <- results[date >= window_start & date <= window_end, .(
  mean_resid = mean(resid, na.rm = TRUE),
  min_resid = min(resid, na.rm = TRUE),
  max_drop = min(resid, na.rm = TRUE),
  drop_date = date[which.min(resid)]
), by = county]

fwrite(results, file.path(output_dir, "hainan_beast_decomp.csv"))
fwrite(impact, file.path(output_dir, "hainan_disaster_impact.csv"))

message("BEAST decomposition and impact summaries saved to: ", output_dir)

# ---- results ----
library(ggplot2)
ntl_impact <- fread("../data/ntl/BEAST/hainan_disaster_impact.csv")
ntl_results <- fread("../data/ntl/BEAST/hainan_beast_decomp.csv")
ntl_results[, date := as.Date(date)]

windows <- data.table(
  stage = c("Maliks", "Prapiroon", "Yagi"),
  xmin  = as.Date(c("2024-05-30", "2024-07-21", "2024-09-05")),
  xmax  = as.Date(c("2024-06-01", "2024-07-23", "2024-09-08"))
)

county_select <- "海南省三亚市天涯区"
ntl_plot <- ntl_results[county == county_select]

# ntl + trend + season + 台风窗口
ggplot(ntl_plot, aes(x = date)) +
  geom_rect(
    data = windows,
    aes(xmin = xmin -5, xmax = xmax + 5, ymin = -Inf, ymax = Inf),
    inherit.aes = FALSE,
    fill = "grey80", alpha = 0.35
  ) +
  geom_line(aes(y = ntl), color = "grey30", linewidth = 0.5) +
  geom_line(aes(y = trend), color = "red", linewidth = 1.0) +
  #geom_line(aes(y = resid), color = "#a6251a", linewidth = 0.6) +
  geom_line(aes(y = season), color = "blue",
            linewidth = 0.7, linetype = "dashed") +
  theme_bw() +
  labs(
    title = paste("Typhoon Impact on Nighttime Lights –", county_select),
    subtitle = "Grey: NTL | Red: Trend | Blue: Seasonality",
    x = "Date",
    y = "Nighttime Light Intensity"
  )

# 灾害信号 (resid)
ggplot(ntl_plot, aes(x = date, y = resid)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "grey60") +
  geom_line(color = "black", linewidth = 0.6) +
  theme_bw() +
  labs(
    title = paste("Residual Nighttime Light Signal –", county_select),
    subtitle = "Negative values indicate disaster-related loss",
    x = "Date",
    y = "Residual (NTL − Trend − Season)"
  )

