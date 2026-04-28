library(data.table)
library(lubridate)
library(Rbeast)

input_file <- "C:/Users/liuy1/Desktop/0.文章/3.重点研发Chao/0.nightime/data/ntl/Presults/unified/ntl_2022_2024_unified.csv"
output_dir <- "C:/Users/liuy1/Desktop/0.文章/3.重点研发Chao/0.nightime/data/ntl/BEAST/pixelevel/20260323"

season <- "harmonic"
period <- "1 year"
delta_time <- "1 month"
method <- "bayes"
has_outlier <- FALSE

dt <- fread(input_file)
setnames(dt, names(dt), tolower(names(dt)))
dt <- dt[, .(
  date = as.Date(date),
  pixel_id = as.character(pixel_id),
  lon = as.numeric(lon),
  lat = as.numeric(lat),
  county = fifelse(is.na(county) | trimws(county) == "", "unknown_county", as.character(county)),
  ntl_sfac = as.numeric(ntl_sfac)
)]

dt <- dt[!is.na(date)]
setorder(dt, county, pixel_id, date)

first_not_na <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) == 0) NA else x[1]
}

flatten_beast <- function(x, county, pixel_id, lon, lat, prefix = NULL) {
  out <- list()

  walk <- function(obj, path) {
    if (is.null(obj)) return()

    if (is.list(obj) && !is.data.frame(obj)) {
      nm <- names(obj)
      if (is.null(nm)) nm <- seq_along(obj)
      for (i in seq_along(obj)) {
        walk(obj[[i]], c(path, nm[i]))
      }
      return()
    }

    dims <- dim(obj)
    field <- paste(path, collapse = ".")

    if (is.null(dims)) {
      out[[length(out) + 1L]] <<- data.table(
        county = county,
        pixel_id = pixel_id,
        lon = lon,
        lat = lat,
        field = field,
        i = seq_along(obj),
        j = NA_integer_,
        value = as.character(obj)
      )
    } else if (length(dims) == 2) {
      idx <- arrayInd(seq_len(length(obj)), dims)
      out[[length(out) + 1L]] <<- data.table(
        county = county,
        pixel_id = pixel_id,
        lon = lon,
        lat = lat,
        field = field,
        i = idx[, 1],
        j = idx[, 2],
        value = as.character(c(obj))
      )
    } else {
      idx <- arrayInd(seq_len(length(obj)), dims)
      out[[length(out) + 1L]] <<- data.table(
        county = county,
        pixel_id = pixel_id,
        lon = lon,
        lat = lat,
        field = field,
        i = idx[, 1],
        j = idx[, 2],
        value = as.character(c(obj))
      )
    }
  }

  walk(x, prefix)
  if (length(out) == 0) data.table() else rbindlist(out, fill = TRUE)
}

extract_ts <- function(res, county, pixel_id, lon, lat) {
  n <- length(res$time)

  out <- data.table(
    county = county,
    pixel_id = pixel_id,
    lon = lon,
    lat = lat,
    t = seq_len(n),
    time = res$time,
    observed = res$data
  )

  if (!is.null(res$trend)) {
    if (!is.null(res$trend$Y)) out[, trend := res$trend$Y]
    if (!is.null(res$trend$SD)) out[, trend_sd := res$trend$SD]
    if (!is.null(res$trend$slp)) out[, trend_slp := res$trend$slp]
    if (!is.null(res$trend$slpSD)) out[, trend_slp_sd := res$trend$slpSD]
    if (!is.null(res$trend$cpOccPr)) out[, trend_cpOccPr := res$trend$cpOccPr]
  }

  if (!is.null(res$season)) {
    if (!is.null(res$season$Y)) out[, season_comp := res$season$Y]
    if (!is.null(res$season$SD)) out[, season_sd := res$season$SD]
    if (!is.null(res$season$amp)) out[, season_amp := res$season$amp]
    if (!is.null(res$season$ampSD)) out[, season_amp_sd := res$season$ampSD]
    if (!is.null(res$season$cpOccPr)) out[, season_cpOccPr := res$season$cpOccPr]
  }

  if (!is.null(res$outlier)) {
    if (!is.null(res$outlier$Y)) out[, outlier_comp := res$outlier$Y]
    if (!is.null(res$outlier$cpOccPr)) out[, outlier_cpOccPr := res$outlier$cpOccPr]
  }

  out
}

extract_cp <- function(comp, comp_name, county, pixel_id, lon, lat) {
  if (is.null(comp) || is.null(comp$cp)) return(NULL)

  ci <- comp$cpCI
  out <- data.table(
    county = county,
    pixel_id = pixel_id,
    lon = lon,
    lat = lat,
    component = comp_name,
    rank = seq_along(comp$cp),
    cp = comp$cp,
    cpPr = comp$cpPr,
    cpAbruptChange = comp$cpAbruptChange
  )

  if (!is.null(ci) && is.matrix(ci) && ncol(ci) >= 2) {
    out[, cpCI_low := ci[, 1]]
    out[, cpCI_high := ci[, 2]]
  }

  out[!is.na(cp) & !is.nan(cp)]
}

fit_one_pixel <- function(d) {
  county <- d$county[1]
  pixel_id <- d$pixel_id[1]

  d <- d[, .(
    pixel_id = first_not_na(pixel_id),
    lon = first_not_na(lon),
    lat = first_not_na(lat),
    county = first_not_na(county),
    ntl_sfac = if (all(is.na(ntl_sfac))) NA_real_ else mean(ntl_sfac, na.rm = TRUE)
  ), by = .(date)][order(date)]

  pixel_id <- d$pixel_id[1]
  lon <- d$lon[1]
  lat <- d$lat[1]

  if (sum(!is.na(d$ntl_sfac)) < 2) {
    return(list(
      summary = data.table(
        county = county,
        pixel_id = pixel_id,
        lon = lon,
        lat = lat,
        n = nrow(d),
        n_non_missing = sum(!is.na(d$ntl_sfac)),
        fit_status = "skipped"
      ),
      ts = NULL,
      cp = NULL,
      full = NULL
    ))
  }

  res <- beast123(
    Y = d$ntl_sfac,
    metadata = list(
      time = d$date,
      deltaTime = delta_time,
      period = period,
      hasOutlier = has_outlier
    ),
    season = season,
    method = method,
    extra = list(
      dumpInputData = TRUE,
      computeCredible = TRUE,
      computeSeasonChngpt = TRUE,
      computeTrendChngpt = TRUE,
      computeSeasonAmp = TRUE,
      computeTrendSlope = TRUE,
      printProgress = FALSE,
      printParameter = FALSE,
      quiet = TRUE
    )
  )

  list(
    summary = data.table(
      county = county,
      pixel_id = pixel_id,
      lon = lon,
      lat = lat,
      n = nrow(d),
      n_non_missing = sum(!is.na(d$ntl_sfac)),
      fit_status = "success",
      marg_lik = res$marg_lik,
      r2 = res$R2,
      rmse = res$RMSE,
      sig2 = res$sig2,
      trend_ncp = res$trend$ncp %||% NA_real_,
      trend_ncp_mode = res$trend$ncp_mode %||% NA_real_,
      season_ncp = res$season$ncp %||% NA_real_,
      season_ncp_mode = res$season$ncp_mode %||% NA_real_
    ),
    ts = extract_ts(res, county, pixel_id, lon, lat),
    cp = rbindlist(list(
      extract_cp(res$trend, "trend", county, pixel_id, lon, lat),
      extract_cp(res$season, "season", county, pixel_id, lon, lat),
      extract_cp(res$outlier, "outlier", county, pixel_id, lon, lat)
    ), fill = TRUE),
    full = flatten_beast(res, county, pixel_id, lon, lat)
  )
}

`%||%` <- function(x, y) if (is.null(x)) y else x

bind_or_empty <- function(x) {
  x <- Filter(Negate(is.null), x)
  if (length(x) == 0) data.table() else rbindlist(x, fill = TRUE, use.names = TRUE)
}

county_vec <- unique(dt$county)
summary_all <- vector("list", length(county_vec))

for (i in seq_along(county_vec)) {
  county_i <- county_vec[i]
  folder_i <- file.path(output_dir, make.names(county_i))
  dir.create(folder_i, recursive = TRUE, showWarnings = FALSE)

  dt_county <- dt[county == county_i]
  pixel_vec <- unique(dt_county$pixel_id)
  county_res <- vector("list", length(pixel_vec))

  for (j in seq_along(pixel_vec)) {
    county_res[[j]] <- fit_one_pixel(dt_county[pixel_id == pixel_vec[j]])
  }

  summary_dt <- bind_or_empty(lapply(county_res, `[[`, "summary"))
  ts_dt <- bind_or_empty(lapply(county_res, `[[`, "ts"))
  cp_dt <- bind_or_empty(lapply(county_res, `[[`, "cp"))
  full_dt <- bind_or_empty(lapply(county_res, `[[`, "full"))

  fwrite(summary_dt, file.path(folder_i, "summary.csv"))
  fwrite(ts_dt, file.path(folder_i, "time_series.csv"))
  fwrite(cp_dt, file.path(folder_i, "changepoints.csv"))
  fwrite(full_dt, file.path(folder_i, "beast_full_long.csv"))

  summary_all[[i]] <- summary_dt
}

fwrite(bind_or_empty(summary_all), file.path(output_dir, "summary_all.csv"))
