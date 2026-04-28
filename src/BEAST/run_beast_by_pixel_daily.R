# =============================================================================
# BEAST per-pixel NTL change detection — DAILY resolution, matrixed, county-parallel
# -----------------------------------------------------------------------------
# Changes vs. the original monthly script:
#   1. delta_time = 1/365  (daily; was "1 month")
#   2. Within each county, all pixels are stacked into a [time x pixels] matrix
#      and fed to beast123() in a SINGLE call (was: one call per pixel).
#   3. Counties are processed in parallel via future.apply::future_lapply.
#   4. Output schema (file names, folder layout, column names) unchanged.
# =============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(Rbeast)
  library(future)
  library(future.apply)
})

# ----- user paths ------------------------------------------------------------
input_file <- "C:/Users/liuy1/Desktop/0.文章/3.重点研发Chao/0.nightime/data/ntl/Presults/unified/ntl_2022_2024_unified.csv"
output_dir <- "C:/Users/liuy1/Desktop/0.文章/3.重点研发Chao/0.nightime/data/ntl/BEAST/pixelevel/20260427_daily"

# ----- BEAST parameters ------------------------------------------------------
season       <- "harmonic"
period       <- "1 year"
delta_time   <- 1/365         # <-- DAILY (was "1 month")
method       <- "bayes"
has_outlier  <- FALSE

# ----- parallel parameters ---------------------------------------------------
# How many counties to process in parallel. Leave 1-2 cores for OS / BEAST's
# own internal threading. Adjust to taste.
n_workers <- max(1L, parallel::detectCores() - 2L)

# Cap BEAST's internal OpenMP threads per worker, otherwise N workers x M
# internal threads will oversubscribe the CPU.
beast_threads_per_worker <- 1L

# =============================================================================
# Helpers (same semantics as original)
# =============================================================================

`%||%` <- function(x, y) if (is.null(x)) y else x

first_not_na <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) == 0) NA else x[1]
}

bind_or_empty <- function(x) {
  x <- Filter(Negate(is.null), x)
  if (length(x) == 0) data.table() else rbindlist(x, fill = TRUE, use.names = TRUE)
}

# ---- flatten an arbitrary BEAST sub-object into long form -------------------
# Identical behaviour to the original flatten_beast(), used here on a
# single-pixel slice of the matrixed result.
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

    dims  <- dim(obj)
    field <- paste(path, collapse = ".")

    if (is.null(dims)) {
      out[[length(out) + 1L]] <<- data.table(
        county   = county,
        pixel_id = pixel_id,
        lon      = lon,
        lat      = lat,
        field    = field,
        i        = seq_along(obj),
        j        = NA_integer_,
        value    = as.character(obj)
      )
    } else if (length(dims) == 2) {
      idx <- arrayInd(seq_len(length(obj)), dims)
      out[[length(out) + 1L]] <<- data.table(
        county   = county,
        pixel_id = pixel_id,
        lon      = lon,
        lat      = lat,
        field    = field,
        i        = idx[, 1],
        j        = idx[, 2],
        value    = as.character(c(obj))
      )
    } else {
      idx <- arrayInd(seq_len(length(obj)), dims)
      out[[length(out) + 1L]] <<- data.table(
        county   = county,
        pixel_id = pixel_id,
        lon      = lon,
        lat      = lat,
        field    = field,
        i        = idx[, 1],
        j        = idx[, 2],
        value    = as.character(c(obj))
      )
    }
  }

  walk(x, prefix)
  if (length(out) == 0) data.table() else rbindlist(out, fill = TRUE)
}

# =============================================================================
# Slicing helpers — pull pixel k out of a matrixed BEAST result
# =============================================================================
# When beast123() is called with a [time x pixels] matrix and
# whichDimIsTime = 1, scalar fields (R2, RMSE, sig2, marg_lik, *.ncp, ...)
# become length-N vectors, and time-series fields (trend$Y, season$Y, ...)
# become [time x N] matrices, with N = number of pixels in this batch.
# These helpers extract the slice for pixel k while preserving the original
# single-pixel object shape, so the original extract_ts() / extract_cp() /
# flatten_beast() routines can be reused unchanged.

# Pull scalar from a possibly-vectorised BEAST scalar field.
scalar_k <- function(v, k) {
  if (is.null(v)) return(NA_real_)
  if (length(v) == 1L) return(as.numeric(v))
  if (length(v) >= k)  return(as.numeric(v[k]))
  NA_real_
}

# Slice a per-component sub-object (trend / season / outlier) for pixel k.
slice_component <- function(comp, k) {
  if (is.null(comp)) return(NULL)
  out <- list()
  for (nm in names(comp)) {
    x <- comp[[nm]]
    if (is.null(x)) {
      out[[nm]] <- NULL
      next
    }
    d <- dim(x)
    if (is.null(d)) {
      # vector across pixels (e.g. ncp, ncpPr-of-modes, etc.)
      out[[nm]] <- if (length(x) >= k) x[k] else x
    } else if (length(d) == 2L) {
      # [time x pixels] OR [maxKnots x pixels]
      out[[nm]] <- x[, k]
    } else if (length(d) == 3L) {
      # e.g. cpCI: [maxKnots x 2 x pixels]
      out[[nm]] <- x[, , k]
    } else {
      out[[nm]] <- x
    }
  }
  out
}

# Slice the full result object for pixel k, producing something that LOOKS LIKE
# the result of a single-pixel BEAST call.
slice_result <- function(res, k) {
  list(
    time     = res$time,
    data     = if (!is.null(res$data)) {
                 if (is.matrix(res$data)) res$data[, k] else res$data
               } else NULL,
    marg_lik = scalar_k(res$marg_lik, k),
    R2       = scalar_k(res$R2,       k),
    RMSE     = scalar_k(res$RMSE,     k),
    sig2     = scalar_k(res$sig2,     k),
    trend    = slice_component(res$trend,   k),
    season   = slice_component(res$season,  k),
    outlier  = slice_component(res$outlier, k)
  )
}

# =============================================================================
# Per-pixel extractors — same column schema as original
# =============================================================================

extract_ts <- function(res, county, pixel_id, lon, lat) {
  n <- length(res$time)

  out <- data.table(
    county   = county,
    pixel_id = pixel_id,
    lon      = lon,
    lat      = lat,
    t        = seq_len(n),
    time     = res$time,
    observed = res$data
  )

  if (!is.null(res$trend)) {
    if (!is.null(res$trend$Y))       out[, trend := res$trend$Y]
    if (!is.null(res$trend$SD))      out[, trend_sd := res$trend$SD]
    if (!is.null(res$trend$slp))     out[, trend_slp := res$trend$slp]
    if (!is.null(res$trend$slpSD))   out[, trend_slp_sd := res$trend$slpSD]
    if (!is.null(res$trend$cpOccPr)) out[, trend_cpOccPr := res$trend$cpOccPr]
  }

  if (!is.null(res$season)) {
    if (!is.null(res$season$Y))       out[, season_comp := res$season$Y]
    if (!is.null(res$season$SD))      out[, season_sd := res$season$SD]
    if (!is.null(res$season$amp))     out[, season_amp := res$season$amp]
    if (!is.null(res$season$ampSD))   out[, season_amp_sd := res$season$ampSD]
    if (!is.null(res$season$cpOccPr)) out[, season_cpOccPr := res$season$cpOccPr]
  }

  if (!is.null(res$outlier)) {
    if (!is.null(res$outlier$Y))       out[, outlier_comp := res$outlier$Y]
    if (!is.null(res$outlier$cpOccPr)) out[, outlier_cpOccPr := res$outlier$cpOccPr]
  }

  out
}

extract_cp <- function(comp, comp_name, county, pixel_id, lon, lat) {
  if (is.null(comp) || is.null(comp$cp)) return(NULL)

  ci <- comp$cpCI
  out <- data.table(
    county         = county,
    pixel_id       = pixel_id,
    lon            = lon,
    lat            = lat,
    component      = comp_name,
    rank           = seq_along(comp$cp),
    cp             = comp$cp,
    cpPr           = comp$cpPr,
    cpAbruptChange = comp$cpAbruptChange
  )

  if (!is.null(ci) && is.matrix(ci) && ncol(ci) >= 2) {
    out[, cpCI_low  := ci[, 1]]
    out[, cpCI_high := ci[, 2]]
  }

  out[!is.na(cp) & !is.nan(cp)]
}

# =============================================================================
# County-level worker — matrixed BEAST call + per-pixel extraction
# =============================================================================
# Takes the data.table for ONE county and produces the four output data.tables
# (summary / ts / cp / full) ready to be written to disk.
process_county <- function(dt_county, county_i,
                           season, period, delta_time, method, has_outlier,
                           beast_threads = 1L) {

  # --- 1. per-pixel daily aggregation (in case of duplicate dates) ----------
  # Matches the original fit_one_pixel() reduction: average duplicate dates,
  # carry pixel/lon/lat/county forward.
  dt_county <- dt_county[, .(
    pixel_id = first_not_na(pixel_id),
    lon      = first_not_na(lon),
    lat      = first_not_na(lat),
    county   = first_not_na(county),
    ntl_sfac = if (all(is.na(ntl_sfac))) NA_real_ else mean(ntl_sfac, na.rm = TRUE)
  ), by = .(pixel_id_key = pixel_id, date)]
  dt_county[, pixel_id_key := NULL]
  setorder(dt_county, pixel_id, date)

  pixel_meta <- unique(dt_county[, .(pixel_id, lon, lat, county)],
                       by = "pixel_id")
  pixel_vec  <- pixel_meta$pixel_id
  n_pix      <- length(pixel_vec)

  if (n_pix == 0L) {
    return(list(summary = data.table(), ts = data.table(),
                cp = data.table(), full = data.table()))
  }

  # --- 2. build the unified daily date grid for this county -----------------
  date_grid <- seq.Date(min(dt_county$date), max(dt_county$date), by = "day")
  n_t       <- length(date_grid)

  # --- 3. assemble [time x pixels] NTL matrix ------------------------------
  # Wide reshape via dcast is the fastest way; missing dates become NA, which
  # BEAST handles natively as data gaps.
  wide <- dcast(dt_county, date ~ pixel_id, value.var = "ntl_sfac",
                fun.aggregate = mean, fill = NA_real_)
  # ensure all grid dates present (dcast may drop dates with no obs in any px)
  full_grid <- data.table(date = date_grid)
  wide      <- merge(full_grid, wide, by = "date", all.x = TRUE)
  setorder(wide, date)

  # column order = pixel_vec order
  Y <- as.matrix(wide[, ..pixel_vec])
  storage.mode(Y) <- "double"

  # --- 4. count non-missing per pixel; flag pixels to skip ------------------
  n_obs_per_pix <- colSums(!is.na(Y))
  keep_idx      <- which(n_obs_per_pix >= 2L)
  skip_idx      <- which(n_obs_per_pix <  2L)

  # --- 5. matrixed BEAST call on the kept pixels ---------------------------
  res <- NULL
  if (length(keep_idx) > 0L) {
    Y_keep <- Y[, keep_idx, drop = FALSE]

    res <- beast123(
      Y = Y_keep,
      metadata = list(
        time            = date_grid,
        deltaTime       = delta_time,
        period          = period,
        whichDimIsTime  = 1L,        # rows = time
        hasOutlier      = has_outlier
      ),
      season = season,
      method = method,
      mcmc   = list(),
      extra  = list(
        dumpInputData         = TRUE,
        computeCredible       = TRUE,
        computeSeasonChngpt   = TRUE,
        computeTrendChngpt    = TRUE,
        computeSeasonAmp      = TRUE,
        computeTrendSlope     = TRUE,
        numThreadsPerCPU      = beast_threads,
        numParThreads         = beast_threads,
        printProgress         = FALSE,
        printParameter        = FALSE,
        quiet                 = TRUE
      )
    )
  }

  # --- 6. per-pixel extraction (reuse original schema) ---------------------
  summaries <- vector("list", n_pix)
  ts_list   <- vector("list", n_pix)
  cp_list   <- vector("list", n_pix)
  full_list <- vector("list", n_pix)

  # 6a. successful pixels
  for (kk in seq_along(keep_idx)) {
    px_idx   <- keep_idx[kk]
    pid      <- pixel_meta$pixel_id[px_idx]
    lon      <- pixel_meta$lon[px_idx]
    lat      <- pixel_meta$lat[px_idx]

    res_k <- slice_result(res, kk)

    summaries[[px_idx]] <- data.table(
      county          = county_i,
      pixel_id        = pid,
      lon             = lon,
      lat             = lat,
      n               = n_t,
      n_non_missing   = n_obs_per_pix[px_idx],
      fit_status      = "success",
      marg_lik        = res_k$marg_lik,
      r2              = res_k$R2,
      rmse            = res_k$RMSE,
      sig2            = res_k$sig2,
      trend_ncp       = res_k$trend$ncp       %||% NA_real_,
      trend_ncp_mode  = res_k$trend$ncp_mode  %||% NA_real_,
      season_ncp      = res_k$season$ncp      %||% NA_real_,
      season_ncp_mode = res_k$season$ncp_mode %||% NA_real_
    )

    ts_list[[px_idx]] <- extract_ts(res_k, county_i, pid, lon, lat)

    cp_list[[px_idx]] <- rbindlist(list(
      extract_cp(res_k$trend,   "trend",   county_i, pid, lon, lat),
      extract_cp(res_k$season,  "season",  county_i, pid, lon, lat),
      extract_cp(res_k$outlier, "outlier", county_i, pid, lon, lat)
    ), fill = TRUE)

    full_list[[px_idx]] <- flatten_beast(res_k, county_i, pid, lon, lat)
  }

  # 6b. skipped pixels
  for (px_idx in skip_idx) {
    pid <- pixel_meta$pixel_id[px_idx]
    lon <- pixel_meta$lon[px_idx]
    lat <- pixel_meta$lat[px_idx]
    summaries[[px_idx]] <- data.table(
      county        = county_i,
      pixel_id      = pid,
      lon           = lon,
      lat           = lat,
      n             = n_t,
      n_non_missing = n_obs_per_pix[px_idx],
      fit_status    = "skipped"
    )
  }

  list(
    summary = bind_or_empty(summaries),
    ts      = bind_or_empty(ts_list),
    cp      = bind_or_empty(cp_list),
    full    = bind_or_empty(full_list)
  )
}

# =============================================================================
# Main
# =============================================================================

dt <- fread(input_file)
setnames(dt, names(dt), tolower(names(dt)))
dt <- dt[, .(
  date     = as.Date(date),
  pixel_id = as.character(pixel_id),
  lon      = as.numeric(lon),
  lat      = as.numeric(lat),
  county   = fifelse(is.na(county) | trimws(county) == "",
                     "unknown_county", as.character(county)),
  ntl_sfac = as.numeric(ntl_sfac)
)]
dt <- dt[!is.na(date)]
setorder(dt, county, pixel_id, date)

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

county_vec <- unique(dt$county)

# ---- set up county-level parallelism ---------------------------------------
plan(multisession, workers = n_workers)

message(sprintf("[BEAST daily] %d counties, %d workers, ~%d pixels total",
                length(county_vec), n_workers, length(unique(dt$pixel_id))))

# Process counties in parallel. Each worker writes its own four CSVs and
# returns the summary table to the main process.
summary_all <- future_lapply(
  seq_along(county_vec),
  function(i) {
    county_i  <- county_vec[i]
    folder_i  <- file.path(output_dir, make.names(county_i))
    dir.create(folder_i, recursive = TRUE, showWarnings = FALSE)

    dt_county <- dt[county == county_i]

    res_county <- process_county(
      dt_county     = dt_county,
      county_i      = county_i,
      season        = season,
      period        = period,
      delta_time    = delta_time,
      method        = method,
      has_outlier   = has_outlier,
      beast_threads = beast_threads_per_worker
    )

    fwrite(res_county$summary, file.path(folder_i, "summary.csv"))
    fwrite(res_county$ts,      file.path(folder_i, "time_series.csv"))
    fwrite(res_county$cp,      file.path(folder_i, "changepoints.csv"))
    fwrite(res_county$full,    file.path(folder_i, "beast_full_long.csv"))

    res_county$summary
  },
  future.seed = TRUE,
  future.globals = c(
    "process_county", "slice_result", "slice_component", "scalar_k",
    "extract_ts", "extract_cp", "flatten_beast",
    "first_not_na", "bind_or_empty", "%||%",
    "dt", "county_vec", "output_dir",
    "season", "period", "delta_time", "method", "has_outlier",
    "beast_threads_per_worker"
  ),
  future.packages = c("data.table", "Rbeast", "lubridate")
)

fwrite(bind_or_empty(summary_all), file.path(output_dir, "summary_all.csv"))

plan(sequential)
message("[BEAST daily] done.")
