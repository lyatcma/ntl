# =============================================================================
# BEAST per-pixel NTL change detection — WEEKLY, matrixed, BLOCK-parallel (v3)
# -----------------------------------------------------------------------------
# v3 changes vs. v2 (run_beast_by_pixel_daily_v2.R):
#   - Daily NTL is pre-aggregated to weekly MEDIAN per pixel before BEAST.
#     Median (not mean) is robust to single-day cloud/outlier residuals that
#     survive the BRDF Quality Flag mask.
#   - delta_time = 7/365 (weekly time step).
#   - Time grid is the ISO-week sequence min(date)..max(date).
#   - Everything else (block-parallel, OMP lock, MCMC trim, write_full toggle)
#     identical to v2. Output schema unchanged for kept files.
#
# Why weekly works for typhoon impact:
#   - VNP46A2 daily series is ~30% gappy from cloud masking; weekly median
#     usually drops gap rate below 5%.
#   - Typhoon detection in Zheng et al. (2025) allows a +-15 day window for
#     change-point timing — week-level localisation (~+-3.5d) is well within
#     that tolerance.
#   - Reduces sample count ~7x, so BEAST runs ~5-7x faster than v2.
# =============================================================================

# Lock OpenMP BEFORE loading Rbeast.
Sys.setenv(OMP_NUM_THREADS  = "1")
Sys.setenv(MKL_NUM_THREADS  = "1")
Sys.setenv(OPENBLAS_NUM_THREADS = "1")

suppressPackageStartupMessages({
  library(data.table)
  library(lubridate)
  library(Rbeast)
  library(future)
  library(future.apply)
})

# ----- user paths ------------------------------------------------------------
input_file <- "C:/Users/liuy1/Desktop/0.文章/3.重点研发Chao/0.nightime/data/ntl/Presults/unified/ntl_2022_2024_unified.csv"
output_dir <- "C:/Users/liuy1/Desktop/0.文章/3.重点研发Chao/0.nightime/data/ntl/BEAST/pixelevel/20260428_weekly"

# ----- BEAST parameters ------------------------------------------------------
season       <- "harmonic"
period       <- "1 year"
delta_time   <- 7/365          # WEEKLY
method       <- "bayes"
has_outlier  <- FALSE

# ----- weekly aggregation -----------------------------------------------------
# "median" is robust to residual cloud/outlier days; "mean" matches v1/v2.
weekly_agg_fun <- "median"     # one of: "median", "mean"

# Minimum number of valid daily obs required within a week for that week to
# carry a value (otherwise NA -> data gap).
min_obs_per_week <- 1L

# ----- MCMC ------------------------------------------------------------------
# With ~150 weekly points (3 yrs), default-ish MCMC is already cheap.
mcmc_samples     <- 5000L
mcmc_burnin      <- 200L
mcmc_chainNumber <- 2L

# ----- parallel / scheduling -------------------------------------------------
block_size <- 120L      # bigger than v2's 80, since per-pixel cost is ~7x lower
n_workers  <- max(1L, parallel::detectCores() - 2L)

# ----- output knobs ----------------------------------------------------------
write_full <- FALSE

# =============================================================================
# Helpers
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

# Aggregate a numeric vector by the chosen statistic, NA-safe.
agg_fn <- function(x, fun) {
  x <- x[!is.na(x)]
  if (length(x) == 0L) return(NA_real_)
  if (fun == "median") return(as.numeric(stats::median(x)))
  return(as.numeric(mean(x)))
}

# Map a date to the Monday of its ISO week (so all 7 days collapse to one anchor).
to_week_anchor <- function(d) {
  # ISO week starts on Monday. floor_date "week" with week_start = 1 -> Monday.
  as.Date(lubridate::floor_date(d, unit = "week", week_start = 1))
}

flatten_beast <- function(x, county, pixel_id, lon, lat, prefix = NULL) {
  out <- list()
  walk <- function(obj, path) {
    if (is.null(obj)) return()
    if (is.list(obj) && !is.data.frame(obj)) {
      nm <- names(obj)
      if (is.null(nm)) nm <- seq_along(obj)
      for (i in seq_along(obj)) walk(obj[[i]], c(path, nm[i]))
      return()
    }
    dims  <- dim(obj)
    field <- paste(path, collapse = ".")
    if (is.null(dims)) {
      out[[length(out) + 1L]] <<- data.table(
        county = county, pixel_id = pixel_id, lon = lon, lat = lat,
        field = field, i = seq_along(obj), j = NA_integer_,
        value = as.character(obj)
      )
    } else {
      idx <- arrayInd(seq_len(length(obj)), dims)
      out[[length(out) + 1L]] <<- data.table(
        county = county, pixel_id = pixel_id, lon = lon, lat = lat,
        field = field, i = idx[, 1], j = idx[, 2],
        value = as.character(c(obj))
      )
    }
  }
  walk(x, prefix)
  if (length(out) == 0) data.table() else rbindlist(out, fill = TRUE)
}

# =============================================================================
# Slicing helpers — pull pixel k out of a matrixed BEAST result
# =============================================================================

scalar_k <- function(v, k) {
  if (is.null(v)) return(NA_real_)
  if (length(v) == 1L) return(as.numeric(v))
  if (length(v) >= k)  return(as.numeric(v[k]))
  NA_real_
}

slice_component <- function(comp, k) {
  if (is.null(comp)) return(NULL)
  out <- list()
  for (nm in names(comp)) {
    x <- comp[[nm]]
    if (is.null(x)) { out[[nm]] <- NULL; next }
    d <- dim(x)
    if (is.null(d)) {
      out[[nm]] <- if (length(x) >= k) x[k] else x
    } else if (length(d) == 2L) {
      out[[nm]] <- x[, k]
    } else if (length(d) == 3L) {
      out[[nm]] <- x[, , k]
    } else {
      out[[nm]] <- x
    }
  }
  out
}

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
# Per-pixel extractors — schema unchanged
# =============================================================================

extract_ts <- function(res, county, pixel_id, lon, lat) {
  n <- length(res$time)
  out <- data.table(
    county = county, pixel_id = pixel_id, lon = lon, lat = lat,
    t = seq_len(n), time = res$time, observed = res$data
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
    county = county, pixel_id = pixel_id, lon = lon, lat = lat,
    component = comp_name, rank = seq_along(comp$cp),
    cp = comp$cp, cpPr = comp$cpPr, cpAbruptChange = comp$cpAbruptChange
  )
  if (!is.null(ci) && is.matrix(ci) && ncol(ci) >= 2) {
    out[, cpCI_low  := ci[, 1]]
    out[, cpCI_high := ci[, 2]]
  }
  out[!is.na(cp) & !is.nan(cp)]
}

# =============================================================================
# Block worker — weekly aggregate + matrixed BEAST + per-pixel extraction
# =============================================================================
process_block <- function(dt_block, county_i, pixel_meta_block,
                          season, period, delta_time, method, has_outlier,
                          weekly_agg_fun, min_obs_per_week,
                          mcmc_samples, mcmc_burnin, mcmc_chainNumber,
                          write_full) {

  pixel_vec <- pixel_meta_block$pixel_id
  n_pix     <- length(pixel_vec)

  if (n_pix == 0L) {
    return(list(summary = data.table(), ts = data.table(),
                cp = data.table(), full = data.table()))
  }

  # ---- 1. weekly aggregation per (pixel, week) ----------------------------
  dt_block[, week := to_week_anchor(date)]

  wk <- dt_block[, .(
    pixel_id = first_not_na(pixel_id),
    lon      = first_not_na(lon),
    lat      = first_not_na(lat),
    county   = first_not_na(county),
    n_obs    = sum(!is.na(ntl_sfac)),
    ntl_w    = agg_fn(ntl_sfac, weekly_agg_fun)
  ), by = .(pid = pixel_id, week)]
  wk[, pid := NULL]

  # mark weeks with too few daily obs as missing
  wk[n_obs < min_obs_per_week, ntl_w := NA_real_]
  setorder(wk, pixel_id, week)

  # ---- 2. unified weekly grid --------------------------------------------
  week_grid <- seq.Date(min(wk$week), max(wk$week), by = "7 days")
  n_t       <- length(week_grid)

  # ---- 3. matrix [time x pixels] -----------------------------------------
  wide <- dcast(wk, week ~ pixel_id, value.var = "ntl_w",
                fun.aggregate = function(x) agg_fn(x, weekly_agg_fun),
                fill = NA_real_)
  full_grid <- data.table(week = week_grid)
  wide      <- merge(full_grid, wide, by = "week", all.x = TRUE)
  setorder(wide, week)

  Y <- as.matrix(wide[, ..pixel_vec])
  storage.mode(Y) <- "double"

  n_obs_per_pix <- colSums(!is.na(Y))
  keep_idx      <- which(n_obs_per_pix >= 2L)
  skip_idx      <- which(n_obs_per_pix <  2L)

  res <- NULL
  if (length(keep_idx) > 0L) {
    Y_keep <- Y[, keep_idx, drop = FALSE]

    res <- beast123(
      Y = Y_keep,
      metadata = list(
        time           = week_grid,
        deltaTime      = delta_time,
        period         = period,
        whichDimIsTime = 1L,
        hasOutlier     = has_outlier
      ),
      season = season,
      method = method,
      mcmc = list(
        samples     = mcmc_samples,
        burnin      = mcmc_burnin,
        chainNumber = mcmc_chainNumber
      ),
      extra = list(
        dumpInputData       = TRUE,
        computeCredible     = TRUE,
        computeSeasonChngpt = TRUE,
        computeTrendChngpt  = TRUE,
        computeSeasonAmp    = TRUE,
        computeTrendSlope   = TRUE,
        numThreadsPerCPU    = 1L,
        numParThreads       = 1L,
        printProgress       = FALSE,
        printParameter      = FALSE,
        quiet               = TRUE
      )
    )
  }

  summaries <- vector("list", n_pix)
  ts_list   <- vector("list", n_pix)
  cp_list   <- vector("list", n_pix)
  full_list <- vector("list", n_pix)

  for (kk in seq_along(keep_idx)) {
    px_idx <- keep_idx[kk]
    pid    <- pixel_meta_block$pixel_id[px_idx]
    lon    <- pixel_meta_block$lon[px_idx]
    lat    <- pixel_meta_block$lat[px_idx]
    res_k  <- slice_result(res, kk)

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

    if (isTRUE(write_full)) {
      full_list[[px_idx]] <- flatten_beast(res_k, county_i, pid, lon, lat)
    }
  }

  for (px_idx in skip_idx) {
    pid <- pixel_meta_block$pixel_id[px_idx]
    lon <- pixel_meta_block$lon[px_idx]
    lat <- pixel_meta_block$lat[px_idx]
    summaries[[px_idx]] <- data.table(
      county = county_i, pixel_id = pid, lon = lon, lat = lat,
      n = n_t, n_non_missing = n_obs_per_pix[px_idx],
      fit_status = "skipped"
    )
  }

  list(
    summary = bind_or_empty(summaries),
    ts      = bind_or_empty(ts_list),
    cp      = bind_or_empty(cp_list),
    full    = if (isTRUE(write_full)) bind_or_empty(full_list) else data.table()
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

block_plan <- rbindlist(lapply(county_vec, function(cn) {
  pix_in_county <- unique(dt[county == cn, pixel_id])
  n_pix         <- length(pix_in_county)
  if (n_pix == 0L) return(NULL)
  n_blocks      <- max(1L, ceiling(n_pix / block_size))
  block_id      <- rep(seq_len(n_blocks), length.out = n_pix,
                       each = ceiling(n_pix / n_blocks))[seq_len(n_pix)]
  data.table(county = cn, pixel_id = pix_in_county, block = block_id)
}), use.names = TRUE, fill = TRUE)

task_keys <- unique(block_plan[, .(county, block)])

message(sprintf(
  "[BEAST weekly v3] %d pixels in %d counties -> %d blocks, %d workers, agg=%s, MCMC=%d/%d/%d, write_full=%s",
  uniqueN(dt$pixel_id), length(county_vec), nrow(task_keys),
  n_workers, weekly_agg_fun,
  mcmc_samples, mcmc_burnin, mcmc_chainNumber, write_full
))

plan(multisession, workers = n_workers)

block_results <- future_lapply(
  seq_len(nrow(task_keys)),
  function(ti) {
    cn <- task_keys$county[ti]
    bk <- task_keys$block[ti]

    pix_keep <- block_plan[county == cn & block == bk, pixel_id]
    dt_block <- dt[county == cn & pixel_id %in% pix_keep]

    pixel_meta_block <- unique(dt_block[, .(pixel_id, lon, lat, county)],
                               by = "pixel_id")
    pixel_meta_block <- pixel_meta_block[match(pix_keep, pixel_meta_block$pixel_id)]

    process_block(
      dt_block         = dt_block,
      county_i         = cn,
      pixel_meta_block = pixel_meta_block,
      season           = season,
      period           = period,
      delta_time       = delta_time,
      method           = method,
      has_outlier      = has_outlier,
      weekly_agg_fun   = weekly_agg_fun,
      min_obs_per_week = min_obs_per_week,
      mcmc_samples     = mcmc_samples,
      mcmc_burnin      = mcmc_burnin,
      mcmc_chainNumber = mcmc_chainNumber,
      write_full       = write_full
    )
  },
  future.seed = TRUE,
  future.packages = c("data.table", "Rbeast", "lubridate")
)

# regroup by county and write
task_keys[, idx := .I]
summary_all <- vector("list", length(county_vec))

for (ci in seq_along(county_vec)) {
  cn       <- county_vec[ci]
  folder_i <- file.path(output_dir, make.names(cn))
  dir.create(folder_i, recursive = TRUE, showWarnings = FALSE)

  block_idx <- task_keys[county == cn, idx]

  county_summary <- bind_or_empty(lapply(block_idx, function(b) block_results[[b]]$summary))
  county_ts      <- bind_or_empty(lapply(block_idx, function(b) block_results[[b]]$ts))
  county_cp      <- bind_or_empty(lapply(block_idx, function(b) block_results[[b]]$cp))

  fwrite(county_summary, file.path(folder_i, "summary.csv"))
  fwrite(county_ts,      file.path(folder_i, "time_series.csv"))
  fwrite(county_cp,      file.path(folder_i, "changepoints.csv"))

  if (isTRUE(write_full)) {
    county_full <- bind_or_empty(lapply(block_idx, function(b) block_results[[b]]$full))
    fwrite(county_full, file.path(folder_i, "beast_full_long.csv"))
  }

  summary_all[[ci]] <- county_summary
}

fwrite(bind_or_empty(summary_all), file.path(output_dir, "summary_all.csv"))

plan(sequential)
message("[BEAST weekly v3] done.")
