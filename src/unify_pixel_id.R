library(data.table)

# Unify pixel_id across yearly CSVs using lat/lon as the spatial key.
# All original columns are retained, and the old pixel_id is copied to pixel_id_old.

# ---- user parameters ----
input_files <- c(
  "../data/ntl/Presults/2022/ntl_adjusted4_county.csv",
  "../data/ntl/Presults/2023/ntl_adjusted4_county.csv",
  "../data/ntl/Presults/2024/adjusted/ntl_adjusted_shp.csv"
)

output_dir <- "../data/ntl/"
output_file <- "ntl_2022_2024_unified.csv"
coord_digits <- 8
year_labels <- c("2022", "2023", "2024")

if (length(year_labels) != length(input_files)) {
  stop("year_labels length must match input_files length")
}

required_cols <- c("pixel_id", "lat", "lon")

read_and_tag <- function(path, year_label) {
  dt <- fread(path)
  missing_cols <- setdiff(required_cols, names(dt))
  if (length(missing_cols) > 0) {
    stop(sprintf(
      "File %s is missing required columns: %s",
      path,
      paste(missing_cols, collapse = ", ")
    ))
  }

  dt[, source_year := year_label]
  setnames(dt, "pixel_id", "pixel_id_old")
  dt[, lat_key := round(as.numeric(lat), coord_digits)]
  dt[, lon_key := round(as.numeric(lon), coord_digits)]
  dt
}

dt_list <- Map(read_and_tag, input_files, year_labels)
all_dt <- rbindlist(dt_list, fill = TRUE, use.names = TRUE)

# Build a master lookup table: one unified pixel_id for each lat/lon pair.
lookup <- unique(all_dt[, .(lat_key, lon_key)])
setorder(lookup, lat_key, lon_key)
lookup[, pixel_id := .I]

all_dt <- merge(
  all_dt,
  lookup,
  by = c("lat_key", "lon_key"),
  all.x = TRUE,
  sort = FALSE
)

# Optional check: the same old pixel_id maps to multiple coordinates within a year.
old_id_check <- unique(
  all_dt[, .(source_year, pixel_id_old, lat_key, lon_key)]
)[
  ,
  .(n_coord = .N),
  by = .(source_year, pixel_id_old)
][n_coord > 1]

if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

cols_out <- c(
  "pixel_id",
  "pixel_id_old",
  setdiff(names(all_dt), c("pixel_id", "pixel_id_old", "lat_key", "lon_key"))
)

out_dt <- all_dt[, ..cols_out]
setorder(out_dt, source_year, pixel_id)

fwrite(out_dt, file.path(output_dir, output_file))

fwrite(lookup, file.path(output_dir, "pixel_id_lookup.csv"))
fwrite(old_id_check, file.path(output_dir, "pixel_id_old_conflict_check.csv"))

message(
  "Unified file written to: ",
  normalizePath(file.path(output_dir, output_file), winslash = "/")
)
