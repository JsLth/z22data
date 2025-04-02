source("data-raw/0-setup.R")
dir.create("grids", showWarnings = FALSE)

overwrite <- FALSE
years <- c(2015, 2017, 2018, 2019)
reses <- c("100m", "250m", "500m", "1km", "5km", "10km", "100km")

for (year in years) {
  for (res in reses) {
    new_file <- sprintf("grids/grid_%s_%s.parquet", year, res)
    if (file.exists(new_file) && !overwrite) next
    cli_progress_step("Packaging grid: year: {year}, resolution: {res}")
    tempf <- tempfile()
    resp <- request("https://daten.gdz.bkg.bund.de/produkte/sonstige/geogitter/") |>
      req_url_path_append(year) |>
      req_url_path_append(sprintf("DE_Grid_ETRS89-LAEA_%s.csv.zip", res)) |>
      req_perform(path = tempf)

    files <- unzip_ext(resp$body, "csv", exdir = tempdir())

    csv <- lapply(files, \(x) arrow::read_csv2_arrow(x, col_names = FALSE)[4:5]) |>
      bind_rows() |>
      rename(x = f3, y = f4)

    arrow::write_parquet(
      csv,
      new_file,
      compression = "zstd",
      compression_level = -7
    )
  }
}
