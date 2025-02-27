source("data-raw/0-setup.R")

dir.create("data_1km", showWarnings = FALSE)

for (ds in tables_1km) {
  ds_name <- names(tables_1km)[match(ds, tables_1km)]
  csv <- download_table(ds_name)
  tb <- read.csv2(csv)

  for (feat in colnames(tb)[-1:-3]) {
    tb_feat <- select(
      tb,
      all_of(feat),
      x = x_mp_1km,
      y = y_mp_1km
    )
    file <- file.path("data_1km/", paste0(ds, "_", feat, ".parquet"))

    arrow::write_parquet(tb_feat, file, compression = "zstd", compression_level = -7)
  }

  grid <- select(tb, x = x_mp_1km, y = y_mp_1km)
  file <- file.path("data_1km/", "_grid.parquet")
  arrow::write_parquet(grid, file, compression = "zstd", compression_level = -7)
}
