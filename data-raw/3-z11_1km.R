source("data-raw/0-setup.R")

dir.create("z11_data_1km", showWarnings = FALSE)

csv <- download_zipped_csv(z11_base_url, z11_1km_file)
tb <- read.csv2(csv)

for (feat in colnames(tb)[-1:-3]) {
  feat_new <- overview[overview$z11_1km %in% feat,]$name
  tb_feat <- select(
    tb,
    value = all_of(feat),
    x = x_mp_1km,
    y = y_mp_1km
  )
  file <- file.path("z11_data_1km/", paste0(feat_new, "_0.parquet"))
  arrow::write_parquet(tb_feat, file, compression = "zstd", compression_level = -7)
}
