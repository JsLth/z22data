source("data-raw/0-setup.R")
library(tidypolars)
library(dplyr)

con <- connect()

overwrite <- FALSE

for (table in names(tables)) {
  table_file <- paste0(table, ".csv")
  parq_dir <- "data-raw/zensus_grid"
  dir.create(parq_dir, showWarnings = FALSE)
  parq_file <- file.path(parq_dir, exchange_ext(table_file, "parquet")) |>
    normalizePath("/", mustWork = FALSE)

  # only overwrite existing files when explicitly asked to
  if (file.exists(parq_file) && !overwrite) next

  dir.create("data-raw/raw", showWarnings = FALSE)
  table_path <- new_path <- file.path("data-raw/raw", table_file)
  if (!file.exists(new_path)) {
    table_path <- download_table(table)

    # Grid CSVs are stored in Latin-1 encoding but polars only supports UTF-8.
    # This leads to a broken CSV file downstream which DuckDB cannot read.
    # -> convert encoding to UTF-8 before CSV is scanned by polars
    # Unfortunately, this can take a while because I don't want to rely on
    # external software like iconv which is not included by default on Windows.
    if (!csv_is_utf8(table_path)) {
      table_path <- fix_encoding(table_path, out = new_path)
    } else {
      file.rename(table_path, table_path <- new_path)
    }
  }

  is_pop <- identical(table, names(tables)[1])
  dtypes <- if (is_pop) {
    list(
      Gitter_ID_100m = "String",
      x_mp_100m = "Int32",
      y_mp_100m = "Int32",
      Einwohner = "Int32"
    )
  } else {
    list(
      Gitter_ID_100m = "String",
      Gitter_ID_100m_neu = "String",
      Merkmal = "String",
      Auspraegung_Code = "Int32",
      Auspraegung_Text = "String",
      Anzahl = "Int32",
      Anzahl_q = "Int32"
    )
  }

  new_names <- if (is_pop) {
    list(
      Gitter_ID_100m = "grid_100m",
      x_mp_100m = "x",
      y_mp_100m = "y",
      Einwohner = "pop"
    )
  } else {
    list(
      Gitter_ID_100m = "grid_100m",
      Gitter_ID_100m_neu = "grid_100m_new",
      Merkmal = "feature",
      Auspraegung_Code = "cat_code",
      Auspraegung_Text = "cat_text",
      Anzahl = "value",
      Anzahl_q = "quality"
    )
  }

  status("Converting", basename(table_path), "from csv to parquet")
  sep <- guess_sep(table_path)
  csv <- scan_csv_polars(table_path, separator = sep, dtypes = dtypes) |>
    rename_with(\(old_names) unlist(new_names[old_names]))

  sink_parquet(csv, path = parq_file)
}

info()

for (parq in dir(parq_dir, full.names = TRUE)) {
  status("Creating DuckDB table", basename(parq))
  table_en <- tables[[remove_ext(basename(parq))]]
  dbExecute(con, sprintf(
    "CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM '%s'",
    table_en, parq
  ))
}

info()
shutdown()
