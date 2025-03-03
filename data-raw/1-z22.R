source("data-raw/0-setup.R")

con <- connect()
dir.create("data-raw/zensus_grid", showWarnings = FALSE)
dir.create("data-raw/raw", showWarnings = FALSE)
dir.create("z22_data_100m", showWarnings = FALSE)
dir.create("z22_data_1km", showWarnings = FALSE)
dir.create("z22_data_10km", showWarnings = FALSE)
dir.create("lookup", showWarnings = FALSE)

for (feat in z22_feats) {
  file_paths <- download_table2(feat)

  for (file in file_paths) {
    new_path <- file.path("data-raw/raw", basename(file))
    # Grid CSVs are stored in Latin-1 encoding but polars only supports UTF-8.
    # This leads to a broken CSV file downstream which DuckDB cannot read.
    # -> convert encoding to UTF-8 before CSV is scanned by polars
    # Unfortunately, this can take a while because I don't want to rely on
    # external software like iconv which is not included by default on Windows.
    if (!csv_is_utf8(file)) {
      file <- fix_encoding(file, out = new_path)
    } else {
      file.rename(file, file <- new_path)
    }

    res <- regex_match(file, "100m|10km|1km")[[1]]
    table <- paste0(feat, "_", res)
    clean_file <- sprintf("data-raw/zensus_grid/%s.parquet", table)
    is_dn <- identical(feat, "dwelling_number")
    csv <- scan_csv_polars(
      file,
      separator = ";",
      null_values = c("\u2013", "\u0096"),
      ignore_errors = is_dn
    )

    # The number of dwelling attribute only comes together with the net rent.
    # Net rent, however, as its own data file, meaning it would be duplicated.
    if (is_dn) {
      csv <- select(csv, -durchschnMieteQM)
    }

    # The 2022 grid data files use the comma for two different uses:
    # - as a thousand separator (in case of integers)
    # - as a decimal separator (in case of decimal numbers)
    # As there isn't really any way to differentiate between integers and
    # floats if the columns need to be parsed as strings, I am simply making
    # the difference between "dwelling_number" (where the problem occurs)
    # and every other table. Yes, this is silly. The following lines replace
    # commas with nothing or a dot and then cast to numeric
    for (col in z22_select_feat_column(csv)) {
      new_sep <- ifelse(is_dn, "", ".")
      csv <- csv$with_columns(
        pl$col(col)$
          cast(pl$String)$
          str$replace(",", new_sep)$
          cast(pl$Float64)
      )
    }

    # Census 2022 data do not have a quality column which divides the quality
    # into 3 categories. Instead, they have an "extra info" column that gives
    # binary info on whether a value is reliable. The following lines
    # harmonize this approach with the one from 2011 by assigning only the
    # highest or lowest quality value depending on whether the value "KLAMMERN"
    # exists. If it exists, the value aggregation is probably unreliable.
    if ("werterlaeuternde_Zeichen" %in% csv$columns) {
      csv <- mutate(
        csv,
        quality = if_else(werterlaeuternde_Zeichen %in% "KLAMMERN", 2, 0),
        .keep = "unused"
      )
    }

    # Pivot to long format to have all feature names in a column
    csv <- rename_with(csv, .cols = starts_with(c("x", "y")), \(x) c("x", "y")) |>
      pivot_longer(
        !any_of("quality") & !starts_with(c("GITTER", "x", "y")),
        names_to = "category",
        values_to = "value"
      )

    cols <- csv$columns

    # This does not work if columns contain decimal numbers.
    # https://github.com/pola-rs/polars/issues/17289
    # sink_parquet(csv, clean_file)

    # Workaround using `collect()`. This is applied to all tables for
    # consistency, even if they would work using `sink_parquet`.
    # It takes a lot longer but what can you do.
    t <- try(arrow::write_parquet(collect(csv), clean_file))
    if (inherits(t, "try-error")) browser()

    # Import to DuckDB database
    dbExecute(con, sprintf(paste(
      "CREATE TABLE IF NOT EXISTS %s AS SELECT x, y, category, value FROM '%s'"
    ), table, clean_file))

    cats <- dbGetQuery(con, sprintf("SELECT DISTINCT category FROM %s", table))[[1]]

    # Uncomment to check whether category codes match the ones from z11
    lookup <- data.frame(code = seq_along(cats), cat = cats)
    write.csv(lookup, sprintf("lookup/lookup_%s_%s.csv", res, feat), row.names = FALSE)

    for (cat_code in seq_along(cats)) {
      out_file <- paste0(feat, "_", cat_code, ".parquet")

      dbExecute(
        con,
        sprintf(
          "COPY (
            SELECT x, y, value
            FROM %s
            WHERE category = '%s'
          )
          TO 'z22_data_%s/%s' (
            FORMAT PARQUET,
            CODEC 'zstd',
            COMPRESSION_LEVEL -7
          )",
          table, cats[cat_code], res, out_file
        )
      )
    }
  }
}
