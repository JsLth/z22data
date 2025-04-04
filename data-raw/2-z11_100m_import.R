source("data-raw/0-setup.R")

con <- connect("z11")

overwrite <- FALSE
dir.create("data-raw/raw", showWarnings = FALSE)
dir.create("z11_data_100m", showWarnings = FALSE)

dtypes <- list(
  Gitter_ID_100m = "VARCHAR",
  Gitter_ID_100m_neu = "VARCHAR",
  Merkmal = "VARCHAR",
  Auspraegung_Code = "INTEGER",
  Auspraegung_Text = "VARCHAR",
  Anzahl = "INTEGER",
  Anzahl_q = "INTEGER"
) |>
  toJSON(auto_unbox = TRUE, pretty = TRUE)

new_names <- list(
  Gitter_ID_100m = "grid_100m",
  Gitter_ID_100m_neu = "grid_100m_new",
  Merkmal = "feature",
  Auspraegung_Code = "cat_code",
  Auspraegung_Text = "cat_text",
  Anzahl = "value",
  Anzahl_q = "quality"
)

# Create a grid table for later reference. This is used to join the
# coordinates to the feature tables.
grid_file <- "data-raw/raw/grid.csv"
if (!file.exists(grid_file)) {
  down_file <- download_z11_grid()
  file.rename(down_file, grid_file)
  dbExecute(con, sprintf(
    "CREATE TABLE IF NOT EXISTS _grid AS
    SELECT
      Gitter_ID_100m AS grid_100m,
      x_mp_100m AS x,
      y_mp_100m AS y
    FROM  read_csv(
      '%s',
      delim = '%s',
      header = true,
      columns = {
        'Gitter_ID_100m': 'String',
        'x_mp_100m': 'Int32',
        'y_mp_100m': 'Int32',
        'Einwohner': 'Int32'
      },
      escape = '\"',
      encoding = 'latin-1'
    )",
    grid_file, guess_sep(grid_file)
  ))
}


for (theme in names(z11_100m_files)) {
  all_feats <- overview[tolower(overview$theme) %in% theme, ]
  all_feats <- all_feats[!is.na(all_feats$z11_100m), ]
  theme_file <- sprintf("data-raw/raw/%s.csv", theme)

  if (overwrite || !file.exists(theme_file)) {
    url <- z11_100m_files[[theme]]
    down_file <- download_table(theme, year = 2011)
    file.rename(down_file, theme_file)

    # Grid CSVs are stored in Latin-1 encoding but polars only supports UTF-8.
    # This leads to a broken CSV file downstream which DuckDB cannot read.
    # -> convert encoding to UTF-8 before CSV is scanned by polars
    # Unfortunately, this can take a while because I don't want to rely on
    # external software like iconv which is not included by default on Windows.
    #if (!csv_is_utf8(down_file)) {
    #  table_path <- fix_encoding(down_file, out = new_path)
    #} else {
    #  file.rename(table_path, table_path <- new_path)
    #}
  }

  # Read theme tables from CSV. Latin-1 encoding is important.
  sep <- guess_sep(theme_file)
  dbExecute(con, sprintf(
    "CREATE TABLE IF NOT EXISTS
      %s
    AS SELECT
      *
    FROM
      read_csv(
        '%s',
        delim = '%s',
        header = true,
        columns = %s,
        escape = '\"',
        encoding = 'latin-1'
      )",
    theme, theme_file, sep, dtypes
  ))

  # Rename columns to more readable names
  if (!any(dbListFields(con, theme) %in% new_names)) {
    name_select <- paste(
      paste(names(new_names), "AS", new_names ),
      collapse = ", "
    )
    db_alter(con, sprintf("SELECT %s FROM %s", name_select, theme))
  }

  # Join with grid to retrieve x/y coordinates
  db_alter(con, sprintf(
    "SELECT %s, g.x, g.y FROM %s AS t
    LEFT JOIN _grid AS g ON t.grid_100m = g.grid_100m",
    paste(paste0("t.", new_names), collapse = ", "), theme
  ))

  # For some reason, some feature names contain leading white space
  dbExecute(con, sprintf("UPDATE %s SET feature = TRIM(feature)", theme))

  # Extract a combination of all combinations of feature and category code
  # This is used to loop through each combination and create a distinct table
  # for each.
  feat_combos <- dbGetQuery(con, sprintf(
    "SELECT DISTINCT feature, cat_code
    FROM %s
    ORDER BY feature, cat_code",
    theme
  ))

  for (feat in unique(feat_combos$feature)) {
    is_ambiguous <- which(endsWith(all_feats$z11_100m, theme))
    feat_new <- all_feats$name[startsWith(all_feats$z11_100m, feat)]
    cat_codes <- feat_combos[feat_combos$feature %in% feat, ]$cat_code

    # HHTYP_SENIOR_HH is grouped under families, but really doesnt belong there,
    # so we manually adjust
    if (identical(feat, "HHTYP_SENIOR_HH")) {
      feat_new <- "household_senior"
    }

    for (cat in cat_codes) {
      combo_table <- sprintf("%s_%s", feat_new, cat)
      parq_file <- sprintf("z11_data_100m/%s.parquet", combo_table)

      if (!overwrite && file.exists(parq_file)) {
        next
      }

      cli::cli_inform("Storing {combo_table}")

      # Extract a table for each combination of feature/category
      # and export it to parquet
      # This export is the final data file.
      dbExecute(con, sprintf(
        "COPY (
          SELECT value, quality, x, y
          FROM %s
          WHERE feature = '%s' AND cat_code = '%s'
        ) TO '%s' (FORMAT PARQUET, CODEC 'zstd', COMPRESSION_LEVEL -7)",
        theme, feat, cat, parq_file
      ))

      # Create load.sql and schema.sql to reproduce the database at a later time
      cat(
        sprintf("CREATE TABLE %s(%s)", combo_table, dtypes), "\n",
        file = "z11_data_100m/schema.sql",
        append = TRUE
      )

      cat(
        sprintf(
          "COPY %s FROM 'data/%s.parquet' (FORMAT 'parquet')",
          combo_table, combo_table
        ), "\n",
        file = "z11_data_100m/load.sql",
        append = TRUE
      )
    }
  }
}
