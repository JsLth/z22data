source("data-raw/0-setup.R")

con <- connect("z22")
dir.create("data-raw/raw", showWarnings = FALSE)
dir.create("z22_data_100m", showWarnings = FALSE)
dir.create("z22_data_1km", showWarnings = FALSE)
dir.create("z22_data_10km", showWarnings = FALSE)

for (feat in na.omit(overview$z22)) {
  file_paths <- download_table(feat, year = 2022)

  for (file in file_paths) {
    feat_file <- file.path("data-raw/raw", basename(file))
    file.rename(file, feat_file)
    res <- regex_match(file, "100m|10km|1km")[[1]]
    table <- paste0(feat, "_", res)
    is_total <- feat %in% c("families", "buildings", "households")
    is_dwellings <- feat %in% "dwellings"
    sep <- if_else(is_dwellings, ".", ",")
    enc <- readr::guess_encoding(feat_file)$encoding[1]
    enc <- if_else(!identical(enc[1], "windows-1252"), "utf-8", enc)

    if (identical(enc, "windows-1252")) {
      temp_file <- fix_encoding(feat_file, tempfile())
      file.remove(feat_file)
      file.rename(temp_file, feat_file)
      enc <- "utf-8"
    }

    dbExecute(con, sprintf(
      "CREATE TABLE IF NOT EXISTS
        %s
      AS SELECT
        *
      FROM
        read_csv(
          '%s',
          delim = ';',
          decimal_separator = '%s',
          header = true,
          escape = '\"',
          nullstr = ['\u2013', '\u0096'],
          encoding = '%s'
        )",
      table, feat_file, sep, enc
    ))

    has_quality <- "werterlaeuternde_Zeichen" %in% dbListFields(con, table)

    # While in Zensus22, each feature usually gets its own file, totals do
    # not follow that pattern. They are included as an own category inside
    # categorized features concerning the respective unit (families, buildings).
    # I created special cases to extract totals for buildings, dwellings,
    # families and households. In the case of families and buildings, the
    # totals are included in their own column.
    cnames <- dbListFields(con, table)
    col_total <- cnames[startsWith(cnames, "Insgesamt")]
    if (is_total) {
      db_alter(con, sprintf(
        "SELECT
          GITTER_ID_%s, x_mp_%s, y_mp_%s,
          %s AS %s
        FROM
          %s",
        res, res, res, col_total, feat, table
      ))
    } else if (!is_dwellings && length(col_total)) {
      # If not dealing with totals, the total column must be dropped if it
      # exists, otherwise it will be recognized as a category downstream
      dbExecute(con, sprintf("ALTER TABLE %s DROP COLUMN %s", table, col_total))
    }

    if (is_dwellings) {
      # The number of dwelling attribute only comes together with the net rent.
      # Net rent, however, has its own data file, meaning it would be duplicated.
      dbExecute(con, sprintf("ALTER TABLE %s DROP COLUMN durchschnMieteQM", table))

      # The 2022 grid data files use the comma for two different uses:
      # - as a thousand separator (in case of integers)
      # - as a decimal separator (in case of decimal numbers)
      # As there isn't really any way to differentiate between integers and
      # floats if the columns need to be parsed as strings, I am simply making
      # the difference between "dwellings" (where the problem occurs)
      # and every other table. Yes, this is silly. The following lines replace
      # commas with nothing or a dot and then cast to numeric
      cnames <- setdiff(dbListFields(con, table), "AnzahlWohnungen")
      cnames <- paste(cnames, collapse = ", ")
      db_alter(
        con,
        sprintf(
          "SELECT
            %s,
            cast(
              regexp_replace(cast(AnzahlWohnungen AS VARCHAR), ',', '')
              AS INTEGER
            ) AS AnzahlWohnungen
          FROM %s",
          cnames, table
        )
      )
    }


    # Census 2022 data do not have a quality column which divides the quality
    # into 3 categories. Instead, they have an "extra info" column that gives
    # binary info on whether a value is reliable. The following lines
    # harmonize this approach with the one from 2011 by assigning only the
    # highest or lowest quality value depending on whether the value "KLAMMERN"
    # exists. If it exists, the value aggregation is probably unreliable.
    if (has_quality) {
      cnames <- dbListFields(con, table)
      cnames <- setdiff(cnames, "werterlaeuternde_Zeichen")
      cnames <- paste(cnames, collapse = ", ")
      db_alter(
        con,
        sprintf(
          "SELECT
            %s,
            CASE WHEN (werterlaeuternde_Zeichen IN ('KLAMMERN')) THEN 2
            WHEN NOT (werterlaeuternde_Zeichen IN ('KLAMMERN')) THEN 0 END
            AS quality
          FROM %s",
          cnames, table
        )
      )
    }

    # Pivot to long format to have all feature names in a column
    cnames <- dbListFields(con, table)
    cat_names <- cnames[
      !map_lgl(tolower(cnames), ~any(startsWith(.x, c("gitter", "x", "y")))) &
        !cnames %in% "quality"
    ]
    meta_names <- setdiff(cnames, cat_names)

    sql <- lapply(cat_names, function(col) {
      meta_names <- paste(meta_names, collapse = ", ")
      sprintf(
        "SELECT %s, '%s' AS category, \"%s\" AS value FROM %s",
        meta_names, col, col, table
      )
    })
    sql <- paste(sql, collapse = "\nUNION ALL\n")
    db_alter(con, sql)

    # Rename inspire grid, x and y
    db_alter(
      con,
      sprintf(
        "SELECT
          GITTER_ID_%s AS inspire,
          x_mp_%s AS x,
          y_mp_%s AS y,
          category, value%s
        FROM %s",
        res, res, res, ifelse(has_quality, ", quality", ""), table
      )
    )

    # z22 features should always use the category codes established by z11.
    # If this is not possible, just take the index numbers.
    # If only a single category exists (for all continuous features), create
    # a single category with category code 0
    cat_names <- cat_names[!startsWith(cat_names, "Insgesamt")]
    cat_codes <- categories[[feat]]$code %||% 0
    for (cat_i in seq_along(cat_names)) {
      cat <- cat_names[cat_i]
      cat_code <- cat_codes[cat_i]

      log <- "- Storing feat: {feat}, resolution: {res}, category: {cat} ({cat_code})"
      log <- cli::format_inline(log)
      cli::cli_inform(log)
      cat(log, "\n", file = sprintf("z22_data_%s/export.log", res), append = TRUE)
      dbExecute(
        con,
        sprintf(
          "COPY (
            SELECT x, y, value
            FROM %s
            WHERE category = '%s'
          )
          TO 'z22_data_%s/%s_%s.parquet' (
            FORMAT PARQUET,
            CODEC 'zstd',
            COMPRESSION_LEVEL -7
          )",
          table, cat, res, feat, cat_code
        )
      )
    }
  }
}
