source("data-raw/0-setup.R")

con <- connect()

parq_dir <- "data-raw/zensus_grid"
for (parq in dir(parq_dir, full.names = TRUE)) {
  table_en <- tables[[remove_ext(basename(parq))]]
  cli_inform("Creating DuckDB table {table_en}")
  dbExecute(con, sprintf(
    "CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM '%s'",
    table_en, parq
  ))
}

query_all(con, "UPDATE {table} SET feature = TRIM(feature)")

# Extract a dataframe containing all combinations of table name, feature, and category
all_feats <- map(tables[-1], .progress = "Generating new table IDs", function(x) {
  tb <- tbl(con, x) |>
    distinct(feature, cat_code) |>
    collect() |>
    arrange(
      factor(feature, levels = unique(unlist(lapply(cat_dict, names)))),
      cat_code
    )
  bind_cols(table = x, tb)
}) |>
  bind_rows()

dbExecute(con, "CREATE TABLE _grid AS SELECT grid_100m, x, y FROM population")

# Population is a special case as it is not formatted in the same way as
# the other tables. This gives the population table the same format as the
# other tables for consistency
if ("x" %in% dbListFields(con, "population")) {
  db_alter(con, "SELECT pop AS value, x, y FROM population WHERE pop <> -1")
}

# Create a new table for each combination of table name, feature and category
pwalk(
  all_feats,
  .progress = "Fragmenting tables",
  function(table, feature, cat_code) {
    if (!all(c("feature", "cat_code") %in% dbListFields(con, table))) {
      stop(sprintf("Table %s is corrupt and does not contain the feature or cat_code columns.", table))
    }

    # construct new name from table name, feature and category code
    new_name <- paste(table, feature, cat_code, sep = "_")

    # create multiple smaller tables based on feature and category combinations
    dbExecute(con, sprintf(paste(
      "CREATE TABLE %s AS",
      "SELECT t.value, t.quality, g.x, g.y FROM %s AS t",
      "LEFT JOIN _grid AS g ON t.grid_100m = g.grid_100m",
      "WHERE (t.feature = '%s' AND t.cat_code = %s)"),
      new_name, table, feature, cat_code
    ))
  }
)

# Reduce size of _grid table
db_alter(con, "SELECT x, y FROM _grid")

# Drop all original tables (except population)
walk(tables[-1], \(x) dbExecute(con, sprintf("DROP TABLE %s", x)))


# Export to parquet
dir.create("data", showWarnings = FALSE)
walk(dbListTables(con), .progress = "Exporting tables", function(table) {
  dbExecute(con, sprintf(
    "COPY %s TO 'data/%s.parquet' (FORMAT PARQUET, CODEC 'zstd', COMPRESSION_LEVEL -7);",
    table, table
  ))
  cat(create_schema(con, table), "\n", file = "data/schema.sql", append = TRUE)
  cat(create_load(con, table), "\n", file = "data/load.sql", append = TRUE)
})

shutdown(con)
