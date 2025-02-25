library(DBI)
library(dbplyr)
library(dplyr)
library(duckdb)
library(polars)
library(purrr)
library(dplyr)
library(tidypolars)
library(purrr)
library(cli)
library(httr2)

# Maps CSV file names to remote file names
url_to_table <- list(
  `Zensus_Bevoelkerung_100m-Gitter` = "csv_Bevoelkerung_100m_Gitter.zip",
  Bevoelkerung100M = "csv_Demographie_100_Meter-Gitter.zip",
  Familie100m = "Download-Tabelle_Familien_im_100_Meter-Gitter_im_CSV-Format%20(2).zip",
  Haushalte100m = "Download-Tabelle_Haushalt_im_100_Meter-Gitter_im_CSV-Format.zip",
  Wohnungen100m = "Download-Tabelle_Wohnungen_im_100_Meter-Gitter_im_CSV-Format.zip",
  Gebaeude100m = "Download-Tabelle_Gebaeude_und_Wohnungen_im_100_Meter-Gitter_im_CSV-Format.zip"
)

# Maps CSV file names to clean english names
tables <- list(
  `Zensus_Bevoelkerung_100m-Gitter` = "population",
  Bevoelkerung100M = "demography",
  Familie100m = "families",
  Haushalte100m = "households",
  Wohnungen100m = "dwellings",
  Gebaeude100m = "buildings"
)

download_table <- function(table, path = tempfile(), timeout = 1000) {
  old <- options(timeout = timeout)
  on.exit(options(old))

  if (table %in% tables) {
    table <- names(tables)[match(table, tables)]
  } else if (!table %in% names(tables)) {
    stop("Table not found.")
  }

  file <- url_to_table[[table]]
  path <- normalizePath(path, "/", mustWork = FALSE)
  url <- paste0("https://www.zensus2022.de/static/DE/gitterzellen/", file)
  target_dir <- dirname(path)
  cli_inform("Downloading {table} to {target_dir}")
  request(url) |>
    req_progress() |>
    req_perform(path = path)
  zipfiles <- unzip(path, list = TRUE)$Name
  target_file <- zipfiles[has_file_ext(zipfiles, "csv")]
  unzip(path, files = target_file, exdir = target_dir)
  file.path(target_dir, target_file)
}

has_file_ext <- function(file, ext) {
  grepl(sprintf("\\.%s$", ext), file)
}

# Remove a file extension
remove_ext <- function(file) {
  gsub("(\\.[[:alpha:]]+)$", "", file)
}

# Exchange file extension with a different file extension
exchange_ext <- function(file, ext) {
  gsub("\\.[[:alpha:]]+$", paste0(".", ext), file)
}

regex_match <- function(text, pattern, ...) {
  regmatches(text, regexec(pattern, text, ...))
}

guess_sep <- function(file) {
  header <- readLines(file, n = 1)
  if (grepl(",", header)) return(",")
  if (grepl(";", header)) return(";")
  if (grepl("\t", header)) return("\t")
}

# Function to check if a CSV file is likely encoded in UTF-8
csv_is_utf8 <- function(file, nrows = 1000, ...) {
  csv <- read.csv2(file, nrows = nrows, ...)
  all(vapply(
    csv,
    FUN.VALUE = logical(1),
    \(x) if (is.character(x)) all(validUTF8(x)) else TRUE
  ))
}

# Iteratively converts file encoding from Latin-1 to UTF-8
fix_encoding <- function(file, out, chunk_size = 1e6, from = "ISO-8859-1") {
  incon <- file(file, "rb", encoding = "bytes")
  outcon <- file(out, "w", encoding = "UTF-8")
  i <- 1
  cli_progress_step(
    "Converted chunk {i} in {file} from {from} to UTF-8.",
    msg_done = "Successfully converted file {file} to UTF-8.",
    msg_failed = "Could not convert {file}."
  )

  repeat {
    i <- i + 1
    cli_progress_update()
    lines <- readLines(incon, n = chunk_size, encoding = "bytes")

    if (!length(lines)) {
      break
    }

    utf8_lines <- iconv(lines, from = "ISO-8859-1", to = "UTF-8")
    writeLines(utf8_lines, outcon, useBytes = TRUE)
  }

  close(incon)
  close(outcon)
  out
}

# Execute an SQL query for all tables. To exclude the population table,
# set population = FALSE (because it's structured differently than other
# tables). To add additional variables, set "%s" in the statement and
# provide the strings using ...
query_all <- function(con, statement, ..., population = FALSE) {
  statement <- sprintf(statement, ...)
  if (!population) tables <- tables[-1]
  for (table in tables) {
    istatement <- gsub("{table}", table, statement, fixed = TRUE)
    dbExecute(con, istatement)
  }
}

change_colnames <- function(con, old, new) {
  query_all(con, "ALTER TABLE {table} RENAME COLUMN %s TO %s;", old, new)
}

db_alter <- function(con, statement, tempname = NULL) {
  table_name <- regex_match(statement, "FROM ([a-zA-Z0-9_]+)")[[1]][2]
  if (is.null(tempname)) {
    tempname <- sprintf("_%s_temp", table_name)
  }

  dbExecute(con, sprintf("CREATE TABLE %s AS %s", tempname, statement))
  dbExecute(con, sprintf("DROP TABLE %s", table_name))
  dbExecute(con, sprintf("ALTER TABLE %s RENAME TO %s", tempname, table_name))
}

create_schema <- function(con, table) {
  dtypes <- dbFetch(dbSendQuery(
    con,
    paste(
      "SELECT column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE",
      "table_name = 'population'"
    )
  ))
  dtypes$column_name <- dQuote(dtypes$column_name, q = FALSE)
  dtypes <- paste(pmap(dtypes, paste), collapse = ", ")
  sprintf("CREATE TABLE %s(%s);", table, dtypes)
}

# COPY buildings_BAUJAHR_MZ_1 FROM 'data/buildings_baujahr_mz__.parquet' (FORMAT 'parquet');
create_load <- function(con, table) {
  sprintf("COPY %s FROM 'data/%s.parquet' (FORMAT 'parquet');", table, table)
}

status <- function(...) {
  cat(..., "\r", strrep(" ", 10), file = stderr())
}

info <- function(...) {
  cat(..., "\n", file = stderr())
}

connect <- function() {
  dbConnect(duckdb::duckdb(), dbdir = "data-raw/z22.duckdb")
}

shutdown <- function(con) {
  try(dbDisconnect(con, shutdown = TRUE), silent = TRUE)
}

restart <- function(con) {
  shutdown(con)
  connect()
}
