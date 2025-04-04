library(DBI)
library(dbplyr)
library(dplyr)
library(tidyr)
library(duckdb)
library(purrr)
library(dplyr)
library(purrr)
library(cli)
library(httr2)
library(jsonlite)

source("data-raw/categories.R")

"%||%" <- function(x, y) if (is.null(x)) y else x
"%|||%" <- function(x, y) if (is.null(x) || all(is.na(x))) y else x

overview <- tribble(
  ~theme, ~name, ~z22, ~z11_100m, ~z11_1km, ~desc,
  "Population", "population", "population", "INSGESAMT_population", "Einwohner", "Population",
  "Population", "citizens", "citizens", NA, NA, "Number of german citizens, 18 or older",
  "Population", "foreigners", "foreigners", NA, "Auslaender_A", "Share of foreigners",
  "Population", "foreigners_from_18", "foreigners_from_18", NA, NA, "Share of foreigners, 18 or older",
  "Population", "birth_country", "birth_country", "GEBURTLAND_GRP", NA, "Country of birth (groups)",
  "Population", "sex", NA, "GESCHLECHT", NA, "Sex",
  "Population", "women", NA, NA, "Frauen_A", "Share of women",
  "Population", "religion", NA, "RELIGION_KURZ", NA, "Religion",
  "Population", "citizenship", "citizenship", "STAATSANGE_KURZ", NA, "Citizenship",
  "Population", "citizenship_group", "citizenship_group", "STAATSANGE_GRP", NA, "Citizenship (groups)",
  "Population", "citizenship_origin", NA, "STAATSANGE_HLND", NA, "Citizenship by selected countries",
  "Population", "citizenship_total", NA, "STAATZHL", NA, "Number of citizenships",
  "Population", "age_avg", "age_avg", NA, "Alter_D", "Average age",
  "Population", "age_short", "age_short", "ALTER_KURZ", NA, "Age (five classes of years)",
  "Population", "age_long", "age_long", "ALTER_10JG", NA, "Age (ten years age groups)",
  "Population", "age_under_18", "age_under_18", NA, "unter18_A", "Share of people under 18",
  "Population", "age_from_65", "age_from_65", NA, "ab65_A", "Share of people 65 or older",
  "Population", "marital_status", "marital_status", "FAMSTND_AUSF", NA, "Marital status",
  "Families", "families", "families", "INSGESAMT_families", NA, "Total number of families",
  "Families", "family_type", "family_type", "FAMTYP_KIND", NA, "Type of core family (by children)",
  "Families", "family_size", NA, "FAMGROESS_KLASS", NA, "Size of core family",
  "Households", "households", "households", "INSGESAMT_households", NA, "Total number of private households",
  "Households", "household_family", NA, "HHTYP_FAM", NA, "Private households by family types",
  "Households", "household_lifestyle", NA, "HHTYP_LEB", NA, "Private households by lifestyle",
  "Households", "household_senior", NA, "HHTYP_SENIOR_HH", NA, "Private households by senior status",
  "Households", "household_size_avg", "household_size_avg", NA, "HHGroesse_D", "Average household size",
  "Households", "household_size_group", "household_size_group", "HHGROESS_KLASS", NA, "Household size (groups)",
  "Dwellings", "dwellings", "dwellings", "INSGESAMT_dwellings", NA, "Total number of dwellings",
  "Dwellings", "rent_avg", "rent_avg", NA, NA, "Average net cold rent",
  "Dwellings", "dwelling_occupancy", NA, "NUTZUNG_DETAIL_HHGEN", NA, "Use by household occupancy",
  "Dwellings", "dwelling_ownership_home", NA, "WOHNEIGENTUM", NA, "Ownership of the dwelling",
  "Dwellings", "dwelling_ownership_property", NA, "EIGENTUM_dwellings", NA, "Dwellings by form of ownership",
  "Dwellings", "owner_occupier", "owner_occupier", NA, NA, "Share of owner occupiers",
  "Dwellings", "vacancies", "vacancies", NA, "Leerstandsquote", "Share of vacancies",
  "Dwellings", "market_vacancies", "market_vacancies", NA, NA, "Share of market active vacancies",
  "Dwellings", "inhabitant_space", "inhabitant_space", NA, "Wohnfl_Bew_D", "Average living space per inhabitant",
  "Dwellings", "dwelling_space", "dwelling_space", NA, "Wohnfl_Whg_D", "Average living space per dwelling",
  "Dwellings", "floor_space", "floor_space", "WOHNFLAECHE_10S", NA, "Floor space of the dwelling (10m\u00b2 intervals)",
  "Dwellings", "dwelling_rooms", "dwelling_rooms", "RAUMANZAHL", NA, "Dwellings by number of rooms",
  "Dwellings", "dwelling_constr_year", NA, "BAUJAHR_MZ_dwellings", NA, "Dwellings by construction year (microcensus classes)",
  "Dwellings", "dwelling_building_dwellings", NA, "ZAHLWOHNGN_HHG_dwellings", NA, "Dwellings by number of dwellings in the building",
  "Dwellings", "dwelling_building_size", "dwelling_building_size", "GEBTYPGROESSE_dwellings", NA, "Dwellings by building type",
  "Dwellings", "dwelling_building_type", NA, "GEBAEUDEART_SYS_dwellings", NA, "Dwellings by building classification",
  "Dwellings", "dwelling_building_design", NA, "GEBTYPBAUWEISE_dwellings", NA, "Dwelling by building design",
  "Dwellings", "dwelling_heat_type", "dwelling_heat_type", "HEIZTYP_dwellings", NA, "Dwellings by predominant heating type",
  "Dwellings", "dwelling_heat_src", "dwelling_heat_src", NA, NA, "Dwellings by energy source of heating",
  "Buildings", "buildings", "buildings", "INSGESAMT_buildings", NA, "Total number of buildings",
  "Buildings", "building_ownership_property", NA, "EIGENTUM_buildings", NA, "Buildings by form of ownership",
  "Buildings", "building_constr_year", "building_constr_year", "BAUJAHR_MZ_buildings", NA, "Buildings by construction year (microcensus classes)",
  "Buildings", "building_dwellings", "building_dwellings", "ZAHLWOHNGN_HHG_buildings", NA, "Residential buildings by number of dwellings in the building",
  "Buildings", "building_size", "building_size", "GEBTYPGROESSE_buildings", NA, "Residential buildings by building type",
  "Buildings", "building_type", NA, "GEBAEUDEART_SYS_buildings", NA, "Buildings by building design",
  "Buildings", "building_design", NA, "GEBTYPBAUWEISE_buildings", NA, "Buildings by building design",
  "Buildings", "building_heat_type", "building_heat_type", "HEIZTYP_buildings", NA, "Buildings by predominant heating type",
  "Buildings", "building_heat_src", "building_heat_src", NA, NA, "Buildings by energy source of heating"
)

# README table
# overview |>
#   transmute(
#     Theme = theme,
#     Name = paste0("`", name, "`"),
#     Description = desc,
#     Zensus22 = if_else(!is.na(z22), "\u2705", "\u274c"),
#     `Zensus11 (100m)` = if_else(!is.na(z11_100m), "\u2705", "\u274c"),
#     `Zensus11 (1km)` = if_else(!is.na(z11_1km), "\u2705", "\u274c")
#   )

z22_base_url <- "https://www.zensus2022.de/static/Zensus_Veroeffentlichung/"
z11_base_url <- "https://www.zensus2022.de/static/DE/gitterzellen/"

list(
  population = "Zensus2022_Bevoelkerungszahl.zip",
  citizens = "Deutsche_Staatsangehoerige_ab_18_Jahren.zip",
  foreigners = "Auslaenderanteil_in_Gitterzellen.zip",
  foreigners_from_18 = "Auslaenderanteil_ab_18_Jahren.zip",
  birth_country = "Zensus2022_Geburtsland_Gruppen_in_Gitterzellen.zip",
  citizenship = "Zensus2022_Staatsangehoerigkeit_in_Gitterzellen.zip",
  citizenship_group = "Zensus2022_Staatsangehoerigkeit_Gruppen_in_Gitterzellen.zip",
  age_avg = "Durchschnittsalter_in_Gitterzellen.zip",
  age_short = "Alter_in_5_Altersklassen.zip",
  age_long = "Alter_in_10er-Jahresgruppen.zip",
  age_under_18 = "Anteil_unter_18-jaehrige_in_Gitterzellen.zip",
  age_from_65 = "Anteil_ab_65-jaehrige_in_Gitterzellen.zip",
  marital_status = "Familienstand_in_Gitterzellen.zip",
  families = "Typ_der_Kernfamilie_nach_Kindern.zip",
  family_type = "Typ_der_Kernfamilie_nach_Kindern.zip",
  households = "Zensus2022_Groesse_des_privaten_Haushalts_in_Gitterzellen.zip",
  household_size_avg = "Durchschnittliche_Haushaltsgroesse_in_Gitterzellen.zip",
  household_size_group = "Zensus2022_Groesse_des_privaten_Haushalts_in_Gitterzellen.zip",
  dwellings = "Durchschnittliche_Nettokaltmiete_und_Anzahl_der_Wohnungen_in_Gitterzellen.zip",
  rent_avg = "Zensus2022_Durchschn_Nettokaltmiete.zip",
  owner_occupier = "Eigentuemerquote_in_Gitterzellen.zip",
  vacancies = "Leerstandsquote_in_Gitterzellen.zip",
  market_vacancies = "Marktaktive_Leerstandsquote_in_Gitterzellen.zip",
  inhabitant_space = "Durchschnittliche_Wohnflaeche_je_Bewohner_in_Gitterzellen.zip",
  dwelling_space = "Durchschnittliche_Flaeche_je_Wohnung_in_Gitterzellen.zip",
  floor_space = "Flaeche_der_Wohnung_10m2_Intervalle.zip",
  dwelling_rooms = "Wohnungen_nach_Zahl_der_Raeume.zip",
  dwelling_building_size = "Wohnungen_nach_Gebaeudetyp_Groesse.zip",
  dwelling_heat_type = "Zensus2022_Heizungsart.zip",
  dwelling_heat_src = "Zensus2022_Energietraeger.zip",
  buildings = "Gebaeude_nach_Baujahr_in_Mikrozensus_Klassen.zip",
  building_constr_year = "Gebaeude_nach_Baujahr_in_Mikrozensus_Klassen.zip",
  building_dwellings = "Gebaeude_mit_Wohnraum_nach_Anzahl_der_Wohnungen_im_Gebaeude.zip",
  building_size = "Gebaeude_mit_Wohnraum_nach_Gebaeudetyp_Groesse.zip",
  building_heat_type = "Gebaeude_mit_Wohnraum_nach_ueberwiegender_Heizungsartt.zip",
  building_heat_src = "Gebaeude_mit_Wohnraum_nach_Energietraeger_der_Heizung.zip"
)

# Maps CSV file names to remote file names
z11_100m_files <- list(
  population = "csv_Demographie_100_Meter-Gitter.zip",
  families = "Download-Tabelle_Familien_im_100_Meter-Gitter_im_CSV-Format%20(2).zip",
  households = "Download-Tabelle_Haushalt_im_100_Meter-Gitter_im_CSV-Format.zip",
  dwellings = "Download-Tabelle_Wohnungen_im_100_Meter-Gitter_im_CSV-Format.zip",
  buildings = "Download-Tabelle_Gebaeude_und_Wohnungen_im_100_Meter-Gitter_im_CSV-Format.zip"
)

z11_1km_file <- "Download-Tabelle_und_Datensatzbeschreibung_Spitze_Werte_im_ein_Kilometer-Gitter_im_CSV-Format.zip"


download_table <- function(table, year = 2022, path = tempfile(), timeout = 1000) {
  old <- options(timeout = timeout)
  on.exit(options(old))

  switch(
    as.character(year),
    "2011" = {
      urls <- z11_100m_files
      base_url <- z11_base_url
    },
    "2022" = {
      urls <- z22_files
      base_url <- z22_base_url
    }
  )

  file <- urls[[table]]
  cli_inform("Downloading {table}")
  download_zipped_csv(base_url, file, path)
}

download_z11_grid <- function(path = tempfile()) {
  file <- "csv_Bevoelkerung_100m_Gitter.zip"
  path <- normalizePath(path, "/", mustWork = FALSE)
  target_dir <- dirname(path)
  cli_inform("Downloading grid to {target_dir}")
  download_zipped_csv(z11_base_url, file, path)
}

download_zipped_csv <- function(base_url, file, path = tempfile()) {
  path <- normalizePath(path, "/", mustWork = FALSE)
  target_dir <- dirname(path)
  request(base_url) |>
    req_url_path_append(file) |>
    req_perform(path = path)
  unzip_ext(path, "csv", exdir = target_dir)
}

unzip_ext <- function(path, ext, exdir = ".") {
  zipfiles <- unzip(path, list = TRUE)$Name
  target_file <- zipfiles[has_file_ext(zipfiles, ext)]
  unzip(path, files = target_file, exdir = exdir)
  file.path(exdir, target_file)
}

has_file_ext <- function(file, ext) {
  suppressWarnings(grepl(sprintf("\\.%s$", ext), file))
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
  lines <- readLines(file, n = nrows)
  all(validUTF8(lines))
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

    utf8_lines <- iconv(lines, from = from, to = "UTF-8")
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

z22_select_feat_column <- function(csv) {
  csv |>
    select(!any_of(c("quality", "werterlaeuternde_Zeichen")) &
             !starts_with(c("GITTER", "x", "y"))) |>
    _$columns
}

db_peek <- function(con, table, n = 10) {
  dbGetQuery(con, sprintf("SELECT * FROM %s LIMIT %s", table, n))
}

db_alter <- function(con, statement, tempname = NULL) {
  table_name <- regex_match(statement, "FROM[[:space:]]+([a-zA-Z0-9_]+)")[[1]][2]
  if (is.null(tempname)) {
    tempname <- sprintf("_%s_temp", table_name)
  }

  dbExecute(con, sprintf("CREATE TABLE %s AS %s", tempname, statement))
  dbExecute(con, sprintf("DROP TABLE %s", table_name))
  dbExecute(con, sprintf("ALTER TABLE %s RENAME TO %s", tempname, table_name))
}

db_drop_table <- function(con, x) {
  dbExecute(con, sprintf("DROP TABLE %s", x))
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

connect <- function(db = "z22") {
  dbConnect(duckdb::duckdb(), dbdir = sprintf("data-raw/%s.duckdb", db))
}

shutdown <- function(con) {
  try(dbDisconnect(con, shutdown = TRUE), silent = TRUE)
}

restart <- function(con) {
  shutdown(con)
  connect()
}
