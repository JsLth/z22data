code_files <- setdiff(dir(
  "data-raw",
  pattern = "\\.R",
  full.names = TRUE
), "data-raw/run.R")

for (file in code_files) {
  callr::rscript(file)
}

callr::rscript("data-raw/1-import.R")
callr::rscript("data-raw/2-db_split.R")
callr::rscript("data-raw/3-cleanup.R")
callr::rscript("data-raw/4-1km_import.R")
