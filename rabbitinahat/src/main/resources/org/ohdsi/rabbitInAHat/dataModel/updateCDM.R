library(tidyverse)
cdmVersion <- '5.5'

cdmTableCsvLoc <- system.file(file.path("csv", paste0("OMOP_CDMv", cdmVersion, "_Table_Level.csv")), package = "CommonDataModel", mustWork = TRUE)
cdmFieldCsvLoc <- system.file(file.path("csv", paste0("OMOP_CDMv", cdmVersion, "_Field_Level.csv")), package = "CommonDataModel", mustWork = TRUE)

tableSpecs <- read.csv(cdmTableCsvLoc, stringsAsFactors = FALSE)
cdmSpecs <- read.csv(cdmFieldCsvLoc, stringsAsFactors = FALSE)

cdmSpecs |>
  left_join(
    tableSpecs,
    join_by(cdmTableName)
  ) |>
  select(
    table = cdmTableName,
    field = cdmFieldName,
    required = isRequired.x,
    type = cdmDatatype,
    description = userGuidance.x,
    schema = schema
  ) |>
  mutate(
    table = str_to_upper(table),
    field = str_to_lower(field),
    required = case_when(
      str_to_lower(required) %in% c("yes", "true") ~ "Yes",
      str_to_lower(required) %in% c("no", "false") ~ "No",
      TRUE ~ required
    ),
    type = str_to_upper(type),
    schema = str_to_lower(schema)
  ) |>
  write_csv(
    "CDMV5.5.csv",
    na = "",
    quote = "needed",
    eol = "\r\n"
  )
