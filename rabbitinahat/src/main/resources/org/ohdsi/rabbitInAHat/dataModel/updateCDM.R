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
  write.csv(
    'rabbitinahat/src/main/resources/org/ohdsi/rabbitInAHat/dataModel/CDMV5.5.csv',
    row.names = FALSE
  )