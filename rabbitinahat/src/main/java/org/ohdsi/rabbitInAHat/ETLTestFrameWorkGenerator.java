/*******************************************************************************
 * Copyright 2019 Observational Health Data Sciences and Informatics
 * 
 * This file is part of WhiteRabbit
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.ohdsi.rabbitInAHat;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import org.ohdsi.rabbitInAHat.dataModel.*;
import org.ohdsi.rabbitInAHat.dataModel.ETL.FileFormat;

// TODO: use templating to generate the R code (e.g. Jinja/Apache FreeMarker/Mustache). At least put static R code in separate (.R) files.
public class ETLTestFrameWorkGenerator {

	private static int DEFAULT = 0;
	private static int NEGATE = 1;
	private static int COUNT = 2;
    private static final Set<String> keywordSet = new HashSet<>(Arrays.asList("ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUTHORIZATION", "BACKUP", "BEGIN", "BETWEEN",
            "BREAK", "BROWSE", "BULK", "BY", "CASCADE", "CASE", "CHECK", "CHECKPOINT", "CLOSE", "CLUSTERED", "COALESCE", "COLLATE", "COLUMN", "COMMIT",
            "COMPUTE", "CONSTRAINT", "CONTAINS", "CONTAINSTABLE", "CONTINUE", "CONVERT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME",
            "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATABASE", "DBCC", "DEALLOCATE", "DECLARE", "DEFAULT", "DELETE", "DENY", "DESC", "DISK", "DISTINCT",
            "DISTRIBUTED", "DOUBLE", "DROP", "DUMP", "ELSE", "END", "ERRLVL", "ESCAPE", "EXCEPT", "EXEC", "EXECUTE", "EXISTS", "EXIT", "EXTERNAL", "FETCH",
            "FILE", "FILLFACTOR", "FOR", "FOREIGN", "FREETEXT", "FREETEXTTABLE", "FROM", "FULL", "FUNCTION", "GOTO", "GRANT", "GROUP", "HAVING", "HOLDLOCK",
            "IDENTITY", "IDENTITY_INSERT", "IDENTITYCOL", "IF", "IN", "INDEX", "INNER", "INSERT", "INTERSECT", "INTO", "IS", "JOIN", "KEY", "KILL", "LEFT",
            "LIKE", "LINENO", "LOAD", "MERGE", "NATIONAL", "NOCHECK", "NONCLUSTERED", "NOT", "NULL", "NULLIF", "OF", "OFF", "OFFSETS", "ON", "OPEN",
            "OPENDATASOURCE", "OPENQUERY", "OPENROWSET", "OPENXML", "OPTION", "OR", "ORDER", "OUTER", "OVER", "PERCENT", "PIVOT", "PLAN", "PRECISION",
            "PRIMARY", "PRINT", "PROC", "PROCEDURE", "PUBLIC", "RAISERROR", "READ", "READTEXT", "RECONFIGURE", "REFERENCES", "REPLICATION", "RESTORE",
            "RESTRICT", "RETURN", "REVERT", "REVOKE", "RIGHT", "ROLLBACK", "ROWCOUNT", "ROWGUIDCOL", "RULE", "SAVE", "SCHEMA", "SECURITYAUDIT", "SELECT",
            "SEMANTICKEYPHRASETABLE", "SEMANTICSIMILARITYDETAILSTABLE", "SEMANTICSIMILARITYTABLE", "SESSION_USER", "SET", "SETUSER", "SHUTDOWN", "SOME",
            "STATISTICS", "SYSTEM_USER", "TABLE", "TABLESAMPLE", "TEXTSIZE", "THEN", "TO", "TOP", "TRAN", "TRANSACTION", "TRIGGER", "TRUNCATE", "TRY_CONVERT",
            "TSEQUAL", "UNION", "UNIQUE", "UNPIVOT", "UPDATE", "UPDATETEXT", "USE", "USER", "VALUES", "VARYING", "VIEW", "WAITFOR", "WHEN", "WHERE", "WHILE",
            "WITH", "WITHIN GROUP", "WRITETEXT"));
    private PrintWriter writer;
	private ETL etl;

	public static void main(String[] args) {
		ETL etl = ETL.fromFile("C:\\Home\\Research\\ETLs\\JMDC ETL\\JMDC ETL CDMv5\\JMDC to CDMv5 ETL v08.json.gz", FileFormat.GzipJson);
		ETLTestFrameWorkGenerator generator = new ETLTestFrameWorkGenerator();
		generator.generate(etl, "C:\\Home\\Research\\ETLs\\JMDC ETL\\JMDC ETL CDMv5\\JmdcTestFramework.R");
	}

	ETLTestFrameWorkGenerator() {
	}

	void generate(ETL etl, String filename) {
		this.etl = etl;

		Path path = Paths.get(filename);
		try (BufferedWriter bw = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
			 PrintWriter writer = new PrintWriter(bw)) {
			this.writer = writer;
			generateRScript();
		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	private void generateRScript() {
		createInitFunction();
		createSetDefaultFunctions();
		createGetDefaultFunctions();
		createDeclareTestFunction();
		createAddFunctions();
		createExpectFunctions(DEFAULT);
		createExpectFunctions(NEGATE);
		createExpectFunctions(COUNT);
		createLookupFunctions();
		createGenerateInsertSqlFunction();
		createSourceCsvFunction();
		createExtractTestTypeStringFunction();
		createGenerateTestSqlFunction();
		createTestsOverviewFunctions();
		createSummaryFunction();
        createGetUntestedFieldsFunctions();
        createOutputTestResultsSummaryFunction();
	}

	private void createInitFunction() {
		writer.println("initFramework <- function() {");
		writer.println("  frameworkContext <- new.env(parent = globalenv())");
		writer.println("  class(frameworkContext) <- 'frameworkContext'");
		writer.println("  assign('frameworkContext', frameworkContext, envir = globalenv())");
		writer.println("  frameworkContext$inserts <- list()");
		writer.println("  frameworkContext$expects <- list()");
		writer.println("  frameworkContext$testId <- -1");
		writer.println("  frameworkContext$testDescription <- \"\"");
		writer.println("  frameworkContext$defaultValues <- new.env(parent = frameworkContext)");
		for (Table table : etl.getSourceDatabase().getTables()) {
			if (!table.isStem()) {
				String rTableName = convertToRName(table.getName());
				writer.println("");
				writer.println("  defaults <- list()");
				for (Field field : table.getFields()) {
					String rFieldName = convertToRName(field.getName());
					String defaultValue = field.getValueCounts().getMostFrequentValue();
					if (defaultValue != null) {
						writer.println("  defaults$" + rFieldName + " <- '" + defaultValue + "'");
					} else {
						writer.println("  defaults$" + rFieldName + " <- ''");
					}
				}
				writer.println("  assign('" + rTableName + "', defaults, envir = frameworkContext$defaultValues)");
			}
		}
		writer.println("");
		createFieldsMapped();
        writer.println("");

        writer.println("  frameworkContext$sourceFieldsTested <- c()");
        writer.println("  frameworkContext$targetFieldsTested <- c()");

		writer.println("}");
		writer.println("");
		writer.println("initFramework()");
		writer.println("");
	}


	private void createFieldsMapped() {
		// Collect all stem fields mapped to
		Set<String> stemTargets = new HashSet<>();
		if (etl.hasStemTable()) {
			Optional<Table> stemTableOptional = etl.getTargetDatabase().getTables().stream().filter(Table::isStem).findFirst();
			if (stemTableOptional.isPresent()) {
				Table stemTable = stemTableOptional.get();
				for (Table table : etl.getSourceDatabase().getTables()) {
					Mapping<Field> toStemMapping = etl.getFieldToFieldMapping(table, stemTable);
					stemTargets.addAll(toStemMapping.getSourceToTargetMaps().stream().map(x -> x.getTargetItem().toString()).collect(Collectors.toSet()));
				}
			}
		}

		// Collect all fields that are either mapped from or mapped to. Excluding stem table.
		Set<Field> sourceFieldsMapped = new HashSet<>();
		Set<Field> targetFieldsMapped = new HashSet<>();
		for (ItemToItemMap tableToTableMap : etl.getTableToTableMapping().getSourceToTargetMaps()) {
			Table sourceTable = (Table) tableToTableMap.getSourceItem();
			Table targetTable = (Table) tableToTableMap.getTargetItem();

			Mapping<Field> fieldToFieldMapping = etl.getFieldToFieldMapping(sourceTable, targetTable);
			for (ItemToItemMap fieldToFieldMap : fieldToFieldMapping.getSourceToTargetMaps()) {
				if (!sourceTable.isStem()) {
					sourceFieldsMapped.add((Field) fieldToFieldMap.getSourceItem());
				}
				if (!targetTable.isStem()) {
					// if from stem to target, only use stem fields that are mapped to from source.
					if (sourceTable.isStem()) {
						if (stemTargets.contains(fieldToFieldMap.getSourceItem().toString())) {
							targetFieldsMapped.add((Field) fieldToFieldMap.getTargetItem());
						}
					} else {
						targetFieldsMapped.add((Field) fieldToFieldMap.getTargetItem());
					}
				}
			}
		}

		writer.println("  frameworkContext$sourceFieldsMapped <- c(");
		boolean isFirst = true;
		for (Field field : sourceFieldsMapped) {
			String prefix = isFirst ? "     '" : "    ,'";
			writer.println(prefix + convertFieldToFullName(field) + "'");
			isFirst = false;
		}
		writer.println("  )");
		writer.println("");

		writer.println("  frameworkContext$targetFieldsMapped <- c(");
		isFirst = true;
		for (Field field : targetFieldsMapped) {
			String prefix = isFirst ? "     '" : "    ,'";
			writer.println(prefix + convertFieldToFullName(field) + "'");
			isFirst = false;
		}
		writer.println("  )");
	}

	private void createSetDefaultFunctions() {
		for (Table table : etl.getSourceDatabase().getTables()) {
			if (!table.isStem()) {
				String rTableName = convertToRName(table.getName());

				writer.print("set_defaults_" + rTableName + " <- function(");
				writer.print(table.getFields().stream().map(Field::getName).map(this::convertToRName).collect(Collectors.joining(", ")));
				writer.println(") {");
				writer.println("  defaults <- get('" + rTableName + "', envir = frameworkContext$defaultValues)");
				for (Field field : table.getFields()) {
					String rFieldName = convertToRName(field.getName());
					writer.println("  if (!missing(" + rFieldName + ")) {");
					writer.println("    defaults$" + rFieldName + " <- " + rFieldName);
					writer.println("  }");
				}
				writer.println("  assign('" + rTableName + "', defaults, envir = frameworkContext$defaultValues)");
				writer.println("  invisible(defaults)");
				writer.println("}");
				writer.println("");
			}
		}
	}

	private void createGetDefaultFunctions() {
		for (Table table : etl.getSourceDatabase().getTables()) {
			String rTableName = convertToRName(table.getName());
			writer.println("get_defaults_" + rTableName + " <- function() {");
			writer.println("  defaults <- get('" + rTableName + "', envir = frameworkContext$defaultValues)");
			writer.println("  return(defaults)");
			writer.println("}");
			writer.println("");
		}
	}

	private void createDeclareTestFunction() {
		writer.println("declareTest <- function(id, description) {");
		writer.println("  frameworkContext$testId <- id");
		writer.println("  frameworkContext$testDescription <- description");
		writer.println("}");
		writer.println("");
	}

	private void createAddFunctions() {
		for (Table table : etl.getSourceDatabase().getTables()) {
			if (!table.isStem()) {
				String rTableName = convertToRName(table.getName());
				String sqlTableName = convertToSqlName(table.getName());
				writer.print("add_" + rTableName + " <- function(");
				writer.print(table.getFields().stream().map(Field::getName).map(this::convertToRName).collect(Collectors.joining(", ")));
				writer.println(") {");
				writer.println("  defaults <- get('" + rTableName + "', envir = frameworkContext$defaultValues)");
				writer.println("  fields <- c()");
				writer.println("  values <- c()");
				for (Field field : table.getFields()) {
					String rFieldName = convertToRName(field.getName());
					String sqlFieldName = convertToSqlName(field.getName());
					writer.println("  if (missing(" + rFieldName + ")) {");
					writer.println("    " + rFieldName + " <- defaults$" + rFieldName);
					writer.println("  } else {");
					writer.println("    frameworkContext$sourceFieldsTested <- c(frameworkContext$sourceFieldsTested, '" + convertFieldToFullName(field) + "')");
					writer.println("  }");
					writer.println("  fields <- c(fields, \"" + sqlFieldName + "\")");
					writer.println("  values <- c(values, " + createValueCode(rFieldName) + ")");
					writer.println("");
				}
				writer.println("  inserts <- list(testId = frameworkContext$testId, testDescription = frameworkContext$testDescription, table = \"" + sqlTableName
						+ "\", fields = fields, values = values)");
				writer.println("  frameworkContext$inserts[[length(frameworkContext$inserts) + 1]] <- inserts");
				writer.println("  invisible(NULL)");
				writer.println("}");
				writer.println("");
			}
		}
	}

	private void createExpectFunctions(int type) {
		for (Table table : etl.getTargetDatabase().getTables()) {
			if (!table.isStem()) {
				String rTableName = convertToRName(table.getName());
				String sqlTableName = convertToSqlName(table.getName());
				if (type == DEFAULT)
					writer.print("expect_" + rTableName + " <- function(");
				else if (type == NEGATE)
					writer.print("expect_no_" + rTableName + " <- function(");
				else
					writer.print("expect_count_" + rTableName + " <- function(rowCount, ");
				writer.print(table.getFields().stream().map(Field::getName).map(this::convertToRName).collect(Collectors.joining(", ")));
				writer.println(") {");
				writer.println("  fields <- c()");
				writer.println("  values <- c()");
				for (Field field : table.getFields()) {
					String rFieldName = convertToRName(field.getName());
					String sqlFieldName = convertToSqlName(field.getName());
					writer.println("  if (!missing(" + rFieldName + ")) {");
					writer.println("    fields <- c(fields, \"" + sqlFieldName + "\")");
					writer.println("    values <- c(values, " + createSqlValueCode(rFieldName) + ")");
					if (type != NEGATE) {
                        writer.println("    frameworkContext$targetFieldsTested <- c(frameworkContext$targetFieldsTested, '" + convertFieldToFullName(field) + "')");
                    }
                    writer.println("  }");
					writer.println("");
				}
				writer.println("  expects <- list(testId = frameworkContext$testId, testDescription = frameworkContext$testDescription, type = " + type + ", table = \""
						+ sqlTableName + "\", fields = fields, values = values)");
				if (type == COUNT)
					writer.println("  expects$rowCount = rowCount");
				writer.println("  frameworkContext$expects[[length(frameworkContext$expects) + 1]] <- expects");
				writer.println("  invisible(NULL)");
				writer.println("}");
				writer.println("");
			}
		}
	}

	 private void createLookupFunctions() {
		for (Table table : etl.getTargetDatabase().getTables()) {
			if (!table.isStem()) {
				String rTableName = convertToRName(table.getName());
				String sqlTableName = convertToSqlName(table.getName());
				writer.print("lookup_" + rTableName + " <- function(fetchField, ");
				writer.print(table.getFields().stream().map(Field::getName).map(this::convertToRName).collect(Collectors.joining(", ")));
				writer.println(") {");

				writer.print("  statement <- paste0('SELECT ', fetchField , ' FROM @cdm_database_schema.");
				writer.print(sqlTableName);
				writer.println(" WHERE')");

				writer.println("  first <- TRUE");
				for (Field field : table.getFields()) {
					String rFieldName = convertToRName(field.getName());
					String sqlFieldName = convertToSqlName(field.getName());
					writer.println("  if (!missing(" + rFieldName + ")) {");
					writer.println("    if (first) {");
					writer.println("      first <- FALSE");
					writer.println("    } else {");
					writer.println("      statement <- paste0(statement, \" AND\")");
					writer.println("    }");
					writer.println("    statement <- paste0(statement, \" " + sqlFieldName + "\"," + createSqlValueCode(rFieldName) + ")");
					writer.println("  }");
					writer.println("");
				}
				writer.println("  class(statement) <- 'subQuery'");
				writer.println("  return(statement)");
				writer.println("}");
				writer.println("");
			}
		}
	}

	 private void createGenerateInsertSqlFunction() {
		writer.println("generateInsertSql <- function(databaseSchema = NULL) {");
		writer.println("  insertSql <- c()");
		for (Table table : etl.getSourceDatabase().getTables()) {
			if (!table.isStem())
				writer.println("  insertSql <- c(insertSql, \"TRUNCATE TABLE @cdm_database_schema." + convertToSqlName(table.getName()) + ";\")");
		}
		writer.println("  createInsertStatement <- function(insert, env) {");
		writer.println("    s <- c()");
		writer.println("    if (env$testId != insert$testId) {");
		writer.println("      s <- c(s, paste0('-- ', insert$testId, ': ', insert$testDescription))");
		writer.println("      env$testId <- insert$testId");
		writer.println("    }");
		writer.println("    s <- c(s, paste0(\"INSERT INTO @cdm_database_schema.\",");
		writer.println("                     insert$table,");
		writer.println("                     \"(\",");
		writer.println("                     paste(insert$fields, collapse = \", \"),");
		writer.println("                     \") VALUES (\",");
		writer.println("                     paste(insert$values, collapse = \", \"), ");
		writer.println("                     \");\"))");
		writer.println("    return(s)");
		writer.println("  }");
		writer.println("  env <- new.env()");
		writer.println("  env$testId <- -1");
		writer.println("  insertSql <- c(insertSql, do.call(c, lapply(frameworkContext$inserts, createInsertStatement, env)))");
		writer.println("  if (is.null(databaseSchema)) {");
		writer.println("  	insertSql <- gsub('@cdm_database_schema.', '', insertSql)");
		writer.println("  } else {");
		writer.println("  	insertSql <- gsub('@cdm_database_schema', databaseSchema, insertSql)");
		writer.println("  }");
		writer.println("  return(insertSql)");
		writer.println("}");
		writer.println("");
	}

	 private void createSourceCsvFunction() {
		writer.println("writeSourceCsv <- function(directory = NULL, separator = ',') {");
		// Function to remove artificial quotes, escape quotes and separator
		writer.println("  clean_value <- function(x) {");
		writer.println("    if (x == 'NULL') {");
		writer.println("      return('')");
		writer.println("    }");
		writer.println("    value <- substring(x, 2, nchar(x)-1)");
		writer.println("    value <- gsub('\"', '\"\"', value)");
		writer.println("    if (grepl(separator, value)) {");
		writer.println("      return(paste0('\"', value, '\"'))");
		writer.println("    }");
		writer.println("    return(value)");
		writer.println("  }");
		writer.println("");

		// Function to remove leading and trailing [], if present
		writer.println("  clean_fields <- function(x) {");
		writer.println("    if (grepl(\"^\\\\[.+?\\\\]$\", x)) {");
		writer.println("      return(substring(x, 2, nchar(x)-1))");
		writer.println("    }");
		writer.println("    return(x)");
		writer.println("  }");
		writer.println("  dir.create(directory, showWarnings = F)");
		writer.println("  ");

		// Write values
		// Initialize all new source files with header. Overwrites existing source files from previous runs in the directory.
		writer.println("  seen_tables <- c()");
		writer.println("  for (insert in frameworkContext$inserts) {");
		writer.println("    filename <- file.path(directory, paste0(insert$table, '.csv'))");
		writer.println("    if (!(insert$table %in% seen_tables)) {");
		writer.println("      write(paste(sapply(insert$fields, clean_fields), collapse = separator), filename, append=F)");
		writer.println("      seen_tables <- c(seen_tables, insert$table)");
		writer.println("    }");
		writer.println("    write(paste(sapply(insert$values, clean_value), collapse = separator), filename, append=T)");
		writer.println("  }");
		writer.println("  ");

		// Create source files for which there are no inserts
		writer.println("  for (table_name in names(frameworkContext$defaultValues)) {");
		writer.println("    if (!(table_name %in% seen_tables)) {");
		writer.println("      filename <- file.path(directory, paste0(table_name, '.csv'))");
		writer.println("      write(paste(names(frameworkContext$defaultValues[[table_name]]), collapse = separator), filename, append=F)");
		writer.println("    }");
		writer.println("  }");
		writer.println("}");
		writer.println("");
	}

	 private void createGenerateTestSqlFunction() {
		writer.println("generateTestSql <- function(databaseSchema = NULL) {");
		writer.println("  testSql <- c()");
		writer.println("  testSql <- c(testSql, \"IF OBJECT_ID('@cdm_database_schema.test_results', 'U') IS NOT NULL DROP TABLE @cdm_database_schema.test_results;\")");
		writer.println("  testSql <- c(testSql, \"CREATE TABLE @cdm_database_schema.test_results (id INT, description VARCHAR(512), test VARCHAR(256), status VARCHAR(5));\")");
		writer.println("  createExpectStatement <- function(expect, env) {");
		writer.println("    s <- c()");
		writer.println("    if (env$testId != expect$testId) {");
		writer.println("      s <- c(s, paste0('-- ', expect$testId, ': ', expect$testDescription))");
		writer.println("      env$testId <- expect$testId");
		writer.println("    }");
		writer.println("    operators <- rep(\"=\", length(expect$fields))");
		writer.println("    operators[expect$values == \"NULL\"] <- rep(\"IS\", sum(expect$values == \"NULL\"))");
		writer.println("    s <- c(s, paste0(\"INSERT INTO @cdm_database_schema.test_results SELECT \",");
		writer.println("                     expect$testId,");
		writer.println("                     \" AS id, '\",");
		writer.println("                     expect$testDescription,");
		writer.println("                     \"' AS description, '\",");
		writer.println("                     extractTestTypeString(expect), \" \", expect$table,");
		writer.println("                     \"' AS test, CASE WHEN (SELECT COUNT(*) FROM @cdm_database_schema.\",");
		writer.println("                     expect$table,");
		writer.println("                     \" WHERE \",");
		writer.println("                     paste(paste(expect$fields, expect$values), collapse = \" AND \"),");
		writer.println("                     \") \",");
		writer.println("                     if (expect$type == " + DEFAULT + ") \"= 0\" else if (expect$type == " + NEGATE
				+ ") \"!= 0\" else paste(\"!=\", expect$rowCount),");
		writer.println("                     \" THEN 'FAIL' ELSE 'PASS' END AS status;\"))");
		writer.println("    return(s)");
		writer.println("  }");
		writer.println("  env <- new.env()");
		writer.println("  env$testId <- -1");
		writer.println("  testSql <- c(testSql, do.call(c, lapply(frameworkContext$expects, createExpectStatement, env)))");
		writer.println("  if (is.null(databaseSchema)) {");
		writer.println("  	testSql <- gsub('@cdm_database_schema.', '', testSql)");
		writer.println("  } else {");
		writer.println("  	testSql <- gsub('@cdm_database_schema', databaseSchema, testSql)");
		writer.println("  }");
		writer.println("  return(testSql)");
		writer.println("}");
		writer.println("");
	}

	private void createExtractTestTypeStringFunction() {
		writer.println("extractTestTypeString <- function(x) {");
		writer.println("  if (x$type == 0) {");
		writer.println("    return('Expect')");
		writer.println("  } else if (x$type==1) {");
		writer.println("    return('Expect No')");
		writer.println("  } else if (x$type==2) {");
		writer.println("    return(paste('Expect', x$rowCount))");
		writer.println("  }");
		writer.println("}");
		writer.println("");
	}

	 private void createTestsOverviewFunctions() {
		writer.println("getTestsOverview <- function() {");
		writer.println("  df <- data.frame(");
		writer.println("    testId = sapply(frameworkContext$expects, function(x) x$testId),");
		writer.println("    testDescription = sapply(frameworkContext$expects, function(x) x$testDescription),");
		writer.println("    testType = sapply(frameworkContext$expects, extractTestTypeString),");
		writer.println("    testTable = sapply(frameworkContext$expects, function(x) x$table)");
		writer.println("  )");
		writer.println("  return(df)");
		writer.println("}");
		writer.println("");
		writer.println("exportTestsOverviewToFile <- function(filename) {");
		writer.println("  df <- getTestsOverview()");
		writer.println("  write.csv(unique(df), filename, row.names=F)");
		writer.println("}");
		writer.println("");
	}

    private void createSummaryFunction() {
        writer.println("summary.frameworkContext <- function(object, ...) {");
        writer.println("  nSourceFieldsTested <- length(intersect(object$sourceFieldsMapped, object$sourceFieldsTested))");
		writer.println("  nTargetFieldsTested <- length(intersect(object$targetFieldsMapped, object$targetFieldsTested))");
		writer.println("  nTotalSourceFields <- length(object$sourceFieldsMapped)");
		writer.println("  nTotalTargetFields <- length(object$targetFieldsMapped)");
		writer.println("  summary <- c(");
		writer.println("    length(object$expects),");
		writer.println("    length(unique(sapply(object$expects, function(x) x$testId))),");
		writer.println("    nSourceFieldsTested,");
		writer.println("    nTotalSourceFields,");
		writer.println("    round(100*nSourceFieldsTested/nTotalSourceFields, 2),");
		writer.println("    nTargetFieldsTested,");
		writer.println("    nTotalTargetFields,");
		writer.println("    round(100*nTargetFieldsTested/nTotalTargetFields, 2)");
		writer.println("  )");
		writer.println("  names(summary) <- c('n_tests', 'n_cases', 'n_source_fields_tested', 'n_source_fields_mapped_from', 'source_coverage (%)', 'n_target_fields_tested', 'n_target_fields_mapped_to', 'target_coverage (%)')");
		writer.println("  return(as.data.frame(summary))");
        writer.println("}");
		writer.println("");
		writer.println("summaryTestFramework <- function() {");
		writer.println("  return(summary(frameworkContext));");
		writer.println("}");
		writer.println("");
	}

    private void createGetUntestedFieldsFunctions() {
        writer.println("getUntestedSourceFields <- function() {");
        writer.println("  sort(setdiff(frameworkContext$sourceFieldsMapped, frameworkContext$sourceFieldsTested))");
        writer.println("}");
		writer.println("");

        writer.println("getUntestedTargetFields <- function() {");
        writer.println("  sort(setdiff(frameworkContext$targetFieldsMapped, frameworkContext$targetFieldsTested))");
        writer.println("}");
		writer.println("");
    }

    private void createOutputTestResultsSummaryFunction() {
	    // Suppress any errors or warnings if unable to load DatabaseConnector, as the rest of the test framework does not need it.
        writer.println("outputTestResultsSummary <- function(connection, databaseSchema = NULL) {");
        writer.println("  suppressWarnings(require(DatabaseConnector, quietly = TRUE))");
        writer.println("  query = 'SELECT * FROM @cdm_database_schema.test_results;'");
        writer.println("  if (is.null(databaseSchema)) {");
        writer.println("    query <- gsub('@cdm_database_schema.', '', query)");
        writer.println("  } else {");
        writer.println("    query <- gsub('@cdm_database_schema', databaseSchema, query)");
        writer.println("  }");
        writer.println("  df_results <- DatabaseConnector::querySql(connection, query)");
        writer.println("  n_tests <- nrow(df_results)");
        writer.println("  n_failed_tests <- sum(df_results$'STATUS' == 'FAIL')");
        writer.println("  if (n_failed_tests > 0) {");
        writer.println("    write(sprintf('FAILED unit tests: %d/%d (%.1f%%)', n_failed_tests, n_tests, n_failed_tests/n_tests * 100), file='')");
        writer.println("    print(df_results[df_results$'STATUS' == 'FAIL',])");
        writer.println("  } else {");
        writer.println("    write(sprintf('All %d tests PASSED', n_tests), file='')");
        writer.println("  }");
        writer.println("}");
        writer.println("");
    }

	private String removeExtension(String name) {
		return name.replaceAll("\\.\\w{3,4}$", "");
	}

	private String convertToRName(String name) {
		name = removeExtension(name);
		// Replace space, dash and brackets by an underscore. If name starts with underscore, remove.
		name = name.replaceAll("[\\s-()\\[\\]{}]", "_").replaceAll("^_+", "");
		// Remove BOM
		if (name.startsWith("\uFEFF")) {
			name = name.substring(1);
		}
		return name;
	}

	private String createSqlValueCode(String rFieldName) {
		return "if (is.null(" + rFieldName + ")) \" IS NULL\" " +
				"else if (is(" + rFieldName + ", \"subQuery\")) " +
				"paste0(\" = (\", as.character(" + rFieldName + "), \")\") " +
				"else paste0(\" = '\", as.character(" + rFieldName + "), \"'\")";
	}

	 private String createValueCode(String rFieldName) {
         return "if (is.null(" + rFieldName + ")) \"NULL\" " +
                 "else if (is(" + rFieldName + ", \"subQuery\")) " +
                 "paste0(\"(\", as.character(" + rFieldName + "), \")\") " +
                 "else paste0(\"'\", as.character(" + rFieldName + "), \"'\")";
	}

	 private String convertToSqlName(String name) {
		name = removeExtension(name);
		if (name.startsWith("[") && name.endsWith("]")) {
			return name;
		}
		name = name.replace('[', '_').replace(']', '_');
		if (name.contains(" ") || name.contains(".") || keywordSet.contains(name.toUpperCase())) {
			return "[" + name + "]";
		}
		return name;
	}

	private String convertFieldToFullName(Field field) {
        return convertToRName(field.getTable().getName()) + "." + convertToRName(field.getName());
    }
}
