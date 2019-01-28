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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.ohdsi.rabbitInAHat.dataModel.Database;
import org.ohdsi.rabbitInAHat.dataModel.ETL;
import org.ohdsi.rabbitInAHat.dataModel.ETL.FileFormat;
import org.ohdsi.rabbitInAHat.dataModel.Field;
import org.ohdsi.rabbitInAHat.dataModel.Table;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.WriteTextFile;

public class ETLTestFrameWorkGenerator {

	private static int		DEFAULT		= 0;
	private static int		NEGATE		= 1;
	private static int		COUNT		= 2;
	private String[]		keywords	= new String[] { "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUTHORIZATION", "BACKUP", "BEGIN", "BETWEEN",
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
			"WITH", "WITHIN GROUP", "WRITETEXT" };
	protected Set<String>	keywordSet;
	private List<String>	r;
	private Database		sourceDb;
	private Database		targetDb;

	public static void main(String[] args) {
		ETL etl = ETL.fromFile("C:\\Home\\Research\\ETLs\\JMDC ETL\\JMDC ETL CDMv5\\JMDC to CDMv5 ETL v08.json.gz", FileFormat.GzipJson);
		ETLTestFrameWorkGenerator generator = new ETLTestFrameWorkGenerator();
		generator.generate(etl, "C:\\Home\\Research\\ETLs\\JMDC ETL\\JMDC ETL CDMv5\\JmdcTestFramework.R");
	}

	public ETLTestFrameWorkGenerator() {
		keywordSet = new HashSet<String>();
		for (String keyword : keywords)
			keywordSet.add(keyword);
	}

	public void generate(ETL etl, String filename) {
		List<String> r = generateRScript(etl);
		WriteTextFile out = new WriteTextFile(filename);
		for (String line : r)
			out.writeln(line);
		out.close();
	}

	private List<String> generateRScript(ETL etl) {
		this.sourceDb = etl.getSourceDatabase();
		this.targetDb = etl.getTargetDatabase();
		this.r = new ArrayList<String>();
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
		createGenerateTestSqlFunction();
		return r;
	}

	private void createInitFunction() {
		r.add("initFramework <- function() {");
		r.add("  frameworkContext <- new.env(parent = globalenv())");
		r.add("  assign('frameworkContext', frameworkContext, envir = globalenv())");
		r.add("  frameworkContext$inserts <- list()");
		r.add("  frameworkContext$expects <- list()");
		r.add("  frameworkContext$testId <- -1");
		r.add("  frameworkContext$testDescription <- \"\"");
		r.add("  frameworkContext$defaultValues <- new.env(parent = frameworkContext)");
		for (Table table : sourceDb.getTables()) {
			if (!table.isStem()) {
				String rTableName = convertToRName(table.getName());
				r.add("");
				r.add("  defaults <- list()");
				for (Field field : table.getFields()) {
					String rFieldName = convertToRName(field.getName());
					String defaultValue;
					if (field.getValueCounts().length == 0)
						defaultValue = "";
					else
						defaultValue = field.getValueCounts()[0][0];
					if (!defaultValue.equals("") && !defaultValue.equals("List truncated..."))
						r.add("  defaults$" + rFieldName + " <- '" + defaultValue + "'");
				}
				r.add("  assign('" + rTableName + "', defaults, envir = frameworkContext$defaultValues)");
			}
		}
		r.add("}");
		r.add("");
		r.add("initFramework()");
		r.add("");
	}

	private void createSetDefaultFunctions() {
		for (Table table : sourceDb.getTables()) {
			if (!table.isStem()) {
				StringBuilder line = new StringBuilder();
				String rTableName = convertToRName(table.getName());
				List<String> argDefs = new ArrayList<String>();
				List<String> insertLines = new ArrayList<String>();
				for (Field field : table.getFields()) {
					String rFieldName = convertToRName(field.getName());
					argDefs.add(rFieldName);
					insertLines.add("  if (!missing(" + rFieldName + ")) {");
					insertLines.add("    defaults$" + rFieldName + " <- " + rFieldName);
					insertLines.add("  }");
				}

				line.append("set_defaults_" + rTableName + " <- function(");
				line.append(StringUtilities.join(argDefs, ", "));
				line.append(") {");
				r.add(line.toString());
				r.add("  defaults <- get('" + rTableName + "', envir = frameworkContext$defaultValues)");
				r.addAll(insertLines);
				r.add("  assign('" + rTableName + "', defaults, envir = frameworkContext$defaultValues)");
				r.add("  invisible(defaults)");
				r.add("}");
				r.add("");
			}
		}
	}

	private void createGetDefaultFunctions() {
		for (Table table : sourceDb.getTables()) {
			String rTableName = convertToRName(table.getName());
			r.add("get_defaults_" + rTableName + " <- function() {");
			r.add("  defaults <- get('" + rTableName + "', envir = frameworkContext$defaultValues)");
			r.add("  return(defaults)");
			r.add("}");
			r.add("");
		}
	}

	private void createDeclareTestFunction() {
		r.add("declareTest <- function(id, description) {");
		r.add("  frameworkContext$testId <- id");
		r.add("  frameworkContext$testDescription <- description");
		r.add("}");
		r.add("");
	}

	private void createAddFunctions() {
		for (Table table : sourceDb.getTables()) {
			if (!table.isStem()) {
				StringBuilder line = new StringBuilder();
				String rTableName = convertToRName(table.getName());
				String sqlTableName = convertToSqlName(table.getName());
				List<String> argDefs = new ArrayList<String>();
				for (Field field : table.getFields()) {
					String rFieldName = convertToRName(field.getName());
					argDefs.add(rFieldName);
				}
				line.append("add_" + rTableName + " <- function(");
				line.append(StringUtilities.join(argDefs, ", "));
				line.append(") {");
				r.add(line.toString());
				r.add("  defaults <- get('" + rTableName + "', envir = frameworkContext$defaultValues)");
				r.add("  fields <- c()");
				r.add("  values <- c()");
				for (Field field : table.getFields()) {
					String rFieldName = convertToRName(field.getName());
					String sqlFieldName = convertToSqlName(field.getName());
					r.add("  if (missing(" + rFieldName + ")) {");
					r.add("    " + rFieldName + " <- defaults$" + rFieldName);
					r.add("  }");
					r.add("  if (!is.null(" + rFieldName + ")) {");
					r.add("    fields <- c(fields, \"" + sqlFieldName + "\")");
					r.add("    values <- c(values, " + createSqlValueCode(rFieldName) + ")");
					r.add("  }");
					r.add("");
				}
				r.add("  inserts <- list(testId = frameworkContext$testId, testDescription = frameworkContext$testDescription, table = \"" + sqlTableName
						+ "\", fields = fields, values = values)");
				r.add("  frameworkContext$inserts[[length(frameworkContext$inserts) + 1]] <- inserts");
				r.add("  invisible(NULL)");
				r.add("}");
				r.add("");
			}
		}
	}

	private void createExpectFunctions(int type) {
		for (Table table : targetDb.getTables()) {
			if (!table.isStem()) {
				StringBuilder line = new StringBuilder();
				String rTableName = convertToRName(table.getName());
				String sqlTableName = convertToSqlName(table.getName());
				List<String> argDefs = new ArrayList<String>();
				for (Field field : table.getFields()) {
					String rFieldName = convertToRName(field.getName());
					argDefs.add(rFieldName);
				}
				if (type == DEFAULT)
					line.append("expect_" + rTableName + " <- function(");
				else if (type == NEGATE)
					line.append("expect_no_" + rTableName + " <- function(");
				else
					line.append("expect_count_" + rTableName + " <- function(rowCount, ");
				line.append(StringUtilities.join(argDefs, ", "));
				line.append(") {");
				r.add(line.toString());
				r.add("  fields <- c()");
				r.add("  values <- c()");
				for (Field field : table.getFields()) {
					String rFieldName = convertToRName(field.getName());
					String sqlFieldName = convertToSqlName(field.getName());
					r.add("  if (!missing(" + rFieldName + ")) {");
					r.add("    fields <- c(fields, \"" + sqlFieldName + "\")");
					r.add("    values <- c(values, " + createSqlValueCode(rFieldName) + ")");
					r.add("  }");
					r.add("");
				}
				r.add("  expects <- list(testId = frameworkContext$testId, testDescription = frameworkContext$testDescription, type = " + type + ", table = \""
						+ sqlTableName + "\", fields = fields, values = values)");
				if (type == COUNT)
					r.add("  expects$rowCount = rowCount");
				r.add("  frameworkContext$expects[[length(frameworkContext$expects) + 1]] <- expects");
				r.add("  invisible(NULL)");
				r.add("}");
				r.add("");
			}
		}
	}

	protected void createLookupFunctions() {
		for (Table table : targetDb.getTables()) {
			if (!table.isStem()) {
				StringBuilder line = new StringBuilder();
				String rTableName = convertToRName(table.getName());
				String sqlTableName = convertToSqlName(table.getName());
				List<String> argDefs = new ArrayList<String>();
				for (Field field : table.getFields()) {
					String rFieldName = convertToRName(field.getName());
					argDefs.add(rFieldName);
				}
				line.append("lookup_" + rTableName + " <- function(fetchField, ");
				line.append(StringUtilities.join(argDefs, ", "));
				line.append(") {");
				r.add(line.toString());
				line = new StringBuilder();
				line.append("  statement <- paste0('SELECT ', fetchField , ' FROM @cdm_database_schema.");
				line.append(sqlTableName);
				line.append(" WHERE')");
				r.add(line.toString());
				r.add("  first <- TRUE");
				for (Field field : table.getFields()) {
					String rFieldName = convertToRName(field.getName());
					String sqlFieldName = convertToSqlName(field.getName());
					argDefs.add(rFieldName);
					r.add("  if (!missing(" + rFieldName + ")) {");
					r.add("    if (first) {");
					r.add("      first <- FALSE");
					r.add("    } else {");
					r.add("      statement <- paste0(statement, \" AND\")");
					r.add("    }");
					r.add("    statement <- paste0(statement, \" " + sqlFieldName + " = \", " + createSqlValueCode(rFieldName) + ")");
					r.add("  }");
					r.add("");
				}
				r.add("  class(statement) <- 'subQuery'");
				r.add("  return(statement)");
				r.add("}");
				r.add("");
			}
		}
	}

	protected void createGenerateInsertSqlFunction() {
		r.add("generateInsertSql <- function(databaseSchema = NULL) {");
		r.add("  insertSql <- c()");
		for (Table table : sourceDb.getTables())
			if (!table.isStem())
				r.add("  insertSql <- c(insertSql, \"TRUNCATE TABLE @cdm_database_schema." + convertToSqlName(table.getName()) + ";\")");
		r.add("  createInsertStatement <- function(insert, env) {");
		r.add("    s <- c()");
		r.add("    if (env$testId != insert$testId) {");
		r.add("      s <- c(s, paste0('-- ', insert$testId, ': ', insert$testDescription))");
		r.add("      env$testId <- insert$testId");
		r.add("    }");
		r.add("    s <- c(s, paste0(\"INSERT INTO @cdm_database_schema.\",");
		r.add("                     insert$table,");
		r.add("                     \"(\",");
		r.add("                     paste(insert$fields, collapse = \", \"),");
		r.add("                     \") VALUES (\",");
		r.add("                     paste(insert$values, collapse = \", \"), ");
		r.add("                     \");\"))");
		r.add("    return(s)");
		r.add("  }");
		r.add("  env <- new.env()");
		r.add("  env$testId <- -1");
		r.add("  insertSql <- c(insertSql, do.call(c, lapply(frameworkContext$inserts, createInsertStatement, env)))");
		r.add("  if (is.null(databaseSchema)) {");
		r.add("  	insertSql <- gsub('@cdm_database_schema.', '', insertSql)");
		r.add("  } else {");
		r.add("  	insertSql <- gsub('@cdm_database_schema', databaseSchema, insertSql)");
		r.add("  }");
		r.add("  return(insertSql)");
		r.add("}");
		r.add("");
	}

	protected void createGenerateTestSqlFunction() {
		r.add("generateTestSql <- function(databaseSchema = NULL) {");
		r.add("  testSql <- c()");
		r.add("  testSql <- c(testSql, \"IF OBJECT_ID('@cdm_database_schema.test_results', 'U') IS NOT NULL DROP TABLE @cdm_database_schema.test_results;\")");
		r.add("  testSql <- c(testSql, \"CREATE TABLE @cdm_database_schema.test_results (id INT, description VARCHAR(512), test VARCHAR(256), status VARCHAR(5));\")");
		r.add("  createExpectStatement <- function(expect, env) {");
		r.add("    s <- c()");
		r.add("    if (env$testId != expect$testId) {");
		r.add("      s <- c(s, paste0('-- ', expect$testId, ': ', expect$testDescription))");
		r.add("      env$testId <- expect$testId");
		r.add("    }");
		r.add("    operators <- rep(\"=\", length(expect$fields))");
		r.add("    operators[expect$values == \"NULL\"] <- rep(\"IS\", sum(expect$values == \"NULL\"))");
		r.add("    s <- c(s, paste0(\"INSERT INTO @cdm_database_schema.test_results SELECT \",");
		r.add("                     expect$testId,");
		r.add("                     \" AS id, '\",");
		r.add("                     expect$testDescription,");
		r.add("                     \"' AS description, 'Expect \",");
		r.add("                     expect$table,");
		r.add("                     \"' AS test, CASE WHEN (SELECT COUNT(*) FROM @cdm_database_schema.\",");
		r.add("                     expect$table,");
		r.add("                     \" WHERE \",");
		r.add("                     paste(paste(expect$fields, operators, expect$values), collapse = \" AND \"),");
		r.add("                     \") \",");
		r.add("                     if (expect$type == " + DEFAULT + ") \"= 0\" else if (expect$type == " + NEGATE
				+ ") \"!= 0\" else paste(\"!=\", expect$rowCount),");
		r.add("                     \" THEN 'FAIL' ELSE 'PASS' END AS status;\"))");
		r.add("    return(s)");
		r.add("  }");
		r.add("  env <- new.env()");
		r.add("  env$testId <- -1");
		r.add("  testSql <- c(testSql, do.call(c, lapply(frameworkContext$expects, createExpectStatement, env)))");
		r.add("  if (is.null(databaseSchema)) {");
		r.add("  	testSql <- gsub('@cdm_database_schema.', '', testSql)");
		r.add("  } else {");
		r.add("  	testSql <- gsub('@cdm_database_schema', databaseSchema, testSql)");
		r.add("  }");
		r.add("  return(testSql)");
		r.add("}");
		r.add("");
	}

	private String convertToRName(String name) {
		name = name.replaceAll(" ", "_").replaceAll("-", "_").replaceAll("^_+", "");
		return name;
	}

	protected String createSqlValueCode(String rFieldName) {
		StringBuilder expression = new StringBuilder();
		expression.append("if (is.null(" + rFieldName + ")) \"NULL\" ");
		expression.append("else if (is(" + rFieldName + ", \"subQuery\")) paste0(\"(\", as.character(" + rFieldName + "), \")\") ");
		expression.append("else paste0(\"'\", as.character(" + rFieldName + "), \"'\")");
		return (expression.toString());
	}

	protected String convertToSqlName(String name) {
		if (name.startsWith("[") && name.endsWith("]"))
			return name;
		name = name.replace('[', '_').replace(']', '_');
		if (name.contains(" ") || name.contains(".") || keywordSet.contains(name.toUpperCase()))
			return "[" + name + "]";
		return name;
	}
}
