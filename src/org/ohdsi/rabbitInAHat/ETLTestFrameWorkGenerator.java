/*******************************************************************************
 * Copyright 2016 Observational Health Data Sciences and Informatics
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
import org.ohdsi.rabbitInAHat.dataModel.Field;
import org.ohdsi.rabbitInAHat.dataModel.Table;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.WriteTextFile;

public class ETLTestFrameWorkGenerator {

	public static String[]		keywords	= new String[] { "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "AUTHORIZATION", "BACKUP", "BEGIN", "BETWEEN",
		"BREAK", "BROWSE", "BULK", "BY", "CASCADE", "CASE", "CHECK", "CHECKPOINT", "CLOSE", "CLUSTERED", "COALESCE", "COLLATE", "COLUMN", "COMMIT",
		"COMPUTE", "CONSTRAINT", "CONTAINS", "CONTAINSTABLE", "CONTINUE", "CONVERT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE", "CURRENT_TIME",
		"CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATABASE", "DBCC", "DEALLOCATE", "DECLARE", "DEFAULT", "DELETE", "DENY", "DESC", "DISK",
		"DISTINCT", "DISTRIBUTED", "DOUBLE", "DROP", "DUMP", "ELSE", "END", "ERRLVL", "ESCAPE", "EXCEPT", "EXEC", "EXECUTE", "EXISTS", "EXIT", "EXTERNAL",
		"FETCH", "FILE", "FILLFACTOR", "FOR", "FOREIGN", "FREETEXT", "FREETEXTTABLE", "FROM", "FULL", "FUNCTION", "GOTO", "GRANT", "GROUP", "HAVING",
		"HOLDLOCK", "IDENTITY", "IDENTITY_INSERT", "IDENTITYCOL", "IF", "IN", "INDEX", "INNER", "INSERT", "INTERSECT", "INTO", "IS", "JOIN", "KEY", "KILL",
		"LEFT", "LIKE", "LINENO", "LOAD", "MERGE", "NATIONAL", "NOCHECK", "NONCLUSTERED", "NOT", "NULL", "NULLIF", "OF", "OFF", "OFFSETS", "ON", "OPEN",
		"OPENDATASOURCE", "OPENQUERY", "OPENROWSET", "OPENXML", "OPTION", "OR", "ORDER", "OUTER", "OVER", "PERCENT", "PIVOT", "PLAN", "PRECISION",
		"PRIMARY", "PRINT", "PROC", "PROCEDURE", "PUBLIC", "RAISERROR", "READ", "READTEXT", "RECONFIGURE", "REFERENCES", "REPLICATION", "RESTORE",
		"RESTRICT", "RETURN", "REVERT", "REVOKE", "RIGHT", "ROLLBACK", "ROWCOUNT", "ROWGUIDCOL", "RULE", "SAVE", "SCHEMA", "SECURITYAUDIT", "SELECT",
		"SEMANTICKEYPHRASETABLE", "SEMANTICSIMILARITYDETAILSTABLE", "SEMANTICSIMILARITYTABLE", "SESSION_USER", "SET", "SETUSER", "SHUTDOWN", "SOME",
		"STATISTICS", "SYSTEM_USER", "TABLE", "TABLESAMPLE", "TEXTSIZE", "THEN", "TO", "TOP", "TRAN", "TRANSACTION", "TRIGGER", "TRUNCATE", "TRY_CONVERT",
		"TSEQUAL", "UNION", "UNIQUE", "UNPIVOT", "UPDATE", "UPDATETEXT", "USE", "USER", "VALUES", "VARYING", "VIEW", "WAITFOR", "WHEN", "WHERE", "WHILE",
		"WITH", "WITHIN GROUP", "WRITETEXT" };

	private static Set<String>	keywordSet;
	private static int			DEFAULT		= 0;
	private static int			NEGATE		= 1;
	private static int			COUNT		= 2;

	public static void generate(ETL etl, String filename) {
		keywordSet = new HashSet<String>();
		for (String keyword : keywords)
			keywordSet.add(keyword);
		List<String> r = generateRScript(etl);
		WriteTextFile out = new WriteTextFile(filename);
		for (String line : r)
			out.writeln(line);
		out.close();
	}

	private static List<String> generateRScript(ETL etl) {
		List<String> r = new ArrayList<String>();
		createInitFunction(r, etl.getSourceDatabase());
		createDeclareTestFunction(r);
		createSetDefaultFunctions(r, etl.getSourceDatabase());
		createGetDefaultFunctions(r, etl.getSourceDatabase());
		createAddFunctions(r, etl.getSourceDatabase());
		createExpectFunctions(r, DEFAULT, etl.getTargetDatabase());
		createExpectFunctions(r, NEGATE, etl.getTargetDatabase());
		createExpectFunctions(r, COUNT, etl.getTargetDatabase());
		createLookupFunctions(r, etl.getTargetDatabase());
		return r;
	}

	private static void createDeclareTestFunction(List<String> r) {
		r.add("declareTest <- function(id, description) {");
		r.add("  assign(\"testId\", id, envir = globalenv()) ");
		r.add("  assign(\"testDescription\", description, envir = globalenv()) ");
		r.add("  sql <- c(\"\", paste0(\"-- \", id, \": \", description))");
		r.add("  assign(\"insertSql\", c(get(\"insertSql\", envir = globalenv()), sql), envir = globalenv())");
		r.add("  assign(\"testSql\", c(get(\"testSql\", envir = globalenv()), sql), envir = globalenv())");
		r.add("}");
		r.add("");
	}

	private static void createExpectFunctions(List<String> r, int type, Database database) {
		for (Table table : database.getTables()) {
			if (!table.isStem()) {
				StringBuilder line = new StringBuilder();
				String rTableName = convertToRName(table.getName());
				String sqlTableName = convertToSqlName(table.getName());
				List<String> argDefs = new ArrayList<String>();
				List<String> testDefs = new ArrayList<String>();
				for (Field field : table.getFields()) {
					String rFieldName = convertToRName(field.getName());
					String sqlFieldName = convertToSqlName(field.getName());
					argDefs.add(rFieldName);
					testDefs.add("  if (!missing(" + rFieldName + ")) {");
					testDefs.add("    if (first) {");
					testDefs.add("      first <- FALSE");
					testDefs.add("    } else {");
					testDefs.add("      statement <- paste0(statement, \" AND\")");
					testDefs.add("    }");
					testDefs.add("    if (is.null(" + rFieldName + ")) {");
					testDefs.add("      statement <- paste0(statement, \" " + sqlFieldName + " IS NULL\")");
					testDefs.add("    } else if (is(" + rFieldName + ", \"subQuery\")){");
					testDefs.add("      statement <- paste0(statement, \" " + sqlFieldName + " = (\", as.character(" + rFieldName + "), \")\")");
					testDefs.add("    } else {");
					testDefs.add("      statement <- paste0(statement, \" " + sqlFieldName + " = '\", " + rFieldName + ",\"'\")");
					testDefs.add("    }");
					testDefs.add("  }");
					testDefs.add("");
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

				line = new StringBuilder();
				line.append("  statement <- paste0(\"INSERT INTO test_results SELECT ");
				line.append("\", get(\"testId\", envir = globalenv()), \" AS id, ");
				line.append("'\", get(\"testDescription\", envir = globalenv()), \"' AS description, ");
				line.append("'Expect " + table.getName() + "' AS test, ");
				line.append("CASE WHEN(SELECT COUNT(*) FROM " + sqlTableName + " WHERE\")");
				r.add(line.toString());

				r.add("  first <- TRUE");

				r.addAll(testDefs);

				if (type == DEFAULT)
					r.add("  statement <- paste0(statement, \") = 0 THEN 'FAIL' ELSE 'PASS' END AS status;\")");
				else if (type == NEGATE)
					r.add("  statement <- paste0(statement, \") != 0 THEN 'FAIL' ELSE 'PASS' END AS status;\")");
				else
					r.add("  statement <- paste0(statement, \") != \",rowCount ,\" THEN 'FAIL' ELSE 'PASS' END AS status;\")");

				r.add("  assign(\"testSql\", c(get(\"testSql\", envir = globalenv()), statement), envir = globalenv())");
				r.add("  invisible(statement)");
				r.add("}");
				r.add("");
			}
		}
	}

	private static void createLookupFunctions(List<String> r, Database database) {
		for (Table table : database.getTables()) {
			if (!table.isStem()) {
				StringBuilder line = new StringBuilder();
				String rTableName = convertToRName(table.getName());
				String sqlTableName = convertToSqlName(table.getName());
				List<String> argDefs = new ArrayList<String>();
				List<String> testDefs = new ArrayList<String>();
				for (Field field : table.getFields()) {
					String rFieldName = convertToRName(field.getName());
					String sqlFieldName = convertToSqlName(field.getName());
					argDefs.add(rFieldName);
					testDefs.add("  if (!missing(" + rFieldName + ")) {");
					testDefs.add("    if (first) {");
					testDefs.add("      first <- FALSE");
					testDefs.add("    } else {");
					testDefs.add("      statement <- paste0(statement, \" AND\")");
					testDefs.add("    }");
					testDefs.add("    if (is.null(" + rFieldName + ")) {");
					testDefs.add("      statement <- paste0(statement, \" " + sqlFieldName + " IS NULL\")");
					testDefs.add("    } else if (is(" + rFieldName + ", \"subQuery\")){");
					testDefs.add("      statement <- paste0(statement, \" " + sqlFieldName + " = (\", as.character(" + rFieldName + "), \")\")");
					testDefs.add("    } else {");
					testDefs.add("      statement <- paste0(statement, \" " + sqlFieldName + " = '\", " + rFieldName + ",\"'\")");
					testDefs.add("    }");
					testDefs.add("  }");
					testDefs.add("");
				}
				line.append("lookup_" + rTableName + " <- function(fetchField, ");
				line.append(StringUtilities.join(argDefs, ", "));
				line.append(") {");
				r.add(line.toString());

				line = new StringBuilder();
				line.append("  statement <- paste0(\"SELECT \", fetchField , \" FROM ");
				line.append(sqlTableName);
				line.append(" WHERE\")");
				r.add(line.toString());
				r.add("  first <- TRUE");
				r.addAll(testDefs);
				r.add("  class(statement) <- \"subQuery\"");
				r.add("  return(statement)");
				r.add("}");
				r.add("");
			}
		}
	}

	private static String convertToSqlName(String name) {
		if (name.contains(" ") || name.contains(".") || keywordSet.contains(name.toUpperCase()))
			name = "[" + name + "]";
		return name;
	}

	private static void createInitFunction(List<String> r, Database database) {
		r.add("initFramework <- function() {");
		r.add("  insertSql <- c()");
		for (Table table : database.getTables()) {
			String sqlTableName = convertToSqlName(table.getName());
			r.add("  insertSql <- c(insertSql, \"TRUNCATE TABLE " + sqlTableName + ";\")");
		}
		r.add("  assign(\"insertSql\", insertSql, envir = globalenv())");

		r.add("  testSql <- c()");
		r.add("  testSql <- c(testSql, \"IF OBJECT_ID('test_results', 'U') IS NOT NULL\")");
		r.add("  testSql <- c(testSql, \"  DROP TABLE test_results;\")");
		r.add("  testSql <- c(testSql, \"\")");
		r.add("  testSql <- c(testSql, \"CREATE TABLE test_results (id INT, description VARCHAR(512), test VARCHAR(256), status VARCHAR(5));\")");
		r.add("  testSql <- c(testSql, \"\")");

		r.add("  assign(\"testSql\", testSql, envir = globalenv())");
		r.add("  assign(\"testId\", 1, envir = globalenv())");
		r.add("  assign(\"testDescription\", \"\", envir = globalenv())");
		r.add("");
		r.add("  defaultValues <- new.env(parent = globalenv())");
		r.add("  assign(\"defaultValues\", defaultValues, envir = globalenv())");
		for (Table table : database.getTables()) {
			if (!table.isStem()) {
				String rTableName = convertToRName(table.getName());
				r.add("");
				r.add("  defaults <- list()");
				for (Field field : table.getFields()) {
					String rFieldName = field.getName().replaceAll(" ", "_").replaceAll("-", "_");
					String defaultValue;
					if (field.getValueCounts().length == 0)
						defaultValue = "";
					else
						defaultValue = field.getValueCounts()[0][0];
					if (!defaultValue.equals(""))
						r.add("  defaults$" + rFieldName + " <- \"" + defaultValue + "\"");
				}
				r.add("  assign(\"" + rTableName + "\", defaults, envir = defaultValues)");
			}
		}
		r.add("}");
		r.add("");
		r.add("initFramework()");
		r.add("");
	}

	private static void createAddFunctions(List<String> r, Database database) {
		for (Table table : database.getTables()) {
			if (!table.isStem()) {
				StringBuilder line = new StringBuilder();
				String rTableName = convertToRName(table.getName());
				String sqlTableName = convertToSqlName(table.getName());
				List<String> argDefs = new ArrayList<String>();
				List<String> insertLines = new ArrayList<String>();
				for (Field field : table.getFields()) {
					String rFieldName = field.getName().replaceAll(" ", "_").replaceAll("-", "_");
					String sqlFieldName = convertToSqlName(field.getName());
					argDefs.add(rFieldName);
					insertLines.add("  if (missing(" + rFieldName + ")) {");
					insertLines.add("    " + rFieldName + " <- defaults$" + rFieldName);
					insertLines.add("  }");
					insertLines.add("  if (!is.null(" + rFieldName + ")) {");
					insertLines.add("    insertFields <- c(insertFields, \"" + sqlFieldName + "\")");
					insertLines.add("    insertValues <- c(insertValues, " + rFieldName + ")");
					insertLines.add("  }");
					insertLines.add("");
				}


				line.append("add_" + rTableName + " <- function(");
				line.append(StringUtilities.join(argDefs, ", "));
				line.append(") {");
				r.add(line.toString());
				r.add("  defaults <- get(\"" + rTableName + "\", envir = defaultValues)");
				r.add("  insertFields <- c()");
				r.add("  insertValues <- c()");
				r.addAll(insertLines);

				line = new StringBuilder();
				line.append("  statement <- paste0(\"INSERT INTO " + sqlTableName + " (\", ");
				line.append("paste(insertFields, collapse = \", \"), ");
				line.append("\") VALUES ('\", ");
				line.append("paste(insertValues, collapse = \"', '\"), ");
				line.append("\"');\")");
				r.add(line.toString());

				r.add("  assign(\"insertSql\", c(get(\"insertSql\", envir = globalenv()), statement), envir = globalenv())");
				r.add("  invisible(statement)");
				r.add("}");
				r.add("");
			}
		}
	}

	private static void createSetDefaultFunctions(List<String> r, Database database) {
		for (Table table : database.getTables()) {
			if (!table.isStem()) {
				StringBuilder line = new StringBuilder();
				String rTableName = convertToRName(table.getName());
				List<String> argDefs = new ArrayList<String>();
				List<String> insertLines = new ArrayList<String>();
				for (Field field : table.getFields()) {
					String rFieldName = field.getName().replaceAll(" ", "_").replaceAll("-", "_");
					argDefs.add(rFieldName);
					insertLines.add("  if (!missing(" + rFieldName + ")) {");
					insertLines.add("    defaults$" + rFieldName + " <- " + rFieldName);
					insertLines.add("  }");
				}

				line.append("set_defaults_" + rTableName + " <- function(");
				line.append(StringUtilities.join(argDefs, ", "));
				line.append(") {");
				r.add(line.toString());
				r.add("  defaults <- get(\"" + rTableName + "\", envir = defaultValues)");
				r.addAll(insertLines);

				r.add("  assign(\"" + rTableName + "\", defaults, envir = defaultValues)");
				r.add("  invisible(defaults)");
				r.add("}");
				r.add("");
			}
		}
	}

	private static void createGetDefaultFunctions(List<String> r, Database database) {
		for (Table table : database.getTables()) {
			String rTableName = convertToRName(table.getName());
			r.add("get_defaults_" + rTableName + " <- function() {");
			r.add("  defaults <- get(\"" + rTableName + "\", envir = defaultValues)");
			r.add("  return(defaults)");
			r.add("}");
			r.add("");
		}
	}

	private static String convertToRName(String name) {
		name = name.replaceAll(" ", "_").replaceAll("-", "_");
		return name;
	}
}
