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
import java.util.List;

import org.ohdsi.databases.DbType;
import org.ohdsi.rabbitInAHat.dataModel.Database;
import org.ohdsi.rabbitInAHat.dataModel.ETL;
import org.ohdsi.rabbitInAHat.dataModel.Field;
import org.ohdsi.rabbitInAHat.dataModel.Table;
import org.ohdsi.rabbitInAHat.dataModel.Db.*;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.WriteTextFile;

public class ETLPackageTestFrameWorkGenerator {

	private static int DEFAULT = 0;
	private static int NEGATE = 1;
	private static int COUNT = 2;

	private ETLPackageTestFrameWorkGenerator() {
	}

	public static void generate(ETL etl, String filename, DbType dbms) {
		List<String> r = generateRScript(etl, dbms);
		WriteTextFile out = new WriteTextFile(filename);
		for (String line : r)
			out.writeln(line);
		out.close();
	}

	private static DbOperations getDbOperations(Database db, DbType dbms) {
		if (dbms == DbType.MSSQL)
			return new SqlServerDatabase(db);
		else if (dbms == DbType.REDSHIFT)
			return new RedshiftDatabase(db);
		
		return null;
	}

	private static List<String> generateRScript(ETL etl, DbType dbms) {
		List<String> r = new ArrayList<String>();

		Database sourceDb = etl.getSourceDatabase();
		Database targetDb = etl.getTargetDatabase();
		DbOperations sourceDbOps = getDbOperations(sourceDb, dbms);
		DbOperations targetDbOps = getDbOperations(targetDb, dbms);

		createInitFunction(r, sourceDbOps);
		createDeclareTestFunction(r);
		createSetDefaultFunctions(r, sourceDbOps);
		createGetDefaultFunctions(r, sourceDbOps);
		createAddFunctions(r, sourceDbOps);
		createExpectFunctions(r, DEFAULT, targetDbOps);
		createExpectFunctions(r, NEGATE, targetDbOps);
		createExpectFunctions(r, COUNT, targetDbOps);
		createLookupFunctions(r, targetDbOps);

		return r;
	}

	private static void createDeclareTestFunction(List<String> r) {
		r.add("declareTestGroup <- function(groupName) {");
		r.add("  frameworkContext$groupIndex <- frameworkContext$groupIndex + 1 ;");
		r.add("  frameworkContext$currentGroup <- {}");
		r.add("");
		r.add("  frameworkContext$currentGroup$groupName <- groupName;");
		r.add("  frameworkContext$currentGroup$groupItemIndex <- -1;");
		r.add("  sql <- c(\"\",paste0(\"-- \", frameworkContext$groupIndex, \". \", groupName));");
		r.add("  frameworkContext$testDescription = c(sql, frameworkContext$testDescription);");
		r.add("}");
		r.add("");
		r.add("declareTest <- function(description, source_pid = NULL, cdm_pid = NULL) {");
		r.add("  frameworkContext$testId = frameworkContext$testId + 1;");
		r.add("  frameworkContext$testDescription = description;");
		r.add("  frameworkContext$testNewAdded = TRUE;");
		r.add("  frameworkContext$testNewExpected = TRUE;");
		r.add("  frameworkContext$patient$source_pid = source_pid;");
		r.add("  frameworkContext$patient$cdm_pid = cdm_pid;");
		r.add("  if (is.null(frameworkContext$currentGroup)) {  ");
		r.add("    sql <- c(paste0(\"-- Test \", frameworkContext$testId, \": \", frameworkContext$testDescription));");
		r.add("  } else {");
		r.add("    frameworkContext$currentGroup$groupItemIndex = frameworkContext$currentGroup$groupItemIndex + 1;");
		r.add("    sql <- c(paste0(\"-- \", frameworkContext$groupIndex, \".\", frameworkContext$currentGroup$groupItemIndex, \" \", frameworkContext$testDescription, \" [Test ID: \", frameworkContext$testId, \"]\"));");
		r.add("  }");
		r.add("  frameworkContext$testDescription = c(\"--\", sql, \"--\");");
		r.add("}");
		r.add("");
	}

	private static void createExpectFunctions(List<String> r, int type, DbOperations dbOps) {
		for (Table table : dbOps.getDatabase().getTables()) {
			StringBuilder line = new StringBuilder();
			String rTableName = convertToRName(table.getName());
			String sqlTableName = dbOps.convertToSqlName(table.getName());
			List<String> argDefs = new ArrayList<String>();
			List<String> testDefs = new ArrayList<String>();
			for (Field field : table.getFields()) {
				String rFieldName = convertToRName(field.getName());
				String sqlFieldName = dbOps.convertToSqlName(field.getName());
				argDefs.add(rFieldName);
				testDefs.add("  if (!missing(" + rFieldName + ")) {");
				testDefs.add("    if (is.null(" + rFieldName + ")) {");
				testDefs.add("      whereClauses <- c(whereClauses, \"" + sqlFieldName + " IS NULL\")");
				testDefs.add("    } else if (is(" + rFieldName + ", \"subQuery\")){");
				testDefs.add("      whereClauses <- c(whereClauses, paste0(\"" + sqlFieldName + " = (\", as.character("
						+ rFieldName + "), \")\"))");
				testDefs.add("    } else {");
				testDefs.add("      whereClauses <- c(whereClauses, paste0(\"" + sqlFieldName + " = '\", " + rFieldName
						+ ",\"'\"))");
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

			r.add("");
			r.add("  if (is.null(frameworkContext$currentGroup)) {");
			r.add("    testName <- frameworkContext$testDescription;");
			r.add("  } else {");
			r.add("    testName <- paste0(frameworkContext$groupIndex, \".\", frameworkContext$currentGroup$groupItemIndex, \" \", frameworkContext$testDescription);");
			r.add("  }");
			r.add("");
			r.add("  source_pid <- frameworkContext$patient$source_pid;");
			r.add("  if (is.null(source_pid)) {");
			r.add("    source_pid <- \"NULL\";");
			r.add("  } else {");
			r.add("    source_pid <- paste0(\"'\", as.character(source_pid), \"'\");");
			r.add("  }");
			r.add("");
			r.add("  cdm_pid <- frameworkContext$patient$cdm_pid;");
			r.add("  if (is.null(cdm_pid)) {");
			r.add("    cdm_pid <- \"NULL\"");
			r.add("  }");
			r.add("");
			line = new StringBuilder();
			line.append("  statement <- paste0(\"" + dbOps.getExpectTestLine());
			line.append("\", frameworkContext$testId, \" AS id, ");
			line.append("'\", testName, \"' AS description, ");
			line.append("'Expect " + table.getName() + "' AS test, ");
			line.append("\", source_pid, \" as source_pid, ");
			line.append("\", cdm_pid, \" as cdm_pid, ");
			line.append("CASE WHEN(SELECT COUNT(*) FROM @cdm_schema." + sqlTableName + " WHERE \")");
			r.add(line.toString());

			r.add("  whereClauses = NULL;");

			r.addAll(testDefs);

			r.add(" statement <- paste0(statement, paste0(whereClauses, collapse=\" AND \"));");

			if (type == DEFAULT)
				r.add("  statement <- paste0(statement, \") = 0 THEN 'FAIL' ELSE 'PASS' END AS status;\")");
			else if (type == NEGATE)
				r.add("  statement <- paste0(statement, \") != 0 THEN 'FAIL' ELSE 'PASS' END AS status;\")");
			else
				r.add("  statement <- paste0(statement, \") != \",rowCount ,\" THEN 'FAIL' ELSE 'PASS' END AS status;\")");

			// add test header
			r.add("  if (exists('testNewExpected', where = frameworkContext) && get('testNewExpected'))");
			r.add("  {");
			r.add("    frameworkContext$testNewExpected <- FALSE");
			r.add("    id <- frameworkContext$testId");
			r.add("    description <- frameworkContext$testDescription");
			r.add("    comment <- paste0('-- ', id, ': ', description)");
			r.add("    testSql <- c(testSql, comment)");
			r.add("  }");

			r.add("  frameworkContext$testSql = c(frameworkContext$testSql, statement);");
			r.add("  invisible(statement)");
			r.add("}");
			r.add("");
		}
	}

	private static void createLookupFunctions(List<String> r, DbOperations dbOps) {
		for (Table table : dbOps.getDatabase().getTables()) {
			StringBuilder line = new StringBuilder();
			String rTableName = convertToRName(table.getName());
			String sqlTableName = dbOps.convertToSqlName(table.getName());
			List<String> argDefs = new ArrayList<String>();
			List<String> testDefs = new ArrayList<String>();
			for (Field field : table.getFields()) {
				String rFieldName = convertToRName(field.getName());
				String sqlFieldName = dbOps.convertToSqlName(field.getName());
				argDefs.add(rFieldName);
				testDefs.add("  if (!missing(" + rFieldName + ")) {");
				testDefs.add("    if (is.null(" + rFieldName + ")) {");
				testDefs.add("      whereClauses <- c(whereClauses, \"" + sqlFieldName + " IS NULL\")");
				testDefs.add("    } else {");
				testDefs.add("      whereClauses <- c(whereClauses, paste0(\"" + sqlFieldName + " = '\", " + rFieldName
						+ ",\"'\"))");
				testDefs.add("    }");
				testDefs.add("  }");
				testDefs.add("");
			}
			line.append("lookup_" + rTableName + " <- function(fetchField, ");
			line.append(StringUtilities.join(argDefs, ", "));
			line.append(") {");
			r.add(line.toString());

			r.add("  whereClauses = NULL;");
			line = new StringBuilder();
			line.append("  statement <- paste0(\"SELECT \", fetchField , \" FROM @cdm_schema.");
			line.append(sqlTableName);
			line.append(" WHERE \")");
			r.add(line.toString());
			r.addAll(testDefs);
			r.add("  statement <- paste0(statement, paste0(whereClauses, collapse=\" AND \"));");
			r.add("  class(statement) <- \"subQuery\"");
			r.add("  return(statement)");
			r.add("}");
			r.add("");
		}
	}

	private static void createInitFunction(List<String> r, DbOperations dbOps) {
		r.add("frameworkContext <- new.env(parent = emptyenv());");
		r.add("initFramework <- function() {");
		r.add("  frameworkContext$groupIndex <- 0;");
		r.add("  insertDf <- data.frame(table = character(), sql = character(), stringsAsFactors = FALSE)");
		for (Table table : dbOps.getDatabase().getTables()) {
			String sqlTableName = dbOps.convertToSqlName(table.getName());
			r.add("  insertDf[nrow(insertDf) + 1,] <- c('@source_schema." + sqlTableName + "', '"
					+ dbOps.clearTable(sqlTableName) + "')");
		}
		r.add("  frameworkContext$insertDf <- insertDf;");

		r.add("  testSql <- c(testSql, \"" + dbOps.dropTableIfExists("test_results") + "\")");
		r.add("  testSql <- c(testSql, '')");
		r.add("  testSql <- c(testSql, \"" + dbOps.createTestResults() + "\")");
		r.add("  testSql <- c(testSql, '')");

		r.add("  frameworkContext$testSql <- testSql;");
		r.add("  frameworkContext$testId = 0;");
		r.add("  frameworkContext$testDescription = '';");
		r.add("");
		r.add("  patient <- {}");
		r.add("  patient$source_pid <- NULL");
		r.add("  patient$cdm_pid <- NULL");
		r.add("  frameworkContext$patient = patient;");
		r.add("");
		r.add("  frameworkContext$defaultValues = new.env(parent = emptyenv());");
		for (Table table : dbOps.getDatabase().getTables()) {
			String rTableName = convertToRName(table.getName());
			r.add("");
			r.add("  defaults <- new.env(parent = emptyenv())");
			for (Field field : table.getFields()) {
				String rFieldName = field.getName().replaceAll(" ", "_").replaceAll("-", "_");
				String defaultValue;
				if (field.getValueCounts().length == 0
						|| field.getValueCounts()[0][0].equalsIgnoreCase("List truncated..."))
					defaultValue = "";
				else
					defaultValue = field.getValueCounts()[0][0];
				if (!defaultValue.equals(""))
					r.add("  defaults$" + rFieldName + " <- \"" + defaultValue + "\"");
			}
			r.add("  frameworkContext$defaultValues$" + rTableName + " = defaults;");
		}
		r.add("}");
		r.add("");
	}

	private static void createAddFunctions(List<String> r, DbOperations dbOps) {
		for (Table table : dbOps.getDatabase().getTables()) {
			StringBuilder line = new StringBuilder();
			String rTableName = convertToRName(table.getName());
			List<String> argDefs = new ArrayList<String>();

			for (Field field : table.getFields()) {
				String rFieldName = field.getName().replaceAll(" ", "_").replaceAll("-", "_");
				argDefs.add(rFieldName);
			}
			List<String> insertLines = dbOps.getInsertValues(table);

			line.append("add_" + rTableName + " <- function(");
			line.append(StringUtilities.join(argDefs, ", "));
			line.append(") {");
			r.add(line.toString());
			r.add("  defaults <- frameworkContext$defaultValues$" + rTableName + ";");
			r.add("  insertFields <- c()");
			r.add("  insertValues <- c()");
			r.addAll(insertLines);

			// add test header
			r.add("  if (exists('testNewAdded', where = frameworkContext) && get('testNewAdded'))");
			r.add("  {");
			r.add("    frameworkContext$testNewAdded <- FALSE");
			r.add("    id <- frameworkContext$testId");
			r.add("    description <- frameworkContext$testDescription");
			r.add("    comment <- paste0('-- ', id, ': ', description)");
			r.add("    insertDf[nrow(insertDf) + 1,] <<- c('@source_schema." + table + "', comment)");
			r.add("  }");

			r.add(dbOps.getInsertStatement(table));

			r.add("  insertDf[nrow(insertDf) + 1,] <<- c('@source_schema." + table + "', statement)");
			r.add("  frameworkContext$insertDf = insertDf;");
			r.add("  invisible(statement);");
			r.add("}");
			r.add("");
		}
	}

	private static void createSetDefaultFunctions(List<String> r, DbOperations dbOps) {
		for (Table table : dbOps.getDatabase().getTables()) {
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
			r.add("  defaults <- frameworkContext$defaultValues$" + rTableName + ";");
			r.addAll(insertLines);

			r.add("  invisible(defaults)");
			r.add("}");
			r.add("");
		}
	}

	private static void createGetDefaultFunctions(List<String> r, DbOperations dbOps) {
		for (Table table : dbOps.getDatabase().getTables()) {
			String rTableName = convertToRName(table.getName());
			r.add("get_defaults_" + rTableName + " <- function() {");
			r.add("  return(frameworkContext$defaultValues)");
			r.add("}");
			r.add("");
		}
	}

	private static String convertToRName(String name) {
		if (name.startsWith("_"))
			name = "U_" + name.substring(1);

		name = name.replaceAll(" ", "_").replaceAll("-", "_");
		return name;
	}
}
