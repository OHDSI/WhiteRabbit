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

import org.ohdsi.rabbitInAHat.dataModel.Database;
import org.ohdsi.rabbitInAHat.dataModel.ETL;
import org.ohdsi.rabbitInAHat.dataModel.Field;
import org.ohdsi.rabbitInAHat.dataModel.Table;
import org.ohdsi.rabbitInAHat.dataModel.Db.*;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.WriteTextFile;

public class ETLTestFrameWorkGenerator {

	private static int			DEFAULT		= 0;
	private static int			NEGATE		= 1;
	private static int			COUNT		= 2;
	
	private ETLTestFrameWorkGenerator() {}	

	public static void generate(ETL etl, String filename, DBMS dbms) {
		List<String> r = generateRScript(etl, dbms);
		WriteTextFile out = new WriteTextFile(filename);
		for (String line : r)
			out.writeln(line);
		out.close();
	}

	private static List<String> generateRScript(ETL etl, DBMS dbms) {
		List<String> r = new ArrayList<String>();
		
		Database sourceDb = etl.getSourceDatabase();
		Database targetDb = etl.getTargetDatabase();
		DbOperations sourceDbOps = getDbOperations(sourceDb, dbms);
		DbOperations targetDbOps = getDbOperations(targetDb, dbms);
		
		createInitFunction(r, sourceDbOps);
		createWriteSQLFunction(r, sourceDbOps);
		createDeclareTestFunction(r);
		createSetDefaultFunctions(r, sourceDbOps);
		createGetDefaultFunctions(r, sourceDbOps);
		createAddFunctions(r, sourceDbOps);
		createExpectFunctions(r, DEFAULT, targetDbOps);
		createExpectFunctions(r, NEGATE, targetDbOps);
		createExpectFunctions(r, COUNT, targetDbOps);
		createLookupFunctions(r, targetDbOps);
		createFinalizeFunction(r, targetDbOps);
		return r;
	}

	private static void createWriteSQLFunction(List<String> r, DbOperations dbOps) {
		r.add("writeSql <- function(df, file)");
		r.add("{");
		r.add("  finalizeFramework()");
		r.add("  if (file.exists(file)) file.remove(file)");
		r.add("  tables <- unique(df$table)");
		r.add("  lapply(tables, function(t) {");
		r.add("    write(subset(df, table == t)$sql, file, append = TRUE)");
		r.add("    write(\";\", file, append = TRUE)");
		r.add("    })");
		r.add("  invisible(df)");
		r.add("}");		
		r.add("");
	}

	private static DbOperations getDbOperations(Database db, DBMS dbms) {
		switch (dbms) {
			case SQLServer:
				return new SqlServerDatabase(db);
			case Redshift:
				return new RedshiftDatabase(db);	
			case APS:
				break;
			case Access:
				break;
			case MySQL:
				break;
			case Oracle:
				break;
			case PostgreSQL:
				break;
			case Text:
				break;
			default:
				return null;
		}
		return null;
	}

	private static void createDeclareTestFunction(List<String> r) {
		r.add("declareTest <- function(id, description) {");
		r.add("  assign(\"testId\", id, envir = globalenv()) ");
		r.add("  assign(\"testDescription\", description, envir = globalenv()) ");
		r.add("  assign(\"testNewAdded\", TRUE, envir = globalenv()) ");
		r.add("  assign(\"testNewExpected\", TRUE, envir = globalenv()) ");
//		r.add("  sql <- paste0(\"-- \", id, \": \", description)");
//		r.add("  insertDf <- get(\"insertDf\", envir = globalenv())");
//		r.add("  insertDf[nrow(insertDf) + 1,] <- c(\"_test\", sql)");
//		r.add("  assign(\"insertDf\", insertDf, envir = globalenv())");
//		r.add("  assign(\"testSql\", c(get(\"testSql\", envir = globalenv()), sql), envir = globalenv())");
		//r.add("  invisible(insertDf)");
		r.add("}");
		r.add("");
	}

	private static void createExpectFunctions(List<String> r, int type, DbOperations dbOps) {
		for (Table table : dbOps.getDatabase().getTables()) {
			if (!table.isStem()) {
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
				line.append("  statement <- paste0(\"" + dbOps.getInsertTestLine());				
				line.append("\", get(\"testId\", envir = globalenv()), \" AS id, ");
				line.append("'\", get(\"testDescription\", envir = globalenv()), \"' AS description, ");
				line.append("'Expect " + table.getName() + "' AS test, ");
				line.append("CASE WHEN(SELECT COUNT(*) FROM " + sqlTableName + " WHERE\")");
				r.add(line.toString());

				r.add("  first <- TRUE");

				r.addAll(testDefs);

				if (type == DEFAULT)
					r.add("  statement <- paste0(statement, \") = 0 THEN 'FAIL' ELSE 'PASS' END AS status\")");
				else if (type == NEGATE)
					r.add("  statement <- paste0(statement, \") != 0 THEN 'FAIL' ELSE 'PASS' END AS status\")");
				else
					r.add("  statement <- paste0(statement, \") != \",rowCount ,\" THEN 'FAIL' ELSE 'PASS' END AS status\")");

				r.add("  assign(\"testSql\", c(get(\"testSql\", envir = globalenv()), statement), envir = globalenv())");
				r.add("  invisible(statement)");
				r.add("}");
				r.add("");
			}
		}
	}

	private static void createLookupFunctions(List<String> r, DbOperations dbOps) {
		for (Table table : dbOps.getDatabase().getTables()) {
			if (!table.isStem()) {
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

	private static void createInitFunction(List<String> r, DbOperations dbOps) {
		r.add("initFramework <- function() {");
		r.add("  insertDf <- data.frame(table = character(), sql = character(), stringsAsFactors = FALSE)");
		for (Table table : dbOps.getDatabase().getTables()) {
			String sqlTableName = dbOps.convertToSqlName(table.getName());
			r.add("  insertDf[nrow(insertDf) + 1,] <- c(\"" + sqlTableName + "\", \"" + dbOps.clearTable(sqlTableName) + "\")");
		}
		r.add("  assign(\"insertDf\", insertDf, envir = globalenv())");

		r.add("  testSql <- c()");
		r.add("  testSql <- c(testSql, \"" + dbOps.dropTableIfExists("test_results") + "\")");
		r.add("  testSql <- c(testSql, \"\")");
		r.add("  testSql <- c(testSql, \"" + dbOps.createTestResults() + "\")");
		r.add("  testSql <- c(testSql, \"\")");

		r.add("  assign(\"testSql\", testSql, envir = globalenv())");
		r.add("  assign(\"testId\", 1, envir = globalenv())");
		r.add("  assign(\"testDescription\", \"\", envir = globalenv())");
		r.add("");
		r.add("  defaultValues <- new.env(parent = globalenv())");
		r.add("  assign(\"defaultValues\", defaultValues, envir = globalenv())");
		for (Table table : dbOps.getDatabase().getTables()) {
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
					if (!defaultValue.equals("") && !defaultValue.equals("List truncated..."))
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

	private static void createAddFunctions(List<String> r, DbOperations dbOps) {
		for (Table table : dbOps.getDatabase().getTables()) {
			if (!table.isStem()) {
				StringBuilder line = new StringBuilder();
				String rTableName = convertToRName(table.getName());
				//String sqlTableName = dbOps.convertToSqlName(table.getName());
				List<String> argDefs = new ArrayList<String>();
				//List<String> insertLines = new ArrayList<String>();
//				for (Field field : table.getFields()) {
//					String rFieldName = field.getName().replaceAll(" ", "_").replaceAll("-", "_");
//					String sqlFieldName = dbOps.convertToSqlName(field.getName());
//					argDefs.add(rFieldName);
//					insertLines.add("  if (missing(" + rFieldName + ")) {");
//					insertLines.add("    " + rFieldName + " <- defaults$" + rFieldName);
//					insertLines.add("  }");
//					insertLines.add("  if (!is.null(" + rFieldName + ")) {");
//					insertLines.add("    insertFields <- c(insertFields, \"" + sqlFieldName + "\")");
//					insertLines.add("    insertValues <- c(insertValues, " + rFieldName + ")");
//					insertLines.add("  }");
//					insertLines.add("");
//				}
				for (Field field : table.getFields()) {
					String rFieldName = field.getName().replaceAll(" ", "_").replaceAll("-", "_");					
					argDefs.add(rFieldName);
				}
				List<String> insertLines = dbOps.getInsertLines(table);
				
				line.append("add_" + rTableName + " <- function(");
				line.append(StringUtilities.join(argDefs, ", "));
				line.append(") {");
				r.add(line.toString());
				r.add("  defaults <- get(\"" + rTableName + "\", envir = defaultValues)");
				r.add("  insertFields <- c()");
				r.add("  insertValues <- c()");
				r.addAll(insertLines);

//				line = new StringBuilder();
//				line.append("  statement <- paste0(\"INSERT INTO " + sqlTableName + " (\", ");
//				line.append("paste(insertFields, collapse = \", \"), ");
//				line.append("\") VALUES ('\", ");
//				line.append("paste(insertValues, collapse = \"', '\"), ");
//				line.append("\"');\")");

				r.add("  insertDf <- get(\"insertDf\", envir = globalenv())");
				// add test header
				r.add("  if (exists(\"testNewAdded\", where = globalenv()) && get(\"testNewAdded\"))");
				r.add("  {");
				r.add("    assign(\"testNewAdded\", FALSE, envir = globalenv())");
				r.add("    id <- get(\"testId\", envir = globalenv())");
				r.add("    description <- get(\"testDescription\", envir = globalenv())");
				r.add("    comment <- paste0(\"-- \", id, \": \", description)");				
				r.add("    insertDf[nrow(insertDf) + 1,] <- c(\"" + table + "\", comment)");
				r.add("  }");
					
				r.add(dbOps.getInsertStatement(table));
				
				r.add("  insertDf[nrow(insertDf) + 1,] <- c(\"" + table + "\", statement)");
				r.add("  assign(\"insertDf\", insertDf, envir = globalenv())");
				//r.add("  assign(\"insertDf\", c(get(\"insertDf\", envir = globalenv()), statement), envir = globalenv())");
				r.add("  invisible(statement)");
				r.add("}");
				r.add("");
			}
		}
	}

	private static void createSetDefaultFunctions(List<String> r, DbOperations dbOps) {
		for (Table table : dbOps.getDatabase().getTables()) {
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

	private static void createGetDefaultFunctions(List<String> r, DbOperations dbOps) {
		for (Table table : dbOps.getDatabase().getTables()) {
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
	
	private static void createFinalizeFunction(List<String> r, DbOperations dbOps) {
		r.add("finalizeFramework <- function()");
		r.add("{");
		String tableFooter = dbOps.getTableFooter();
		if (tableFooter != null && !tableFooter.isEmpty())
		{
			r.add("  tables <<- unique(insertDf$table)");
			r.add("  lapply(tables, function(t) {");
			r.add("    insertDf[nrow(insertDf) + 1,] <<- c(t, \";\")");
			r.add("    insertDf[nrow(insertDf) + 1,] <<- c(t, " + dbOps.dropTableIfExists() + ")");
			r.add("    insertDf[nrow(insertDf) + 1,] <<- c(t, " + tableFooter + ")");
			r.add("    })");
		}
		r.add("}");
		r.add("");
	}
}
