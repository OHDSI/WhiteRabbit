package org.ohdsi.rabbitInAHat.dataModel.Db;

import java.util.List;

import org.ohdsi.rabbitInAHat.dataModel.Database;
import org.ohdsi.rabbitInAHat.dataModel.Table;

/// SQL and R expressions for operations on specific database management system
public interface DbOperations {
	Database getDatabase();

	// SQL
	String convertToSqlName(String name);

	String clearTable(String table);

	String dropTableIfExists(String table);

	String createTestResults(); // create test_results table

	String getExpectTestLine(); // createExpectFunctions

	String renameTable(String oldName, String newName);

	// R
	String dropTableIfExists();

	List<String> getInsertValues(Table table); // createAddFunctions

	String getInsertStatement(Table table);

	String getTableFooter();
}