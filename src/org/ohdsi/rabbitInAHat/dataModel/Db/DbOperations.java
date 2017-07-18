package org.ohdsi.rabbitInAHat.dataModel.Db;

import java.util.List;

import org.ohdsi.rabbitInAHat.dataModel.Database;
import org.ohdsi.rabbitInAHat.dataModel.Table;

/// SQL expressions for operations on specific database management system
public interface DbOperations {
	Database getDatabase();
	String convertToSqlName(String name);
	String clearTable(String table);
	String dropTableIfExists(String table); // SQL
	String dropTableIfExists(); // R
	String createTestResults();	
	String renameTable(String oldName, String newName);
	List<String> getInsertLines(Table table);
	String getInsertStatement(Table table);
	// SQL for createExpectFunctions
	String getInsertTestLine();
	String getTableFooter(); // R
}
